//! Implementation of a node management system
//!
//! Nodes are Actix actors in charge of building
//! and scheduling the state of GStreamer pipelines.
//!
//! Nodes can produce and / or consume data, and are connected to one
//! another through [`StreamProducer`](crate::utils::StreamProducer)

use crate::destination::Destination;
use crate::mixer::Mixer;
use crate::source::Source;
use crate::utils::StreamProducer;
use actix::prelude::*;
use actix::WeakRecipient;
use anyhow::{anyhow, Error};
use auteur_controlling::controller::{
    Command, CommandResult, ControlPoint, DestinationFamily, Info, NodeInfo, State,
};
use chrono::{DateTime, Utc};
use futures::channel::oneshot;
use futures::prelude::*;
use std::collections::HashMap;
use tracing::{debug, info, instrument, trace};
use tracing_actix::ActorInstrument;
use tracing_futures::Instrument;

/// NodeManager acts as a tracker of all nodes, and dispatches
/// messages accordingly
///
/// Nodes can be producers, consumers or both. NodeManager knows
/// how to make logical links from one to another, actual connection
/// to [`producers`](crate::utils::StreamProducer) is delegated to the consumers
/// however, as they might want to only perform the connection once their
/// state has progressed.
///
/// Links have an identifier (provided by the client), that identifier
/// can be used by the client to perform disconnection, and are used
/// by consumers that expose multiple consumer slots to identify the
/// correct slot to disconnect (eg mixers)

// We track separately:
//
// * all nodes by type
//
// * the links that have been established, used when disconnecting
//
// * all nodes that can consume data
//
// * all nodes that can produce data

#[derive(Default)]
pub struct NodeManager {
    /// All nodes by id
    nodes: HashMap<String, Node>,
    /// All links, link_id -> consumer recipient
    links: HashMap<String, Recipient<ConsumerMessage>>,
    /// All consumers by id
    consumers: HashMap<String, Recipient<ConsumerMessage>>,
    /// All producers by id
    producers: HashMap<String, Recipient<GetProducerMessage>>,
    /// Used when the manager is "stopping", to ensure all nodes have fully
    /// stopped before exiting. Useful for propagating EOS.
    no_more_modes_sender: Option<oneshot::Sender<()>>,
    /// Listeners for unit tests and potential user interfaces.
    /// Listener id -> recipient
    listeners: HashMap<String, WeakRecipient<NodeStatusMessage>>,
}

/// Sent from [`controllers`](crate::controller::Controller), this is our
/// public interface.
#[derive(Debug)]
pub struct CommandMessage {
    /// The command to run
    pub command: Command,
}

impl Message for CommandMessage {
    type Result = CommandResult;
}

/// Getter for [`stream producers`](crate::utils::StreamProducer),
/// sent from [`NodeManager`] to any producer node to connect them
/// to consumers
#[derive(Debug)]
pub struct GetProducerMessage;

impl Message for GetProducerMessage {
    type Result = Result<(Option<StreamProducer>, Option<StreamProducer>), Error>;
}

/// Sent from [`NodeManager`] to any consumer node in order to
/// let them connect and disconnect from
/// [`stream producers`](crate::utils::StreamProducer), and to
/// control properties on individual slots
pub enum ConsumerMessage {
    /// Lets the consumer perform a connection, it should store the
    /// slot id for latter disconnection, and potentially future
    /// controlling of slot properties (eg volume on a [`Mixer`] slot)
    Connect {
        /// The id of the slot
        link_id: String,
        /// The video producer to connect to
        video_producer: Option<StreamProducer>,
        /// The audio producer to connect to
        audio_producer: Option<StreamProducer>,
        /// Initial configuration of the consumer slot
        config: Option<HashMap<String, serde_json::Value>>,
    },
    /// Lets the consumer disconnect a slot from its associated
    /// producers
    Disconnect {
        /// The id of the slot to disconnect
        slot_id: String,
    },
    /// Lets the consumer put a property under control
    AddControlPoint {
        /// The id of the slot to control
        slot_id: String,
        /// The name of the property to control
        property: String,
        /// The control point
        control_point: ControlPoint,
    },
    /// Instructs the consumer to remove a control point
    RemoveControlPoint {
        /// The id of the control point
        controller_id: String,
        /// The id of the controlled slot
        slot_id: String,
        /// The name of the controlled property
        property: String,
    },
}

impl Message for ConsumerMessage {
    type Result = Result<(), Error>;
}

/// Start a node, sent from [`NodeManager`] to any [`Node`]
#[derive(Debug)]
pub struct StartMessage {
    /// The start time of the [`Node`], if [`None`] it starts
    /// immediately
    pub cue_time: Option<DateTime<Utc>>,
    /// The end time of the [`Node`], if [`None`] the node will
    /// only stop if the underlying media ends
    pub end_time: Option<DateTime<Utc>>,
}

impl Message for StartMessage {
    type Result = Result<(), Error>;
}

/// Reschedule a node, sent from [`NodeManager`] to any [`Node`]
#[derive(Debug)]
pub struct ScheduleMessage {
    /// The new start time of the [`Node`], if [`None`] the current time
    /// should be left unchanged. If the node was already started, it is
    /// allowed to error out otherwise.
    pub cue_time: Option<DateTime<Utc>>,
    /// The new end time of the [`Node`], if [`None`] the current time
    /// should be left unchanged. If the node was already stopped, it is
    /// allowed to error out otherwise.
    pub end_time: Option<DateTime<Utc>>,
}

impl Message for ScheduleMessage {
    type Result = Result<(), Error>;
}

/// Add a property control point, sent from [`NodeManager`] to any [`Node`]
#[derive(Debug)]
pub struct AddControlPointMessage {
    /// The name of the property to control
    pub property: String,
    /// The control point
    pub control_point: ControlPoint,
}

impl Message for AddControlPointMessage {
    type Result = Result<(), Error>;
}

/// Remove a control point, sent from [`NodeManager`] to any [`Node`]
#[derive(Debug)]
pub struct RemoveControlPointMessage {
    /// The id of the control point
    pub controller_id: String,
    /// The name of the controlled property
    pub property: String,
}

impl Message for RemoveControlPointMessage {
    type Result = ();
}

/// Sent from [`NodeManager`] to any [`Node`] in order to make it initiate
/// orderly teardown immediately.
///
/// Will also be sent from the application to [`NodeManager`] when it is
/// terminated.
///
/// Individual nodes MUST send back stopped messages once they have completely
/// stopped.
#[derive(Debug)]
pub struct StopMessage;

impl Message for StopMessage {
    type Result = Result<(), Error>;
}

/// A node has stopped, sent from any node to [`NodeManager`]
#[derive(Debug)]
pub struct StoppedMessage {
    /// Unique identifier of the node
    pub id: String,
    /// The output video producer, if any
    pub video_producer: Option<StreamProducer>,
    /// The output audio producer, if any
    pub audio_producer: Option<StreamProducer>,
}

impl Message for StoppedMessage {
    type Result = ();
}

/// Retrieves node-specific information. Sent from [`NodeManager`] to
/// any [`Node`].
#[derive(Debug)]
pub struct GetNodeInfoMessage;

impl Message for GetNodeInfoMessage {
    type Result = Result<NodeInfo, Error>;
}

/// Sent from [`Node`] to [`NodeManager`] so that it can inform listeners
/// of nodes' status
#[derive(Debug, Clone)]
pub enum NodeStatusMessage {
    /// Node state changed
    State { id: String, state: State },
    /// Node encountered an error
    Error { id: String, message: String },
}

impl Message for NodeStatusMessage {
    type Result = ();
}

/// Sent from any [`NodeStatusMessage`] recipient to [`NodeManager`] to register
/// a state listener
#[derive(Debug)]
pub struct RegisterListenerMessage {
    pub id: String,
    pub recipient: WeakRecipient<NodeStatusMessage>,
}

impl Message for RegisterListenerMessage {
    type Result = Result<(), Error>;
}

/// All the node types NodeManager supports
#[derive(Clone)]
enum Node {
    /// A source node is a producer
    Source(Addr<Source>),
    /// A destination node is a consumer
    Destination(Addr<Destination>),
    /// A mixer node is both a consumer and a producer
    Mixer(Addr<Mixer>),
}

impl Node {
    /// Start the node
    fn start(&mut self, msg: StartMessage) -> ResponseFuture<Result<(), Error>> {
        let recipient: Recipient<StartMessage> = match self {
            Node::Source(addr) => addr.clone().recipient(),
            Node::Destination(addr) => addr.clone().recipient(),
            Node::Mixer(addr) => addr.clone().recipient(),
        };
        Box::pin(async move {
            match recipient.send(msg).await {
                Ok(res) => res,
                Err(err) => Err(anyhow!("Internal server error {}", err)),
            }
        })
    }

    /// Reschedule the node
    fn schedule(&mut self, msg: ScheduleMessage) -> ResponseFuture<Result<(), Error>> {
        let recipient: Recipient<ScheduleMessage> = match self {
            Node::Source(addr) => addr.clone().recipient(),
            Node::Destination(addr) => addr.clone().recipient(),
            Node::Mixer(addr) => addr.clone().recipient(),
        };
        Box::pin(async move {
            match recipient.send(msg).await {
                Ok(res) => res,
                Err(err) => Err(anyhow!("Internal server error {}", err)),
            }
        })
    }

    /// Stop the node
    fn stop(&mut self) {
        let recipient: Recipient<StopMessage> = match self {
            Node::Source(addr) => addr.clone().recipient(),
            Node::Destination(addr) => addr.clone().recipient(),
            Node::Mixer(addr) => addr.clone().recipient(),
        };
        let _ = recipient.do_send(StopMessage);
    }

    /// Get node-specific info
    fn get_info(&mut self) -> ResponseFuture<Result<NodeInfo, Error>> {
        let recipient: Recipient<GetNodeInfoMessage> = match self {
            Node::Source(addr) => addr.clone().recipient(),
            Node::Destination(addr) => addr.clone().recipient(),
            Node::Mixer(addr) => addr.clone().recipient(),
        };
        Box::pin(async move {
            match recipient.send(GetNodeInfoMessage).await {
                Ok(res) => res,
                Err(err) => Err(anyhow!("Internal server error {}", err)),
            }
        })
    }

    /// Control a property of the node
    fn add_control_point(
        &mut self,
        msg: AddControlPointMessage,
    ) -> ResponseFuture<Result<(), Error>> {
        let recipient: Recipient<AddControlPointMessage> = match self {
            Node::Source(addr) => addr.clone().recipient(),
            Node::Destination(addr) => addr.clone().recipient(),
            Node::Mixer(addr) => addr.clone().recipient(),
        };

        Box::pin(async move {
            match recipient.send(msg).await {
                Ok(res) => res,
                Err(err) => Err(anyhow!("Internal server error {}", err)),
            }
        })
    }

    /// Remove a control point for the node
    fn remove_control_point(&self, msg: RemoveControlPointMessage) {
        let recipient: Recipient<RemoveControlPointMessage> = match self {
            Node::Source(addr) => addr.clone().recipient(),
            Node::Destination(addr) => addr.clone().recipient(),
            Node::Mixer(addr) => addr.clone().recipient(),
        };
        let _ = recipient.do_send(msg);
    }
}

impl Actor for NodeManager {
    type Context = Context<Self>;
}

impl actix::Supervised for NodeManager {}

impl SystemService for NodeManager {
    fn service_started(&mut self, _ctx: &mut Context<Self>) {
        info!("Node manager coming online");
    }
}

impl NodeManager {
    /// Create a [`Source`] and store it as a producer
    fn create_source(&mut self, id: &str, uri: &str, audio: bool, video: bool) -> CommandResult {
        if self.nodes.contains_key(id) {
            return CommandResult::Error(format!("A node already exists with id {}", id));
        }

        if !audio && !video {
            return CommandResult::Error(format!(
                "Source with id {} must have either audio or video enabled",
                id
            ));
        }

        let source = Source::new(id, uri, audio, video);
        let source_addr = source.start();

        self.nodes
            .insert(id.to_string(), Node::Source(source_addr.clone()));

        self.producers
            .insert(id.to_string(), source_addr.recipient());

        trace!("Created source {}", id);

        CommandResult::Success
    }

    /// Create a [`Destination`] and store it as a consumer
    fn create_destination(
        &mut self,
        id: &str,
        family: &DestinationFamily,
        audio: bool,
        video: bool,
    ) -> CommandResult {
        if self.nodes.contains_key(id) {
            return CommandResult::Error(format!("A node already exists with id {}", id));
        }

        if !audio && !video {
            return CommandResult::Error(format!(
                "Destination with id {} must have either audio or video enabled",
                id
            ));
        }

        let dest = Destination::new(id, family, audio, video);

        let addr = dest.start();

        self.nodes
            .insert(id.to_string(), Node::Destination(addr.clone()));
        self.consumers.insert(id.to_string(), addr.recipient());

        trace!("Created destination {}", id);

        CommandResult::Success
    }

    /// Create a [`Mixer`] and store it as both a consumer and a producer
    fn create_mixer(
        &mut self,
        id: &str,
        config: Option<HashMap<String, serde_json::Value>>,
        audio: bool,
        video: bool,
    ) -> CommandResult {
        if self.nodes.contains_key(id) {
            return CommandResult::Error(format!("A node already exists with id {}", id));
        }

        if !audio && !video {
            return CommandResult::Error(format!(
                "Mixer with id {} must have either audio or video enabled",
                id
            ));
        }

        let mixer = match Mixer::new(id, config, audio, video) {
            Ok(mixer) => mixer,
            Err(err) => {
                return CommandResult::Error(format!("Failed to create mixer: {}", err));
            }
        };

        let addr = mixer.start();

        self.nodes.insert(id.to_string(), Node::Mixer(addr.clone()));
        self.producers
            .insert(id.to_string(), addr.clone().recipient());
        self.consumers.insert(id.to_string(), addr.recipient());

        trace!("Created mixer {}", id);

        CommandResult::Success
    }

    /// Remove a node from our collections by id
    fn remove_node(&mut self, id: &str) {
        let _ = self.nodes.remove(id);
        if self.nodes.is_empty() {
            if let Some(sender) = self.no_more_modes_sender.take() {
                let _ = sender.send(());
            }
        }
    }

    /// Tell a node to stop, by id
    fn stop_node(&mut self, id: &str) -> CommandResult {
        if let Some(node) = self.nodes.get_mut(id) {
            node.stop();
            CommandResult::Success
        } else {
            CommandResult::Error(format!("No node with id {}", id))
        }
    }

    /// Tell a node to disconnect one of its consumer slots
    fn disconnect_consumer(&self, consumer: &mut Recipient<ConsumerMessage>, slot_id: String) {
        let _ = consumer.do_send(ConsumerMessage::Disconnect { slot_id });
    }

    /// Look up all consumers for a given producer and tell them to disconnect
    /// the associated slots
    fn disconnect_consumers(
        &mut self,
        video_producer: Option<StreamProducer>,
        audio_producer: Option<StreamProducer>,
    ) {
        debug!("Disconnecting consumers");

        if let Some(video_producer) = video_producer {
            for slot_id in video_producer.get_consumer_ids() {
                if let Some(mut consumer) = self.links.remove(&slot_id) {
                    self.disconnect_consumer(&mut consumer, slot_id);
                }
            }
        }

        if let Some(audio_producer) = audio_producer {
            for slot_id in audio_producer.get_consumer_ids() {
                if let Some(mut consumer) = self.links.remove(&slot_id) {
                    self.disconnect_consumer(&mut consumer, slot_id);
                }
            }
        }
    }

    /// Start a [`Node`]
    #[instrument(level = "trace", name = "start-command", skip(self))]
    fn send_start_command_future(
        &mut self,
        id: &str,
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> ResponseActFuture<Self, CommandResult> {
        if let Some(node) = self.nodes.get(id) {
            let mut node = node.clone();
            Box::pin(
                {
                    async move { node.start(StartMessage { cue_time, end_time }).await }
                        .into_actor(self)
                        .then(move |res, _slf, _ctx| {
                            actix::fut::ready(match res {
                                Ok(_) => CommandResult::Success,
                                Err(err) => CommandResult::Error(format!("{}", err)),
                            })
                        })
                }
                .in_current_actor_span(),
            )
        } else {
            Box::pin(actix::fut::ready(CommandResult::Error(format!(
                "No node with id {}",
                id
            ))))
        }
    }

    /// Reschedule a [`Node`]
    #[instrument(level = "trace", name = "schedule-command", skip(self))]
    fn send_schedule_command_future(
        &mut self,
        id: &str,
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> ResponseActFuture<Self, CommandResult> {
        if let Some(node) = self.nodes.get(id) {
            let mut node = node.clone();
            Box::pin(
                {
                    async move { node.schedule(ScheduleMessage { cue_time, end_time }).await }
                        .into_actor(self)
                        .then(move |res, _slf, _ctx| {
                            actix::fut::ready(match res {
                                Ok(_) => CommandResult::Success,
                                Err(err) => CommandResult::Error(format!("{}", err)),
                            })
                        })
                }
                .in_current_actor_span(),
            )
        } else {
            Box::pin(actix::fut::ready(CommandResult::Error(format!(
                "No node with id {}",
                id
            ))))
        }
    }

    /// Connect a producer and a consumer
    #[instrument(level = "trace", name = "connect-command", skip(self))]
    fn connect_future(
        &mut self,
        link_id: &str,
        src: &str,
        sink: &str,
        audio: bool,
        video: bool,
        config: Option<HashMap<String, serde_json::Value>>,
    ) -> ResponseActFuture<Self, CommandResult> {
        if !audio && !video {
            return Box::pin({
                actix::fut::ready(CommandResult::Error(format!(
                    "Link with id {} must have either audio or video enabled",
                    link_id
                )))
            });
        }

        let producer = match self.producers.get(src) {
            Some(producer) => producer.clone(),
            None => {
                return Box::pin({
                    actix::fut::ready(CommandResult::Error(format!("No producer with id {}", src)))
                });
            }
        };

        let consumer = match self.consumers.get(sink) {
            Some(consumer) => consumer.clone(),
            None => {
                return Box::pin({
                    actix::fut::ready(CommandResult::Error(format!(
                        "No consumer with id {}",
                        sink
                    )))
                });
            }
        };

        let link_id = link_id.to_string();

        let consumer_clone = consumer.clone();
        let link_id_clone = link_id.clone();

        Box::pin(
            {
                async move { producer.send(GetProducerMessage {}).in_current_span().await }
                    .then(move |res| async move {
                        let res = res.unwrap();

                        let (video_producer, audio_producer) = match res {
                            Ok(res) => res,
                            Err(err) => {
                                return Ok(Err(anyhow!("Failed to get producer: {:?}", err)));
                            }
                        };

                        consumer
                            .send(ConsumerMessage::Connect {
                                link_id,
                                video_producer: if video { video_producer } else { None },
                                audio_producer: if audio { audio_producer } else { None },
                                config,
                            })
                            .in_current_span()
                            .await
                    })
                    .into_actor(self)
                    .then(move |res, slf, _ctx| {
                        actix::fut::ready(match res {
                            Ok(res) => match res {
                                Ok(_) => {
                                    debug!("Link established");
                                    slf.links.insert(link_id_clone, consumer_clone);
                                    CommandResult::Success
                                }
                                Err(err) => CommandResult::Error(format!("{}", err)),
                            },
                            Err(err) => {
                                CommandResult::Error(format!("Internal server error {}", err))
                            }
                        })
                    })
            }
            .in_current_actor_span(),
        )
    }

    /// Disconnect a consumer by id
    #[instrument(level = "trace", name = "disconnect-command", skip(self))]
    fn disconnect(&mut self, link_id: &str) -> CommandResult {
        if let Some(mut consumer) = self.links.remove(link_id) {
            self.disconnect_consumer(&mut consumer, link_id.to_string());
            CommandResult::Success
        } else {
            CommandResult::Error(format!("no link with id {}", link_id))
        }
    }

    /// Get the info either of a specific [`Node`], or of all nodes
    #[instrument(level = "trace", name = "get-info-command", skip(self))]
    fn get_info_future(&mut self, id: Option<&String>) -> ResponseActFuture<Self, CommandResult> {
        let mut nodes: Vec<(String, Node)> = match id {
            Some(id) => {
                if let Some(node) = self.nodes.get(id) {
                    vec![(id.clone(), node.clone())]
                } else {
                    return Box::pin(actix::fut::ready(CommandResult::Error(format!(
                        "No node with id {}",
                        id
                    ))));
                }
            }
            None => self
                .nodes
                .iter()
                .map(|(id, node)| (id.clone(), node.clone()))
                .collect(),
        };

        Box::pin(
            {
                async move {
                    let all_futures = nodes.drain(..).map(|(node_id, mut node)| async move {
                        node.get_info().await.map(|res| (node_id, res))
                    });
                    futures::future::join_all(all_futures).await
                }
                .into_actor(self)
                .then(move |mut res, _slf, _ctx| {
                    actix::fut::ready(CommandResult::Info(Info {
                        nodes: res
                            .drain(..)
                            .filter(|res| res.is_ok())
                            .map(|res| res.unwrap())
                            .collect(),
                    }))
                })
            }
            .in_current_actor_span(),
        )
    }

    /// Add a control point for a property and keep track of the controllee
    #[instrument(level = "trace", name = "control-property-command", skip(self))]
    fn control_property_future(
        &mut self,
        controllee_id: String,
        property: String,
        control_point: ControlPoint,
    ) -> ResponseActFuture<Self, CommandResult> {
        if let Some(consumer) = self.links.get(&controllee_id) {
            let consumer = consumer.clone();
            Box::pin(
                {
                    async move {
                        consumer
                            .send(ConsumerMessage::AddControlPoint {
                                slot_id: controllee_id.to_string(),
                                property: property.to_string(),
                                control_point,
                            })
                            .in_current_span()
                            .await
                    }
                    .into_actor(self)
                    .then(move |res, _slf, _ctx| {
                        actix::fut::ready(match res {
                            Ok(res) => match res {
                                Ok(_) => CommandResult::Success,
                                Err(err) => CommandResult::Error(format!("{}", err)),
                            },
                            Err(err) => {
                                CommandResult::Error(format!("Internal server error {}", err))
                            }
                        })
                    })
                }
                .in_current_actor_span(),
            )
        } else if let Some(node) = self.nodes.get(&controllee_id) {
            let mut node = node.clone();
            Box::pin(
                {
                    async move {
                        node.add_control_point(AddControlPointMessage {
                            property: property.to_string(),
                            control_point,
                        })
                        .in_current_span()
                        .await
                    }
                    .into_actor(self)
                    .then(move |res, _slf, _ctx| {
                        actix::fut::ready(match res {
                            Ok(_) => CommandResult::Success,
                            Err(err) => CommandResult::Error(format!("{}", err)),
                        })
                    })
                }
                .in_current_actor_span(),
            )
        } else {
            Box::pin({
                actix::fut::ready(CommandResult::Error(format!(
                    "No node or slot with id {}",
                    controllee_id
                )))
            })
        }
    }

    /// Remove a control point by id
    #[instrument(level = "trace", name = "remove-control-point-command", skip(self))]
    fn remove_control_point(
        &mut self,
        id: &str,
        controllee_id: &str,
        property: &str,
    ) -> CommandResult {
        if let Some(consumer) = self.links.get(controllee_id) {
            let _ = consumer.do_send(ConsumerMessage::RemoveControlPoint {
                controller_id: id.to_string(),
                slot_id: controllee_id.to_string(),
                property: property.to_string(),
            });
            CommandResult::Success
        } else if let Some(node) = self.nodes.get(controllee_id) {
            node.remove_control_point(RemoveControlPointMessage {
                controller_id: id.to_string(),
                property: property.to_string(),
            });
            CommandResult::Success
        } else {
            CommandResult::Error(format!("no node or slot with id {}", controllee_id))
        }
    }

    fn add_listener(
        &mut self,
        id: String,
        recipient: WeakRecipient<NodeStatusMessage>,
    ) -> Result<(), Error> {
        #[allow(clippy::map_entry)]
        if self.listeners.contains_key(&id) {
            Err(anyhow!("A node already exists with id {}", id))
        } else {
            self.listeners.insert(id, recipient);

            Ok(())
        }
    }

    fn notify_listeners(&mut self, message: NodeStatusMessage) {
        self.listeners.retain(|_id, recipient| {
            if let Some(recipient) = recipient.upgrade() {
                let _ = recipient.do_send(message.clone());
                true
            } else {
                false
            }
        })
    }
}

impl Handler<CommandMessage> for NodeManager {
    type Result = ResponseActFuture<Self, CommandResult>;

    #[instrument(level = "trace", name = "command", skip(self, _ctx))]
    fn handle(&mut self, msg: CommandMessage, _ctx: &mut Context<Self>) -> Self::Result {
        match msg.command {
            Command::Connect {
                link_id,
                src_id,
                sink_id,
                audio,
                video,
                config,
            } => self.connect_future(&link_id, &src_id, &sink_id, audio, video, config),
            Command::Disconnect { link_id } => {
                Box::pin(actix::fut::ready(self.disconnect(&link_id)))
            }
            Command::CreateSource {
                id,
                uri,
                audio,
                video,
            } => Box::pin(actix::fut::ready(
                self.create_source(&id, &uri, audio, video),
            )),
            Command::CreateDestination {
                id,
                family,
                audio,
                video,
            } => Box::pin(actix::fut::ready(
                self.create_destination(&id, &family, audio, video),
            )),
            Command::CreateMixer {
                id,
                config,
                audio,
                video,
            } => Box::pin(actix::fut::ready(
                self.create_mixer(&id, config, audio, video),
            )),
            Command::Start {
                id,
                cue_time,
                end_time,
            } => self.send_start_command_future(&id, cue_time, end_time),
            Command::Reschedule {
                id,
                cue_time,
                end_time,
            } => self.send_schedule_command_future(&id, cue_time, end_time),
            Command::Remove { id } => Box::pin(actix::fut::ready(self.stop_node(&id))),
            Command::GetInfo { id } => self.get_info_future(id.as_ref()),
            Command::AddControlPoint {
                controllee_id,
                property,
                control_point,
            } => self.control_property_future(controllee_id, property, control_point),
            Command::RemoveControlPoint {
                id,
                controllee_id,
                property,
            } => Box::pin(actix::fut::ready(self.remove_control_point(
                &id,
                &controllee_id,
                &property,
            ))),
        }
    }
}

impl Handler<StoppedMessage> for NodeManager {
    type Result = MessageResult<StoppedMessage>;

    #[instrument(level = "debug", name = "removing-node", skip(self, _ctx, msg), fields(id = %msg.id))]
    fn handle(&mut self, msg: StoppedMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.remove_node(&msg.id);
        let _ = self.consumers.remove(&msg.id);
        let _ = self.producers.remove(&msg.id);

        self.disconnect_consumers(msg.video_producer, msg.audio_producer);

        debug!("node {} removed from NodeManager", msg.id);

        MessageResult(())
    }
}

impl Handler<StopMessage> for NodeManager {
    type Result = ResponseFuture<Result<(), Error>>;

    #[instrument(level = "info", name = "stopping manager", skip(self, _ctx, _msg))]
    fn handle(&mut self, _msg: StopMessage, _ctx: &mut Context<Self>) -> Self::Result {
        for (_id, node) in self.nodes.iter_mut() {
            node.stop();
        }

        let mut no_more_modes_receiver = {
            if !self.nodes.is_empty() {
                let (no_more_modes_sender, no_more_modes_receiver) = oneshot::channel::<()>();

                self.no_more_modes_sender = Some(no_more_modes_sender);

                info!("waiting for all nodes to stop");

                Some(no_more_modes_receiver)
            } else {
                None
            }
        };

        Box::pin(async move {
            if let Some(receiver) = no_more_modes_receiver.take() {
                let _ = receiver.await;
            }

            info!("all nodes have stopped, good bye!");

            Ok(())
        })
    }
}

impl Handler<RegisterListenerMessage> for NodeManager {
    type Result = Result<(), Error>;

    #[instrument(level = "debug", name = "registering listener", skip(self, _ctx))]
    fn handle(&mut self, msg: RegisterListenerMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.add_listener(msg.id, msg.recipient)
    }
}

impl Handler<NodeStatusMessage> for NodeManager {
    type Result = ();

    #[instrument(level = "trace", name = "notifying listeners", skip(self, _ctx))]
    fn handle(&mut self, msg: NodeStatusMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.notify_listeners(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::tests::*;
    use test_log::test;

    #[actix_rt::test]
    #[test]
    async fn test_connect() {
        gst::init().unwrap();
        let uri = asset_uri("ball.mp4");

        // Create a valid source
        create_source("test-source", &uri, true, true)
            .await
            .unwrap();
        create_local_destination("test-dest", "foo", None)
            .await
            .unwrap();
        connect("link", "test-source", "test-dest", true, true, None)
            .await
            .unwrap();

        let info = node_info_unchecked("test-dest").await;

        if let NodeInfo::Destination(dinfo) = info {
            assert_eq!(dinfo.audio_slot_id, Some("link".to_string()));
            assert_eq!(dinfo.video_slot_id, Some("link".to_string()));
        } else {
            panic!("Wrong info type");
        }
    }

    #[actix_rt::test]
    #[test]
    async fn test_connect_video_only() {
        gst::init().unwrap();
        let uri = asset_uri("ball.mp4");

        // Create a valid source
        create_source("test-source", &uri, true, true)
            .await
            .unwrap();
        create_local_destination("test-dest", "foo", None)
            .await
            .unwrap();
        connect("link", "test-source", "test-dest", true, false, None)
            .await
            .unwrap();

        let info = node_info_unchecked("test-dest").await;

        if let NodeInfo::Destination(dinfo) = info {
            assert_eq!(dinfo.audio_slot_id, None);
            assert_eq!(dinfo.video_slot_id, Some("link".to_string()));
        } else {
            panic!("Wrong info type");
        }
    }

    #[actix_rt::test]
    #[test]
    #[should_panic(expected = "link must result in at least one audio / video connection")]
    async fn test_connect_invalid_audio_only() {
        gst::init().unwrap();
        let uri = asset_uri("ball.mp4");

        // Create a valid source
        create_source("test-source", &uri, true, false)
            .await
            .unwrap();
        create_local_destination("test-dest", "foo", None)
            .await
            .unwrap();
        connect("link", "test-source", "test-dest", false, true, None)
            .await
            .unwrap();
    }
}
