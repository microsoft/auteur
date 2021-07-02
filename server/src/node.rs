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
    Command, CommandResult, DestinationCommand, DestinationFamily, GraphCommand, Info,
    MixerCommand, MixerConfig, NodeCommand, NodeCommands, NodeInfo, SourceCommand, State,
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

/// Source-specific commands, sent from [`NodeManager`] to [`Source`]
#[derive(Debug)]
pub struct SourceCommandMessage {
    /// The command to dispatch to the source node
    pub command: SourceCommand,
}

impl Message for SourceCommandMessage {
    type Result = Result<(), Error>;
}

/// Destination-specific commands, sent from [`NodeManager`] to [`Destination`]
#[derive(Debug)]
pub struct DestinationCommandMessage {
    /// The command to dispatch to the destination node
    pub command: DestinationCommand,
}

impl Message for DestinationCommandMessage {
    type Result = Result<(), Error>;
}

/// Mixer-specific commands, sent from [`NodeManager`] to [`Mixer`]
#[derive(Debug)]
pub struct MixerCommandMessage {
    /// The command to dispatch to the mixer node
    pub command: MixerCommand,
}

impl Message for MixerCommandMessage {
    type Result = Result<(), Error>;
}

/// Getter for [`stream producers`](crate::utils::StreamProducer),
/// sent from [`NodeManager`] to any producer node to connect them
/// to consumers
#[derive(Debug)]
pub struct GetProducerMessage;

impl Message for GetProducerMessage {
    type Result = Result<(StreamProducer, StreamProducer), Error>;
}

/// Sent from [`NodeManager`] to any consumer node in order to
/// let them connect and disconnect from
/// [`stream producers`](crate::utils::StreamProducer)
pub enum ConsumerMessage {
    /// Lets the consumer perform a connection, it should store the
    /// slot id for latter disconnection, and potentially future
    /// controlling of slot properties (eg volume on a [`Mixer`] slot)
    Connect {
        /// The id of the slot
        link_id: String,
        /// The video producer to connect to
        video_producer: StreamProducer,
        /// The audio producer to connect to
        audio_producer: StreamProducer,
    },
    /// Lets the consumer disconnect a slot from its associated
    /// producers
    Disconnect {
        /// The id of the slot to disconnect
        slot_id: String,
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
}

impl Default for NodeManager {
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
            links: HashMap::new(),
            consumers: HashMap::new(),
            producers: HashMap::new(),
            listeners: HashMap::new(),
            no_more_modes_sender: None,
        }
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
    fn create_source(&mut self, id: &str, uri: &str) -> CommandResult {
        if self.nodes.contains_key(id) {
            return CommandResult::Error(format!("A node already exists with id {}", id));
        }

        let source = Source::new(id, uri);
        let source_addr = source.start();

        self.nodes
            .insert(id.to_string(), Node::Source(source_addr.clone()));

        self.producers
            .insert(id.to_string(), source_addr.recipient());

        trace!("Created source {}", id);

        CommandResult::Success
    }

    /// Create a [`Destination`] and store it as a consumer
    fn create_destination(&mut self, id: &str, family: &DestinationFamily) -> CommandResult {
        if self.nodes.contains_key(id) {
            return CommandResult::Error(format!("A node already exists with id {}", id));
        }

        let dest = Destination::new(id, family);

        let addr = dest.start();

        self.nodes
            .insert(id.to_string(), Node::Destination(addr.clone()));
        self.consumers.insert(id.to_string(), addr.recipient());

        trace!("Created destination {}", id);

        CommandResult::Success
    }

    /// Create a [`Mixer`] and store it as both a consumer and a producer
    fn create_mixer(&mut self, id: &str, config: MixerConfig) -> CommandResult {
        if self.nodes.contains_key(id) {
            return CommandResult::Error(format!("A node already exists with id {}", id));
        }

        let mixer = Mixer::new(id, config);
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

    /// Dispatch a [`Source`] command
    #[instrument(level = "trace", name = "source-command", skip(self))]
    fn send_source_command_future(
        &mut self,
        id: &str,
        command: SourceCommand,
    ) -> ResponseActFuture<Self, CommandResult> {
        let source = match self.nodes.get(id) {
            Some(Node::Source(source)) => source.clone(),
            Some(_) => {
                return Box::pin(actix::fut::ready(CommandResult::Error(format!(
                    "node with id {} is not a source",
                    id
                ))));
            }
            None => {
                return Box::pin(actix::fut::ready(CommandResult::Error(format!(
                    "No source with id {}",
                    id
                ))));
            }
        };

        Box::pin(
            {
                async move { source.send(SourceCommandMessage { command }).await }
                    .into_actor(self)
                    .then(move |res, _slf, _ctx| {
                        actix::fut::ready(match res {
                            Ok(_) => CommandResult::Success,
                            Err(err) => {
                                CommandResult::Error(format!("Internal server error {}", err))
                            }
                        })
                    })
            }
            .in_current_actor_span(),
        )
    }

    /// Dispatch a [`Destination`] command
    #[instrument(level = "trace", name = "destination-command", skip(self))]
    fn send_destination_command_future(
        &mut self,
        id: &str,
        command: DestinationCommand,
    ) -> ResponseActFuture<Self, CommandResult> {
        let dest = match self.nodes.get(id) {
            Some(Node::Destination(dest)) => dest.clone(),
            Some(_) => {
                return Box::pin(actix::fut::ready(CommandResult::Error(format!(
                    "node with id {} is not a destination",
                    id
                ))));
            }
            None => {
                return Box::pin(actix::fut::ready(CommandResult::Error(format!(
                    "No destination with id {}",
                    id
                ))));
            }
        };

        Box::pin(
            {
                async move { dest.send(DestinationCommandMessage { command }).await }
                    .into_actor(self)
                    .then(move |res, _slf, _ctx| {
                        actix::fut::ready(match res {
                            Ok(_) => CommandResult::Success,
                            Err(err) => {
                                CommandResult::Error(format!("Internal server error {}", err))
                            }
                        })
                    })
            }
            .in_current_actor_span(),
        )
    }

    /// Dispatch a [`Mixer`] command
    #[instrument(level = "trace", name = "mixer-command", skip(self))]
    fn send_mixer_command_future(
        &mut self,
        id: &str,
        command: MixerCommand,
    ) -> ResponseActFuture<Self, CommandResult> {
        let mixer = match self.nodes.get(id) {
            Some(Node::Mixer(mixer)) => mixer.clone(),
            Some(_) => {
                return Box::pin(actix::fut::ready(CommandResult::Error(format!(
                    "node with id {} is not a mixer",
                    id
                ))));
            }
            None => {
                return Box::pin(actix::fut::ready(CommandResult::Error(format!(
                    "No mixer with id {}",
                    id
                ))));
            }
        };

        Box::pin(
            {
                async move { mixer.send(MixerCommandMessage { command }).await }
                    .into_actor(self)
                    .then(move |res, _slf, _ctx| {
                        actix::fut::ready(match res {
                            Ok(_) => CommandResult::Success,
                            Err(err) => {
                                CommandResult::Error(format!("Internal server error {}", err))
                            }
                        })
                    })
            }
            .in_current_actor_span(),
        )
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
    ) -> ResponseActFuture<Self, CommandResult> {
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
                                video_producer,
                                audio_producer,
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
            Command::Graph(cmd) => match cmd {
                GraphCommand::Connect {
                    link_id,
                    src_id,
                    sink_id,
                } => self.connect_future(&link_id, &src_id, &sink_id),
                GraphCommand::Disconnect { link_id } => {
                    Box::pin(actix::fut::ready(self.disconnect(&link_id)))
                }
                GraphCommand::CreateSource { id, uri } => {
                    Box::pin(actix::fut::ready(self.create_source(&id, &uri)))
                }
                GraphCommand::CreateDestination { id, family } => {
                    Box::pin(actix::fut::ready(self.create_destination(&id, &family)))
                }
                GraphCommand::CreateMixer { id, config } => {
                    Box::pin(actix::fut::ready(self.create_mixer(&id, config)))
                }
                GraphCommand::Start {
                    id,
                    cue_time,
                    end_time,
                } => self.send_start_command_future(&id, cue_time, end_time),
                GraphCommand::Reschedule {
                    id,
                    cue_time,
                    end_time,
                } => self.send_schedule_command_future(&id, cue_time, end_time),
                GraphCommand::Remove { id } => Box::pin(actix::fut::ready(self.stop_node(&id))),
                GraphCommand::GetInfo { id } => self.get_info_future(id.as_ref()),
            },
            Command::Node(cmd) => {
                let NodeCommand { id, command } = cmd;
                match command {
                    NodeCommands::Source(src_cmd) => self.send_source_command_future(&id, src_cmd),
                    NodeCommands::Destination(dest_cmd) => {
                        self.send_destination_command_future(&id, dest_cmd)
                    }
                    NodeCommands::Mixer(mixer_cmd) => {
                        self.send_mixer_command_future(&id, mixer_cmd)
                    }
                }
            }
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

        debug!("mixer {} removed from NodeManager", msg.id);

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
