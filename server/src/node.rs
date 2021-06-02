use crate::destination::{Destination, DestinationStoppedMessage};
use crate::mixer::{Mixer, MixerStoppedMessage};
use crate::source::{Source, SourceStoppedMessage};
use crate::utils::StreamProducer;
use actix::prelude::*;
use anyhow::{anyhow, Error};
use futures::prelude::*;
use rtmp_switcher_controlling::controller::{
    Command, DestinationCommand, DestinationFamily, GraphCommand, MixerCommand, NodeCommand,
    NodeCommands, SourceCommand,
};
use std::collections::HashMap;
use tracing::{debug, info, instrument, trace, warn};
use tracing_futures::Instrument;

/// NodeManager acts as a tracker of all nodes, and dispatches
/// messages accordingly

#[derive(Debug)]
pub struct CommandMessage {
    pub command: Command,
}

impl Message for CommandMessage {
    type Result = Result<(), Error>;
}

#[derive(Debug)]
pub struct SourceCommandMessage {
    pub command: SourceCommand,
}

impl Message for SourceCommandMessage {
    type Result = Result<(), Error>;
}

#[derive(Debug)]
pub struct DestinationCommandMessage {
    pub command: DestinationCommand,
}

impl Message for DestinationCommandMessage {
    type Result = Result<(), Error>;
}

#[derive(Debug)]
pub struct MixerCommandMessage {
    pub command: MixerCommand,
}

impl Message for MixerCommandMessage {
    type Result = Result<(), Error>;
}

/// Nodes can be producers, consumers or both. NodeManager knows
/// how to make logical links from one to another, actual connection
/// with StreamProducer::add_consumer() is delegated to the consumers
/// however, as they might want to only perform the connection once their
/// state has progressed.
///
/// Links have an identifier (provided by the client), that identifier
/// can be used by the client to perform disconnection, and are used
/// by consumers that expose multiple consumer slots to identify the
/// correct slot to disconnect (eg mixers)

#[derive(Debug)]
pub struct GetProducerMessage;

impl Message for GetProducerMessage {
    type Result = Result<(StreamProducer, StreamProducer), Error>;
}

pub enum ConsumerMessage {
    Connect {
        link_id: String,
        video_producer: StreamProducer,
        audio_producer: StreamProducer,
    },
    Disconnect {
        slot_id: String,
    },
}

impl Message for ConsumerMessage {
    type Result = Result<(), Error>;
}

/// All the node types NodeManager supports

enum Node {
    Source(Addr<Source>),
    Destination(Addr<Destination>),
    Mixer(Addr<Mixer>),
}

/// We track separately:
///
/// * all nodes by type
///
/// * the links that have been established, used when disconnecting
///
/// * all nodes that can consume data
///
/// * all nodes that can produce data

pub struct NodeManager {
    nodes: HashMap<String, Node>,
    links: HashMap<String, Recipient<ConsumerMessage>>,
    consumers: HashMap<String, Recipient<ConsumerMessage>>,
    producers: HashMap<String, Recipient<GetProducerMessage>>,
}

impl Default for NodeManager {
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
            links: HashMap::new(),
            consumers: HashMap::new(),
            producers: HashMap::new(),
        }
    }
}

impl Actor for NodeManager {
    type Context = Context<Self>;
}

impl actix::Supervised for NodeManager {}

impl SystemService for NodeManager {
    // Note: a SystemService never stops once started, it simply gets dropped.
    // This means that at program teardown, individual nodes can't rely on having
    // their stopped function called, and setting the state of pipelines must be
    // done on Drop (utils::PipelineManager takes care of that)
    fn service_started(&mut self, _ctx: &mut Context<Self>) {
        info!("Node manager coming online");
    }
}

impl NodeManager {
    fn create_source(&mut self, id: &str, uri: &str) -> Result<(), Error> {
        if self.nodes.contains_key(id) {
            return Err(anyhow!("A node already exists with id {}", id));
        }

        let source = Source::new(id, uri);
        let source_addr = source.start();

        self.nodes
            .insert(id.to_string(), Node::Source(source_addr.clone()));

        self.producers
            .insert(id.to_string(), source_addr.recipient().clone());

        trace!("Created source {}", id);

        Ok(())
    }

    fn create_destination(&mut self, id: &str, family: &DestinationFamily) -> Result<(), Error> {
        if self.nodes.contains_key(id) {
            return Err(anyhow!("A node already exists with id {}", id));
        }

        let dest = Destination::new(id, family);

        let addr = dest.start();

        self.nodes
            .insert(id.to_string(), Node::Destination(addr.clone()));
        self.consumers
            .insert(id.to_string(), addr.recipient().clone());

        trace!("Created destination {}", id);

        Ok(())
    }

    fn create_mixer(&mut self, id: &str) -> Result<(), Error> {
        if self.nodes.contains_key(id) {
            return Err(anyhow!("A node already exists with id {}", id));
        }

        let mixer = Mixer::new(id);
        let addr = mixer.start();

        self.nodes.insert(id.to_string(), Node::Mixer(addr.clone()));
        self.producers
            .insert(id.to_string(), addr.clone().recipient());
        self.consumers
            .insert(id.to_string(), addr.clone().recipient());

        trace!("Created mixer {}", id);

        Ok(())
    }

    fn disconnect_consumers(
        &mut self,
        video_producer: StreamProducer,
        audio_producer: StreamProducer,
    ) {
        debug!("Disconnecting consumers");

        for slot_id in video_producer.get_consumer_ids() {
            if let Some(consumer) = self.links.remove(&slot_id) {
                let _ = consumer.do_send(ConsumerMessage::Disconnect { slot_id });
            }
        }

        for slot_id in audio_producer.get_consumer_ids() {
            if let Some(consumer) = self.links.remove(&slot_id) {
                let _ = consumer.do_send(ConsumerMessage::Disconnect { slot_id });
            }
        }
    }

    #[instrument(level = "trace", name = "source-command", skip(self))]
    fn send_source_command_future(
        &mut self,
        id: &str,
        command: SourceCommand,
    ) -> ResponseActFuture<Self, Result<(), Error>> {
        let source = match self.nodes.get(id) {
            Some(Node::Source(source)) => source.clone(),
            Some(_) => {
                return Box::pin(actix::fut::ready(Err(anyhow!(
                    "node with id {} is not a source",
                    id
                ))));
            }
            None => {
                return Box::pin(actix::fut::ready(Err(anyhow!("No source with id {}", id))));
            }
        };

        Box::pin({
            async move { source.send(SourceCommandMessage { command }).await }
                .into_actor(self)
                .then(move |res, _slf, _ctx| actix::fut::ready(res.unwrap()))
        })
    }

    #[instrument(level = "trace", name = "destination-command", skip(self))]
    fn send_destination_command_future(
        &mut self,
        id: &str,
        command: DestinationCommand,
    ) -> ResponseActFuture<Self, Result<(), Error>> {
        let dest = match self.nodes.get(id) {
            Some(Node::Destination(dest)) => dest.clone(),
            Some(_) => {
                return Box::pin(actix::fut::ready(Err(anyhow!(
                    "node with id {} is not a destination",
                    id
                ))));
            }
            None => {
                return Box::pin(actix::fut::ready(Err(anyhow!(
                    "No destination with id {}",
                    id
                ))));
            }
        };

        Box::pin({
            async move { dest.send(DestinationCommandMessage { command }).await }
                .into_actor(self)
                .then(move |res, _slf, _ctx| actix::fut::ready(res.unwrap()))
        })
    }

    #[instrument(level = "trace", name = "mixer-command", skip(self))]
    fn send_mixer_command_future(
        &mut self,
        id: &str,
        command: MixerCommand,
    ) -> ResponseActFuture<Self, Result<(), Error>> {
        let mixer = match self.nodes.get(id) {
            Some(Node::Mixer(mixer)) => mixer.clone(),
            Some(_) => {
                return Box::pin(actix::fut::ready(Err(anyhow!(
                    "node with id {} is not a mixer",
                    id
                ))));
            }
            None => {
                return Box::pin(actix::fut::ready(Err(anyhow!("No mixer with id {}", id))));
            }
        };

        Box::pin({
            async move { mixer.send(MixerCommandMessage { command }).await }
                .into_actor(self)
                .then(move |res, _slf, _ctx| actix::fut::ready(res.unwrap()))
        })
    }

    #[instrument(level = "trace", name = "connect-command", skip(self))]
    fn connect_future(
        &mut self,
        link_id: &str,
        src: &str,
        sink: &str,
    ) -> ResponseActFuture<Self, Result<(), Error>> {
        let producer = match self.producers.get(src) {
            Some(producer) => producer.clone(),
            None => {
                return Box::pin({
                    actix::fut::ready(Err(anyhow!("No producer with id {}", src)))
                });
            }
        };

        let consumer = match self.consumers.get(sink) {
            Some(consumer) => consumer.clone(),
            None => {
                return Box::pin({
                    actix::fut::ready(Err(anyhow!("No consumer with id {}", sink)))
                });
            }
        };

        let link_id = link_id.to_string();

        let consumer_clone = consumer.clone();
        let link_id_clone = link_id.clone();

        Box::pin({
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
                    let res = res.unwrap();

                    if res.is_ok() {
                        debug!("Link established");
                        slf.links.insert(link_id_clone, consumer_clone);
                    } else {
                        warn!("Failed to establish link");
                    }

                    actix::fut::ready(res)
                })
        })
    }
}

impl Handler<CommandMessage> for NodeManager {
    type Result = ResponseActFuture<Self, Result<(), Error>>;

    #[instrument(level = "trace", name = "command", skip(self, _ctx))]
    fn handle(&mut self, msg: CommandMessage, _ctx: &mut Context<Self>) -> Self::Result {
        match msg.command {
            Command::Graph(cmd) => match cmd {
                GraphCommand::Connect {
                    link_id,
                    src_id,
                    sink_id,
                } => self.connect_future(&link_id, &src_id, &sink_id),
                GraphCommand::CreateSource { id, uri } => {
                    Box::pin(actix::fut::ready(self.create_source(&id, &uri)))
                }
                GraphCommand::CreateDestination { id, family } => {
                    Box::pin(actix::fut::ready(self.create_destination(&id, &family)))
                }
                GraphCommand::CreateMixer { id } => {
                    Box::pin(actix::fut::ready(self.create_mixer(&id)))
                }
            },
            Command::Node(cmd) => match cmd {
                NodeCommand { id, command } => match command {
                    NodeCommands::Source(src_cmd) => self.send_source_command_future(&id, src_cmd),
                    NodeCommands::Destination(dest_cmd) => {
                        self.send_destination_command_future(&id, dest_cmd)
                    }
                    NodeCommands::Mixer(mixer_cmd) => {
                        self.send_mixer_command_future(&id, mixer_cmd)
                    }
                },
            },
        }
    }
}

/// Nodes notify us when they're stopped so we can perform
/// cleanup / orderly disconnection
impl Handler<SourceStoppedMessage> for NodeManager {
    type Result = MessageResult<SourceStoppedMessage>;

    #[instrument(level = "debug", name = "removing-source", skip(self, msg, _ctx), fields(id = %msg.id))]
    fn handle(&mut self, msg: SourceStoppedMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let _ = self.nodes.remove(&msg.id);
        let _ = self.producers.remove(&msg.id);

        self.disconnect_consumers(msg.video_producer, msg.audio_producer);

        MessageResult(())
    }
}

impl Handler<DestinationStoppedMessage> for NodeManager {
    type Result = MessageResult<DestinationStoppedMessage>;

    #[instrument(level = "debug", name = "removing-destination", skip(self, _ctx, msg), fields(id = %msg.id))]
    fn handle(&mut self, msg: DestinationStoppedMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let _ = self.nodes.remove(&msg.id);
        let _ = self.consumers.remove(&msg.id);

        debug!("destination {} removed from NodeManager", msg.id);

        MessageResult(())
    }
}

impl Handler<MixerStoppedMessage> for NodeManager {
    type Result = MessageResult<MixerStoppedMessage>;

    #[instrument(level = "debug", name = "removing-mixer", skip(self, _ctx, msg), fields(id = %msg.id))]
    fn handle(&mut self, msg: MixerStoppedMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let _ = self.nodes.remove(&msg.id);
        let _ = self.consumers.remove(&msg.id);

        self.disconnect_consumers(msg.video_producer, msg.audio_producer);

        debug!("mixer {} removed from NodeManager", msg.id);

        MessageResult(())
    }
}
