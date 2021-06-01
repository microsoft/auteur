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
use std::collections::{HashMap, HashSet};
use tracing::{debug, info};

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

pub struct NodeManager {
    nodes: HashSet<String>,
    sources: HashMap<String, Addr<Source>>,
    mixers: HashMap<String, Addr<Mixer>>,
    links: HashMap<String, Recipient<ConsumerMessage>>,
    destinations: HashMap<String, Addr<Destination>>,
    consumers: HashMap<String, Recipient<ConsumerMessage>>,
    producers: HashMap<String, Recipient<GetProducerMessage>>,
}

impl Default for NodeManager {
    fn default() -> Self {
        Self {
            nodes: HashSet::new(),
            sources: HashMap::new(),
            mixers: HashMap::new(),
            links: HashMap::new(),
            destinations: HashMap::new(),
            consumers: HashMap::new(),
            producers: HashMap::new(),
        }
    }
}

impl Actor for NodeManager {
    type Context = Context<Self>;

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("Node manager stopped");
    }
}

impl actix::Supervised for NodeManager {}

impl SystemService for NodeManager {
    fn service_started(&mut self, _ctx: &mut Context<Self>) {
        info!("Node manager coming online");
    }
}

impl NodeManager {
    fn create_source(&mut self, id: &str, uri: &str) -> Result<(), Error> {
        if self.nodes.contains(id) {
            return Err(anyhow!("A node already exists with id {}", id));
        }

        let source = Source::new(id, uri);
        let source_addr = source.start();

        self.sources.insert(id.to_string(), source_addr.clone());
        self.producers
            .insert(id.to_string(), source_addr.recipient().clone());
        self.nodes.insert(id.to_string());

        Ok(())
    }

    fn create_destination(&mut self, id: &str, family: &DestinationFamily) -> Result<(), Error> {
        if self.nodes.contains(id) {
            return Err(anyhow!("A node already exists with id {}", id));
        }

        let dest = Destination::new(id, family);

        let addr = dest.start();

        self.destinations.insert(id.to_string(), addr.clone());
        self.consumers
            .insert(id.to_string(), addr.recipient().clone());
        self.nodes.insert(id.to_string());

        Ok(())
    }

    fn create_mixer(&mut self, id: &str) -> Result<(), Error> {
        if self.nodes.contains(id) {
            return Err(anyhow!("A node already exists with id {}", id));
        }

        let mixer = Mixer::new(id);
        let addr = mixer.start();

        self.mixers.insert(id.to_string(), addr.clone());
        self.producers
            .insert(id.to_string(), addr.clone().recipient());
        self.consumers
            .insert(id.to_string(), addr.clone().recipient());
        self.nodes.insert(id.to_string());

        Ok(())
    }

    fn disconnect_consumers(
        &mut self,
        video_producer: StreamProducer,
        audio_producer: StreamProducer,
    ) {
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

    fn send_source_command_future(
        &mut self,
        id: &str,
        command: SourceCommand,
    ) -> ResponseActFuture<Self, Result<(), Error>> {
        let source = match self.sources.get(id) {
            Some(source) => source.clone(),
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

    fn send_destination_command_future(
        &mut self,
        id: &str,
        command: DestinationCommand,
    ) -> ResponseActFuture<Self, Result<(), Error>> {
        let dest = match self.destinations.get(id) {
            Some(dest) => dest.clone(),
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

    fn send_mixer_command_future(
        &mut self,
        id: &str,
        command: MixerCommand,
    ) -> ResponseActFuture<Self, Result<(), Error>> {
        let dest = match self.mixers.get(id) {
            Some(dest) => dest.clone(),
            None => {
                return Box::pin(actix::fut::ready(Err(anyhow!("No mixer with id {}", id))));
            }
        };

        Box::pin({
            async move { dest.send(MixerCommandMessage { command }).await }
                .into_actor(self)
                .then(move |res, _slf, _ctx| actix::fut::ready(res.unwrap()))
        })
    }

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
            async move { producer.send(GetProducerMessage {}).await }
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
                        .await
                })
                .into_actor(self)
                .then(move |res, slf, _ctx| {
                    let res = res.unwrap();

                    if res.is_ok() {
                        slf.links.insert(link_id_clone, consumer_clone);
                    }

                    actix::fut::ready(res)
                })
        })
    }
}

impl Handler<SourceStoppedMessage> for NodeManager {
    type Result = MessageResult<SourceStoppedMessage>;

    fn handle(&mut self, msg: SourceStoppedMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let _ = self.sources.remove(&msg.id);
        let _ = self.nodes.remove(&msg.id);
        let _ = self.producers.remove(&msg.id);

        self.disconnect_consumers(msg.video_producer, msg.audio_producer);

        debug!("source {} removed from NodeManager", msg.id);

        MessageResult(())
    }
}

impl Handler<DestinationStoppedMessage> for NodeManager {
    type Result = MessageResult<DestinationStoppedMessage>;

    fn handle(&mut self, msg: DestinationStoppedMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let _ = self.destinations.remove(&msg.id);
        let _ = self.nodes.remove(&msg.id);
        let _ = self.consumers.remove(&msg.id);

        MessageResult(())
    }
}

impl Handler<MixerStoppedMessage> for NodeManager {
    type Result = MessageResult<MixerStoppedMessage>;

    fn handle(&mut self, msg: MixerStoppedMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let _ = self.mixers.remove(&msg.id);
        let _ = self.nodes.remove(&msg.id);
        let _ = self.consumers.remove(&msg.id);

        self.disconnect_consumers(msg.video_producer, msg.audio_producer);

        MessageResult(())
    }
}

impl Handler<CommandMessage> for NodeManager {
    type Result = ResponseActFuture<Self, Result<(), Error>>;

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
