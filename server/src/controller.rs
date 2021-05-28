// Copyright (C) 2021 Mathieu Duponchelle <mathieu@centricular.com>
//
// Licensed under the MIT license, see the LICENSE file or <http://opensource.org/licenses/MIT>

use crate::channel::Channel;
use crate::config::Config;

use anyhow::{format_err, Error};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use actix::{
    Actor, ActorContext, ActorFuture, Addr, AsyncContext, Handler, Message, StreamHandler,
    WrapFuture,
};
use actix_web::dev::ConnectionInfo;
use actix_web_actors::ws;

use chrono::{DateTime, Utc};
use tracing::{debug, error, trace};

use rtmp_switcher_controlling::controller::{
    ControllerCommand, ControllerMessage, DestinationFamily, ServerCommandResult, ServerMessage,
};

/// Actor that represents an application controller.
#[derive(Debug)]
pub struct Controller {
    cfg: Arc<Config>,
    channels: Arc<Mutex<HashMap<uuid::Uuid, Addr<Channel>>>>,
    remote_addr: String,
}

impl Controller {
    /// Create a new `Controller` actor.
    pub fn new(
        cfg: Arc<Config>,
        channels: Arc<Mutex<HashMap<uuid::Uuid, Addr<Channel>>>>,
        connection_info: &ConnectionInfo,
    ) -> Result<Self, Error> {
        debug!("Creating new controller {:?}", connection_info);

        let remote_addr = connection_info
            .realip_remote_addr()
            .ok_or_else(|| format_err!("WebSocket connection without remote address"))?;

        Ok(Controller {
            cfg,
            channels,
            remote_addr: String::from(remote_addr),
        })
    }

    fn get_channel_info_future(
        &self,
        command_id: uuid::Uuid,
        channel: Addr<Channel>,
    ) -> impl ActorFuture<Actor = Self, Output = ()> {
        async move { channel.send(crate::channel::GetInfoMessage {}).await }
            .into_actor(self)
            .then(move |res, _, ctx| {
                ctx.text(
                    serde_json::to_string(&ServerMessage {
                        id: Some(command_id),
                        result: ServerCommandResult::ChannelInfo(res.unwrap()),
                    })
                    .expect("failed to serialize ChannelInfo message"),
                );
                actix::fut::ready(())
            })
    }

    fn add_source_future(
        &self,
        command_id: uuid::Uuid,
        channel: Addr<Channel>,
        uri: String,
        cue_time: DateTime<Utc>,
        end_time: Option<DateTime<Utc>>,
    ) -> impl ActorFuture<Actor = Self, Output = ()> {
        async move {
            channel
                .send(crate::channel::AddSourceMessage {
                    uri,
                    cue_time,
                    end_time,
                })
                .await
        }
        .into_actor(self)
        .then(move |res, _, ctx| {
            match res.unwrap() {
                Ok(id) => {
                    ctx.text(
                        serde_json::to_string(&ServerMessage {
                            id: Some(command_id),
                            result: ServerCommandResult::SourceAdded { id },
                        })
                        .expect("failed to serialize SourceAdded message"),
                    );
                }
                Err(err) => {
                    ctx.notify(ErrorMessage {
                        msg: format!("Failed to add source: {:?}", err),
                        command_id: Some(command_id),
                    });
                }
            }
            actix::fut::ready(())
        })
    }

    fn modify_source_future(
        &self,
        command_id: uuid::Uuid,
        channel: Addr<Channel>,
        source_id: uuid::Uuid,
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> impl ActorFuture<Actor = Self, Output = ()> {
        async move {
            channel
                .send(crate::channel::ModifySourceMessage {
                    id: source_id,
                    cue_time,
                    end_time,
                })
                .await
        }
        .into_actor(self)
        .then(move |res, _, ctx| {
            match res.unwrap() {
                Ok(()) => {
                    ctx.text(
                        serde_json::to_string(&ServerMessage {
                            id: Some(command_id),
                            result: ServerCommandResult::SourceModified { id: source_id },
                        })
                        .expect("failed to serialize SourceModified message"),
                    );
                }
                Err(err) => {
                    ctx.notify(ErrorMessage {
                        msg: format!("Failed to modify source: {:?}", err),
                        command_id: Some(command_id),
                    });
                }
            }
            actix::fut::ready(())
        })
    }

    fn remove_source_future(
        &self,
        command_id: uuid::Uuid,
        channel: Addr<Channel>,
        source_id: uuid::Uuid,
    ) -> impl ActorFuture<Actor = Self, Output = ()> {
        async move {
            channel
                .send(crate::channel::RemoveSourceMessage { id: source_id })
                .await
        }
        .into_actor(self)
        .then(move |res, _, ctx| {
            match res.unwrap() {
                Ok(()) => {
                    ctx.text(
                        serde_json::to_string(&ServerMessage {
                            id: Some(command_id),
                            result: ServerCommandResult::SourceRemoved { id: source_id },
                        })
                        .expect("failed to serialize SourceRemoved message"),
                    );
                }
                Err(err) => {
                    ctx.notify(ErrorMessage {
                        msg: format!("Failed to remove source: {:?}", err),
                        command_id: Some(command_id),
                    });
                }
            }
            actix::fut::ready(())
        })
    }

    fn add_destination_future(
        &self,
        command_id: uuid::Uuid,
        channel: Addr<Channel>,
        family: DestinationFamily,
        cue_time: DateTime<Utc>,
        end_time: Option<DateTime<Utc>>,
    ) -> impl ActorFuture<Actor = Self, Output = ()> {
        async move {
            channel
                .send(crate::channel::AddDestinationMessage {
                    family,
                    cue_time,
                    end_time,
                })
                .await
        }
        .into_actor(self)
        .then(move |res, _, ctx| {
            match res.unwrap() {
                Ok(id) => {
                    ctx.text(
                        serde_json::to_string(&ServerMessage {
                            id: Some(command_id),
                            result: ServerCommandResult::DestinationAdded { id },
                        })
                        .expect("failed to serialize DestinationAdded message"),
                    );
                }
                Err(err) => {
                    ctx.notify(ErrorMessage {
                        msg: format!("Failed to add destination: {:?}", err),
                        command_id: Some(command_id),
                    });
                }
            }
            actix::fut::ready(())
        })
    }

    fn modify_destination_future(
        &self,
        command_id: uuid::Uuid,
        channel: Addr<Channel>,
        destination_id: uuid::Uuid,
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> impl ActorFuture<Actor = Self, Output = ()> {
        async move {
            channel
                .send(crate::channel::ModifyDestinationMessage {
                    id: destination_id,
                    cue_time,
                    end_time,
                })
                .await
        }
        .into_actor(self)
        .then(move |res, _, ctx| {
            match res.unwrap() {
                Ok(()) => {
                    ctx.text(
                        serde_json::to_string(&ServerMessage {
                            id: Some(command_id),
                            result: ServerCommandResult::SourceModified { id: destination_id },
                        })
                        .expect("failed to serialize DestinationModified message"),
                    );
                }
                Err(err) => {
                    ctx.notify(ErrorMessage {
                        msg: format!("Failed to modify destination: {:?}", err),
                        command_id: Some(command_id),
                    });
                }
            }
            actix::fut::ready(())
        })
    }

    fn remove_destination_future(
        &self,
        command_id: uuid::Uuid,
        channel: Addr<Channel>,
        destination_id: uuid::Uuid,
    ) -> impl ActorFuture<Actor = Self, Output = ()> {
        async move {
            channel
                .send(crate::channel::RemoveSourceMessage { id: destination_id })
                .await
        }
        .into_actor(self)
        .then(move |res, _, ctx| {
            match res.unwrap() {
                Ok(()) => {
                    ctx.text(
                        serde_json::to_string(&ServerMessage {
                            id: Some(command_id),
                            result: ServerCommandResult::SourceRemoved { id: destination_id },
                        })
                        .expect("failed to serialize SourceRemoved message"),
                    );
                }
                Err(err) => {
                    ctx.notify(ErrorMessage {
                        msg: format!("Failed to remove source: {:?}", err),
                        command_id: Some(command_id),
                    });
                }
            }
            actix::fut::ready(())
        })
    }

    fn run_command(
        &mut self,
        ctx: &mut ws::WebsocketContext<Self>,
        command_id: uuid::Uuid,
        command: ControllerCommand,
    ) {
        match command {
            ControllerCommand::StartChannel { name } => {
                let channel = Channel::new(self.cfg.clone(), self.channels.clone(), &name);
                let id = channel.id;

                self.channels.lock().unwrap().insert(id, channel.start());

                ctx.text(
                    serde_json::to_string(&ServerMessage {
                        id: Some(command_id),
                        result: ServerCommandResult::ChannelStarted { id },
                    })
                    .expect("failed to serialize ChannelStarted message"),
                );
            }
            ControllerCommand::StopChannel { id } => {
                match self.channels.lock().unwrap().remove(&id) {
                    Some(_) => {
                        ctx.text(
                            serde_json::to_string(&ServerMessage {
                                id: Some(command_id),
                                result: ServerCommandResult::ChannelStopped { id },
                            })
                            .expect("failed to serialize ChannelStopped message"),
                        );
                    }
                    None => {
                        ctx.notify(ErrorMessage {
                            msg: format!("No channel with id {:?}", id),
                            command_id: Some(command_id),
                        });
                    }
                }
            }
            ControllerCommand::ListChannels => {
                ctx.text(
                    serde_json::to_string(&ServerMessage {
                        id: Some(command_id),
                        result: ServerCommandResult::ChannelList {
                            channels: self
                                .channels
                                .lock()
                                .unwrap()
                                .keys()
                                .map(|id| id.clone())
                                .collect(),
                        },
                    })
                    .expect("failed to serialize ChannelList message"),
                );
            }
            ControllerCommand::GetChannelInfo { id } => {
                if let Some(channel) = self.channels.lock().unwrap().get(&id) {
                    ctx.spawn(self.get_channel_info_future(command_id, channel.clone()));
                } else {
                    ctx.notify(ErrorMessage {
                        msg: format!("No channel with id {:?}", id),
                        command_id: Some(command_id),
                    });
                }
            }
            ControllerCommand::AddSource {
                id,
                uri,
                cue_time,
                end_time,
            } => {
                if let Some(channel) = self.channels.lock().unwrap().get(&id) {
                    ctx.spawn(self.add_source_future(
                        command_id,
                        channel.clone(),
                        uri,
                        cue_time,
                        end_time,
                    ));
                } else {
                    ctx.notify(ErrorMessage {
                        msg: format!("No channel with id {:?}", id),
                        command_id: Some(command_id),
                    });
                }
            }
            ControllerCommand::ModifySource {
                id,
                source_id,
                cue_time,
                end_time,
            } => {
                if let Some(channel) = self.channels.lock().unwrap().get(&id) {
                    ctx.spawn(self.modify_source_future(
                        command_id,
                        channel.clone(),
                        source_id,
                        cue_time,
                        end_time,
                    ));
                } else {
                    ctx.notify(ErrorMessage {
                        msg: format!("No channel with id {:?}", id),
                        command_id: Some(command_id),
                    });
                }
            }
            ControllerCommand::RemoveSource { id, source_id } => {
                if let Some(channel) = self.channels.lock().unwrap().get(&id) {
                    ctx.spawn(self.remove_source_future(command_id, channel.clone(), source_id));
                } else {
                    ctx.notify(ErrorMessage {
                        msg: format!("No channel with id {:?}", id),
                        command_id: Some(command_id),
                    });
                }
            }
            ControllerCommand::AddDestination {
                id,
                family,
                cue_time,
                end_time,
            } => {
                if let Some(channel) = self.channels.lock().unwrap().get(&id) {
                    ctx.spawn(self.add_destination_future(
                        command_id,
                        channel.clone(),
                        family,
                        cue_time,
                        end_time,
                    ));
                } else {
                    ctx.notify(ErrorMessage {
                        msg: format!("No channel with id {:?}", id),
                        command_id: Some(command_id),
                    });
                }
            }
            ControllerCommand::ModifyDestination {
                id,
                destination_id,
                cue_time,
                end_time,
            } => {
                if let Some(channel) = self.channels.lock().unwrap().get(&id) {
                    ctx.spawn(self.modify_destination_future(
                        command_id,
                        channel.clone(),
                        destination_id,
                        cue_time,
                        end_time,
                    ));
                } else {
                    ctx.notify(ErrorMessage {
                        msg: format!("No channel with id {:?}", id),
                        command_id: Some(command_id),
                    });
                }
            }
            ControllerCommand::RemoveDestination { id, destination_id } => {
                if let Some(channel) = self.channels.lock().unwrap().get(&id) {
                    ctx.spawn(self.remove_destination_future(
                        command_id,
                        channel.clone(),
                        destination_id,
                    ));
                } else {
                    ctx.notify(ErrorMessage {
                        msg: format!("No channel with id {:?}", id),
                        command_id: Some(command_id),
                    });
                }
            }
        }
    }

    /// Handle JSON messages from the controller.
    fn handle_message(&mut self, ctx: &mut ws::WebsocketContext<Self>, text: &str) {
        trace!("Handling message: {}", text);
        match serde_json::from_str::<ControllerMessage>(text) {
            Ok(ControllerMessage { id, command }) => {
                self.run_command(ctx, id, command);
            }
            Err(err) => {
                error!(
                    "Controller {} has websocket error: {}",
                    self.remote_addr, err
                );
                ctx.notify(ErrorMessage {
                    msg: String::from("Internal processing error"),
                    command_id: None,
                });
            }
        }
    }

    fn shutdown(&mut self, ctx: &mut ws::WebsocketContext<Self>, from_close: bool) {
        debug!("Shutting down controller {}", self.remote_addr);

        if !from_close {
            ctx.close(None);
        }
        ctx.stop();
    }
}

impl Actor for Controller {
    type Context = ws::WebsocketContext<Self>;

    /// Called once the controller is started.
    fn started(&mut self, _ctx: &mut Self::Context) {
        trace!("Started controller {}", self.remote_addr);
    }

    /// Called when the controller is fully stopped.
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        trace!("Controller {} stopped", self.remote_addr);
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for Controller {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Text(text)) => {
                self.handle_message(ctx, &text);
            }
            Ok(ws::Message::Close(reason)) => {
                debug!(
                    "Controller {} websocket connection closed: {:?}",
                    self.remote_addr, reason
                );
                self.shutdown(ctx, true);
            }
            Ok(ws::Message::Binary(_binary)) => {
                error!("Unsupported binary message, ignoring");
            }
            Ok(ws::Message::Continuation(_)) => {
                error!("Unsupported continuation message, ignoring");
            }
            Ok(ws::Message::Nop) | Ok(ws::Message::Pong(_)) => {
                // Do nothing
            }
            Err(err) => {
                error!(
                    "Controller {} websocket connection error: {:?}",
                    self.remote_addr, err
                );
                self.shutdown(ctx, false);
            }
        }
    }
}

/// Message to report channel errors to the actor.
#[derive(Debug)]
struct ErrorMessage {
    msg: String,
    command_id: Option<uuid::Uuid>,
}

impl Message for ErrorMessage {
    type Result = ();
}

impl Handler<ErrorMessage> for Controller {
    type Result = ();

    fn handle(&mut self, msg: ErrorMessage, ctx: &mut ws::WebsocketContext<Self>) -> Self::Result {
        error!(
            "Got error message '{}' on controller {}",
            msg.msg, self.remote_addr
        );

        ctx.text(
            serde_json::to_string(&ServerMessage {
                id: msg.command_id,
                result: ServerCommandResult::Error { message: msg.msg },
            })
            .expect("Failed to serialize error message"),
        );
    }
}
