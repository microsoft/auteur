// Copyright (C) 2021 Mathieu Duponchelle <mathieu@centricular.com>
//
// Licensed under the MIT license, see the LICENSE file or <http://opensource.org/licenses/MIT>

use crate::channel::Channel;
use crate::{channel::GetInfoMessage, config::Config};

use anyhow::{format_err, Error};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use actix::{
    Actor, ActorContext, ActorFuture, Addr, AsyncContext, Handler, Message, StreamHandler,
    WrapFuture,
};
use actix_web::dev::ConnectionInfo;
use actix_web_actors::ws;

use log::{debug, error, trace};

use rtmp_switcher_controlling::controller::{ControllerMessage, ServerMessage};

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
        channel: Addr<Channel>,
    ) -> impl ActorFuture<Actor = Self, Output = ()> {
        async move { channel.send(crate::channel::GetInfoMessage {}).await }
            .into_actor(self)
            .then(move |res, _, ctx| {
                ctx.text(
                    serde_json::to_string(&res.unwrap())
                        .expect("failed to serialize ChannelList message"),
                );
                actix::fut::ready(())
            })
    }

    /// Handle JSON messages from the controller.
    fn handle_message(&mut self, ctx: &mut ws::WebsocketContext<Self>, text: &str) {
        trace!("Handling message: {}", text);
        match serde_json::from_str::<ControllerMessage>(text) {
            Ok(ControllerMessage::StartChannel { name, destination }) => {
                let channel = Channel::new(self.cfg.clone(), &name, &destination);
                let id = channel.id;

                self.channels.lock().unwrap().insert(id, channel.start());

                ctx.text(
                    serde_json::to_string(&ServerMessage::ChannelStarted { id })
                        .expect("failed to serialize ChannelStarted message"),
                );
            }
            Ok(ControllerMessage::StopChannel { id }) => {
                self.channels.lock().unwrap().remove(&id);

                ctx.text(
                    serde_json::to_string(&ServerMessage::ChannelStopped { id })
                        .expect("failed to serialize ChannelStopped message"),
                );
            }
            Ok(ControllerMessage::ListChannels) => {
                ctx.text(
                    serde_json::to_string(&ServerMessage::ChannelList {
                        channels: self
                            .channels
                            .lock()
                            .unwrap()
                            .keys()
                            .map(|id| id.clone())
                            .collect(),
                    })
                    .expect("failed to serialize ChannelList message"),
                );
            }
            Ok(ControllerMessage::GetChannelInfo { id }) => {
                if let Some(channel) = self.channels.lock().unwrap().get(&id) {
                    ctx.spawn(self.get_channel_info_future(channel.clone()));
                }
            }
            Ok(_) => {}
            Err(err) => {
                error!(
                    "Controller {} has websocket error: {}",
                    self.remote_addr, err
                );
                ctx.notify(ErrorMessage(String::from("Internal processing error")));
                self.shutdown(ctx, false);
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
    fn started(&mut self, ctx: &mut Self::Context) {
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

/// Message to report pipeline errors to the actor.
#[derive(Debug)]
struct ErrorMessage(String);

impl Message for ErrorMessage {
    type Result = ();
}

impl Handler<ErrorMessage> for Controller {
    type Result = ();

    fn handle(&mut self, msg: ErrorMessage, ctx: &mut ws::WebsocketContext<Self>) -> Self::Result {
        error!(
            "Got error message '{}' on controller {}",
            msg.0, self.remote_addr
        );

        ctx.text(
            serde_json::to_string(&ServerMessage::Error { message: msg.0 })
                .expect("Failed to serialize error message"),
        );
        self.shutdown(ctx, false);
    }
}
