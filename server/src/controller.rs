//! Interface with remote controller
//!
//! Receives JSON messages through websockets, conforming with
//! [`this protocol`](auteur_controlling::controller)

use crate::node::{CommandMessage, NodeManager};

use anyhow::{format_err, Error};

use actix::prelude::*;
use actix_web::dev::ConnectionInfo;
use actix_web_actors::ws;

use tracing::{debug, error, instrument, trace};

use auteur_controlling::controller::{Command, CommandResult, ControllerMessage, ServerMessage};

/// Actor that represents an application controller.
#[derive(Debug)]
pub struct Controller {
    /// Address of the remote controller
    remote_addr: String,
}

impl Controller {
    /// Create a new `Controller` actor.
    pub fn new(connection_info: &ConnectionInfo) -> Result<Self, Error> {
        debug!("Creating new controller {:?}", connection_info);

        let remote_addr = connection_info
            .realip_remote_addr()
            .ok_or_else(|| format_err!("WebSocket connection without remote address"))?;

        Ok(Controller {
            remote_addr: String::from(remote_addr),
        })
    }

    /// Send a command to [`NodeManager`] for dispatching, then notify
    /// the remote controller
    fn send_command_future(
        &self,
        command_id: uuid::Uuid,
        command: Command,
    ) -> impl ActorFuture<Self, Output = ()> {
        let node_manager = NodeManager::from_registry();

        async move { node_manager.send(CommandMessage { command }).await }
            .into_actor(self)
            .then(move |res, _, ctx| {
                match res {
                    Ok(res) => {
                        ctx.text(
                            serde_json::to_string(&ServerMessage {
                                id: Some(command_id),
                                result: res,
                            })
                            .expect("failed to serialize CommandResult message"),
                        );
                    }
                    Err(err) => {
                        ctx.notify(ErrorMessage {
                            msg: format!("Internal server error: {}", err),
                            command_id: Some(command_id),
                        });
                    }
                }

                actix::fut::ready(())
            })
    }

    /// Handle JSON messages from the controller.
    #[instrument(level = "trace", name = "controller-message", skip(self, ctx))]
    fn handle_message(&mut self, ctx: &mut ws::WebsocketContext<Self>, text: &str) {
        trace!("Handling message: {}", text);
        match serde_json::from_str::<ControllerMessage>(text) {
            Ok(ControllerMessage { id, command }) => {
                ctx.spawn(self.send_command_future(id, command));
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

    /// Shut down the controller
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

/// Sent from [`Controller` to itself to notify the remote controller
/// of an error.
#[derive(Debug)]
struct ErrorMessage {
    /// Error message
    msg: String,
    /// Identifier of the command that caused the error
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
                result: CommandResult::Error(msg.msg),
            })
            .expect("Failed to serialize error message"),
        );
    }
}
