// Copyright (C) 2021 Mathieu Duponchelle <mathieu@centricular.com>
//
// Licensed under the MIT license, see the LICENSE file or <http://opensource.org/licenses/MIT>

use anyhow::{Context, Error};
use std::path::PathBuf;
use std::sync::{atomic, Arc, Mutex};

use async_tungstenite::tungstenite;
use tungstenite::Message as WsMessage;

use futures::channel::mpsc;
use futures::prelude::*;

use log::{debug, error, info, trace, warn};

use auteur_controlling::controller::{Command, CommandResult, ControllerMessage, ServerMessage};

/// Controller handle.
#[derive(Debug, Clone)]
pub struct Controller {
    /// Sender for the event loop
    event_sender: mpsc::UnboundedSender<ControllerEvent>,
    /// Sender for the websocket messages
    websocket_sender: mpsc::UnboundedSender<ControllerMessage>,
    /// Whether the controller was stopped
    stopped: Arc<atomic::AtomicBool>,
    /// Whether the controller should exit the event loop after receiving
    /// the reply for a command with that id
    exit_on_response_id: Arc<Mutex<Option<uuid::Uuid>>>,
}

/// Future that can be awaited on to wait for the controller to stop or error out.
#[derive(Debug)]
pub struct ControllerJoinHandle {
    /// The handle
    handle: tokio::task::JoinHandle<Result<(), Error>>,
}

/// Simply wrapping around the tokio `JoinHandle`
impl std::future::Future for ControllerJoinHandle {
    type Output = Result<(), Error>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut std::task::Context,
    ) -> std::task::Poll<Result<(), Error>> {
        match self.as_mut().handle.poll_unpin(ctx) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(Err(err)) => {
                std::task::Poll::Ready(Err(Error::from(err).context("Joining Controller")))
            }
            std::task::Poll::Ready(Ok(ok)) => std::task::Poll::Ready(ok),
        }
    }
}

/// Events for the Publisher event loop.
#[derive(Debug)]
enum ControllerEvent {
    /// Sent from the websocket receiver.
    WebSocket(ServerMessage),
    /// Sent from anywhere if an error happens to report back.
    Error(anyhow::Error),
    /// Sent from stop() and other places.
    Close,
}

impl Controller {
    /// Run a new controller in the background
    ///
    /// This tries to connect to the configured server and waits for
    /// a command to run
    pub async fn new(
        server: String,
        certificate_file: Option<PathBuf>,
    ) -> Result<(Controller, ControllerJoinHandle), Error> {
        let ws = Self::connect(server, certificate_file)
            .await
            .context("Connecting to server")?;

        // Channel for the controller event loop
        let (event_sender, mut event_receiver) = mpsc::unbounded::<ControllerEvent>();

        // Channel for asynchronously sending out websocket message
        let (mut ws_sink, mut ws_stream) = ws.split();
        let (websocket_sender, mut websocket_receiver) = mpsc::unbounded::<ControllerMessage>();
        let websocket_send_task = tokio::spawn(async move {
            while let Some(msg) = websocket_receiver.next().await {
                ws_sink
                    .send(WsMessage::Text(
                        serde_json::to_string(&msg).expect("Failed to serialize message"),
                    ))
                    .await?;
            }

            ws_sink.send(WsMessage::Close(None)).await?;
            ws_sink.close().await?;

            Ok::<(), Error>(())
        });

        // Read websocket messages and pass them as events to the controller
        let mut event_sender_clone = event_sender.clone();
        tokio::spawn(async move {
            while let Some(msg) = ws_stream.next().await {
                match msg {
                    Ok(WsMessage::Text(msg)) => {
                        let msg = match serde_json::from_str::<ServerMessage>(&msg) {
                            Ok(msg) => msg,
                            Err(err) => {
                                warn!("Failed to deserialize server message: {:?}", err);
                                continue;
                            }
                        };

                        trace!("Received server message {:?}", msg);
                        if event_sender_clone
                            .send(ControllerEvent::WebSocket(msg))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(WsMessage::Close(reason)) => {
                        debug!("Websocket closed, reason: {:?}", reason);
                        let _ = event_sender_clone.send(ControllerEvent::Close).await;
                        break;
                    }
                    Ok(_) => {
                        warn!("Unsupported websocket message {:?}", msg);
                    }
                    Err(err) => {
                        let _ = event_sender_clone
                            .send(ControllerEvent::Error(
                                Error::from(err).context("Receiving websocket message"),
                            ))
                            .await;
                        break;
                    }
                }
            }

            debug!("Stopped websocket receiving");
        });

        // To remember if we already stopped before
        let stopped = Arc::new(atomic::AtomicBool::new(false));
        let exit_on_response_id = Arc::new(Mutex::new(None));

        let websocket_sender_clone = websocket_sender.clone();
        let exit_on_response_id_clone = exit_on_response_id.clone();
        let event_sender_clone = event_sender.clone();
        // Spawn our event loop
        let loop_join_handle = tokio::spawn(async move {
            info!("Controller running");

            // Handle all the events
            while let Some(event) = event_receiver.next().await {
                match event {
                    // Here we simply print the results of commands and
                    // shut down once our command has been executed
                    ControllerEvent::WebSocket(msg) => {
                        match msg.result {
                            CommandResult::Error(message) => {
                                eprintln!("Server error: {}", message);
                            }
                            CommandResult::Success => {
                                println!("Command ran successfully");
                            }
                            CommandResult::Info(info) => {
                                println!("Received info:");
                                println!("{}", serde_json::to_string_pretty(&info).unwrap());
                            }
                        }

                        let exit_on_response_id = exit_on_response_id_clone.lock().unwrap();
                        if let Some(id) = *exit_on_response_id {
                            info!("Command completed");

                            if Some(id) == msg.id {
                                let _ = event_sender_clone
                                    .unbounded_send(ControllerEvent::Close)
                                    .context("Stopping controller");
                            }
                        }
                    }
                    ControllerEvent::Error(err) => {
                        error!("Received error {:?}, stopping", err);
                        return Err(err);
                    }
                    ControllerEvent::Close => {
                        info!("Shutting down");
                        websocket_sender_clone.close_channel();
                        event_receiver.close();
                        websocket_send_task.await.context("Closing websocket")??;
                        break;
                    }
                }
            }

            Ok(())
        });

        Ok((
            Controller {
                event_sender,
                websocket_sender,
                stopped,
                exit_on_response_id,
            },
            ControllerJoinHandle {
                handle: loop_join_handle,
            },
        ))
    }

    /// Connect to the WebSocket server and create a room.
    async fn connect(
        server: String,
        certificate_file: Option<PathBuf>,
    ) -> Result<async_tungstenite::WebSocketStream<impl AsyncRead + AsyncWrite>, Error> {
        debug!("Connecting to {}", server);

        // Connect to the configured server and create a room
        let (ws, _) = if let Some(ref certificate_file) = certificate_file {
            use openssl::ssl::{SslConnector, SslMethod};

            let mut builder = SslConnector::builder(SslMethod::tls())?;
            builder.set_ca_file(certificate_file)?;

            let connector = builder.build().configure()?;

            async_tungstenite::tokio::connect_async_with_tls_connector(&server, Some(connector))
                .await?
        } else {
            async_tungstenite::tokio::connect_async(&server).await?
        };

        Ok(ws)
    }

    /// Send a command to the event loop
    pub async fn run_command(&mut self, command: Command, exit_on_response: bool) {
        let command_id = uuid::Uuid::new_v4();

        if exit_on_response {
            *self.exit_on_response_id.lock().unwrap() = Some(command_id);
        }

        let _ = self
            .websocket_sender
            .send(ControllerMessage {
                id: command_id,
                command,
            })
            .await;
    }

    /// Stops the controller.
    pub fn stop(&mut self) -> Result<(), Error> {
        if let Ok(false) = self.stopped.compare_exchange(
            false,
            true,
            atomic::Ordering::SeqCst,
            atomic::Ordering::SeqCst,
        ) {
            self.event_sender
                .unbounded_send(ControllerEvent::Close)
                .context("Stopping controller")
        } else {
            Ok(())
        }
    }
}
