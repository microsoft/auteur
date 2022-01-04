use crate::node::{CommandMessage, NodeManager, NodeStatusMessage, RegisterListenerMessage};
use actix::prelude::*;
use anyhow::{anyhow, Error};
use auteur_controlling::controller::{Command, CommandResult, DestinationFamily, NodeInfo, State};
use chrono::{DateTime, Utc};
use futures::channel::oneshot;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use tracing::error;

/// The result once StateListener has tracked progression
#[derive(Debug)]
pub struct StateProgressionResult {
    /// Whether the node went through all the expected states
    pub progressed_as_expected: bool,
    /// Whether the node notified an error
    pub errored_out: bool,
}

/// Sent from tests to [`StateListener`] to wait for the node
/// to have progressed through the expected states
#[derive(Debug)]
pub struct WaitForProgressionMessage;

impl Message for WaitForProgressionMessage {
    type Result = StateProgressionResult;
}

/// Actor that registers itself with NodeManager to track the
/// progression of a node through its states
pub struct StateListener {
    /// Unique id of the tracked node
    id: String,
    /// The expected state progression
    expected_progression: VecDeque<State>,
    /// To signal that we're done tracking progress
    progress_sender: Option<oneshot::Sender<StateProgressionResult>>,
    /// To wait for progress to finish tracking
    progress_receiver: Option<oneshot::Receiver<StateProgressionResult>>,
    /// Track whether the node encountered an error
    errored_out: bool,
}

impl Handler<WaitForProgressionMessage> for StateListener {
    type Result = ResponseFuture<StateProgressionResult>;

    fn handle(
        &mut self,
        _msg: WaitForProgressionMessage,
        _ctx: &mut Context<Self>,
    ) -> Self::Result {
        let progress_receiver = self.progress_receiver.take().unwrap();

        Box::pin(async move { progress_receiver.await.unwrap() })
    }
}

impl StateListener {
    fn new(node_id: &str, expected_progression: VecDeque<State>) -> Self {
        let (progress_sender, progress_receiver) = oneshot::channel::<StateProgressionResult>();
        Self {
            id: node_id.to_string(),
            expected_progression,
            progress_sender: Some(progress_sender),
            progress_receiver: Some(progress_receiver),
            errored_out: false,
        }
    }
}

impl Actor for StateListener {
    type Context = Context<Self>;
}

impl Handler<NodeStatusMessage> for StateListener {
    type Result = ();

    fn handle(&mut self, msg: NodeStatusMessage, _ctx: &mut Self::Context) -> Self::Result {
        match msg {
            NodeStatusMessage::State { id, state } => {
                if id != self.id {
                    return;
                }

                let expected = self.expected_progression.pop_front().unwrap();

                if state == expected {
                    if self.expected_progression.is_empty() {
                        let progress_sender = self.progress_sender.take().unwrap();
                        progress_sender
                            .send(StateProgressionResult {
                                progressed_as_expected: true,
                                errored_out: self.errored_out,
                            })
                            .unwrap();
                    }
                } else {
                    error!(
                        "Unexpected state progression, expected {:?} got {:?}",
                        expected, state
                    );
                    let progress_sender = self.progress_sender.take().unwrap();
                    progress_sender
                        .send(StateProgressionResult {
                            progressed_as_expected: false,
                            errored_out: self.errored_out,
                        })
                        .unwrap();
                }
            }
            NodeStatusMessage::Error { id, .. } => {
                if id != self.id {
                    return;
                }

                self.errored_out = true;
            }
        }
    }
}

/// Create a source
pub async fn create_source(id: &str, uri: &str, video: bool, audio: bool) -> Result<(), Error> {
    let manager = NodeManager::from_registry();

    match manager
        .send(CommandMessage {
            command: Command::CreateSource {
                id: id.to_string(),
                uri: uri.to_string(),
                audio,
                video,
            },
        })
        .await
        .unwrap()
    {
        CommandResult::Success => Ok(()),
        CommandResult::Error(err) => Err(anyhow!(err)),
        CommandResult::Info(_) => unreachable!(),
    }
}

/// Create a local file destination
pub async fn create_local_destination(
    id: &str,
    base_name: &str,
    max_size_time: Option<u32>,
) -> Result<(), Error> {
    let manager = NodeManager::from_registry();

    match manager
        .send(CommandMessage {
            command: Command::CreateDestination {
                id: id.to_string(),
                audio: true,
                video: true,
                family: DestinationFamily::LocalFile {
                    base_name: base_name.to_string(),
                    max_size_time,
                },
            },
        })
        .await
        .unwrap()
    {
        CommandResult::Success => Ok(()),
        CommandResult::Error(err) => Err(anyhow!(err)),
        CommandResult::Info(_) => unreachable!(),
    }
}

/// Start any node
pub async fn start_node(
    id: &str,
    cue_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
) -> Result<(), Error> {
    let manager = NodeManager::from_registry();

    match manager
        .send(CommandMessage {
            command: Command::Start {
                id: id.to_string(),
                cue_time,
                end_time,
            },
        })
        .await
        .unwrap()
    {
        CommandResult::Success => Ok(()),
        CommandResult::Error(err) => Err(anyhow!(err)),
        CommandResult::Info(_) => unreachable!(),
    }
}

/// Reschedule any node
pub async fn reschedule_node(
    id: &str,
    cue_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
) -> Result<(), Error> {
    let manager = NodeManager::from_registry();

    match manager
        .send(CommandMessage {
            command: Command::Reschedule {
                id: id.to_string(),
                cue_time,
                end_time,
            },
        })
        .await
        .unwrap()
    {
        CommandResult::Success => Ok(()),
        CommandResult::Error(err) => Err(anyhow!(err)),
        CommandResult::Info(_) => unreachable!(),
    }
}

/// Get NodeInfo *for an existing node*. Unwraps for convenience
pub async fn node_info_unchecked(id: &str) -> NodeInfo {
    let manager = NodeManager::from_registry();

    if let CommandResult::Info(mut info) = manager
        .send(CommandMessage {
            command: Command::GetInfo {
                id: Some(id.to_string()),
            },
        })
        .await
        .unwrap()
    {
        info.nodes.remove(&id.to_owned()).unwrap()
    } else {
        unreachable!()
    }
}

/// Get the uri of an asset in our test assets directory
pub fn asset_uri(name: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/assets/");
    path.push(&name);

    format!("file://{}", path.to_str().unwrap())
}

/// Register a state listener to track the progress of node's states
pub async fn register_listener(
    node_id: &str,
    id: &str,
    expected_progression: VecDeque<State>,
) -> Addr<StateListener> {
    let manager = NodeManager::from_registry();

    let listener = StateListener::new(node_id, expected_progression);
    let listener_addr = listener.start();

    manager
        .send(RegisterListenerMessage {
            id: id.to_string(),
            recipient: listener_addr.clone().downgrade().recipient(),
        })
        .await
        .unwrap()
        .unwrap();

    listener_addr
}

/// Connect two nodes
pub async fn connect(
    link_id: &str,
    src_id: &str,
    sink_id: &str,
    video: bool,
    audio: bool,
    config: Option<HashMap<String, serde_json::Value>>,
) -> Result<(), Error> {
    let manager = NodeManager::from_registry();

    match manager
        .send(CommandMessage {
            command: Command::Connect {
                link_id: link_id.to_string(),
                src_id: src_id.to_string(),
                sink_id: sink_id.to_string(),
                audio,
                video,
                config,
            },
        })
        .await
        .unwrap()
    {
        CommandResult::Success => Ok(()),
        CommandResult::Error(err) => Err(anyhow!(err)),
        CommandResult::Info(_) => unreachable!(),
    }
}
