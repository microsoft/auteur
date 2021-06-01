// Copyright (C) 2021 Mathieu Duponchelle <mathieu@centricular.com>
//
// Licensed under the MIT license, see the LICENSE file or <http://opensource.org/licenses/MIT>

use chrono::offset::Utc;
use chrono::DateTime;
use serde::{Deserialize, Serialize};

/// Commands to execute on a source
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceCommand {
    Play {
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    },
}

/// Commands to execute on a destination
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DestinationCommand {
    Start {
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    },
}

/// Commands to execute on a mixer
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MixerCommand {
    Start {
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeCommands {
    Source(SourceCommand),
    Destination(DestinationCommand),
    Mixer(MixerCommand),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct NodeCommand {
    pub id: String,
    pub command: NodeCommands,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GraphCommand {
    CreateSource {
        id: String,
        uri: String,
    },
    CreateDestination {
        id: String,
        family: DestinationFamily,
    },
    CreateMixer {
        id: String,
    },
    Connect {
        link_id: String,
        src_id: String,
        sink_id: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Command {
    Node(NodeCommand),
    Graph(GraphCommand),
}

/// Messages sent from the controller to the switcher.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct ControllerMessage {
    /// Identifier of the command
    pub id: uuid::Uuid,
    /// The command to run
    pub command: Command,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SourceStatus {
    Initial,
    Prerolling,
    Playing,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DestinationStatus {
    Initial,
    Streaming,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MixerStatus {
    Initial,
    Mixing,
    Stopped,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DestinationFamily {
    RTMP { uri: String },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct SourceInfo {
    pub id: String,
    pub uri: String,
    pub cue_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub status: SourceStatus,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct DestinationInfo {
    pub id: String,
    pub family: DestinationFamily,
    pub cue_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub status: DestinationStatus,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct ChannelInfo {
    pub id: String,
    pub name: String,
    pub sources: Vec<SourceInfo>,
    pub destinations: Vec<DestinationInfo>,
}

/// Messages sent from the the server to the controller.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CommandResult {
    Error { message: String },
    Success,
}

/// Messages sent from the the server to the controller.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct ServerMessage {
    /// Identifier of the command result
    pub id: Option<uuid::Uuid>,
    /// The command result
    pub result: CommandResult,
}
