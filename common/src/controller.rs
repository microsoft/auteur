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
    UpdateConfig {
        width: Option<i32>,
        height: Option<i32>,
        sample_rate: Option<i32>,
    },
    SetSlotVolume {
        slot_id: String,
        volume: f64,
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

// Simplistic, will be extended
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct MixerConfig {
    pub width: i32,
    pub height: i32,
    pub sample_rate: i32,
    pub fallback_image: Option<String>,
    pub fallback_timeout: Option<u32>,
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
        config: MixerConfig,
    },
    Connect {
        link_id: String,
        src_id: String,
        sink_id: String,
    },
    Reschedule {
        id: String,
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    },
    Remove {
        id: String,
    },
    Disconnect {
        link_id: String,
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
