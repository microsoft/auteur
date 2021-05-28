// Copyright (C) 2021 Mathieu Duponchelle <mathieu@centricular.com>
//
// Licensed under the MIT license, see the LICENSE file or <http://opensource.org/licenses/MIT>

use chrono::offset::Utc;
use chrono::DateTime;
use serde::{Deserialize, Serialize};

/// Commands to execute on the switcher
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ControllerCommand {
    StartChannel {
        /// Display name
        name: String,
        /// RTMP address
        destination: String,
    },
    StopChannel {
        /// Assigned identifier
        id: uuid::Uuid,
    },
    GetChannelInfo {
        /// Assigned identifier
        id: uuid::Uuid,
    },
    AddSource {
        /// What channel the source should be added to
        id: uuid::Uuid,
        /// URI of the source
        uri: String,
        /// When the source should be cued
        cue_time: DateTime<Utc>,
        /// Until when the source should play back. If None, playback will
        /// continue until either:
        ///
        /// * the underlying media goes EOS
        /// * the source is removed
        /// * a new end time provided with ModifySource
        ///
        /// end_time <= cue_time is considered an error
        end_time: Option<DateTime<Utc>>,
    },
    ModifySource {
        /// The id of the channel the source belongs to
        id: uuid::Uuid,
        /// The ID of the source to modify
        source_id: uuid::Uuid,
        /// The new cue time of the source, None leaves it
        /// unchanged
        cue_time: Option<DateTime<Utc>>,
        /// The new end time of the source, None leaves it
        /// unchanged
        end_time: Option<DateTime<Utc>>,
    },
    /// Remove a source
    RemoveSource {
        /// The id of the channel the source belongs to
        id: uuid::Uuid,
        /// The ID of the source to remove
        source_id: uuid::Uuid,
    },
    /// List all channel IDs
    ListChannels,
}

/// Messages sent from the controller to the switcher.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct ControllerMessage {
    /// Identifier of the command
    pub id: uuid::Uuid,
    /// The command to run
    pub command: ControllerCommand,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SourceStatus {
    Initial,
    Prerolling,
    Playing,
    Stopped,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct SourceInfo {
    pub id: uuid::Uuid,
    pub uri: String,
    pub cue_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub status: SourceStatus,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct ChannelInfo {
    pub id: uuid::Uuid,
    pub name: String,
    pub destination: String,
    pub sources: Vec<SourceInfo>,
}

/// Messages sent from the the server to the controller.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServerCommandResult {
    Error { message: String },
    ChannelList { channels: Vec<uuid::Uuid> },
    ChannelStarted { id: uuid::Uuid },
    ChannelStopped { id: uuid::Uuid },
    ChannelInfo(ChannelInfo),
    SourceAdded { id: uuid::Uuid },
    SourceModified { id: uuid::Uuid },
    SourceRemoved { id: uuid::Uuid },
}

/// Messages sent from the the server to the controller.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct ServerMessage {
    /// Identifier of the command result
    pub id: Option<uuid::Uuid>,
    /// The command result
    pub result: ServerCommandResult,
}
