// Copyright (C) 2021 Mathieu Duponchelle <mathieu@centricular.com>
//
// Licensed under the MIT license, see the LICENSE file or <http://opensource.org/licenses/MIT>

use chrono::offset::Utc;
use chrono::DateTime;
use serde::{Deserialize, Serialize};

/// Messages sent from the controller to the switcher.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ControllerMessage {
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
        /// When the source should be cued, if None the source will be
        /// switched to after the last source cued on the channel is over
        cue_time: Option<DateTime<Utc>>,
    },
    ModifySource {
        /// The ID of the source to modify
        id: uuid::Uuid,
        /// When the source should be cued, if None the source will be
        /// switched to after the last source cued on the channel is over
        cue_time: Option<DateTime<Utc>>,
    },
    /// Immediately switch to a source
    SwitchSource {
        /// The ID of the source to switch to. All previous sources
        /// on that channel are discarded
        id: uuid::Uuid,
    },
    /// Remove a source
    RemoveSource {
        /// The ID of the source to remove
        id: uuid::Uuid,
    },
    /// List all channel IDs
    ListChannels,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct ChannelInfo {
    pub id: uuid::Uuid,
    pub name: String,
    pub destination: String,
}

/// Messages sent from the the server to the controller.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServerMessage {
    Error { message: String },
    ChannelList { channels: Vec<uuid::Uuid> },
    ChannelStarted { id: uuid::Uuid },
    ChannelStopped { id: uuid::Uuid },
    ChannelInfo(ChannelInfo),
}
