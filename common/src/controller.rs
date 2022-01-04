//! Definition of the service protocol
//!
//! WARNING: unstable

use chrono::offset::Utc;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;

fn default_as_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
/// Defines how a property should be controlled
pub enum ControlMode {
    /// The value should be set once once the desired time has been reached
    Set,
    /// The value should be interpolated to be reached upon the desired time
    Interpolate,
}

/// A property control point
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub struct ControlPoint {
    /// The identifier of the control point, unique per property
    /// per node / slot
    pub id: String,
    /// When the value will be reached
    pub time: DateTime<Utc>,
    /// The value that will be reached
    pub value: serde_json::Value,
    /// How the value will be reached
    pub mode: ControlMode,
}

impl Ord for ControlPoint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time.cmp(&other.time)
    }
}

impl PartialOrd for ControlPoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Command variants
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Command {
    /// Create a source
    ///
    /// No slot properties are available.
    CreateSource {
        /// Unique identifier of the source
        id: String,
        /// URI to play back
        uri: String,
        /// Whether the source should produce audio, default true
        #[serde(default = "default_as_true")]
        audio: bool,
        /// Whether the source should produce video, default true
        #[serde(default = "default_as_true")]
        video: bool,
    },
    /// Create a destination
    ///
    /// No slot properties are available.
    CreateDestination {
        /// Unique identifier of the destination
        id: String,
        /// Type of the destination
        family: DestinationFamily,
        /// Whether the destination should consume audio, default true
        #[serde(default = "default_as_true")]
        audio: bool,
        /// Whether the destination should consume video, default true
        #[serde(default = "default_as_true")]
        video: bool,
    },
    /// Create a mixer
    ///
    /// Available slot properties are all those exposed by audiomixer
    /// and compositor pads, accessible through a `audio::` or
    /// `video::` prefix respectively, eg `video::width`.
    CreateMixer {
        /// Unique identifier of the mixer
        id: String,
        /// Initial configuration of the mixer
        ///
        /// Available settings:
        ///
        /// * width, i32, 1 -> 2147483647, default 1920, controllable
        /// * height, i32, 1 -> 2147483647, default 1920, controllable
        /// * sample-rate, i32, 1 -> 2147483647, default 48000
        /// * fallback-image, String, default ""
        /// * fallback-timeout (ms), i32, 1 -> 2147483647, default 500, controllable
        config: Option<HashMap<String, serde_json::Value>>,
        /// Whether the mixer should mix audio, default true
        #[serde(default = "default_as_true")]
        audio: bool,
        /// Whether the mixer should mix video, default true
        #[serde(default = "default_as_true")]
        video: bool,
    },
    /// Connect a producer with a consumer
    Connect {
        /// Unique identifier of the created connection
        link_id: String,
        /// Identifier of an existing producer
        src_id: String,
        /// Identifier of an existing consumer
        sink_id: String,
        /// Whether the audio stream should be connected, default true
        #[serde(default = "default_as_true")]
        audio: bool,
        /// Whether the video stream should be connected, default true
        #[serde(default = "default_as_true")]
        video: bool,
        /// Initial configuration of the consumer slot
        ///
        /// Check out the documentation for the node creation function
        /// for more information on the available consumer slot config keys.
        config: Option<HashMap<String, serde_json::Value>>,
    },
    /// Schedule any node for starting, possibly immediately
    Start {
        /// Unique identifier of the node
        id: String,
        /// When to start the node. Immediate if None
        cue_time: Option<DateTime<Utc>>,
        /// When to stop the node. Never if None
        end_time: Option<DateTime<Utc>>,
    },
    /// Reschedule any node
    Reschedule {
        /// Identifier of an existing node
        id: String,
        /// When to start the node, the current time is left unchanged if None
        cue_time: Option<DateTime<Utc>>,
        /// When to stop the node, the current time is left unchanged if None
        end_time: Option<DateTime<Utc>>,
    },
    /// Remove a node
    Remove {
        /// Identifier of an existing node
        id: String,
    },
    /// Remove a connection between two nodes
    Disconnect {
        /// Identifier of an existing connection
        link_id: String,
    },
    /// Retrieve the info of one or all nodes
    GetInfo {
        /// The id of an existing node, or None, in which case the info
        /// of all nodes in the system will be gathered
        id: Option<String>,
    },
    /// Control a property on a node or slot
    AddControlPoint {
        /// Identifier of an existing node or slot
        controllee_id: String,
        /// Name of the controlled property
        property: String,
        /// The control point that should be added
        control_point: ControlPoint,
    },
    /// Remove a previously-created control point
    RemoveControlPoint {
        /// Unique identifier of the control point
        id: String,
        /// Identifier of an existing node or slot
        controllee_id: String,
        /// Name of the controlled property
        property: String,
    },
}

/// Messages sent from the controller to the server.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct ControllerMessage {
    /// Identifier of the command
    pub id: uuid::Uuid,
    /// The command to run
    pub command: Command,
}

/// The state of a node
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    /// The node is not running yet
    Initial,
    /// The node is preparing
    Starting,
    /// The node is playing
    Started,
    /// The node is stopping
    Stopping,
    /// The node has stopped
    Stopped,
}

/// The available types of destinations
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DestinationFamily {
    /// Stream to a RTMP server
    Rtmp {
        /// The URI of the server
        uri: String,
    },
    /// Stream to a local file. No control is currently offered for determining
    /// the encoding and container format, the topology of the output file will
    /// be an H264 stream and an AAC stream in a MP4 container.
    LocalFile {
        /// The base name of the file(s), eg `/path/to/video`
        base_name: String,
        /// An optional duration after which a new file should be opened.
        /// If provided, the output files will be named `/path/to/video_%05d.mp4,
        /// otherwise the single output file will be named `/path/to/video.mp4`
        max_size_time: Option<u32>,
    },
    /// Play on local devices
    LocalPlayback,
}

/// Source-specific information
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct SourceInfo {
    /// The URI played back by the source
    pub uri: String,
    /// The identifiers of the video consumers of the source
    pub video_consumer_slot_ids: Option<Vec<String>>,
    /// The identifiers of the audio consumers of the source
    pub audio_consumer_slot_ids: Option<Vec<String>>,
    /// When the source was scheduled to start
    pub cue_time: Option<DateTime<Utc>>,
    /// When the source was scheduled to end
    pub end_time: Option<DateTime<Utc>>,
    /// The state of the source
    pub state: State,
}

/// Destination-specific information
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct DestinationInfo {
    /// The type of the destination
    pub family: DestinationFamily,
    /// The identifier of the destination's input audio slot
    pub audio_slot_id: Option<String>,
    /// The identifier of the destination's input video slot
    pub video_slot_id: Option<String>,
    /// When the destination was scheduled to start
    pub cue_time: Option<DateTime<Utc>>,
    /// When the destination was scheduled to end
    pub end_time: Option<DateTime<Utc>>,
    /// The state of the destination
    pub state: State,
}

/// Mixer-slot-specific information
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct MixerSlotInfo {
    /// The volume of the slot
    pub volume: f64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
/// Mixer-specific information
pub struct MixerInfo {
    /// The mixer's input slots
    pub slots: HashMap<String, MixerSlotInfo>,
    /// The identifiers of the video consumers of the mixer
    pub video_consumer_slot_ids: Option<Vec<String>>,
    /// The identifiers of the audio consumers of the mixer
    pub audio_consumer_slot_ids: Option<Vec<String>>,
    /// When the mixer was scheduled to start
    pub cue_time: Option<DateTime<Utc>>,
    /// When the mixer was scheduled to end
    pub end_time: Option<DateTime<Utc>>,
    /// The state of the mixer
    pub state: State,
    /// All the mixer settings
    pub settings: HashMap<String, serde_json::Value>,
    /// All controllers active on the mixer settings
    pub control_points: HashMap<String, Vec<ControlPoint>>,
    /// All the mixer's slot settings
    pub slot_settings: HashMap<String, HashMap<String, serde_json::Value>>,
    /// All controllers active on the mixer's input slots
    pub slot_control_points: HashMap<String, HashMap<String, Vec<ControlPoint>>>,
}

/// Info variants
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeInfo {
    Source(SourceInfo),
    Destination(DestinationInfo),
    Mixer(MixerInfo),
}

/// A map of node-specific information in reply to a GetInfo command
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Info {
    pub nodes: HashMap<String, NodeInfo>,
}

/// Messages sent from the the server to the controller.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CommandResult {
    /// The command resulted in an error
    Error(String),
    /// The command was successful
    Success,
    /// Information about one or all nodes
    Info(Info),
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
