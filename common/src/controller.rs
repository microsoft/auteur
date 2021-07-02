//! Definition of the service protocol
//!
//! WARNING: unstable

use chrono::offset::Utc;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Commands to execute on a source
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceCommand {}

/// Commands to execute on a destination
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DestinationCommand {}

/// Commands to execute on a mixer
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MixerCommand {
    /// Update the output format of the mixer
    UpdateConfig {
        /// Width of the output picture
        width: Option<i32>,
        /// Height of the output picture
        height: Option<i32>,
        /// Sample rate of the output audio
        sample_rate: Option<i32>,
    },
    /// Set the volume of an input stream
    SetSlotVolume {
        /// Unique identifier of the slot
        slot_id: String,
        /// New volume, 0.0 -> 10.0, 1.0 default
        volume: f64,
    },
}

/// Node-specific command variants
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeCommands {
    /// Source-specific commands
    Source(SourceCommand),
    /// Destination-specific commands
    Destination(DestinationCommand),
    /// Mixer-specific commands
    Mixer(MixerCommand),
}

/// Node-specific commands
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct NodeCommand {
    /// Unique identifier of the node
    pub id: String,
    /// The command to execute
    pub command: NodeCommands,
}

// Simplistic, will be extended
/// Configuration of a mixer's output stream
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct MixerConfig {
    /// The width of the output picture
    pub width: i32,
    /// The height of the output picture
    pub height: i32,
    /// The sample rate of the output audio stream
    pub sample_rate: i32,
    /// Whether an image should be displayed as the base plate
    pub fallback_image: Option<String>,
    /// After how long to show the image when no other input stream
    /// is being mixed
    pub fallback_timeout: Option<u32>,
}

/// Generic commands for creating and removing nodes, managing connections
/// and rescheduling
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GraphCommand {
    /// Create a source
    CreateSource {
        /// Unique identifier of the source
        id: String,
        /// URI to play back
        uri: String,
    },
    /// Create a destination
    CreateDestination {
        /// Unique identifier of the destination
        id: String,
        /// Type of the destination
        family: DestinationFamily,
    },
    /// Create a mixer
    CreateMixer {
        /// Unique identifier of the mixer
        id: String,
        /// Initial configuration of the mixer
        config: MixerConfig,
    },
    /// Connect a producer with a consumer
    Connect {
        /// Unique identifier of the created connection
        link_id: String,
        /// Identifier of an existing producer
        src_id: String,
        /// Identifier of an existing consumer
        sink_id: String,
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
}

/// Command variants
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Command {
    Node(NodeCommand),
    Graph(GraphCommand),
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
}

/// Source-specific information
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct SourceInfo {
    /// The URI played back by the source
    pub uri: String,
    /// The identifiers of the consumers of the source
    pub consumer_slot_ids: Vec<String>,
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
    /// The identifier of the destination's input slot
    pub slot_id: Option<String>,
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
    /// The width of the output picture of the mixer
    pub width: i32,
    /// The height of the output picture of the mixer
    pub height: i32,
    /// The sample rate of the output audio stream of the mixer
    pub sample_rate: i32,
    /// The mixer's input slots
    pub slots: HashMap<String, MixerSlotInfo>,
    /// The identifiers of the consumers of the mixer
    pub consumer_slot_ids: Vec<String>,
    /// When the mixer was scheduled to start
    pub cue_time: Option<DateTime<Utc>>,
    /// When the mixer was scheduled to end
    pub end_time: Option<DateTime<Utc>>,
    /// The state of the mixer
    pub state: State,
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
