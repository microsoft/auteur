//! An example client to interface with the Auteur service

use anyhow::Error;
use chrono::{DateTime, Utc};
use clap::{AppSettings, Clap};
use std::path::PathBuf;

mod controller;
use controller::Controller;

use auteur_controlling::controller::{
    Command, ControlMode, ControlPoint, DestinationFamily, GraphCommand, MixerCommand, MixerConfig,
    NodeCommand, NodeCommands,
};

#[derive(Clap, Debug)]
#[clap(author = "Mathieu Duponchelle <mathieu@centricular.com>")]
#[clap(setting = AppSettings::ColoredHelp)]
/// Top-level options
struct Opts {
    /// Address of the Auteur server, e.g. https://localhost:8080
    server: String,
    /// TLS Certificate chain file.
    pub certificate_file: Option<PathBuf>,

    #[clap(subcommand)]
    subcmd: SubCommand,
}

/// Top-level subcommands
#[derive(Clap, Debug)]
enum SubCommand {
    /// Create and connect nodes
    Node {
        #[clap(subcommand)]
        subcmd: NodeSubCommand,
    },
    /// Control sources
    Source {
        #[clap(subcommand)]
        subcmd: SourceSubCommand,
    },
    /// Control destinations
    Destination {
        #[clap(subcommand)]
        subcmd: DestinationSubCommand,
    },
    /// Control mixers
    Mixer {
        #[clap(subcommand)]
        subcmd: MixerSubCommand,
    },
}

#[derive(Clap, Debug)]
enum ArgControlMode {
    Interpolate,
    Set,
}

impl From<ArgControlMode> for ControlMode {
    fn from(other: ArgControlMode) -> ControlMode {
        match other {
            ArgControlMode::Interpolate => ControlMode::Interpolate,
            ArgControlMode::Set => ControlMode::Set,
        }
    }
}

impl From<ControlMode> for ArgControlMode {
    fn from(other: ControlMode) -> ArgControlMode {
        match other {
            ControlMode::Interpolate => ArgControlMode::Interpolate,
            ControlMode::Set => ArgControlMode::Set,
        }
    }
}

/// Parse a single key-value pair
fn parse_config(
    s: &str,
) -> Result<(String, serde_json::Value), Box<dyn std::error::Error + Send + Sync + 'static>>
where
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{}`", s))?;

    let res: serde_json::Value = serde_json::from_str(&s[pos + 1..])?;

    Ok((s[..pos].parse()?, res))
}

/// Create and connect nodes
#[derive(Clap, Debug)]
enum NodeSubCommand {
    /// Create a new node
    Create {
        #[clap(subcommand)]
        subcmd: CreateNodeSubCommand,
    },
    /// Connect two existing nodes
    Connect {
        /// The id of the link
        link_id: String,
        /// The id of an existing producer node
        src_id: String,
        /// The id of an existing consumer node
        sink_id: String,
        /// Initial configuration of the consumer slot
        #[clap(parse(try_from_str = parse_config))]
        config: Vec<(String, serde_json::Value)>,
    },
    /// Remove an existing link
    Disconnect {
        /// The id of the link
        link_id: String,
    },
    /// Cue a node for playback
    Start {
        /// The id of an existing node
        id: String,
        /// When to cue the node, None is immediate
        #[clap(long)]
        cue_time: Option<DateTime<Utc>>,
        /// When to stop the node, None is never
        #[clap(long)]
        end_time: Option<DateTime<Utc>>,
    },
    /// Reschedule any node
    Reschedule {
        /// The id of an existing node
        id: String,
        /// The new cue time. If not specified, left unchanged
        #[clap(long)]
        cue_time: Option<DateTime<Utc>>,
        /// The new end time. If not specified, left unchanged
        #[clap(long)]
        end_time: Option<DateTime<Utc>>,
    },
    /// Remove an existing node
    Remove {
        /// The id of the node
        id: String,
    },
    /// Retrieve the info of all nodes or a specific node
    GetInfo {
        /// The id of the node, if not specified, all nodes
        id: Option<String>,
    },
    /// Control properties over time for a node or slot
    AddControlPoint {
        /// The id of the control point
        id: String,
        /// The id of the controllee
        controllee_id: String,
        /// Name of the controlled property
        property: String,
        /// Time of the control point
        time: DateTime<Utc>,
        /// Desired value of the controlled property
        value: serde_json::Value,
        /// How to apply the control point
        #[clap(arg_enum)]
        mode: ArgControlMode,
    },
    /// Remove a previously set control point
    RemoveControlPoint {
        /// The id of the control point
        id: String,
        /// The id of the controllee
        controllee_id: String,
        /// Name of the controlled property
        property: String,
    },
}

/// Node-specific creation commands
#[derive(Clap, Debug)]
enum CreateNodeSubCommand {
    /// Create a new source
    Source {
        /// Unique identifier for the source
        id: String,
        /// The URI of the source
        uri: String,
    },
    /// Create a new destination
    Destination {
        #[clap(subcommand)]
        subcmd: CreateDestinationSubCommand,
    },
    /// Create a new mixer
    Mixer {
        /// Unique identifier for the mixer
        id: String,
        /// Width of the output picture
        width: i32,
        /// Height of the output picture
        height: i32,
        /// sample rate of the output audio
        sample_rate: i32,
        /// local fallback image path
        #[clap(long)]
        fallback_image: Option<String>,
        /// local fallback image timeout, milliseconds
        #[clap(long)]
        fallback_timeout: Option<u32>,
    },
}

/// Create a destination
#[derive(Clap, Debug)]
enum CreateDestinationSubCommand {
    /// Create a new RTMP destination
    Rtmp {
        /// Unique identifier for the destination
        id: String,
        /// RTMP URI
        uri: String,
    },
    /// Create a new local file destination
    LocalFile {
        /// Unique identifier for the destination
        id: String,
        /// base path, extension and potentially %05d will get appended
        /// on the other end (the latter if max_size_time is set)
        base_name: String,
        /// If set, the destination will split up the stream in multiple
        /// files. milliseconds
        #[clap(long)]
        max_size_time: Option<u32>,
    },
    LocalPlayback {
        /// Unique identifier for the destination
        id: String,
    },
}

/// Source-specific commands
#[derive(Clap, Debug)]
enum SourceSubCommand {}

/// Destination-specific commands
#[derive(Clap, Debug)]
enum DestinationSubCommand {}

/// Mixer-specific commands
#[derive(Clap, Debug)]
enum MixerSubCommand {
    /// Update resolution and / or sample rate
    Update {
        /// The id of an existing mixer
        id: String,
        /// The new width
        #[clap(long)]
        width: Option<i32>,
        /// The new height
        #[clap(long)]
        height: Option<i32>,
        /// The new sample rate
        #[clap(long)]
        sample_rate: Option<i32>,
    },
    /// Set volume of an input slot
    SetSlotVolume {
        /// The id of an existing mixer
        id: String,
        /// The id of an existing slot
        slot_id: String,
        /// The new volume, 0-10, default 1
        volume: f64,
    },
}

/// Client application entry point
fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();

    let env = env_logger::Env::new()
        .filter_or("AUTEUR_CONTROLLER_LOG", "warn")
        .write_style("AUTEUR_CONTROLLER_LOG_STYLE");
    env_logger::init_from_env(env);

    let mut runtime = tokio::runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let command = match opts.subcmd {
            SubCommand::Node { subcmd } => match subcmd {
                NodeSubCommand::Create { subcmd } => match subcmd {
                    CreateNodeSubCommand::Source { id, uri } => {
                        Command::Graph(GraphCommand::CreateSource { id, uri })
                    }
                    CreateNodeSubCommand::Destination { subcmd } => match subcmd {
                        CreateDestinationSubCommand::Rtmp { id, uri } => {
                            Command::Graph(GraphCommand::CreateDestination {
                                id,
                                family: DestinationFamily::Rtmp { uri },
                            })
                        }
                        CreateDestinationSubCommand::LocalFile {
                            id,
                            base_name,
                            max_size_time,
                        } => Command::Graph(GraphCommand::CreateDestination {
                            id,
                            family: DestinationFamily::LocalFile {
                                base_name,
                                max_size_time,
                            },
                        }),
                        CreateDestinationSubCommand::LocalPlayback { id } => {
                            Command::Graph(GraphCommand::CreateDestination {
                                id,
                                family: DestinationFamily::LocalPlayback,
                            })
                        }
                    },
                    CreateNodeSubCommand::Mixer {
                        id,
                        width,
                        height,
                        sample_rate,
                        fallback_image,
                        fallback_timeout,
                    } => Command::Graph(GraphCommand::CreateMixer {
                        id,
                        config: MixerConfig {
                            width,
                            height,
                            sample_rate,
                            fallback_image,
                            fallback_timeout,
                        },
                    }),
                },
                NodeSubCommand::Connect {
                    link_id,
                    src_id,
                    sink_id,
                    config,
                } => Command::Graph(GraphCommand::Connect {
                    link_id,
                    src_id,
                    sink_id,
                    config: Some(config.into_iter().collect()),
                }),
                NodeSubCommand::Disconnect { link_id } => {
                    Command::Graph(GraphCommand::Disconnect { link_id })
                }
                NodeSubCommand::Start {
                    id,
                    cue_time,
                    end_time,
                } => Command::Graph(GraphCommand::Start {
                    id,
                    cue_time,
                    end_time,
                }),
                NodeSubCommand::Reschedule {
                    id,
                    cue_time,
                    end_time,
                } => Command::Graph(GraphCommand::Reschedule {
                    id,
                    cue_time,
                    end_time,
                }),
                NodeSubCommand::Remove { id } => Command::Graph(GraphCommand::Remove { id }),
                NodeSubCommand::GetInfo { id } => Command::Graph(GraphCommand::GetInfo { id }),
                NodeSubCommand::AddControlPoint {
                    id,
                    controllee_id,
                    property,
                    time,
                    value,
                    mode,
                } => Command::Graph(GraphCommand::AddControlPoint {
                    controllee_id,
                    property,
                    control_point: ControlPoint {
                        id,
                        time,
                        value,
                        mode: mode.into(),
                    },
                }),
                NodeSubCommand::RemoveControlPoint {
                    id,
                    controllee_id,
                    property,
                } => Command::Graph(GraphCommand::RemoveControlPoint {
                    id,
                    controllee_id,
                    property,
                }),
            },
            SubCommand::Source { subcmd } => match subcmd {},
            SubCommand::Destination { subcmd } => match subcmd {},
            SubCommand::Mixer { subcmd } => match subcmd {
                MixerSubCommand::Update {
                    id,
                    width,
                    height,
                    sample_rate,
                } => Command::Node(NodeCommand {
                    id,
                    command: NodeCommands::Mixer(MixerCommand::UpdateConfig {
                        width,
                        height,
                        sample_rate,
                    }),
                }),
                MixerSubCommand::SetSlotVolume {
                    id,
                    slot_id,
                    volume,
                } => Command::Node(NodeCommand {
                    id,
                    command: NodeCommands::Mixer(MixerCommand::SetSlotVolume { slot_id, volume }),
                }),
            },
        };

        let (mut controller, join_handle) =
            Controller::new(opts.server, opts.certificate_file).await?;

        controller.run_command(command, true).await;

        // Stop cleanly on ctrl+c
        let mut controller_clone = controller.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            let _ = controller_clone.stop();
        });

        join_handle.await
    })
}
