//! An example client to interface with the Auteur service

use anyhow::Error;
use chrono::{DateTime, Utc};
use clap::{ArgEnum, Parser, Subcommand};
use std::path::PathBuf;

mod controller;
use controller::Controller;

use auteur_controlling::controller::{Command, ControlMode, ControlPoint, DestinationFamily};

#[derive(Parser, Debug)]
#[clap(author = "Mathieu Duponchelle <mathieu@centricular.com>")]
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
#[derive(Subcommand, Debug)]
enum SubCommand {
    /// Create and connect nodes
    Node {
        #[clap(subcommand)]
        subcmd: NodeSubCommand,
    },
}

#[derive(ArgEnum, Debug, Clone)]
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
#[derive(Subcommand, Debug)]
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
        /// Don't connect audio
        #[clap(long)]
        disable_audio: bool,
        /// Don't connect video
        #[clap(long)]
        disable_video: bool,
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
#[derive(Subcommand, Debug)]
enum CreateNodeSubCommand {
    /// Create a new source
    Source {
        /// Unique identifier for the source
        id: String,
        /// The URI of the source
        uri: String,
        /// Don't produce audio
        #[clap(long)]
        disable_audio: bool,
        /// Don't produce video
        #[clap(long)]
        disable_video: bool,
    },
    /// Create a new destination
    Destination {
        /// Don't consume audio
        #[clap(long)]
        disable_audio: bool,
        /// Don't consume video
        #[clap(long)]
        disable_video: bool,
        #[clap(subcommand)]
        subcmd: CreateDestinationSubCommand,
    },
    /// Create a new mixer
    Mixer {
        /// Unique identifier for the mixer
        id: String,
        /// Initial configuration of the mixer
        #[clap(parse(try_from_str = parse_config))]
        config: Vec<(String, serde_json::Value)>,
        /// Don't mix audio
        #[clap(long)]
        disable_audio: bool,
        /// Don't mix video
        #[clap(long)]
        disable_video: bool,
    },
}

/// Create a destination
#[derive(Subcommand, Debug)]
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

/// Client application entry point
fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();

    let env = env_logger::Env::new()
        .filter_or("AUTEUR_CONTROLLER_LOG", "warn")
        .write_style("AUTEUR_CONTROLLER_LOG_STYLE");
    env_logger::init_from_env(env);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let command = match opts.subcmd {
            SubCommand::Node { subcmd } => match subcmd {
                NodeSubCommand::Create { subcmd } => match subcmd {
                    CreateNodeSubCommand::Source {
                        disable_audio,
                        disable_video,
                        id,
                        uri,
                    } => Command::CreateSource {
                        id,
                        uri,
                        audio: !disable_audio,
                        video: !disable_video,
                    },
                    CreateNodeSubCommand::Destination {
                        disable_audio,
                        disable_video,
                        subcmd,
                    } => match subcmd {
                        CreateDestinationSubCommand::Rtmp { id, uri } => {
                            Command::CreateDestination {
                                id,
                                family: DestinationFamily::Rtmp { uri },
                                audio: !disable_audio,
                                video: !disable_video,
                            }
                        }
                        CreateDestinationSubCommand::LocalFile {
                            id,
                            base_name,
                            max_size_time,
                        } => Command::CreateDestination {
                            id,
                            family: DestinationFamily::LocalFile {
                                base_name,
                                max_size_time,
                            },
                            audio: !disable_audio,
                            video: !disable_video,
                        },
                        CreateDestinationSubCommand::LocalPlayback { id } => {
                            Command::CreateDestination {
                                id,
                                family: DestinationFamily::LocalPlayback,
                                audio: !disable_audio,
                                video: !disable_video,
                            }
                        }
                    },
                    CreateNodeSubCommand::Mixer {
                        id,
                        config,
                        disable_audio,
                        disable_video,
                    } => Command::CreateMixer {
                        id,
                        config: Some(config.into_iter().collect()),
                        audio: !disable_audio,
                        video: !disable_video,
                    },
                },
                NodeSubCommand::Connect {
                    link_id,
                    src_id,
                    sink_id,
                    disable_audio,
                    disable_video,
                    config,
                } => Command::Connect {
                    link_id,
                    src_id,
                    sink_id,
                    audio: !disable_audio,
                    video: !disable_video,
                    config: Some(config.into_iter().collect()),
                },
                NodeSubCommand::Disconnect { link_id } => Command::Disconnect { link_id },
                NodeSubCommand::Start {
                    id,
                    cue_time,
                    end_time,
                } => Command::Start {
                    id,
                    cue_time,
                    end_time,
                },
                NodeSubCommand::Reschedule {
                    id,
                    cue_time,
                    end_time,
                } => Command::Reschedule {
                    id,
                    cue_time,
                    end_time,
                },
                NodeSubCommand::Remove { id } => Command::Remove { id },
                NodeSubCommand::GetInfo { id } => Command::GetInfo { id },
                NodeSubCommand::AddControlPoint {
                    id,
                    controllee_id,
                    property,
                    time,
                    value,
                    mode,
                } => Command::AddControlPoint {
                    controllee_id,
                    property,
                    control_point: ControlPoint {
                        id,
                        time,
                        value,
                        mode: mode.into(),
                    },
                },
                NodeSubCommand::RemoveControlPoint {
                    id,
                    controllee_id,
                    property,
                } => Command::RemoveControlPoint {
                    id,
                    controllee_id,
                    property,
                },
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
