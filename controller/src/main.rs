// Copyright (C) 2021 Mathieu Duponchelle <mathieu@centricular.com>
//
// Licensed under the MIT license, see the LICENSE file or <http://opensource.org/licenses/MIT>

use anyhow::Error;
use chrono::{DateTime, Utc};
use clap::{AppSettings, Clap};
use std::path::PathBuf;

mod controller;
use controller::Controller;

use rtmp_switcher_controlling::controller::{
    Command, DestinationCommand, DestinationFamily, GraphCommand, MixerCommand, NodeCommand,
    NodeCommands, SourceCommand, StreamConfig,
};

#[derive(Clap, Debug)]
#[clap(author = "Mathieu Duponchelle <mathieu@centricular.com>")]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    /// Address of the rtmp switcher, e.g. https://localhost:8080
    server: String,
    /// TLS Certificate chain file.
    pub certificate_file: Option<PathBuf>,

    #[clap(subcommand)]
    subcmd: SubCommand,
}

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
    },
}

#[derive(Clap, Debug)]
enum CreateNodeSubCommand {
    /// Create a new source
    Source {
        /// Unique identifier for the source
        id: String,
        /// URI of the source
        uri: String,
    },
    /// Create a new destination
    Destination {
        /// Unique identifier for the destination
        id: String,
        /// The URI of the destination
        uri: String,
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
    },
}

#[derive(Clap, Debug)]
enum SourceSubCommand {
    /// Cue a source for playback
    Play {
        /// The id of an existing source
        id: String,
        /// When to cue the source, None is immediate
        cue_time: Option<DateTime<Utc>>,
        /// When to stop the source, None is never
        end_time: Option<DateTime<Utc>>,
    },
}

#[derive(Clap, Debug)]
enum DestinationSubCommand {
    /// Cue a destination for streaming
    Start {
        /// The id of an existing destination
        id: String,
        /// When to cue the destination, None is immediate
        cue_time: Option<DateTime<Utc>>,
        /// When to stop the destination, None is never
        end_time: Option<DateTime<Utc>>,
    },
}

#[derive(Clap, Debug)]
enum MixerSubCommand {
    /// Cue a mixer for .. mixing
    Start {
        /// The id of an existing mixer
        id: String,
        /// When to cue the mixer, None is immediate
        cue_time: Option<DateTime<Utc>>,
        /// When to stop the mixer, None is never
        end_time: Option<DateTime<Utc>>,
    },
}

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();

    let env = env_logger::Env::new()
        .filter_or("RTMP_SWITCHER_CONTROLLER_LOG", "warn")
        .write_style("RTMP_SWITCHER_CONTROLLER_LOG_STYLE");
    env_logger::init_from_env(env);

    let mut runtime = tokio::runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let (mut controller, join_handle) =
            Controller::new(opts.server, opts.certificate_file).await?;

        let command = match opts.subcmd {
            SubCommand::Node { subcmd } => match subcmd {
                NodeSubCommand::Create { subcmd } => match subcmd {
                    CreateNodeSubCommand::Source { id, uri } => {
                        Command::Graph(GraphCommand::CreateSource { id, uri })
                    }
                    CreateNodeSubCommand::Destination { id, uri } => {
                        Command::Graph(GraphCommand::CreateDestination {
                            id,
                            family: DestinationFamily::RTMP { uri },
                        })
                    }
                    CreateNodeSubCommand::Mixer {
                        id,
                        width,
                        height,
                        sample_rate,
                    } => Command::Graph(GraphCommand::CreateMixer {
                        id,
                        config: StreamConfig {
                            width,
                            height,
                            sample_rate,
                        },
                    }),
                },
                NodeSubCommand::Connect {
                    link_id,
                    src_id,
                    sink_id,
                } => Command::Graph(GraphCommand::Connect {
                    link_id,
                    src_id,
                    sink_id,
                }),
            },
            SubCommand::Source { subcmd } => match subcmd {
                SourceSubCommand::Play {
                    id,
                    cue_time,
                    end_time,
                } => Command::Node(NodeCommand {
                    id,
                    command: NodeCommands::Source(SourceCommand::Play { cue_time, end_time }),
                }),
            },
            SubCommand::Destination { subcmd } => match subcmd {
                DestinationSubCommand::Start {
                    id,
                    cue_time,
                    end_time,
                } => Command::Node(NodeCommand {
                    id,
                    command: NodeCommands::Destination(DestinationCommand::Start {
                        cue_time,
                        end_time,
                    }),
                }),
            },
            SubCommand::Mixer { subcmd } => match subcmd {
                MixerSubCommand::Start {
                    id,
                    cue_time,
                    end_time,
                } => Command::Node(NodeCommand {
                    id,
                    command: NodeCommands::Mixer(MixerCommand::Start { cue_time, end_time }),
                }),
            },
        };

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
