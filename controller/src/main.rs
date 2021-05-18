// Copyright (C) 2021 Mathieu Duponchelle <mathieu@centricular.com>
//
// Licensed under the MIT license, see the LICENSE file or <http://opensource.org/licenses/MIT>

use anyhow::Error;
use chrono::{DateTime, Utc};
use clap::{AppSettings, Clap};
use std::path::PathBuf;

mod controller;
use controller::Controller;

use rtmp_switcher_controlling::controller::ControllerCommand;

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
    /// List all currently-running channels
    List,
    /// Control individual channels
    Channel {
        #[clap(subcommand)]
        subcmd: ChannelSubCommand,
    },
}

#[derive(Clap, Debug)]
enum ChannelSubCommand {
    /// Start a channel
    Start {
        /// The name of the new channel
        name: String,
        /// The destination URI of the channel
        destination: String,
    },
    /// Stop a channel
    Stop {
        /// The id of an existing channel
        id: uuid::Uuid,
    },
    /// Display information about a channel
    Show {
        /// The id of an existing channel
        id: uuid::Uuid,
    },
    /// Cue a source for playback
    Cue {
        /// The id of an existing channel
        id: uuid::Uuid,
        /// The URI of the source
        uri: String,
        /// When to cue the source
        cue_time: Option<DateTime<Utc>>,
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
            SubCommand::List => ControllerCommand::ListChannels {},
            SubCommand::Channel { subcmd } => match subcmd {
                ChannelSubCommand::Start { name, destination } => {
                    ControllerCommand::StartChannel { name, destination }
                }
                ChannelSubCommand::Stop { id } => ControllerCommand::StopChannel { id },
                ChannelSubCommand::Show { id } => ControllerCommand::GetChannelInfo { id },
                ChannelSubCommand::Cue { id, uri, cue_time } => {
                    ControllerCommand::AddSource { id, uri, cue_time }
                }
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
