// Copyright (C) 2021 Mathieu Duponchelle <mathieu@centricular.com>
//
// Licensed under the MIT license, see the LICENSE file or <http://opensource.org/licenses/MIT>

use clap::{AppSettings, Clap};
use std::path::PathBuf;

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
    },
    /// Stop a channel
    Stop {
        /// The id of an existing channel
        id: String,
    },
    /// Display information about a channel
    Show {
        /// The id of an existing channel
        id: String,
    },
}

fn main() {
    let opts: Opts = Opts::parse();

    eprintln!("Opts: {:?}", opts);
}
