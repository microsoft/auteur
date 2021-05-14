// Copyright (C) 2021 Mathieu Duponchelle <mathieu@centricular.com>
//
// Licensed under the MIT license, see the LICENSE file or <http://opensource.org/licenses/MIT>

use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "rtmp-switcher",
    about = "Manages RTMP channels through a REST API"
)]
pub struct Config {
    /// Port to use.
    #[structopt(short, long, default_value = "8080")]
    pub port: u16,
    /// Use TLS.
    #[structopt(short = "t", long, requires("certificate-file"), requires("key-file"))]
    pub use_tls: bool,
    /// Certificate public key file.
    #[structopt(short = "c", long)]
    pub certificate_file: Option<PathBuf>,
    /// Certificate private key file.
    #[structopt(short = "k", long)]
    pub key_file: Option<PathBuf>,
}
