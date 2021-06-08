// Copyright (C) 2021 Mathieu Duponchelle <mathieu@centricular.com>
//
// Licensed under the MIT license, see the LICENSE file or <http://opensource.org/licenses/MIT>

mod config;
mod controller;
mod destination;
mod mixer;
mod node;
mod server;
mod source;
mod utils;

use anyhow::{anyhow, Error};
use structopt::StructOpt;

use config::Config;

use tracing_subscriber::prelude::*;

use std::fs;

fn main() -> Result<(), Error> {
    let cfg = Config::from_args();

    tracing_log::LogTracer::init().expect("Failed to set logger");
    let env_filter = tracing_subscriber::EnvFilter::try_from_env("RTMP_SWITCHER_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));

    let (writer, _guard) = {
        if let Some(ref path) = cfg.log_path {
            let path = fs::canonicalize(&path)
                .map_err(|err| anyhow!("Invalid log path: {}", err.to_string()))?;

            if !path.is_dir() {
                Err(anyhow!("Log path is not a directory: {:?}", path))?;
            }

            let file_appender = tracing_appender::rolling::never(&path, "rtmp-switcher.log");
            let (writer, guard) = tracing_appender::non_blocking(file_appender);

            (writer, guard)
        } else {
            let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());

            (writer, guard)
        }
    };

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_thread_ids(true)
        .with_target(true)
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_writer(writer);

    let subscriber = tracing_subscriber::Registry::default()
        .with(env_filter)
        .with(fmt_layer);
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");

    gst::init()?;

    let system = actix_rt::System::new();
    system.block_on(server::run(cfg))?;

    Ok(())
}
