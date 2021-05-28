// Copyright (C) 2021 Mathieu Duponchelle <mathieu@centricular.com>
//
// Licensed under the MIT license, see the LICENSE file or <http://opensource.org/licenses/MIT>

mod channel;
mod config;
mod controller;
mod server;
mod utils;

use anyhow::Error;
use structopt::StructOpt;

use config::Config;

use tracing_subscriber::prelude::*;

fn main() -> Result<(), Error> {
    tracing_log::LogTracer::init().expect("Failed to set logger");
    let env_filter = tracing_subscriber::EnvFilter::try_from_env("RTMP_SWITCHER_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_thread_ids(true)
        .with_target(true)
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        );
    let subscriber = tracing_subscriber::Registry::default()
        .with(env_filter)
        .with(fmt_layer);
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");

    let cfg = Config::from_args();

    gst::init()?;

    let mut system = actix_rt::System::new("RTMP switcher");
    system.block_on(server::run(cfg))?;

    Ok(())
}
