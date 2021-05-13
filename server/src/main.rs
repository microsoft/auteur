// Copyright (C) 2021 Mathieu Duponchelle <mathieu@centricular.com>
//
// Licensed under the MIT license, see the LICENSE file or <http://opensource.org/licenses/MIT>

mod config;
mod server;

use anyhow::Error;
use structopt::StructOpt;

use config::Config;

fn main() -> Result<(), Error> {
    let cfg = Config::from_args();

    gst::init()?;

    let env = env_logger::Env::new()
        .filter_or("RTMP_SWITCHER_LOG", "warn")
        .write_style("RTMP_SWITCHER_LOG_STYLE");
    env_logger::init_from_env(env);

    let mut system = actix_rt::System::new("WebRTC Audio Server");
    system.block_on(server::run(cfg))?;

    Ok(())
}
