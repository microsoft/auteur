//! A set of utilities for all nodes to use

use actix::prelude::*;
use anyhow::{anyhow, Error};

use chrono::{DateTime, Utc};

pub mod pipeline_manager;
pub mod property_controller;
pub mod schedulable;
pub mod setting_controller;
pub mod stream_producer;
#[cfg(test)]
pub mod tests;

pub use pipeline_manager::{PipelineManager, StopManagerMessage, WaitForEosMessage};
pub use property_controller::PropertyController;
pub use schedulable::{Schedulable, StateChangeResult, StateMachine};
pub use setting_controller::{Setting, SettingController, SettingSpec};
pub use stream_producer::StreamProducer;

/// Wrapper around `gst::ElementFactory::make` with a better error
/// message
pub fn make_element(element: &str, name: Option<&str>) -> Result<gst::Element, Error> {
    gst::ElementFactory::make(element, name)
        .map_err(|err| anyhow!("Failed to make element {}: {}", element, err.message))
}

/// Sent from [`PipelineManager`] to nodes to signal an error
#[derive(Debug)]
pub struct ErrorMessage(pub String);

impl Message for ErrorMessage {
    type Result = ();
}

#[cfg(not(test))]
/// In normal operation, now is the actual system time
pub fn get_now() -> DateTime<Utc> {
    Utc::now()
}

#[cfg(test)]
/// When testing, we sample a base system time, then base all future
/// observations of the system time on the actix time, which progresses
/// immediately on tokio sleep (eg actix run_later)
///
/// FIXME: does unsafe matter in tests? If so, how can we do this safely?
pub fn get_now() -> DateTime<Utc> {
    static mut UTC_BASE: Option<DateTime<Utc>> = None;
    static mut ACTIX_BASE: Option<actix::clock::Instant> = None;

    let ubase = unsafe {
        match UTC_BASE {
            Some(ubase) => ubase,
            None => {
                let ubase = Utc::now();
                UTC_BASE = Some(ubase);
                ubase
            }
        }
    };

    let abase = unsafe {
        match ACTIX_BASE {
            Some(abase) => abase,
            None => {
                let abase = actix::clock::Instant::now();
                ACTIX_BASE = Some(abase);
                abase
            }
        }
    };

    // This is really quite unfortunate, but Instant::now()
    // is actually *not* monotonic on certain systems.
    //
    // https://github.com/rust-lang/rust/issues/56612 though
    // closed seems related.
    let anow = actix::clock::Instant::now();

    let delta = if anow > abase {
        anow - abase
    } else {
        // FIXME: use std::time::Duration::ZERO when stable
        std::time::Duration::new(0, 0)
    };

    ubase + chrono::Duration::from_std(delta).unwrap()
}
