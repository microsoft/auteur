//! Schedule "native" setting updates

use anyhow::{anyhow, Error};
use auteur_controlling::controller::{ControlMode, ControlPoint};
use chrono::{DateTime, Utc};
use gst::prelude::*;
use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::sync::{Arc, Mutex};
use tracing::{instrument, trace};

/// Specifies a setting typology (type, valid range)
#[derive(Debug)]
pub enum SettingSpec {
    /// Integer specification
    I32 { current: i32, min: i32, max: i32 },
    /// String specification
    Str { current: String },
}

/// Represents a (potentially controllable) "native" setting for
/// a node.
#[derive(Debug)]
pub struct Setting {
    /// The name of the setting, for tracing purposes only
    pub name: String,
    /// Whether the setting can be controlled through the control point API
    pub controllable: bool,
    /// The specification of the setting acceptable values
    pub spec: SettingSpec,
}

impl Setting {
    /// The current integer value of the setting
    pub fn as_i32(&self) -> Option<i32> {
        match self.spec {
            SettingSpec::I32 { current, .. } => Some(current),
            _ => None,
        }
    }

    /// The current string value of the setting
    pub fn as_str(&self) -> Option<&str> {
        match self.spec {
            SettingSpec::Str { ref current, .. } => Some(current),
            _ => None,
        }
    }

    pub fn as_value(&self) -> serde_json::Value {
        match self.spec {
            SettingSpec::I32 { current, .. } => current.into(),
            SettingSpec::Str { ref current, .. } => current.clone().into(),
        }
    }
}

/// Represents a controller for a setting
#[derive(Debug)]
pub struct SettingController {
    /// Unique identifier of the controllee (slot, node)
    pub controllee_id: String,
    /// The controlled setting
    pub setting: Arc<Mutex<Setting>>,
    /// The future control points
    control_points: Option<PriorityQueue<String, Reverse<ControlPoint>>>,
}

impl SettingController {
    /// Create a property controller
    pub fn new(controllee_id: &str, setting: Arc<Mutex<Setting>>) -> Self {
        Self {
            controllee_id: controllee_id.to_string(),
            setting,
            control_points: Some(PriorityQueue::new()),
        }
    }

    /// Schedule a control point
    #[instrument(level = "debug", name = "pushing control point", skip(self), fields(controllee_id = %self.controllee_id, setting = %self.setting.lock().unwrap().name))]
    pub fn push_control_point(&mut self, point: ControlPoint) {
        self.control_points
            .as_mut()
            .unwrap()
            .push(point.id.to_string(), Reverse(point));
    }

    /// Remove a control point
    #[instrument(level = "debug", name = "removing control point", skip(self), fields(controllee_id = %self.controllee_id, setting = %self.setting.lock().unwrap().name))]
    pub fn remove_control_point(&mut self, id: &str) {
        self.control_points.as_mut().unwrap().remove(id);
    }

    /// Retrieves all control points
    #[instrument(level = "debug", name = "getting control points", skip(self), fields(controllee_id = %self.controllee_id, setting = %self.setting.lock().unwrap().name))]
    pub fn control_points(&self) -> Vec<ControlPoint> {
        let mut ret: Vec<ControlPoint> = vec![];

        for (_, Reverse(point)) in self.control_points.as_ref().unwrap() {
            ret.push(point.clone());
        }

        ret
    }

    /// Update the value for a controlled setting, duration is the
    /// duration elapsed since the last call and will be used to
    /// perform interpolation.
    ///
    /// This function returns whether control points are still pending
    #[instrument(level = "trace", name = "synchronizing controller", skip(self), fields(id = %self.controllee_id, setting = %self.setting.lock().unwrap().name))]
    pub fn synchronize(&mut self, now: DateTime<Utc>, duration: Option<gst::ClockTime>) -> bool {
        let mut control_points = self.control_points.take().unwrap();
        let mut setting = self.setting.lock().unwrap();

        if let Some((_id, Reverse(point))) = control_points.peek() {
            let mut do_trace = false;

            let initial: serde_json::Value = match setting.spec {
                SettingSpec::I32 { current, .. } => current.into(),
                SettingSpec::Str { ref current } => current.clone().into(),
            };

            if match point.mode {
                ControlMode::Interpolate => {
                    if let Some(duration) = duration {
                        do_trace = true;
                        SettingController::interpolate(
                            &mut setting,
                            now,
                            duration.nseconds(),
                            point,
                        )
                    } else {
                        false
                    }
                }
                ControlMode::Set => SettingController::set(&mut setting, now, point),
            } {
                do_trace = true;
                control_points.pop().unwrap();
            }

            if do_trace {
                let new: serde_json::Value = match setting.spec {
                    SettingSpec::I32 { current, .. } => current.into(),
                    SettingSpec::Str { ref current } => current.clone().into(),
                };

                trace!(setting = %setting.name, "Synchronized setting controller: {:?} -> {:?}", initial, new);
            }
        }

        let ret = control_points.is_empty();

        self.control_points = Some(control_points);

        ret
    }

    /// Validate a desired value against a SettingSpec
    ///
    /// The desired value must be in the valid range, and of the correct type.
    pub fn validate_value_against_setting(
        setting: &Setting,
        value: &serde_json::Value,
    ) -> Result<(), Error> {
        match setting.spec {
            SettingSpec::I32 { min, max, .. } => {
                if let Some(value) = value.as_i64() {
                    if value > max as i64 {
                        return Err(anyhow!(
                            "Invalid value for setting {} ({} > {})",
                            setting.name,
                            value,
                            max
                        ));
                    }

                    if value < min as i64 {
                        return Err(anyhow!(
                            "Invalid value for setting {} ({} < {})",
                            setting.name,
                            value,
                            min
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected i32 value for property {}", setting.name))
                }
            }
            SettingSpec::Str { .. } => {
                if value.is_string() {
                    Ok(())
                } else {
                    Err(anyhow!(
                        "expected string value for property {}",
                        setting.name
                    ))
                }
            }
        }
    }

    /// Validate a desired future value for a setting
    ///
    /// the desired value must be in the valid range, and only certain
    /// setting types are interpolatable, others such as strings or booleans must
    /// have mode == Set
    pub fn validate_control_point(setting: &Setting, point: &ControlPoint) -> Result<(), Error> {
        if !setting.controllable {
            return Err(anyhow!("setting {} is not controllable", setting.name));
        }

        SettingController::validate_value_against_setting(setting, &point.value)
    }

    /// Validate a value for a setting
    ///
    /// The desired value must be in the valid range, and of the correct type
    pub fn validate_value(setting: &Setting, value: &serde_json::Value) -> Result<(), Error> {
        SettingController::validate_value_against_setting(setting, value)
    }

    /// Interpolate the value of a setting towards the next control point
    fn interpolate(
        setting: &mut Setting,
        now: DateTime<Utc>,
        duration: u64,
        point: &ControlPoint,
    ) -> bool {
        let period = if point.time < now {
            duration
        } else {
            (point.time - now).num_nanoseconds().unwrap() as u64 + duration
        };

        match setting.spec {
            SettingSpec::I32 {
                ref mut current, ..
            } => {
                let target = point.value.as_i64().unwrap();

                let step = (target - *current as i64)
                    .mul_div_round(duration as i64, period as i64)
                    .unwrap();

                *current = (*current as i64 + step) as i32;
            }
            SettingSpec::Str { .. } => unreachable!(),
        }

        period <= duration
    }

    /// Set a "native" setting from a json value
    ///
    /// No check is performed, use the validate_* functions
    pub fn set_from_value(setting: &mut Setting, value: &serde_json::Value) {
        match setting.spec {
            SettingSpec::I32 {
                ref mut current, ..
            } => {
                *current = value.as_i64().unwrap() as i32;
            }
            SettingSpec::Str { ref mut current } => {
                *current = value.as_str().unwrap().to_string();
            }
        }
    }

    /// Set the value of a setting
    fn set(setting: &mut Setting, now: DateTime<Utc>, point: &ControlPoint) -> bool {
        if point.time > now {
            return false;
        }

        SettingController::set_from_value(setting, &point.value);

        true
    }
}
