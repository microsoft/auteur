//! Schedule gst::Object property updates

use gst::glib::types::Type;
use gst::prelude::*;
use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::collections::HashMap;

use anyhow::{anyhow, Error};
use auteur_controlling::controller::{ControlMode, ControlPoint};
use chrono::{DateTime, Utc};
use tracing::{instrument, trace};

/// Represents a controller for a property
#[derive(Debug)]
pub struct PropertyController {
    /// Unique identifier of the controllee (slot, node)
    pub controllee_id: String,
    /// The controlled object
    obj: gst::Object,
    /// The controlled property
    pub propname: String,
    /// The future control points
    control_points: Option<PriorityQueue<String, Reverse<ControlPoint>>>,
}

impl PropertyController {
    /// Create a property controller
    pub fn new(controllee_id: &str, obj: gst::Object, propname: &str) -> Self {
        Self {
            controllee_id: controllee_id.to_string(),
            obj,
            propname: propname.to_string(),
            control_points: Some(PriorityQueue::new()),
        }
    }

    /// Schedule a control point
    #[instrument(level = "debug", name = "pushing control point", skip(self), fields(controllee_id = %self.controllee_id, propname = %self.propname))]
    pub fn push_control_point(&mut self, point: ControlPoint) {
        self.control_points
            .as_mut()
            .unwrap()
            .push(point.id.to_string(), Reverse(point));
    }

    /// Remove a control point
    #[instrument(level = "debug", name = "removing control point", skip(self), fields(controllee_id = %self.controllee_id, propname = %self.propname))]
    pub fn remove_control_point(&mut self, id: &str) {
        self.control_points.as_mut().unwrap().remove(id);
    }

    /// Retrieves all control points
    #[instrument(level = "debug", name = "getting control points", skip(self), fields(controllee_id = %self.controllee_id, propname = %self.propname))]
    pub fn control_points(&self) -> Vec<ControlPoint> {
        let mut ret: Vec<ControlPoint> = vec![];

        for (_, Reverse(point)) in self.control_points.as_ref().unwrap() {
            ret.push(point.clone());
        }

        ret
    }

    /// Update the value for a controlled property, duration is the
    /// duration elapsed since the last call and will be used to
    /// perform interpolation.
    ///
    /// This function returns whether control points are still pending
    #[instrument(level = "trace", name = "synchronizing controller", skip(self), fields(id = %self.controllee_id, propname = %self.propname))]
    pub fn synchronize(&mut self, now: DateTime<Utc>, duration: Option<gst::ClockTime>) -> bool {
        let mut control_points = self.control_points.take().unwrap();

        if let Some((_id, Reverse(point))) = control_points.peek() {
            let mut do_trace = false;

            let initial = self.obj.property_value(self.propname.as_str());
            if match point.mode {
                ControlMode::Interpolate => {
                    if let Some(duration) = duration {
                        do_trace = true;
                        PropertyController::interpolate_property(
                            &self.obj,
                            now,
                            duration.nseconds(),
                            &self.propname,
                            point,
                        )
                    } else {
                        false
                    }
                }
                ControlMode::Set => {
                    PropertyController::set_property(&self.obj, now, &self.propname, point)
                }
            } {
                do_trace = true;
                control_points.pop().unwrap();
            }

            if do_trace {
                let new = self.obj.property_value(self.propname.as_str());

                trace!(obj = %self.obj.name(), property = %self.propname, "Synchronized controller: {:?} -> {:?}", initial, new);
            }
        }

        let ret = control_points.is_empty();

        self.control_points = Some(control_points);

        ret
    }

    /// Validate a desired value against a GParamSpec
    ///
    /// The desired value must be in the valid range, and of the correct type.
    pub fn validate_value_against_pspec(
        pspec: &gst::glib::ParamSpec,
        value: &serde_json::Value,
    ) -> Result<(), Error> {
        match pspec.value_type() {
            Type::STRING => {
                if !value.is_string() {
                    Err(anyhow!(
                        "expected string value for property {}",
                        pspec.name()
                    ))
                } else {
                    Ok(())
                }
            }
            Type::BOOL => {
                if !value.is_boolean() {
                    Err(anyhow!(
                        "expected boolean value for property {}",
                        pspec.name()
                    ))
                } else {
                    Ok(())
                }
            }
            Type::U_LONG => {
                if let Some(value) = value.as_u64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecULong>().unwrap();

                    if value > pspec.maximum() {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} > {})",
                            pspec.name(),
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} < {})",
                            pspec.name(),
                            value,
                            pspec.minimum()
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected u64 value for property {}", pspec.name()))
                }
            }
            Type::I_LONG => {
                if let Some(value) = value.as_i64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecLong>().unwrap();

                    if value > pspec.maximum() {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} > {})",
                            pspec.name(),
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} < {})",
                            pspec.name(),
                            value,
                            pspec.minimum()
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected i64 value for property {}", pspec.name()))
                }
            }
            Type::U32 => {
                if let Some(value) = value.as_u64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecUInt>().unwrap();

                    if value > pspec.maximum() as u64 {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} > {})",
                            pspec.name(),
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() as u64 {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} < {})",
                            pspec.name(),
                            value,
                            pspec.minimum()
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected u32 value for property {}", pspec.name()))
                }
            }
            Type::I32 => {
                if let Some(value) = value.as_i64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecInt>().unwrap();

                    if value > pspec.maximum() as i64 {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} > {})",
                            pspec.name(),
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() as i64 {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} < {})",
                            pspec.name(),
                            value,
                            pspec.minimum()
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected u64 value for property {}", pspec.name()))
                }
            }
            Type::U64 => {
                if let Some(value) = value.as_u64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecUInt64>().unwrap();

                    if value > pspec.maximum() {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} > {})",
                            pspec.name(),
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} < {})",
                            pspec.name(),
                            value,
                            pspec.minimum()
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected u64 value"))
                }
            }
            Type::I64 => {
                if let Some(value) = value.as_i64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecInt64>().unwrap();

                    if value > pspec.maximum() {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} > {})",
                            pspec.name(),
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} < {})",
                            pspec.name(),
                            value,
                            pspec.minimum()
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected i64 value for property {}", pspec.name()))
                }
            }
            Type::F32 => {
                if let Some(value) = value.as_f64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecFloat>().unwrap();

                    if value > pspec.maximum() as f64 {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} > {})",
                            pspec.name(),
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() as f64 {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} < {})",
                            pspec.name(),
                            value,
                            pspec.minimum()
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected f32 value for property {}", pspec.name()))
                }
            }
            Type::F64 => {
                if let Some(value) = value.as_f64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecDouble>().unwrap();

                    if value > pspec.maximum() {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} > {})",
                            pspec.name(),
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() {
                        return Err(anyhow!(
                            "Invalid value for property {} ({} < {})",
                            pspec.name(),
                            value,
                            pspec.minimum()
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected f64 value for property {}", pspec.name()))
                }
            }
            _ => {
                if let Some(pspec) = pspec.downcast_ref::<gst::glib::ParamSpecEnum>() {
                    if !value.is_string() {
                        Err(anyhow!(
                            "expected string value for property {}",
                            pspec.name()
                        ))
                    } else if pspec
                        .enum_class()
                        .to_value_by_nick(value.as_str().unwrap())
                        .is_none()
                    {
                        Err(anyhow!(
                            "value {} is not valid for property {}",
                            value,
                            pspec.name()
                        ))
                    } else {
                        Ok(())
                    }
                } else {
                    Err(anyhow!(
                        "Cannot control property {}, unsupported type: {:?}",
                        pspec.name(),
                        pspec.value_type()
                    ))
                }
            }
        }
    }

    /// Validate a desired future value for a property
    ///
    /// The property must exist on the object, the desired value
    /// must be in the valid range, and only certain property types
    /// are interpolatable, others such as strings or booleans must
    /// have mode == Set
    pub fn validate_control_point(
        property: &str,
        obj: &gst::glib::Object,
        point: &ControlPoint,
    ) -> Result<(), Error> {
        let pspec = obj
            .find_property(property)
            .ok_or_else(|| anyhow!("{:?} has no property named {}", obj, property))?;

        if !pspec.flags().contains(gst::glib::ParamFlags::WRITABLE) {
            return Err(anyhow!("property {} is not writable", property));
        }

        if point.mode == ControlMode::Interpolate
            && !pspec.flags().contains(gst::glib::ParamFlags::READABLE)
        {
            return Err(anyhow!("property {} is not readable", property));
        }

        PropertyController::validate_value_against_pspec(&pspec, &point.value)?;

        // Additional verification for non-interpolatable types
        match pspec.value_type() {
            Type::STRING => match point.mode {
                ControlMode::Set => Ok(()),
                ControlMode::Interpolate => Err(anyhow!(
                    "Control points for string values must use mode Set"
                )),
            },
            Type::BOOL => match point.mode {
                ControlMode::Set => Ok(()),
                ControlMode::Interpolate => Err(anyhow!(
                    "Control points for boolean values must use mode Set"
                )),
            },
            _ => {
                if pspec.downcast_ref::<gst::glib::ParamSpecEnum>().is_some() {
                    match point.mode {
                        ControlMode::Set => Ok(()),
                        ControlMode::Interpolate => {
                            Err(anyhow!("Control points for enum values must use mode Set"))
                        }
                    }
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Validate a value for a property
    ///
    /// The property must exist on the object, the desired value
    /// must be in the valid range, and of the correct type
    pub fn validate_value(
        property: &str,
        obj: &gst::glib::Object,
        value: &serde_json::Value,
    ) -> Result<(), Error> {
        let pspec = obj
            .find_property(property)
            .ok_or_else(|| anyhow!("{:?} has no property named {}", obj, property))?;

        if !pspec.flags().contains(gst::glib::ParamFlags::WRITABLE) {
            return Err(anyhow!("property {} is not writable", property));
        }

        PropertyController::validate_value_against_pspec(&pspec, value)
    }

    /// Interpolate the value of a property towards the next control point
    fn interpolate_property(
        obj: &gst::Object,
        now: DateTime<Utc>,
        duration: u64,
        property: &str,
        point: &ControlPoint,
    ) -> bool {
        let current = obj.property_value(property);

        let period = if point.time < now {
            duration
        } else {
            (point.time - now).num_nanoseconds().unwrap() as u64 + duration
        };

        let prop_type = obj.property_type(property).unwrap();

        match prop_type {
            Type::I32 => {
                let current: i64 = current.get::<i32>().unwrap() as i64;
                let target = point.value.as_i64().unwrap();

                let step = (target - current as i64)
                    .mul_div_round(duration as i64, period as i64)
                    .unwrap();

                obj.set_property(property, (current + step) as i32);
            }
            Type::U32 => {
                let current: i64 = current.get::<u32>().unwrap() as i64;
                let target = point.value.as_i64().unwrap();

                let step = (target - current)
                    .mul_div_round(duration as i64, period as i64)
                    .unwrap();

                obj.set_property(property, (current + step) as u32);
            }
            Type::I_LONG | Type::I64 => {
                let current: i64 = current.get().unwrap();
                let target = point.value.as_i64().unwrap();

                // Make sure we avoid over / underflow
                if target >= current {
                    let step = (target - current)
                        .mul_div_round(duration as i64, period as i64)
                        .unwrap();

                    obj.set_property(property, (current + step) as i64);
                } else {
                    let step = (current - target)
                        .mul_div_round(duration as i64, period as i64)
                        .unwrap();

                    obj.set_property(property, (current - step) as i64);
                }
            }
            Type::U_LONG | Type::U64 => {
                let current: u64 = current.get().unwrap();
                let target = point.value.as_u64().unwrap();

                // Make sure we avoid over / underflow
                if target >= current {
                    let step = (target - current).mul_div_round(duration, period).unwrap();

                    obj.set_property(property, (current + step) as u64);
                } else {
                    let step = (current - target).mul_div_round(duration, period).unwrap();

                    obj.set_property(property, (current - step) as u64);
                }
            }
            Type::F32 => {
                let current: f64 = current.get::<f32>().unwrap() as f64;
                let target = point.value.as_f64().unwrap();

                let step = (target - current) / period as f64 * duration as f64;

                let new = current + step;

                // Make sure float arithmetics doesn't make us over / undershoot
                let new =
                    if (target >= current && new > target) || (target <= current && new < target) {
                        target
                    } else {
                        new
                    };

                obj.set_property(property, new as f32);
            }
            Type::F64 => {
                let current: f64 = current.get().unwrap();
                let target = point.value.as_f64().unwrap();

                let step = (target - current) / period as f64 * duration as f64;

                let new = current + step;

                // Make sure float arithmetics doesn't make us over / undershoot
                let new =
                    if (target >= current && new > target) || (target <= current && new < target) {
                        target
                    } else {
                        new
                    };

                obj.set_property(property, new);
            }
            _ => unreachable!(),
        }

        period <= duration
    }

    /// Set a property on a GObject from a json value
    ///
    /// No check is performed, use the validate_* functions
    pub fn set_property_from_value(obj: &gst::Object, property: &str, value: &serde_json::Value) {
        let pspec = obj.find_property(property).unwrap();

        match pspec.value_type() {
            Type::STRING => {
                let target = value.as_str().unwrap();

                obj.set_property(property, target);
            }
            Type::BOOL => {
                let target = value.as_bool().unwrap();

                obj.set_property(property, target);
            }
            Type::I32 => {
                let target = value.as_i64().unwrap();

                obj.set_property(property, target as i32);
            }
            Type::U32 => {
                let target = value.as_i64().unwrap();

                obj.set_property(property, target as u32);
            }
            Type::I_LONG | Type::I64 => {
                let target = value.as_i64().unwrap();

                obj.set_property(property, target as i64);
            }
            Type::U_LONG | Type::U64 => {
                let target = value.as_u64().unwrap();

                obj.set_property(property, target as u64);
            }
            Type::F32 => {
                let target = value.as_f64().unwrap();

                obj.set_property(property, target as f32);
            }
            Type::F64 => {
                let target = value.as_f64().unwrap();

                obj.set_property(property, target as f64);
            }
            _ => {
                if pspec.downcast_ref::<gst::glib::ParamSpecEnum>().is_some() {
                    obj.set_property_from_str(property, value.as_str().unwrap());
                } else {
                    unreachable!()
                }
            }
        }
    }

    /// Set the value of a property
    fn set_property(
        obj: &gst::Object,
        now: DateTime<Utc>,
        property: &str,
        point: &ControlPoint,
    ) -> bool {
        if point.time > now {
            return false;
        }

        PropertyController::set_property_from_value(obj, property, &point.value);

        true
    }

    /// Lists all the current values for controllable properties of an object
    pub fn properties(obj: &gst::Object, prefix: &str) -> HashMap<String, serde_json::Value> {
        let mut ret = HashMap::new();

        for pspec in obj.list_properties().iter() {
            if !pspec.flags().contains(gst::glib::ParamFlags::READABLE) {
                continue;
            }

            if pspec.name() == "name" || pspec.name() == "parent" {
                continue;
            }

            if obj.downcast_ref::<gst::Pad>().is_some() && pspec.name() == "direction" {
                continue;
            }

            let prop_value = obj.property_value(pspec.name());

            let value = match pspec.value_type() {
                Type::STRING => prop_value.get::<String>().unwrap().into(),
                Type::BOOL => prop_value.get::<bool>().unwrap().into(),
                Type::I32 => prop_value.get::<i32>().unwrap().into(),
                Type::U32 => prop_value.get::<u32>().unwrap().into(),
                Type::I_LONG | Type::I64 => prop_value.get::<i64>().unwrap().into(),
                Type::U_LONG | Type::U64 => prop_value.get::<u64>().unwrap().into(),
                Type::F32 => prop_value.get::<f32>().unwrap().into(),
                Type::F64 => prop_value.get::<f64>().unwrap().into(),
                _ => {
                    if pspec.downcast_ref::<gst::glib::ParamSpecEnum>().is_some() {
                        prop_value.serialize().unwrap().to_string().into()
                    } else {
                        continue;
                    }
                }
            };

            ret.insert(prefix.to_owned() + pspec.name(), value);
        }

        ret
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::utils::{get_now, make_element};
    use auteur_controlling::controller::{ControlMode, ControlPoint};

    #[test]
    #[should_panic(expected = "has no property named invalid-property")]
    fn test_property_controller_validate_invalid_property() {
        gst::init().unwrap();

        let queue = make_element("queue", None).unwrap();
        let now = get_now();
        let point = ControlPoint {
            id: "test-controller".to_string(),
            time: now,
            value: 0u64.into(),
            mode: ControlMode::Set,
        };

        PropertyController::validate_control_point("invalid-property", queue.upcast_ref(), &point)
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "expected u64 value")]
    fn test_property_controller_validate_invalid_value() {
        gst::init().unwrap();

        let queue = make_element("queue", None).unwrap();
        let now = get_now();
        let point = ControlPoint {
            id: "test-controller".to_string(),
            time: now,
            value: 0f64.into(),
            mode: ControlMode::Set,
        };

        PropertyController::validate_control_point("max-size-time", queue.upcast_ref(), &point)
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "property current-level-time is not writable")]
    fn test_property_controller_validate_non_writable_property() {
        gst::init().unwrap();

        let queue = make_element("queue", None).unwrap();
        let now = get_now();
        let point = ControlPoint {
            id: "test-controller".to_string(),
            time: now,
            value: 0u64.into(),
            mode: ControlMode::Set,
        };

        PropertyController::validate_control_point(
            "current-level-time",
            queue.upcast_ref(),
            &point,
        )
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "(4294967296 > 4294967295)")]
    fn test_property_controller_validate_out_of_range_value() {
        gst::init().unwrap();

        let queue = make_element("queue", None).unwrap();
        let now = get_now();
        let point = ControlPoint {
            id: "test-controller".to_string(),
            time: now,
            value: 4294967296i64.into(),
            mode: ControlMode::Set,
        };

        PropertyController::validate_control_point("max-size-bytes", queue.upcast_ref(), &point)
            .unwrap();
    }

    #[test]
    fn test_property_controller_validate_ok() {
        gst::init().unwrap();

        let queue = make_element("queue", None).unwrap();
        let now = get_now();
        let point = ControlPoint {
            id: "test-controller".to_string(),
            time: now,
            value: 0u64.into(),
            mode: ControlMode::Set,
        };

        PropertyController::validate_control_point("max-size-bytes", queue.upcast_ref(), &point)
            .unwrap();
    }

    #[test]
    fn test_property_controller_set() {
        gst::init().unwrap();

        let queue = make_element("queue", None).unwrap();
        queue.set_property("max-size-bytes", &10u32);
        let now = get_now();
        let point = ControlPoint {
            id: "test-controller".to_string(),
            time: now + chrono::Duration::nanoseconds(2),
            value: 0u64.into(),
            mode: ControlMode::Set,
        };

        PropertyController::validate_control_point("max-size-bytes", queue.upcast_ref(), &point)
            .unwrap();

        let mut controller =
            PropertyController::new("slot-0", queue.clone().upcast(), "max-size-bytes");

        controller.push_control_point(point);

        assert_eq!(queue.property::<u32>("max-size-bytes"), 10);

        assert_eq!(controller.synchronize(now, gst::ClockTime::NONE), false);

        assert_eq!(queue.property::<u32>("max-size-bytes"), 10);

        assert_eq!(
            controller.synchronize(
                now + chrono::Duration::nanoseconds(1),
                Some(gst::ClockTime::from_nseconds(1))
            ),
            false
        );

        assert_eq!(queue.property::<u32>("max-size-bytes"), 10);

        // Control point should be consumed
        assert_eq!(
            controller.synchronize(
                now + chrono::Duration::nanoseconds(2),
                Some(gst::ClockTime::from_nseconds(1))
            ),
            true
        );

        assert!(controller.control_points().is_empty());

        assert_eq!(queue.property::<u32>("max-size-bytes"), 0);
    }

    #[test]
    fn test_property_controller_interpolate() {
        gst::init().unwrap();

        let queue = make_element("queue", None).unwrap();
        queue.set_property("max-size-bytes", &10u32);
        let now = get_now();
        let point = ControlPoint {
            id: "test-controller".to_string(),
            time: now + chrono::Duration::nanoseconds(2),
            value: 0u64.into(),
            mode: ControlMode::Interpolate,
        };

        PropertyController::validate_control_point("max-size-bytes", queue.upcast_ref(), &point)
            .unwrap();

        let mut controller =
            PropertyController::new("slot-0", queue.clone().upcast(), "max-size-bytes");

        controller.push_control_point(point);

        assert_eq!(queue.property::<u32>("max-size-bytes"), 10);

        assert_eq!(controller.synchronize(now, gst::ClockTime::NONE), false);

        assert_eq!(queue.property::<u32>("max-size-bytes"), 10);

        assert_eq!(
            controller.synchronize(
                now + chrono::Duration::nanoseconds(1),
                Some(gst::ClockTime::from_nseconds(1))
            ),
            false
        );

        assert_eq!(queue.property::<u32>("max-size-bytes"), 5);

        // Control point should be consumed
        assert_eq!(
            controller.synchronize(
                now + chrono::Duration::nanoseconds(2),
                Some(gst::ClockTime::from_nseconds(1))
            ),
            true
        );

        assert!(controller.control_points().is_empty());

        assert_eq!(queue.property::<u32>("max-size-bytes"), 0);
    }
}
