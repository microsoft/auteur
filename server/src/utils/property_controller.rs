//! Schedule gst::Object property updates

use gst::glib::types::Type;
use gst::prelude::*;
use priority_queue::PriorityQueue;
use std::cmp::Reverse;

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
    pub fn synchronize(&mut self, now: DateTime<Utc>, duration: gst::ClockTime) -> bool {
        let mut control_points = self.control_points.take().unwrap();

        if let Some((_id, Reverse(point))) = control_points.peek() {
            let mut do_trace = false;

            let initial = self.obj.property(self.propname.as_str()).unwrap();
            if match point.mode {
                ControlMode::Interpolate => match duration {
                    gst::CLOCK_TIME_NONE => false,
                    _ => {
                        do_trace = true;
                        PropertyController::interpolate_property(
                            &self.obj,
                            now,
                            duration.nseconds().unwrap(),
                            &self.propname,
                            point,
                        )
                    }
                },
                ControlMode::Set => {
                    PropertyController::set_property(&self.obj, now, &self.propname, point)
                }
            } {
                do_trace = true;
                control_points.pop().unwrap();
            }

            if do_trace {
                let new = self.obj.property(self.propname.as_str()).unwrap();

                trace!(obj = %self.obj.name(), property = %self.propname, "Synchronized controller: {:?} -> {:?}", initial, new);
            }
        }

        let ret = control_points.is_empty();

        self.control_points = Some(control_points);

        ret
    }

    /// Validate a desired future value for a property
    ///
    /// The property must exist on the object, the desired value
    /// must be in the valid range, and only certain property types
    /// are interpolatable, others such as strings or booleans must
    /// have mode == Set
    pub fn validate(
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

        match pspec.value_type() {
            Type::STRING => {
                if !point.value.is_string() {
                    Err(anyhow!("expected string value"))
                } else {
                    match point.mode {
                        ControlMode::Set => Ok(()),
                        ControlMode::Interpolate => Err(anyhow!(
                            "Control points for string values must use mode Set"
                        )),
                    }
                }
            }
            Type::BOOL => {
                if !point.value.is_boolean() {
                    Err(anyhow!("expected boolean value"))
                } else {
                    match point.mode {
                        ControlMode::Set => Ok(()),
                        ControlMode::Interpolate => Err(anyhow!(
                            "Control points for boolean values must use mode Set"
                        )),
                    }
                }
            }
            Type::U_LONG => {
                if let Some(value) = point.value.as_u64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecULong>().unwrap();

                    if value > pspec.maximum() {
                        return Err(anyhow!(
                            "Invalid value for control point ({} > {})",
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() {
                        return Err(anyhow!(
                            "Invalid value for control point ({} < {})",
                            value,
                            pspec.minimum()
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected u64 value"))
                }
            }
            Type::I_LONG => {
                if let Some(value) = point.value.as_i64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecLong>().unwrap();

                    if value > pspec.maximum() {
                        return Err(anyhow!(
                            "Invalid value for control point ({} > {})",
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() {
                        return Err(anyhow!(
                            "Invalid value for control point ({} < {})",
                            value,
                            pspec.minimum()
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected i64 value"))
                }
            }
            Type::U32 => {
                if let Some(value) = point.value.as_u64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecUInt>().unwrap();

                    if value > pspec.maximum() as u64 {
                        return Err(anyhow!(
                            "Invalid value for control point ({} > {})",
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() as u64 {
                        return Err(anyhow!(
                            "Invalid value for control point ({} < {})",
                            value,
                            pspec.minimum()
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected u32 value"))
                }
            }
            Type::I32 => {
                if let Some(value) = point.value.as_i64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecInt>().unwrap();

                    if value > pspec.maximum() as i64 {
                        return Err(anyhow!(
                            "Invalid value for control point ({} > {})",
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() as i64 {
                        return Err(anyhow!(
                            "Invalid value for control point ({} < {})",
                            value,
                            pspec.minimum()
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected u64 value"))
                }
            }
            Type::U64 => {
                if let Some(value) = point.value.as_u64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecUInt64>().unwrap();

                    if value > pspec.maximum() {
                        return Err(anyhow!(
                            "Invalid value for control point ({} > {})",
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() {
                        return Err(anyhow!(
                            "Invalid value for control point ({} < {})",
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
                if let Some(value) = point.value.as_i64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecInt64>().unwrap();

                    if value > pspec.maximum() {
                        return Err(anyhow!(
                            "Invalid value for control point ({} > {})",
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() {
                        return Err(anyhow!(
                            "Invalid value for control point ({} < {})",
                            value,
                            pspec.minimum()
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected i64 value"))
                }
            }
            Type::F32 => {
                if let Some(value) = point.value.as_f64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecFloat>().unwrap();

                    if value > pspec.maximum() as f64 {
                        return Err(anyhow!(
                            "Invalid value for control point ({} > {})",
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() as f64 {
                        return Err(anyhow!(
                            "Invalid value for control point ({} < {})",
                            value,
                            pspec.minimum()
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected f32 value"))
                }
            }
            Type::F64 => {
                if let Some(value) = point.value.as_f64() {
                    let pspec = pspec.downcast_ref::<gst::glib::ParamSpecDouble>().unwrap();

                    if value > pspec.maximum() {
                        return Err(anyhow!(
                            "Invalid value for control point ({} > {})",
                            value,
                            pspec.maximum()
                        ));
                    }

                    if value < pspec.minimum() {
                        return Err(anyhow!(
                            "Invalid value for control point ({} < {})",
                            value,
                            pspec.minimum()
                        ));
                    }

                    Ok(())
                } else {
                    Err(anyhow!("expected f64 value"))
                }
            }
            _ => Err(anyhow!(
                "Cannot control property with type {:?}",
                pspec.value_type()
            )),
        }
    }

    fn interpolate_property(
        obj: &gst::Object,
        now: DateTime<Utc>,
        duration: u64,
        property: &str,
        point: &ControlPoint,
    ) -> bool {
        let current = obj.property(property).unwrap();

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

                obj.set_property(property, (current + step) as i32).unwrap();
            }
            Type::U32 => {
                let current: i64 = current.get::<u32>().unwrap() as i64;
                let target = point.value.as_i64().unwrap();

                let step = (target - current)
                    .mul_div_round(duration as i64, period as i64)
                    .unwrap();

                obj.set_property(property, (current + step) as u32).unwrap();
            }
            Type::I_LONG | Type::I64 => {
                let current: i64 = current.get().unwrap();
                let target = point.value.as_i64().unwrap();

                // Make sure we avoid over / underflow
                if target >= current {
                    let step = (target - current)
                        .mul_div_round(duration as i64, period as i64)
                        .unwrap();

                    obj.set_property(property, (current + step) as i64).unwrap();
                } else {
                    let step = (current - target)
                        .mul_div_round(duration as i64, period as i64)
                        .unwrap();

                    obj.set_property(property, (current - step) as i64).unwrap();
                }
            }
            Type::U_LONG | Type::U64 => {
                let current: u64 = current.get().unwrap();
                let target = point.value.as_u64().unwrap();

                // Make sure we avoid over / underflow
                if target >= current {
                    let step = (target - current).mul_div_round(duration, period).unwrap();

                    obj.set_property(property, (current + step) as u64).unwrap();
                } else {
                    let step = (current - target).mul_div_round(duration, period).unwrap();

                    obj.set_property(property, (current - step) as u64).unwrap();
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

                obj.set_property(property, new as f32).unwrap();
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

                obj.set_property(property, new).unwrap();
            }
            _ => unreachable!(),
        }

        period <= duration
    }

    fn set_property(
        obj: &gst::Object,
        now: DateTime<Utc>,
        property: &str,
        point: &ControlPoint,
    ) -> bool {
        if point.time > now {
            return false;
        }

        let prop_type = obj.property_type(property).unwrap();

        match prop_type {
            Type::STRING => {
                let target = point.value.as_str().unwrap();

                obj.set_property(property, target).unwrap();
            }
            Type::BOOL => {
                let target = point.value.as_bool().unwrap();

                obj.set_property(property, target).unwrap();
            }
            Type::I32 => {
                let target = point.value.as_i64().unwrap();

                obj.set_property(property, target as i32).unwrap();
            }
            Type::U32 => {
                let target = point.value.as_i64().unwrap();

                obj.set_property(property, target as u32).unwrap();
            }
            Type::I_LONG | Type::I64 => {
                let target = point.value.as_i64().unwrap();

                obj.set_property(property, target as i64).unwrap();
            }
            Type::U_LONG | Type::U64 => {
                let target = point.value.as_u64().unwrap();

                obj.set_property(property, target as u64).unwrap();
            }
            Type::F32 => {
                let target = point.value.as_f64().unwrap();

                obj.set_property(property, target as f32).unwrap();
            }
            Type::F64 => {
                let target = point.value.as_f64().unwrap();

                obj.set_property(property, target as f64).unwrap();
            }
            _ => unreachable!(),
        }

        true
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

        PropertyController::validate("invalid-property", queue.upcast_ref(), &point).unwrap();
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

        PropertyController::validate("max-size-time", queue.upcast_ref(), &point).unwrap();
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

        PropertyController::validate("current-level-time", queue.upcast_ref(), &point).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid value for control point (4294967296 > 4294967295)")]
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

        PropertyController::validate("max-size-bytes", queue.upcast_ref(), &point).unwrap();
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

        PropertyController::validate("max-size-bytes", queue.upcast_ref(), &point).unwrap();
    }

    #[test]
    fn test_property_controller_set() {
        gst::init().unwrap();

        let queue = make_element("queue", None).unwrap();
        queue.set_property("max-size-bytes", &10u32).unwrap();
        let now = get_now();
        let point = ControlPoint {
            id: "test-controller".to_string(),
            time: now + chrono::Duration::nanoseconds(2),
            value: 0u64.into(),
            mode: ControlMode::Set,
        };

        PropertyController::validate("max-size-bytes", queue.upcast_ref(), &point).unwrap();

        let mut controller =
            PropertyController::new("slot-0", queue.clone().upcast(), "max-size-bytes");

        controller.push_control_point(point);

        assert_eq!(
            queue
                .property("max-size-bytes")
                .unwrap()
                .get::<u32>()
                .unwrap(),
            10
        );

        assert_eq!(controller.synchronize(now, gst::CLOCK_TIME_NONE), false);

        assert_eq!(
            queue
                .property("max-size-bytes")
                .unwrap()
                .get::<u32>()
                .unwrap(),
            10
        );

        assert_eq!(
            controller.synchronize(
                now + chrono::Duration::nanoseconds(1),
                gst::ClockTime::from_nseconds(1)
            ),
            false
        );

        assert_eq!(
            queue
                .property("max-size-bytes")
                .unwrap()
                .get::<u32>()
                .unwrap(),
            10
        );

        // Control point should be consumed
        assert_eq!(
            controller.synchronize(
                now + chrono::Duration::nanoseconds(2),
                gst::ClockTime::from_nseconds(1)
            ),
            true
        );

        assert!(controller.control_points().is_empty());

        assert_eq!(
            queue
                .property("max-size-bytes")
                .unwrap()
                .get::<u32>()
                .unwrap(),
            0
        );
    }

    #[test]
    fn test_property_controller_interpolate() {
        gst::init().unwrap();

        let queue = make_element("queue", None).unwrap();
        queue.set_property("max-size-bytes", &10u32).unwrap();
        let now = get_now();
        let point = ControlPoint {
            id: "test-controller".to_string(),
            time: now + chrono::Duration::nanoseconds(2),
            value: 0u64.into(),
            mode: ControlMode::Interpolate,
        };

        PropertyController::validate("max-size-bytes", queue.upcast_ref(), &point).unwrap();

        let mut controller =
            PropertyController::new("slot-0", queue.clone().upcast(), "max-size-bytes");

        controller.push_control_point(point);

        assert_eq!(
            queue
                .property("max-size-bytes")
                .unwrap()
                .get::<u32>()
                .unwrap(),
            10
        );

        assert_eq!(controller.synchronize(now, gst::CLOCK_TIME_NONE), false);

        assert_eq!(
            queue
                .property("max-size-bytes")
                .unwrap()
                .get::<u32>()
                .unwrap(),
            10
        );

        assert_eq!(
            controller.synchronize(
                now + chrono::Duration::nanoseconds(1),
                gst::ClockTime::from_nseconds(1)
            ),
            false
        );

        assert_eq!(
            queue
                .property("max-size-bytes")
                .unwrap()
                .get::<u32>()
                .unwrap(),
            5
        );

        // Control point should be consumed
        assert_eq!(
            controller.synchronize(
                now + chrono::Duration::nanoseconds(2),
                gst::ClockTime::from_nseconds(1)
            ),
            true
        );

        assert!(controller.control_points().is_empty());

        assert_eq!(
            queue
                .property("max-size-bytes")
                .unwrap()
                .get::<u32>()
                .unwrap(),
            0
        );
    }
}