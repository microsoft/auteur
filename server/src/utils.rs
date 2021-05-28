use std::collections::HashSet;
use std::mem;
use std::sync::{atomic, Arc, Mutex};

use gst::prelude::*;

use tracing::{debug, error, trace, warn};

#[derive(Debug, Clone)]
pub struct StreamProducer {
    appsink: gst_app::AppSink,
    consumers: Arc<Mutex<StreamConsumers>>,
}

impl PartialEq for StreamProducer {
    fn eq(&self, other: &Self) -> bool {
        self.appsink.eq(&other.appsink)
    }
}

impl Eq for StreamProducer {}

impl StreamProducer {
    pub fn add_consumer(&self, consumer: &gst_app::AppSrc) {
        let mut consumers = self.consumers.lock().unwrap();
        if consumers.consumers.contains(consumer) {
            error!(appsink = %self.appsink.name(), appsrc = %consumer.name(), "Consumer already added");
            return;
        }

        debug!(appsink = %self.appsink.name(), appsrc = %consumer.name(), "Adding consumer");

        consumer.set_format(gst::Format::Time);
        consumer.set_is_live(true);
        consumer.set_handle_segment_change(true);
        consumer.set_property("max-buffers", 0u64).unwrap();
        consumer.set_property("max-bytes", 0u64).unwrap();
        consumer
            .set_property("max-time", 500 * gst::MSECOND)
            .unwrap();
        consumer.set_property_from_str("leaky-type", "downstream");

        // Forward force-keyunit events upstream to the appsink
        let srcpad = consumer.static_pad("src").unwrap();
        let appsink_clone = self.appsink.clone();
        let appsrc = consumer.clone();
        let fku_probe_id = srcpad
            .add_probe(gst::PadProbeType::EVENT_UPSTREAM, move |_pad, info| {
                if let Some(gst::PadProbeData::Event(ref ev)) = info.data {
                    if gst_video::UpstreamForceKeyUnitEvent::parse(ev).is_ok() {
                        trace!(appsink = %appsink_clone.name(), appsrc = %appsrc.name(), "Requesting keyframe");
                        let _ = appsink_clone.send_event(ev.clone());
                    }
                }

                gst::PadProbeReturn::Ok
            })
            .unwrap();

        consumers
            .consumers
            .insert(StreamConsumer::new(consumer, fku_probe_id));
    }

    pub fn remove_consumer(&self, consumer: &gst_app::AppSrc) {
        if self.consumers.lock().unwrap().consumers.remove(consumer) {
            debug!(appsink = %self.appsink.name(), appsrc = %consumer.name(), "Removed consumer");
        } else {
            debug!(appsink = %self.appsink.name(), appsrc = %consumer.name(), "Consumer not found");
        }
    }

    pub fn appsink(&self) -> &gst_app::AppSink {
        &self.appsink
    }
}

impl<'a> From<&'a gst_app::AppSink> for StreamProducer {
    fn from(appsink: &'a gst_app::AppSink) -> Self {
        let consumers = Arc::new(Mutex::new(StreamConsumers {
            current_latency: None,
            latency_updated: false,
            consumers: HashSet::new(),
        }));

        let consumers_clone = consumers.clone();
        let consumers_clone2 = consumers.clone();
        appsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |appsink| {
                    let span = tracing::trace_span!("New sample", appsink = %appsink.name());
                    let _guard = span.enter();

                    let mut consumers = consumers_clone.lock().unwrap();
                    let latency = consumers.current_latency;
                    let latency_updated = mem::replace(&mut consumers.latency_updated, false);
                    let mut requested_keyframe = false;

                    let current_consumers = consumers
                        .consumers
                        .iter()
                        .map(|c| {
                            if let Some(latency) = latency {
                                if c.forwarded_latency
                                    .compare_exchange(
                                        false,
                                        true,
                                        atomic::Ordering::SeqCst,
                                        atomic::Ordering::SeqCst,
                                    )
                                    .is_ok()
                                    || latency_updated
                                {
                                    c.appsrc.set_latency(latency, gst::CLOCK_TIME_NONE);
                                }
                            }

                            if c.first_buffer
                                .compare_exchange(
                                    true,
                                    false,
                                    atomic::Ordering::SeqCst,
                                    atomic::Ordering::SeqCst,
                                )
                                .is_ok()
                            {
                                if !requested_keyframe {
                                    trace!(appsrc = %c.appsrc.name(), "Requesting keyframe for first buffer");
                                    appsink.send_event(
                                        gst_video::UpstreamForceKeyUnitEvent::builder()
                                            .all_headers(true)
                                            .build(),
                                    );
                                    requested_keyframe = true;
                                }
                            }

                            c.appsrc.clone()
                        })
                        .collect::<smallvec::SmallVec<[_; 16]>>();
                    drop(consumers);

                    let sample = match appsink.pull_sample() {
                        Ok(sample) => sample,
                        Err(_err) => {
                            debug!("Failed to pull sample");
                            return Err(gst::FlowError::Flushing);
                        }
                    };

                    //trace!("Appsink pushing sample {:?}", sample);
                    for consumer in current_consumers {
                        if let Err(err) = consumer.push_sample(&sample) {
                            warn!(appsrc = %consumer.name(), "Failed to push sample: {}", err);
                        }
                    }

                    Ok(gst::FlowSuccess::Ok)
                })
                .eos(move |appsink| {
                    let span = tracing::debug_span!("EOS", appsink = %appsink.name());
                    let _guard = span.enter();

                    let current_consumers = consumers_clone2
                        .lock()
                        .unwrap()
                        .consumers
                        .iter()
                        .map(|c| c.appsrc.clone())
                        .collect::<smallvec::SmallVec<[_; 16]>>();

                    for consumer in current_consumers {
                        let _ = consumer.end_of_stream();
                    }
                })
                .build(),
        );

        let consumers_clone = consumers.clone();
        let sinkpad = appsink.static_pad("sink").unwrap();
        sinkpad.add_probe(gst::PadProbeType::EVENT_UPSTREAM, move |pad, info| {
            if let Some(gst::PadProbeData::Event(ref ev)) = info.data {
                use gst::EventView;

                match ev.view() {
                    EventView::Latency(ev) => {
                        if let Some(parent) = pad.parent() {
                            let latency = ev.latency();
                            debug!(appsink = %parent.name(), latency = %latency, "Latency updated");

                            let mut consumers = consumers_clone.lock().unwrap();
                            consumers.current_latency = Some(latency);
                            consumers.latency_updated = true;
                        }
                    }
                    _ => (),
                }
            }
            gst::PadProbeReturn::Ok
        });

        StreamProducer {
            appsink: appsink.clone(),
            consumers,
        }
    }
}

#[derive(Debug)]
struct StreamConsumers {
    current_latency: Option<gst::ClockTime>,
    latency_updated: bool,
    consumers: HashSet<StreamConsumer>,
}

#[derive(Debug)]
struct StreamConsumer {
    appsrc: gst_app::AppSrc,
    fku_probe_id: Option<gst::PadProbeId>,
    forwarded_latency: atomic::AtomicBool,
    first_buffer: atomic::AtomicBool,
}

impl StreamConsumer {
    fn new(appsrc: &gst_app::AppSrc, fku_probe_id: gst::PadProbeId) -> Self {
        StreamConsumer {
            appsrc: appsrc.clone(),
            fku_probe_id: Some(fku_probe_id),
            forwarded_latency: atomic::AtomicBool::new(false),
            first_buffer: atomic::AtomicBool::new(true),
        }
    }
}

impl Drop for StreamConsumer {
    fn drop(&mut self) {
        if let Some(fku_probe_id) = self.fku_probe_id.take() {
            let srcpad = self.appsrc.static_pad("src").unwrap();
            srcpad.remove_probe(fku_probe_id);
        }
    }
}

impl PartialEq for StreamConsumer {
    fn eq(&self, other: &Self) -> bool {
        self.appsrc.eq(&other.appsrc)
    }
}

impl Eq for StreamConsumer {}

impl std::hash::Hash for StreamConsumer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.appsrc, state);
    }
}

impl std::borrow::Borrow<gst_app::AppSrc> for StreamConsumer {
    fn borrow(&self) -> &gst_app::AppSrc {
        &self.appsrc
    }
}

/// WebRTC related signalling messages used in multiple places.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WebRTCMessage {
    Ice {
        candidate: String,
        sdp_mline_index: u32,
    },
    Sdp {
        #[serde(rename = "type")]
        type_: String,
        sdp: String,
    },
}
