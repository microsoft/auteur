//! A set of utilities for all nodes to use

use std::collections::HashMap;
use std::mem;
use std::sync::{atomic, Arc, Mutex};

use actix::prelude::*;
use actix::WeakRecipient;
use futures::prelude::*;
use gst::prelude::*;

use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use futures::channel::oneshot;
use tracing::{debug, error, instrument, trace, warn};

/// The interface for transporting media data from one node
/// to another.
///
/// A producer is essentially a GStreamer `appsink` whose output
/// is sent to a set of consumers, who are essentially `appsrc` wrappers
#[derive(Debug, Clone)]
pub struct StreamProducer {
    /// The appsink to dispatch data for
    appsink: gst_app::AppSink,
    /// The consumers to dispatch data to
    consumers: Arc<Mutex<StreamConsumers>>,
}

impl PartialEq for StreamProducer {
    fn eq(&self, other: &Self) -> bool {
        self.appsink.eq(&other.appsink)
    }
}

impl Eq for StreamProducer {}

impl StreamProducer {
    /// Add an appsrc to dispatch data to
    pub fn add_consumer(&self, consumer: &gst_app::AppSrc, consumer_id: &str) {
        let mut consumers = self.consumers.lock().unwrap();
        if consumers.consumers.get(consumer_id).is_some() {
            error!(appsink = %self.appsink.name(), appsrc = %consumer.name(), "Consumer already added");
            return;
        }

        debug!(appsink = %self.appsink.name(), appsrc = %consumer.name(), "Adding consumer");

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

        consumers.consumers.insert(
            consumer_id.to_string(),
            StreamConsumer::new(consumer, fku_probe_id, consumer_id),
        );
    }

    /// Remove a consumer appsrc by id
    pub fn remove_consumer(&self, consumer_id: &str) {
        if let Some(consumer) = self.consumers.lock().unwrap().consumers.remove(consumer_id) {
            debug!(appsink = %self.appsink.name(), appsrc = %consumer.appsrc.name(), "Removed consumer");
        } else {
            debug!(appsink = %self.appsink.name(), consumer_id = %consumer_id, "Consumer not found");
        }
    }

    /// Stop discarding data samples and start forwarding them to the consumers.
    ///
    /// This is useful for example for prerolling live sources.
    pub fn forward(&self) {
        self.consumers.lock().unwrap().discard = false;
    }

    /// Get the GStreamer `appsink` wrapped by this producer
    pub fn appsink(&self) -> &gst_app::AppSink {
        &self.appsink
    }

    /// Get the unique identifiers of all the consumers currently connected
    /// to this producer
    ///
    /// This is useful for disconnecting those automatically when the parent node
    /// stops
    pub fn get_consumer_ids(&self) -> Vec<String> {
        self.consumers
            .lock()
            .unwrap()
            .consumers
            .keys()
            .map(|id| id.to_string())
            .collect()
    }
}

impl<'a> From<&'a gst_app::AppSink> for StreamProducer {
    fn from(appsink: &'a gst_app::AppSink) -> Self {
        let consumers = Arc::new(Mutex::new(StreamConsumers {
            current_latency: None,
            latency_updated: false,
            consumers: HashMap::new(),
            discard: true,
        }));

        let consumers_clone = consumers.clone();
        let consumers_clone2 = consumers.clone();
        appsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |appsink| {
                    let mut consumers = consumers_clone.lock().unwrap();

                    let sample = match appsink.pull_sample() {
                        Ok(sample) => sample,
                        Err(_err) => {
                            debug!("Failed to pull sample");
                            return Err(gst::FlowError::Flushing);
                        }
                    };

                    if consumers.discard {
                        return Ok(gst::FlowSuccess::Ok);
                    }

                    let span = tracing::trace_span!("New sample", appsink = %appsink.name());
                    let _guard = span.enter();

                    let latency = consumers.current_latency;
                    let latency_updated = mem::replace(&mut consumers.latency_updated, false);
                    let mut requested_keyframe = false;

                    let current_consumers = consumers
                        .consumers
                        .values()
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

                    //trace!("Appsink pushing sample {:?}, current running time: {}", sample, appsink.current_running_time());
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
                        .values()
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
                            trace!(appsink = %parent.name(), latency = %latency, "Latency updated");

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

/// Wrapper around a HashMap of consumers, exists for thread safety
/// and also protects some of the producer state
#[derive(Debug)]
struct StreamConsumers {
    /// The currently-observed latency
    current_latency: Option<gst::ClockTime>,
    /// Whether the consumers' appsrc latency needs updating
    latency_updated: bool,
    /// The consumers, link id -> consumer
    consumers: HashMap<String, StreamConsumer>,
    /// Whether appsrc samples should be forwarded to consumers yet
    discard: bool,
}

/// Wrapper around a consumer's `appsrc`
#[derive(Debug)]
struct StreamConsumer {
    /// The GStreamer `appsrc` of the consumer
    appsrc: gst_app::AppSrc,
    /// The id of a pad probe that intercepts force-key-unit events
    fku_probe_id: Option<gst::PadProbeId>,
    /// Whether an initial latency was forwarded to the `appsrc`
    forwarded_latency: atomic::AtomicBool,
    /// Whether a first buffer has made it through, used to determine
    /// whether a new key unit should be requested. Only useful for encoded
    /// streams.
    first_buffer: atomic::AtomicBool,
}

impl StreamConsumer {
    /// Create a new consumer
    fn new(appsrc: &gst_app::AppSrc, fku_probe_id: gst::PadProbeId, consumer_id: &str) -> Self {
        let consumer_id = consumer_id.to_string();
        appsrc.set_callbacks(
            gst_app::AppSrcCallbacks::builder()
                .enough_data(move |_appsrc| {
                    trace!(
                        "consumer {} is not consuming fast enough, old samples are getting dropped",
                        consumer_id
                    );
                })
                .build(),
        );

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

/// Wrapper around `gst::ElementFactory::make` with a better error
/// message
pub fn make_element(element: &str, name: Option<&str>) -> Result<gst::Element, Error> {
    gst::ElementFactory::make(element, name)
        .map_err(|err| anyhow!("Failed to make element {}: {}", element, err.message))
}

/// Maps GStreamer messages for consumption by a [`PipelineManager`]
/// actor
#[derive(Debug)]
struct BusMessage(gst::Message);

impl Message for BusMessage {
    type Result = ();
}

/// Sent from nodes to [`PipelineManager`] to wait for EOS to have
/// been processed by the pipeline
#[derive(Debug)]
pub struct WaitForEosMessage;

impl Message for WaitForEosMessage {
    type Result = ();
}

impl Handler<WaitForEosMessage> for PipelineManager {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _msg: WaitForEosMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let eos_receiver = self.eos_receiver.take();

        Box::pin(async move {
            if let Some(eos_receiver) = eos_receiver {
                let _ = eos_receiver.await;

                ()
            } else {
                ()
            }
        })
    }
}

/// A wrapper around [`gst::Pipeline`] for monitoring its bus. May send
/// messages to signal an error to an appropriate recipient (typically the
/// creator node)
#[derive(Debug)]
pub struct PipelineManager {
    /// The wrapped pipeline
    pipeline: gst::Pipeline,
    /// The recipient for potential error messages
    recipient: actix::WeakRecipient<ErrorMessage>,
    /// The identifier of the creator node, for tracing
    id: String,
    /// To signal that EOS was processed
    eos_sender: Option<oneshot::Sender<()>>,
    /// To wait for EOS to be processed
    eos_receiver: Option<oneshot::Receiver<()>>,
}

impl Actor for PipelineManager {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.pipeline.use_clock(Some(&gst::SystemClock::obtain()));
        self.pipeline.set_start_time(gst::CLOCK_TIME_NONE);
        self.pipeline.set_base_time(gst::ClockTime::from(0));

        let bus = self.pipeline.bus().expect("Pipeline with no bus");
        let bus_stream = bus.stream();
        Self::add_stream(bus_stream.map(BusMessage), ctx);
    }

    #[instrument(level = "debug", name = "stopping", skip(self, _ctx), fields(id = %self.id))]
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        debug!(id = %self.id, "tearing down pipeline");
        let _ = self.pipeline.set_state(gst::State::Null);
    }
}

impl StreamHandler<BusMessage> for PipelineManager {
    #[instrument(name = "Handling GStreamer bus message", level = "trace", skip(self, msg, _ctx), fields(id = %self.id, source = msg.0.src().as_ref().map(|src| src.path_string()).as_deref().unwrap_or("UNKNOWN")))]
    fn handle(&mut self, msg: BusMessage, _ctx: &mut Context<Self>) {
        use gst::MessageView;

        match msg.0.view() {
            MessageView::Element(m) => {
                if let Some(s) = m.structure() {
                    if s.name() == "level" {
                        trace!("audio output level: {}", s);
                    }
                }
            }
            MessageView::Latency(_) => {
                if let Some(src) = msg.0.src() {
                    if &src == self.pipeline.upcast_ref::<gst::Object>()
                        || src.has_as_ancestor(&self.pipeline)
                    {
                        trace!("Pipeline for node {} updated latency", self.id);
                        self.pipeline.call_async(|pipeline| {
                            let _ = pipeline.recalculate_latency();
                        });
                    }
                }
            }
            MessageView::Error(err) => {
                gst::debug_bin_to_dot_file_with_ts(
                    &self.pipeline,
                    gst::DebugGraphDetails::all(),
                    format!("error-{}", self.id),
                );
                if let Some(recipient) = self.recipient.upgrade() {
                    let src = err.src();
                    let dbg = err.debug();
                    let err = err.error();

                    if let Some(dbg) = dbg {
                        let _ = recipient.do_send(ErrorMessage(format!(
                            "Got error from {}: {} ({})",
                            src.as_ref()
                                .map(|src| src.path_string())
                                .as_deref()
                                .unwrap_or("UNKNOWN"),
                            err,
                            dbg
                        )));
                    } else {
                        let _ = recipient.do_send(ErrorMessage(format!(
                            "Got error from {}: {}",
                            src.as_ref()
                                .map(|src| src.path_string())
                                .as_deref()
                                .unwrap_or("UNKNOWN"),
                            err
                        )));
                    }
                }

                // No reason to wait for EOS indefinitely, won't
                // come now
                if let Some(eos_sender) = self.eos_sender.take() {
                    let _ = eos_sender.send(());
                }
            }
            MessageView::Eos(_) => {
                if let Some(eos_sender) = self.eos_sender.take() {
                    let _ = eos_sender.send(());
                }
            }
            _ => (),
        }
    }
}

impl PipelineManager {
    /// Create a new manager
    pub fn new(pipeline: gst::Pipeline, recipient: WeakRecipient<ErrorMessage>, id: &str) -> Self {
        let (eos_sender, eos_receiver) = oneshot::channel::<()>();

        Self {
            pipeline,
            recipient,
            id: id.to_string(),
            eos_sender: Some(eos_sender),
            eos_receiver: Some(eos_receiver),
        }
    }
}

/// Sent from [`PipelineManager`] to nodes to signal an error
#[derive(Debug)]
pub struct ErrorMessage(pub String);

impl Message for ErrorMessage {
    type Result = ();
}

/// Sent from nodes to [`PipelineManager`] to tear it down
#[derive(Debug)]
pub struct StopManagerMessage;

impl Message for StopManagerMessage {
    type Result = ();
}

impl Handler<StopManagerMessage> for PipelineManager {
    type Result = ();

    fn handle(&mut self, _msg: StopManagerMessage, ctx: &mut Context<Self>) {
        ctx.stop();
    }
}

/// Utility function to perform sanity checks on reschedule commands.
///
/// Transactional, either updates all times or none.
pub fn update_times(
    current_cue_time: &mut Option<DateTime<Utc>>,
    new_cue_time: &Option<DateTime<Utc>>,
    current_end_time: &mut Option<DateTime<Utc>>,
    new_end_time: &Option<DateTime<Utc>>,
) -> Result<(), Error> {
    if let Some(cue_time) = new_cue_time {
        if let Some(end_time) = new_end_time.as_ref().or(current_end_time.as_ref()) {
            if end_time <= cue_time {
                return Err(anyhow!("end_time {} <= cue_time {}", end_time, cue_time));
            }
        }
    }

    if let Some(end_time) = new_end_time {
        if let Some(cue_time) = new_cue_time.as_ref().or(current_cue_time.as_ref()) {
            if end_time <= cue_time {
                return Err(anyhow!("end_time {} <= cue_time {}", end_time, cue_time));
            }
        }
    }

    if let Some(cue_time) = new_cue_time {
        *current_cue_time = Some(*cue_time);
    }

    if let Some(end_time) = new_end_time {
        *current_end_time = Some(*end_time);
    }

    Ok(())
}
