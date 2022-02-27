//! Helper gst::Pipeline wrapper

use crate::utils::ErrorMessage;
use actix::prelude::*;
use actix::WeakRecipient;
use futures::channel::oneshot;
use futures::prelude::*;
use gst::prelude::*;
use tracing::{debug, instrument, trace};

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
            }
        })
    }
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
            MessageView::StateChanged(state_changed) => {
                if Some(self.pipeline.upcast_ref()) == state_changed.src().as_ref() {
                    self.pipeline.debug_to_dot_file_with_ts(
                        gst::DebugGraphDetails::all(),
                        format!(
                            "state-change-{}-{:?}-to-{:?}",
                            self.id,
                            state_changed.old(),
                            state_changed.current()
                        ),
                    );
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

        pipeline.use_clock(Some(&gst::SystemClock::obtain()));
        pipeline.set_start_time(gst::ClockTime::NONE);
        pipeline.set_base_time(gst::ClockTime::from_nseconds(0));

        Self {
            pipeline,
            recipient,
            id: id.to_string(),
            eos_sender: Some(eos_sender),
            eos_receiver: Some(eos_receiver),
        }
    }
}
