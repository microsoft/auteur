//! A destination processing node.
//!
//! The actual destination depends on its family, for example RTMP or LocalFile
//! are supported.
//!
//! Destinations spend time in the [`stopping state`](State::Stopping)
//! during which EOS will be propagated down their pipeline before actually
//! stopping.

use actix::prelude::*;
use anyhow::{anyhow, Error};
use gst::prelude::*;
use tracing::{debug, error, instrument, trace};

use auteur_controlling::controller::{DestinationFamily, DestinationInfo, NodeInfo, State};

use crate::node::{
    ConsumerMessage, DestinationCommandMessage, GetNodeInfoMessage, NodeManager, NodeStatusMessage,
    ScheduleMessage, StartMessage, StopMessage, StoppedMessage,
};
use crate::utils::{
    make_element, ErrorMessage, PipelineManager, Schedulable, StateChangeResult, StateMachine,
    StopManagerMessage, StreamProducer, WaitForEosMessage,
};

/// Represents the potential connection to a producer
struct ConsumerSlot {
    /// Identifier of the slot
    id: String,
    /// Video producer
    video_producer: StreamProducer,
    /// Audio producer
    audio_producer: StreamProducer,
}

/// The Destination actor
pub struct Destination {
    /// Unique identifier
    id: String,
    /// Defines the nature of the destination
    family: DestinationFamily,
    /// The wrapped pipeline
    pipeline: gst::Pipeline,
    /// A helper for managing the pipeline
    pipeline_manager: Option<Addr<PipelineManager>>,
    /// Video input to the node
    video_appsrc: gst_app::AppSrc,
    /// Audio input to the node
    audio_appsrc: gst_app::AppSrc,
    /// Optional connection point
    consumer_slot: Option<ConsumerSlot>,
    /// Our state machine
    state_machine: StateMachine,
}

impl Actor for Destination {
    type Context = Context<Self>;

    #[instrument(level = "debug", name = "starting", skip(self, ctx), fields(id = %self.id))]
    fn started(&mut self, ctx: &mut Self::Context) {
        self.pipeline_manager = Some(
            PipelineManager::new(
                self.pipeline.clone(),
                ctx.address().downgrade().recipient(),
                &self.id,
            )
            .start(),
        );
    }

    #[instrument(level = "debug", name = "stopping", skip(self, ctx), fields(id = %self.id))]
    fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
        self.stop_schedule(ctx);

        if self.wait_for_eos(ctx) {
            self.state_machine.state = State::Stopping;
            Running::Continue
        } else {
            debug!("no need to wait for EOS");
            Running::Stop
        }
    }

    #[instrument(level = "debug", name = "stopped", skip(self, _ctx), fields(id = %self.id))]
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if let Some(manager) = self.pipeline_manager.take() {
            let _ = manager.do_send(StopManagerMessage);
        }

        if let Some(slot) = self.consumer_slot.take() {
            slot.video_producer.remove_consumer(&slot.id);
            slot.audio_producer.remove_consumer(&slot.id);
        }

        NodeManager::from_registry().do_send(StoppedMessage {
            id: self.id.clone(),
            video_producer: None,
            audio_producer: None,
        });
    }
}

impl Destination {
    /// Create a destination
    pub fn new(id: &str, family: &DestinationFamily) -> Self {
        let video_appsrc =
            gst::ElementFactory::make("appsrc", Some(&format!("destination-video-appsrc-{}", id)))
                .unwrap()
                .downcast::<gst_app::AppSrc>()
                .unwrap();
        let audio_appsrc =
            gst::ElementFactory::make("appsrc", Some(&format!("destination-audio-appsrc-{}", id)))
                .unwrap()
                .downcast::<gst_app::AppSrc>()
                .unwrap();

        for appsrc in &[&video_appsrc, &audio_appsrc] {
            appsrc.set_format(gst::Format::Time);
            appsrc.set_is_live(true);
            appsrc.set_handle_segment_change(true);
        }

        let pipeline = gst::Pipeline::new(None);

        Self {
            id: id.to_string(),
            family: family.clone(),
            pipeline,
            pipeline_manager: None,
            video_appsrc,
            audio_appsrc,
            consumer_slot: None,
            state_machine: StateMachine::default(),
        }
    }

    /// RTMP family
    #[instrument(level = "debug", name = "streaming", skip(self, ctx), fields(id = %self.id))]
    fn start_rtmp_pipeline(
        &mut self,
        ctx: &mut Context<Self>,
        uri: &str,
    ) -> Result<StateChangeResult, Error> {
        let vconv = make_element("videoconvert", None)?;
        let timecodestamper = make_element("timecodestamper", None)?;
        let timeoverlay = make_element("timeoverlay", None)?;
        let venc = make_element("nvh264enc", None).unwrap_or(make_element("x264enc", None)?);
        let vparse = make_element("h264parse", None)?;
        let venc_queue = make_element("queue", None)?;

        let aconv = make_element("audioconvert", None)?;
        let aresample = make_element("audioresample", None)?;
        let aenc = make_element("faac", None)?;
        let aenc_queue = make_element("queue", None)?;

        let mux = make_element("flvmux", None)?;
        let mux_queue = make_element("queue", None)?;
        let sink = make_element("rtmp2sink", None)?;

        self.pipeline.add_many(&[
            self.video_appsrc.upcast_ref(),
            &vconv,
            &timecodestamper,
            &timeoverlay,
            &venc,
            &vparse,
            &venc_queue,
            self.audio_appsrc.upcast_ref(),
            &aconv,
            &aresample,
            &aenc,
            &aenc_queue,
            &mux,
            &mux_queue,
            &sink,
        ])?;

        if venc.has_property("tune", None) {
            venc.set_property_from_str("tune", "zerolatency");
        } else if venc.has_property("zerolatency", None) {
            venc.set_property("zerolatency", &true).unwrap();
        }

        if venc.has_property("key-int-max", None) {
            venc.set_property("key-int-max", &30u32).unwrap();
        } else if venc.has_property("gop-size", None) {
            venc.set_property("gop-size", &30i32).unwrap();
        }

        vparse.set_property("config-interval", &-1i32).unwrap();
        sink.set_property("location", uri).unwrap();

        mux.set_property("streamable", &true).unwrap();
        mux.set_property("latency", &1000000000u64).unwrap();

        mux.set_property(
            "start-time-selection",
            gst_base::AggregatorStartTimeSelection::First,
        )
        .unwrap();

        timecodestamper.set_property_from_str("source", "rtc");
        timeoverlay.set_property_from_str("time-mode", "time-code");

        for queue in &[&venc_queue, &aenc_queue] {
            queue
                .set_properties(&[
                    ("max-size-buffers", &0u32),
                    ("max-size-bytes", &0u32),
                    ("max-size-time", &(3 * gst::SECOND)),
                ])
                .unwrap();
        }

        gst::Element::link_many(&[
            self.video_appsrc.upcast_ref(),
            &vconv,
            &timecodestamper,
            &timeoverlay,
            &venc,
            &vparse,
            &venc_queue,
            &mux,
            &mux_queue,
            &sink,
        ])?;

        gst::Element::link_many(&[
            self.audio_appsrc.upcast_ref(),
            &aconv,
            &aresample,
            &aenc,
            &aenc_queue,
            &mux,
        ])?;

        if let Some(slot) = &self.consumer_slot {
            debug!("connecting to producers");
            slot.video_producer
                .add_consumer(&self.video_appsrc, &slot.id);
            slot.audio_producer
                .add_consumer(&self.audio_appsrc, &slot.id);
        } else {
            debug!("started but not yet connected");
        }

        let sink_clone = sink.downgrade();
        let id_clone = self.id.clone();
        ctx.run_interval(std::time::Duration::from_secs(1), move |_s, _ctx| {
            if let Some(sink) = sink_clone.upgrade() {
                let val = sink.property("stats").unwrap();
                let s = val.get::<gst::Structure>().unwrap();

                trace!(id = %id_clone, "rtmp destination statistics: {}", s.to_string());
            }
        });

        let addr = ctx.address();
        let id = self.id.clone();
        self.pipeline.call_async(move |pipeline| {
            if let Err(err) = pipeline.set_state(gst::State::Playing) {
                let _ = addr.do_send(ErrorMessage(format!(
                    "Failed to start destination {}: {}",
                    id, err
                )));
            }
        });

        Ok(StateChangeResult::Success)
    }

    /// LocalFile family
    #[instrument(level = "debug", name = "saving to local file", skip(self, ctx), fields(id = %self.id))]
    fn start_local_file_pipeline(
        &mut self,
        ctx: &mut Context<Self>,
        base_name: &str,
        max_size_time: Option<u32>,
    ) -> Result<StateChangeResult, Error> {
        let vconv = make_element("videoconvert", None)?;
        let venc = make_element("nvh264enc", None).unwrap_or(make_element("x264enc", None)?);
        let vparse = make_element("h264parse", None)?;

        let aconv = make_element("audioconvert", None)?;
        let aresample = make_element("audioresample", None)?;
        let aenc = make_element("faac", None)?;

        let multiqueue = make_element("multiqueue", None)?;
        let sink = make_element("splitmuxsink", None)?;

        self.pipeline.add_many(&[
            self.video_appsrc.upcast_ref(),
            &vconv,
            &venc,
            &vparse,
            self.audio_appsrc.upcast_ref(),
            &aconv,
            &aresample,
            &aenc,
            &multiqueue,
            &sink,
        ])?;

        if let Some(max_size_time) = max_size_time {
            sink.set_property("max-size-time", (max_size_time as u64) * gst::MSECOND)
                .unwrap();
            sink.set_property("use-robust-muxing", &true).unwrap();
            let mux = make_element("qtmux", None)?;
            mux.set_property("reserved-moov-update-period", &gst::SECOND)
                .unwrap();
            sink.set_property("muxer", &mux).unwrap();
            let location = base_name.to_owned() + "%05d.mp4";
            sink.set_property("location", &location).unwrap();
        } else {
            let location = base_name.to_owned() + ".mp4";
            sink.set_property("location", &location).unwrap();
        }

        vparse.link_pads(None, &multiqueue, Some("sink_0"))?;
        aenc.link_pads(None, &multiqueue, Some("sink_1"))?;

        multiqueue.link_pads(Some("src_0"), &sink, Some("video"))?;
        multiqueue.link_pads(Some("src_1"), &sink, Some("audio_0"))?;

        gst::Element::link_many(&[self.video_appsrc.upcast_ref(), &vconv, &venc, &vparse])?;

        gst::Element::link_many(&[self.audio_appsrc.upcast_ref(), &aconv, &aresample, &aenc])?;

        if let Some(slot) = &self.consumer_slot {
            debug!("connecting to producers");
            slot.video_producer
                .add_consumer(&self.video_appsrc, &slot.id);
            slot.audio_producer
                .add_consumer(&self.audio_appsrc, &slot.id);
        } else {
            debug!("started but not yet connected");
        }

        let addr = ctx.address();
        let id = self.id.clone();
        self.pipeline.call_async(move |pipeline| {
            if let Err(err) = pipeline.set_state(gst::State::Playing) {
                let _ = addr.do_send(ErrorMessage(format!(
                    "Failed to start destination {}: {}",
                    id, err
                )));
            }
        });

        Ok(StateChangeResult::Success)
    }

    /// Implement Connect command
    #[instrument(level = "debug", name = "connecting", skip(self, video_producer, audio_producer), fields(id = %self.id))]
    fn connect(
        &mut self,
        link_id: &str,
        video_producer: &StreamProducer,
        audio_producer: &StreamProducer,
    ) -> Result<(), Error> {
        if self.consumer_slot.is_some() {
            return Err(anyhow!("destination already has a producer"));
        }

        if self.state_machine.state == State::Started {
            debug!("destination {} connecting to producers", self.id);
            video_producer.add_consumer(&self.video_appsrc, link_id);
            audio_producer.add_consumer(&self.audio_appsrc, link_id);
        }

        self.consumer_slot = Some(ConsumerSlot {
            id: link_id.to_string(),
            video_producer: video_producer.clone(),
            audio_producer: audio_producer.clone(),
        });

        Ok(())
    }

    /// Implement Disconnect command
    #[instrument(level = "debug", name = "disconnecting", skip(self), fields(id = %self.id))]
    fn disconnect(&mut self, link_id: &str) -> Result<(), Error> {
        if let Some(slot) = self.consumer_slot.take() {
            if slot.id == link_id {
                slot.video_producer.remove_consumer(&slot.id);
                slot.audio_producer.remove_consumer(&slot.id);
                Ok(())
            } else {
                let res = Err(anyhow!("invalid slot id {}, current: {}", link_id, slot.id));
                self.consumer_slot = Some(slot);
                res
            }
        } else {
            Err(anyhow!("can't disconnect, not connected"))
        }
    }

    /// Wait for EOS to propagate down our pipeline before stopping
    // Returns true if calling code should wait before fully stopping
    #[instrument(level = "debug", name = "checking if waiting for EOS is needed", skip(self, ctx), fields(id = %self.id))]
    fn wait_for_eos(&mut self, ctx: &mut Context<Self>) -> bool {
        match self.state_machine.state {
            State::Initial | State::Stopped => false,
            State::Starting => false,
            _ => match self.consumer_slot.take() {
                Some(slot) => {
                    let pipeline_manager = self.pipeline_manager.as_ref().unwrap();

                    debug!("waiting for EOS");

                    slot.video_producer.remove_consumer(&slot.id);
                    slot.audio_producer.remove_consumer(&slot.id);

                    self.video_appsrc.send_event(gst::event::Eos::new());
                    self.audio_appsrc.send_event(gst::event::Eos::new());

                    let fut = pipeline_manager
                        .send(WaitForEosMessage)
                        .into_actor(self)
                        .then(|_res, slf, ctx| {
                            let span = tracing::debug_span!("stopping", id = %slf.id);
                            let _guard = span.enter();
                            debug!("waited for EOS");
                            slf.state_machine.state = State::Stopped;
                            ctx.stop();
                            actix::fut::ready(())
                        });

                    ctx.wait(fut);

                    true
                }
                _ => false,
            },
        }
    }

    #[instrument(level = "debug", skip(self, ctx), fields(id = %self.id))]
    fn stop(&mut self, ctx: &mut Context<Self>) {
        self.stop_schedule(ctx);
        ctx.stop();
    }
}

impl Schedulable<Self> for Destination {
    fn state_machine(&self) -> &StateMachine {
        &self.state_machine
    }

    fn state_machine_mut(&mut self) -> &mut StateMachine {
        &mut self.state_machine
    }

    fn node_id(&self) -> &str {
        &self.id
    }

    #[instrument(level = "debug", skip(self, ctx), fields(id = %self.id))]
    fn transition(
        &mut self,
        ctx: &mut Context<Self>,
        target: State,
    ) -> Result<StateChangeResult, Error> {
        match target {
            State::Initial => Ok(StateChangeResult::Skip),
            State::Starting => match self.family.clone() {
                DestinationFamily::Rtmp { uri } => self.start_rtmp_pipeline(ctx, &uri),
                DestinationFamily::LocalFile {
                    base_name,
                    max_size_time,
                } => self.start_local_file_pipeline(ctx, &base_name, max_size_time),
            },
            State::Started => Ok(StateChangeResult::Success),
            State::Stopping => {
                self.stop(ctx);
                Ok(StateChangeResult::Success)
            }
            // We claim back our state machine from there on
            State::Stopped => unreachable!(),
        }
    }
}

impl Handler<ConsumerMessage> for Destination {
    type Result = MessageResult<ConsumerMessage>;

    fn handle(&mut self, msg: ConsumerMessage, _ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            ConsumerMessage::Connect {
                link_id,
                video_producer,
                audio_producer,
            } => MessageResult(self.connect(&link_id, &video_producer, &audio_producer)),
            ConsumerMessage::Disconnect { slot_id } => MessageResult(self.disconnect(&slot_id)),
        }
    }
}

impl Handler<StartMessage> for Destination {
    type Result = MessageResult<StartMessage>;

    fn handle(&mut self, msg: StartMessage, ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(self.start_schedule(ctx, msg.cue_time, msg.end_time))
    }
}

impl Handler<DestinationCommandMessage> for Destination {
    type Result = MessageResult<DestinationCommandMessage>;

    fn handle(
        &mut self,
        _msg: DestinationCommandMessage,
        _ctx: &mut Context<Self>,
    ) -> Self::Result {
        MessageResult(Ok(()))
    }
}

impl Handler<ErrorMessage> for Destination {
    type Result = ();

    fn handle(&mut self, msg: ErrorMessage, ctx: &mut Context<Self>) -> Self::Result {
        error!("Got error message '{}' on destination {}", msg.0, self.id,);

        NodeManager::from_registry().do_send(NodeStatusMessage::Error {
            id: self.id.clone(),
            message: msg.0,
        });

        gst::debug_bin_to_dot_file_with_ts(
            &self.pipeline,
            gst::DebugGraphDetails::all(),
            format!("error-destination-{}", self.id),
        );

        self.stop(ctx);
    }
}

impl Handler<ScheduleMessage> for Destination {
    type Result = Result<(), Error>;

    fn handle(&mut self, msg: ScheduleMessage, ctx: &mut Context<Self>) -> Self::Result {
        self.reschedule(ctx, msg.cue_time, msg.end_time)
    }
}

impl Handler<StopMessage> for Destination {
    type Result = Result<(), Error>;

    fn handle(&mut self, _msg: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        self.stop(ctx);
        Ok(())
    }
}

impl Handler<GetNodeInfoMessage> for Destination {
    type Result = Result<NodeInfo, Error>;

    fn handle(&mut self, _msg: GetNodeInfoMessage, _ctx: &mut Context<Self>) -> Self::Result {
        Ok(NodeInfo::Destination(DestinationInfo {
            family: self.family.clone(),
            slot_id: self.consumer_slot.as_ref().map(|slot| slot.id.clone()),
            cue_time: self.state_machine.cue_time,
            end_time: self.state_machine.end_time,
            state: self.state_machine.state,
        }))
    }
}
