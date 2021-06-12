//! A destination processing node.
//!
//! The actual destination depends on its family, for example RTMP or LocalFile
//! are supported.
//!
//! Destinations spend time in the [`stopping state`](NodeStatus::Stopping)
//! during which EOS will be propagated down their pipeline before actually
//! stopping.

use actix::prelude::*;
use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use gst::prelude::*;
use tracing::{debug, error, instrument, trace};

use rtmp_switcher_controlling::controller::{
    DestinationFamily, DestinationInfo, NodeInfo, NodeStatus,
};

use crate::node::{
    ConsumerMessage, DestinationCommandMessage, GetNodeInfoMessage, NodeManager, ScheduleMessage,
    StartMessage, StopMessage, StoppedMessage,
};
use crate::utils::{
    make_element, update_times, ErrorMessage, PipelineManager, StopManagerMessage, StreamProducer,
    WaitForEosMessage,
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
    /// When the destination will start
    cue_time: Option<DateTime<Utc>>,
    /// When the destination will stop
    end_time: Option<DateTime<Utc>>,
    /// The status of the destination
    status: NodeStatus,
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
    /// Scheduling timer
    state_handle: Option<SpawnHandle>,
    /// Statistics timer
    monitor_handle: Option<SpawnHandle>,
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
        if self.wait_for_eos(ctx) {
            self.status = NodeStatus::Stopping;
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
            cue_time: None,
            end_time: None,
            status: NodeStatus::Initial,
            pipeline,
            pipeline_manager: None,
            video_appsrc,
            audio_appsrc,
            consumer_slot: None,
            state_handle: None,
            monitor_handle: None,
        }
    }

    /// RTMP family
    #[instrument(level = "debug", name = "streaming", skip(self, ctx), fields(id = %self.id))]
    fn start_rtmp_pipeline(&mut self, ctx: &mut Context<Self>, uri: &String) -> Result<(), Error> {
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
        self.monitor_handle = Some(ctx.run_interval(
            std::time::Duration::from_secs(1),
            move |_s, _ctx| {
                if let Some(sink) = sink_clone.upgrade() {
                    let val = sink.property("stats").unwrap();
                    let s = val.get::<gst::Structure>().unwrap();

                    trace!(id = %id_clone, "rtmp destination statistics: {}", s.to_string());
                }
            },
        ));

        self.status = NodeStatus::Started;

        let addr = ctx.address().clone();
        let id = self.id.clone();
        self.pipeline.call_async(move |pipeline| {
            if let Err(err) = pipeline.set_state(gst::State::Playing) {
                let _ = addr.do_send(ErrorMessage(format!(
                    "Failed to start destination {}: {}",
                    id, err
                )));
            }
        });

        Ok(())
    }

    /// LocalFile family
    #[instrument(level = "debug", name = "saving to local file", skip(self, ctx), fields(id = %self.id))]
    fn start_local_file_pipeline(
        &mut self,
        ctx: &mut Context<Self>,
        base_name: &str,
        max_size_time: Option<u32>,
    ) -> Result<(), Error> {
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
            sink.set_property("max-size-time", &(max_size_time as u64) * gst::MSECOND)
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

        self.status = NodeStatus::Started;

        let addr = ctx.address().clone();
        let id = self.id.clone();
        self.pipeline.call_async(move |pipeline| {
            if let Err(err) = pipeline.set_state(gst::State::Playing) {
                let _ = addr.do_send(ErrorMessage(format!(
                    "Failed to start destination {}: {}",
                    id, err
                )));
            }
        });

        Ok(())
    }

    /// Progress through our state machine
    #[instrument(level = "trace", name = "scheduling", skip(self, ctx), fields(id = %self.id))]
    fn schedule_state(&mut self, ctx: &mut Context<Self>) {
        if let Some(handle) = self.state_handle {
            trace!("cancelling current state scheduling");
            ctx.cancel_future(handle);
        }

        let next_time = match self.status {
            NodeStatus::Initial => self.cue_time,
            NodeStatus::Starting => unreachable!(),
            NodeStatus::Started => self.end_time,
            NodeStatus::Stopping => None,
            NodeStatus::Stopped => None,
        };

        if let Some(next_time) = next_time {
            let now = Utc::now();

            let timeout = next_time - now;

            if timeout > chrono::Duration::zero() {
                trace!("not ready to progress to next state");

                self.state_handle = Some(ctx.run_later(timeout.to_std().unwrap(), |s, ctx| {
                    s.schedule_state(ctx);
                }));
            } else {
                trace!("progressing to next state");
                if let Err(err) = match self.status {
                    NodeStatus::Initial => match self.family.clone() {
                        DestinationFamily::RTMP { uri } => self.start_rtmp_pipeline(ctx, &uri),
                        DestinationFamily::LocalFile {
                            base_name,
                            max_size_time,
                        } => self.start_local_file_pipeline(ctx, &base_name, max_size_time),
                    },
                    NodeStatus::Starting => unreachable!(),
                    NodeStatus::Started => {
                        ctx.stop();
                        self.status = NodeStatus::Stopping;
                        Ok(())
                    }
                    NodeStatus::Stopping | NodeStatus::Stopped => Ok(()),
                } {
                    ctx.notify(ErrorMessage(format!(
                        "Failed to advance destination state: {:?}",
                        err
                    )));
                } else {
                    self.schedule_state(ctx);
                }
            }
        } else {
            trace!("going back to sleep");
        }
    }

    /// Implement Play command
    #[instrument(level = "trace", name = "cueing", skip(self, ctx), fields(id = %self.id))]
    fn start(
        &mut self,
        ctx: &mut Context<Self>,
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        let cue_time = cue_time.unwrap_or(Utc::now());

        if let Some(end_time) = end_time {
            if cue_time >= end_time {
                return Err(anyhow!("cue time >= end time"));
            }
        }

        match self.status {
            NodeStatus::Initial => {
                self.cue_time = Some(cue_time);
                self.end_time = end_time;

                self.schedule_state(ctx);
            }
            _ => {
                return Err(anyhow!(
                    "can't start destination with status {:?}",
                    self.status
                ));
            }
        }

        Ok(())
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

        if self.status == NodeStatus::Started {
            debug!("destination {} connecting to producers", self.id);
            video_producer.add_consumer(&self.video_appsrc, &link_id);
            audio_producer.add_consumer(&self.audio_appsrc, &link_id);
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
                return res;
            }
        } else {
            Err(anyhow!("can't disconnect, not connected"))
        }
    }

    /// Implement Reschedule command
    #[instrument(level = "debug", name = "cueing", skip(self, ctx), fields(id = %self.id))]
    fn reschedule(
        &mut self,
        ctx: &mut Context<Self>,
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        match self.status {
            NodeStatus::Initial => Ok(()),
            NodeStatus::Starting => unreachable!(),
            NodeStatus::Started => {
                if cue_time.is_some() {
                    Err(anyhow!("can't change cue time when streaming"))
                } else {
                    Ok(())
                }
            }
            NodeStatus::Stopping | NodeStatus::Stopped => {
                Err(anyhow!("can't reschedule when stopping or stopped"))
            }
        }?;

        update_times(&mut self.cue_time, &cue_time, &mut self.end_time, &end_time)?;

        self.schedule_state(ctx);

        Ok(())
    }

    /// Wait for EOS to propagate down our pipeline before stopping
    // Returns true if calling code should wait before fully stopping
    #[instrument(level = "debug", name = "checking if waiting for EOS is needed", skip(self, ctx), fields(id = %self.id))]
    fn wait_for_eos(&mut self, ctx: &mut Context<Self>) -> bool {
        match self.status {
            NodeStatus::Initial | NodeStatus::Stopped => false,
            NodeStatus::Starting => unreachable!(),
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
                            slf.status = NodeStatus::Stopped;
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
        MessageResult(self.start(ctx, msg.cue_time, msg.end_time))
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

        gst::debug_bin_to_dot_file_with_ts(
            &self.pipeline,
            gst::DebugGraphDetails::all(),
            format!("error-destination-{}", self.id),
        );

        ctx.stop();
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
        ctx.stop();
        Ok(())
    }
}

impl Handler<GetNodeInfoMessage> for Destination {
    type Result = Result<NodeInfo, Error>;

    fn handle(&mut self, _msg: GetNodeInfoMessage, _ctx: &mut Context<Self>) -> Self::Result {
        Ok(NodeInfo::Destination(DestinationInfo {
            family: self.family.clone(),
            slot_id: self.consumer_slot.as_ref().map(|slot| slot.id.clone()),
            cue_time: self.cue_time,
            end_time: self.end_time,
            status: self.status,
        }))
    }
}
