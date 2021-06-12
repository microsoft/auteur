//! A source processing node.
//!
//! The only supported source type is created with a URI. In the future
//! generators could also be supported, for example to display a countdown.
//!
//! The main complexity for this node is the [`starting`](NodeStatus::Starting)
//! feature: when the node is scheduled to play in the future, we spin it
//! up 10 seconds before its cue time. Non-live sources are blocked
//! by fallbacksrc, which only picks a base time once unblocked,
//! and data coming from live sources is discarded by the StreamProducers
//! until forward() is called on them.
//!
//! Part of the extra complexity is due to the rescheduling feature:
//! when rescheduling a prerolling source, we want to tear down the
//! previous pipeline, but as logical connections are tracked by the
//! StreamProducer elements, we want to keep their lifecycle tied to
//! that of the source. This means appsinks may be removed from an old
//! pipeline and moved to a new one, this is safe but needs to be kept
//! in mind.

use crate::node::{
    GetNodeInfoMessage, GetProducerMessage, NodeManager, ScheduleMessage, SourceCommandMessage,
    StopMessage,
};
use crate::utils::{
    make_element, update_times, ErrorMessage, PipelineManager, StopManagerMessage, StreamProducer,
};
use actix::prelude::*;
use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use gst::prelude::*;
use rtmp_switcher_controlling::controller::{NodeInfo, NodeStatus, SourceCommand, SourceInfo};
use tracing::{debug, error, instrument, trace};

/// The pipeline and various GStreamer elements that the source
/// optionally wraps, their lifetime is not directly bound to that
/// of the source itself
#[derive(Debug)]
struct State {
    /// The wrapped pipeline
    pipeline: gst::Pipeline,
    /// A helper for managing the pipeline
    pipeline_manager: Addr<PipelineManager>,
    /// `fallbacksrc`
    src: gst::Element,
    /// Vector of `fallbackswitch`, only used to monitor status
    switches: Vec<gst::Element>,
    /// Increments when the src element exposes pads, decrements
    /// when they receive EOS
    n_streams: u32,
    /// `urisourcebin`, only used to monitor status
    source_bin: Option<gst::Element>,
}

/// The Source actor
#[derive(Debug)]
pub struct Source {
    /// Unique identifier
    id: String,
    /// URI the source will play
    uri: String,
    /// When the source will start
    cue_time: Option<DateTime<Utc>>,
    /// When the source will stop
    end_time: Option<DateTime<Utc>>,
    /// Output audio producer
    audio_producer: StreamProducer,
    /// Output video producer
    video_producer: StreamProducer,
    /// The status of the source
    status: NodeStatus,
    /// GStreamer elements when prerolling or playing
    state: Option<State>,
    /// Scheduling timer
    state_handle: Option<SpawnHandle>,
    /// Statistics timer
    monitor_handle: Option<SpawnHandle>,
}

impl Source {
    /// Create a source
    #[instrument(level = "debug", name = "creating")]
    pub fn new(id: &str, uri: &str) -> Self {
        let audio_appsink =
            gst::ElementFactory::make("appsink", Some(&format!("src-audio-appsink-{}", id)))
                .unwrap()
                .downcast::<gst_app::AppSink>()
                .unwrap();

        let video_appsink =
            gst::ElementFactory::make("appsink", Some(&format!("src-video-appsink-{}", id)))
                .unwrap()
                .downcast::<gst_app::AppSink>()
                .unwrap();

        Self {
            id: id.to_string(),
            uri: uri.to_string(),
            cue_time: None,
            end_time: None,
            audio_producer: StreamProducer::from(&audio_appsink),
            video_producer: StreamProducer::from(&video_appsink),
            status: NodeStatus::Initial,
            state: None,
            state_handle: None,
            monitor_handle: None,
        }
    }

    /// Connect pads exposed by `falllbacksrc` to our output producers
    #[instrument(level = "debug", name = "connecting", skip(pipeline, is_video, pad, video_producer, audio_producer), fields(pad = %pad.name()))]
    fn connect_pad(
        id: String,
        is_video: bool,
        pipeline: &gst::Pipeline,
        pad: &gst::Pad,
        video_producer: &StreamProducer,
        audio_producer: &StreamProducer,
    ) -> Result<gst::Element, Error> {
        if is_video {
            let deinterlace = make_element("deinterlace", None)?;

            pipeline.add(&deinterlace)?;

            let appsink: &gst::Element = video_producer.appsink().upcast_ref();

            debug!(appsink = %appsink.name(), "linking video stream");

            deinterlace.sync_state_with_parent()?;

            let sinkpad = deinterlace.static_pad("sink").unwrap();
            pad.link(&sinkpad)?;
            deinterlace.link(appsink)?;

            Ok(appsink.clone())
        } else {
            let aconv = make_element("audioconvert", None)?;
            let level = make_element("level", None)?;

            pipeline.add_many(&[&aconv, &level])?;

            let appsink: &gst::Element = audio_producer.appsink().upcast_ref();

            debug!(appsink = %appsink.name(), "linking audio stream to appsink");

            aconv.sync_state_with_parent()?;
            level.sync_state_with_parent()?;

            gst::Element::link_many(&[&aconv, &level, appsink])?;

            let sinkpad = aconv.static_pad("sink").unwrap();
            pad.link(&sinkpad)?;

            Ok(appsink.clone())
        }
    }

    /// Preroll the pipeline ahead of time (by default 10 seconds before cue time)
    #[instrument(level = "debug", name = "prerolling", skip(self, ctx), fields(id = %self.id))]
    fn preroll(&mut self, ctx: &mut Context<Self>) -> Result<(), Error> {
        if self.status != NodeStatus::Initial {
            return Err(anyhow!("can't preroll source in state: {:?}", self.status));
        }

        let pipeline = gst::Pipeline::new(Some(&self.id.to_string()));
        let audio_appsink: &gst::Element = self.audio_producer.appsink().upcast_ref();
        let video_appsink: &gst::Element = self.video_producer.appsink().upcast_ref();

        pipeline.add_many(&[audio_appsink, video_appsink]).unwrap();

        let src = make_element("fallbacksrc", None)?;
        pipeline.add(&src)?;

        src.set_property("uri", &self.uri).unwrap();
        src.set_property("manual-unblock", &true).unwrap();

        let pipeline_clone = pipeline.downgrade();
        let addr = ctx.address();
        let video_producer = self.video_producer.clone();
        let audio_producer = self.audio_producer.clone();
        let id = self.id.clone();
        src.connect_pad_added(move |_src, pad| {
            if let Some(pipeline) = pipeline_clone.upgrade() {
                let is_video = pad.name() == "video";
                match Source::connect_pad(
                    id.clone(),
                    is_video,
                    &pipeline,
                    pad,
                    &video_producer,
                    &audio_producer,
                ) {
                    Ok(appsink) => {
                        let addr_clone = addr.clone();
                        let pad = appsink.static_pad("sink").unwrap();
                        pad.add_probe(gst::PadProbeType::EVENT_DOWNSTREAM, move |_pad, info| {
                            match info.data {
                                Some(gst::PadProbeData::Event(ref ev))
                                    if ev.type_() == gst::EventType::Eos =>
                                {
                                    addr_clone.do_send(StreamMessage { starting: false });
                                    gst::PadProbeReturn::Drop
                                }
                                _ => gst::PadProbeReturn::Ok,
                            }
                        });

                        addr.do_send(StreamMessage { starting: true });
                    }
                    Err(err) => addr.do_send(ErrorMessage(format!(
                        "Failed to connect source stream: {:?}",
                        err
                    ))),
                }
            }
        });

        let addr_clone = ctx.address().clone();
        src.connect("notify::status", false, move |_args| {
            let _ = addr_clone.do_send(SourceStatusMessage);
            None
        })
        .unwrap();

        let addr_clone = ctx.address().clone();
        src.connect("notify::statistics", false, move |_args| {
            let _ = addr_clone.do_send(SourceStatusMessage);
            None
        })
        .unwrap();

        let src_bin: &gst::Bin = src.downcast_ref().unwrap();

        let addr = ctx.address().clone();
        src_bin.connect_deep_element_added(move |_src, _bin, element| {
            if element.has_property("primary-health", None) {
                let _ = addr.do_send(NewSwitchMessage(element.clone()));
            }

            if element.type_().name() == "GstURISourceBin" {
                let _ = addr.do_send(NewSourceBinMessage(element.clone()));
            }
        });

        debug!("now prerolling");

        self.status = NodeStatus::Starting;
        self.state = Some(State {
            pipeline: pipeline.clone(),
            pipeline_manager: PipelineManager::new(
                pipeline.clone(),
                ctx.address().downgrade().recipient(),
                &self.id,
            )
            .start(),
            src,
            switches: vec![],
            n_streams: 0,
            source_bin: None,
        });

        let addr = ctx.address().clone();
        let id = self.id.clone();
        pipeline.call_async(move |pipeline| {
            if let Err(err) = pipeline.set_state(gst::State::Playing) {
                let _ = addr.do_send(ErrorMessage(format!(
                    "Failed to start source {}: {}",
                    id, err
                )));
            }
        });

        Ok(())
    }

    /// Unblock a prerolling pipeline
    #[instrument(level = "debug", name = "unblocking", skip(self), fields(id = %self.id))]
    fn unblock(&mut self, ctx: &mut Context<Self>) -> Result<(), Error> {
        if self.status != NodeStatus::Starting {
            return Err(anyhow!("can't play source in state: {:?}", self.status));
        }

        let state = self.state.as_ref().unwrap();

        state.src.emit_by_name("unblock", &[]).unwrap();
        self.video_producer.forward();
        self.audio_producer.forward();

        debug!("unblocked, now playing");

        let id_clone = self.id.clone();
        self.monitor_handle = Some(ctx.run_interval(
            std::time::Duration::from_secs(1),
            move |s, _ctx| {
                if let Some(ref state) = s.state {
                    if let Some(ref source_bin) = state.source_bin {
                        let val = source_bin.property("statistics").unwrap();
                        let s = val.get::<gst::Structure>().unwrap();

                        trace!(id = %id_clone, "source statistics: {}", s.to_string());
                    }
                }
            },
        ));

        self.status = NodeStatus::Started;

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
            NodeStatus::Initial => {
                if let Some(cue_time) = self.cue_time {
                    Some(cue_time - chrono::Duration::seconds(10))
                } else {
                    None
                }
            }
            NodeStatus::Starting => self.cue_time,
            NodeStatus::Started => self.end_time,
            NodeStatus::Stopping => unreachable!(),
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
                    NodeStatus::Initial => self.preroll(ctx),
                    NodeStatus::Starting => self.unblock(ctx),
                    NodeStatus::Started => {
                        ctx.stop();
                        self.status = NodeStatus::Stopped;
                        Ok(())
                    }
                    NodeStatus::Stopping => unreachable!(),
                    NodeStatus::Stopped => Ok(()),
                } {
                    ctx.notify(ErrorMessage(format!("Failed to preroll source: {:?}", err)));
                } else {
                    self.schedule_state(ctx);
                }
            }
        } else {
            debug!("going back to sleep");
        }
    }

    /// Implement Play
    #[instrument(level = "debug", name = "cueing", skip(self, ctx), fields(id = %self.id))]
    fn play(
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
                return Err(anyhow!("can't play source with status {:?}", self.status));
            }
        }

        Ok(())
    }

    /// A new pad was added, or an existing pad EOS'd
    fn handle_stream_change(&mut self, ctx: &mut Context<Self>, starting: bool) {
        if let Some(ref mut state) = self.state {
            if starting {
                state.n_streams += 1;

                debug!(id = %self.id, n_streams = %state.n_streams, "new active stream");
            } else {
                state.n_streams -= 1;

                debug!(id = %self.id, n_streams = %state.n_streams, "active stream finished");

                if state.n_streams == 0 {
                    ctx.stop()
                }
            }
        }
    }

    /// Track the status of a new fallbackswitch
    #[instrument(level = "debug", name = "new-fallbackswitch", skip(self, ctx), fields(id = %self.id))]
    fn monitor_switch(&mut self, ctx: &mut Context<Self>, switch: gst::Element) {
        if let Some(ref mut state) = self.state {
            let addr_clone = ctx.address().clone();
            switch
                .connect("notify::primary-health", false, move |_args| {
                    let _ = addr_clone.do_send(SourceStatusMessage);
                    None
                })
                .unwrap();

            let addr_clone = ctx.address().clone();
            switch
                .connect("notify::fallback-health", false, move |_args| {
                    let _ = addr_clone.do_send(SourceStatusMessage);
                    None
                })
                .unwrap();

            state.switches.push(switch);
        }
    }

    /// Trace the status of the source for monitoring purposes
    #[instrument(level = "trace", name = "new-source-status", skip(self), fields(id = %self.id))]
    fn log_source_status(&mut self) {
        if let Some(ref state) = self.state {
            let value = state.src.property("status").unwrap();
            let status = gst::glib::EnumValue::from_value(&value).expect("Not an enum type");
            trace!("Source status: {}", status.nick());
            trace!(
                "Source statistics: {:?}",
                state.src.property("statistics").unwrap()
            );

            for switch in &state.switches {
                let switch_name = match switch.static_pad("src").unwrap().caps() {
                    Some(caps) => match caps.structure(0) {
                        Some(s) => s.name(),
                        None => "EMPTY",
                    },
                    None => "ANY",
                };

                let value = switch.property("primary-health").unwrap();
                let health = gst::glib::EnumValue::from_value(&value).expect("Not an enum type");
                trace!("switch {} primary health: {}", switch_name, health.nick());

                let value = switch.property("fallback-health").unwrap();
                let health = gst::glib::EnumValue::from_value(&value).expect("Not an enum type");
                trace!("switch {} fallback health: {}", switch_name, health.nick());
            }
        }
    }

    /// Implement Reschedule
    #[instrument(level = "debug", name = "cueing", skip(self, ctx), fields(id = %self.id))]
    fn reschedule(
        &mut self,
        ctx: &mut Context<Self>,
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        match self.status {
            NodeStatus::Initial => Ok(()),
            NodeStatus::Starting => Ok(()),
            NodeStatus::Started => {
                if cue_time.is_some() {
                    Err(anyhow!("can't change cue time when playing"))
                } else {
                    Ok(())
                }
            }
            NodeStatus::Stopping => unreachable!(),
            NodeStatus::Stopped => Err(anyhow!("can't reschedule when stopped")),
        }?;

        update_times(&mut self.cue_time, &cue_time, &mut self.end_time, &end_time)?;

        if cue_time.is_some() {
            if let Some(state) = self.state.take() {
                debug!("tearing down previously prerolling pipeline");
                let _ = state.pipeline.set_state(gst::State::Null);

                let audio_appsink: &gst::Element = self.audio_producer.appsink().upcast_ref();
                let video_appsink: &gst::Element = self.video_producer.appsink().upcast_ref();

                state
                    .pipeline
                    .remove_many(&[audio_appsink, video_appsink])
                    .unwrap();

                let _ = state.pipeline_manager.do_send(StopManagerMessage);
            }
            self.status = NodeStatus::Initial;
        }

        self.schedule_state(ctx);

        Ok(())
    }
}

impl Actor for Source {
    type Context = Context<Self>;

    #[instrument(level = "debug", name = "starting", skip(self, _ctx), fields(id = %self.id))]
    fn started(&mut self, _ctx: &mut Self::Context) {}

    #[instrument(level = "debug", name = "stopping", skip(self, _ctx), fields(id = %self.id))]
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if let Some(state) = self.state.take() {
            let _ = state.pipeline_manager.do_send(StopManagerMessage);
        }

        NodeManager::from_registry().do_send(SourceStoppedMessage {
            id: self.id.clone(),
            video_producer: self.video_producer.clone(),
            audio_producer: self.audio_producer.clone(),
        });
    }
}

/// Sent by the [`Source`] to notify itself that a stream started or ended
#[derive(Debug)]
struct StreamMessage {
    /// Whether the stream is starting or ending
    starting: bool,
}

impl Message for StreamMessage {
    type Result = ();
}

impl Handler<StreamMessage> for Source {
    type Result = ();

    fn handle(&mut self, msg: StreamMessage, ctx: &mut Context<Self>) {
        self.handle_stream_change(ctx, msg.starting);
    }
}

impl Handler<SourceCommandMessage> for Source {
    type Result = MessageResult<SourceCommandMessage>;

    fn handle(&mut self, msg: SourceCommandMessage, ctx: &mut Context<Self>) -> Self::Result {
        match msg.command {
            SourceCommand::Play { cue_time, end_time } => {
                MessageResult(self.play(ctx, cue_time, end_time))
            }
        }
    }
}

impl Handler<GetProducerMessage> for Source {
    type Result = MessageResult<GetProducerMessage>;

    fn handle(&mut self, _msg: GetProducerMessage, _ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(Ok((
            self.video_producer.clone(),
            self.audio_producer.clone(),
        )))
    }
}

/// Sent from [`Source`] to [`NodeManager`] to notify that
/// it is stopped
#[derive(Debug)]
pub struct SourceStoppedMessage {
    pub id: String,
    pub video_producer: StreamProducer,
    pub audio_producer: StreamProducer,
}

impl Message for SourceStoppedMessage {
    type Result = ();
}

impl Handler<ErrorMessage> for Source {
    type Result = ();

    fn handle(&mut self, msg: ErrorMessage, ctx: &mut Context<Self>) -> Self::Result {
        error!("Got error message '{}' on source {}", msg.0, self.id,);

        if let Some(state) = &self.state {
            gst::debug_bin_to_dot_file_with_ts(
                &state.pipeline,
                gst::DebugGraphDetails::all(),
                format!("error-source-{}", self.id),
            );
        }

        ctx.stop();
    }
}

/// Sent by the [`Source`] to notify itself that a new `fallbackswitch`
/// was added in `fallbacksrc`
#[derive(Debug)]
struct NewSwitchMessage(gst::Element);

impl Message for NewSwitchMessage {
    type Result = ();
}

impl Handler<NewSwitchMessage> for Source {
    type Result = ();

    fn handle(&mut self, msg: NewSwitchMessage, ctx: &mut Context<Self>) -> Self::Result {
        self.monitor_switch(ctx, msg.0);
    }
}

/// Sent by the [`Source`] to notify itself that the `urisourcebin`
/// was added in `fallbacksrc`
#[derive(Debug)]
struct NewSourceBinMessage(gst::Element);

impl Message for NewSourceBinMessage {
    type Result = ();
}

impl Handler<NewSourceBinMessage> for Source {
    type Result = ();

    fn handle(&mut self, msg: NewSourceBinMessage, _ctx: &mut Context<Self>) -> Self::Result {
        if let Some(ref mut state) = self.state {
            state.source_bin = Some(msg.0);
        }
    }
}

/// Sent by the [`Source`] to notify itself that the status of one
/// of the monitored elements changed
#[derive(Debug)]
struct SourceStatusMessage;

impl Message for SourceStatusMessage {
    type Result = ();
}

impl Handler<SourceStatusMessage> for Source {
    type Result = ();

    fn handle(&mut self, _msg: SourceStatusMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.log_source_status();
    }
}

impl Handler<ScheduleMessage> for Source {
    type Result = Result<(), Error>;

    fn handle(&mut self, msg: ScheduleMessage, ctx: &mut Context<Self>) -> Self::Result {
        self.reschedule(ctx, msg.cue_time, msg.end_time)
    }
}

impl Handler<StopMessage> for Source {
    type Result = Result<(), Error>;

    fn handle(&mut self, _msg: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        ctx.stop();
        Ok(())
    }
}

impl Handler<GetNodeInfoMessage> for Source {
    type Result = Result<NodeInfo, Error>;

    fn handle(&mut self, _msg: GetNodeInfoMessage, _ctx: &mut Context<Self>) -> Self::Result {
        Ok(NodeInfo::Source(SourceInfo {
            uri: self.uri.clone(),
            consumer_slot_ids: self.video_producer.get_consumer_ids(),
            cue_time: self.cue_time,
            end_time: self.end_time,
            status: self.status,
        }))
    }
}
