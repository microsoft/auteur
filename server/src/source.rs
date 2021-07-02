//! A source processing node.
//!
//! The only supported source type is created with a URI. In the future
//! generators could also be supported, for example to display a countdown.
//!
//! The main complexity for this node is the [`starting`](State::Starting)
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
    GetNodeInfoMessage, GetProducerMessage, NodeManager, NodeStatusMessage, ScheduleMessage,
    SourceCommandMessage, StartMessage, StopMessage, StoppedMessage,
};
use crate::utils::{
    make_element, ErrorMessage, PipelineManager, Schedulable, StateChangeResult, StateMachine,
    StopManagerMessage, StreamProducer,
};
use actix::prelude::*;
use anyhow::Error;
use auteur_controlling::controller::{NodeInfo, SourceInfo, State};
use chrono::{DateTime, Utc};
use gst::prelude::*;
use tracing::{debug, error, instrument, trace};

/// The pipeline and various GStreamer elements that the source
/// optionally wraps, their lifetime is not directly bound to that
/// of the source itself
#[derive(Debug)]
struct Media {
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
    /// Output audio producer
    audio_producer: StreamProducer,
    /// Output video producer
    video_producer: StreamProducer,
    /// GStreamer elements when prerolling or playing
    media: Option<Media>,
    /// Scheduling timer
    state_handle: Option<SpawnHandle>,
    /// Statistics timer
    monitor_handle: Option<SpawnHandle>,
    /// Our state machine
    state_machine: StateMachine,
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
            audio_producer: StreamProducer::from(&audio_appsink),
            video_producer: StreamProducer::from(&video_appsink),
            media: None,
            state_handle: None,
            monitor_handle: None,
            state_machine: StateMachine::default(),
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
    fn preroll(&mut self, ctx: &mut Context<Self>) -> Result<StateChangeResult, Error> {
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

        let addr_clone = ctx.address();
        src.connect("notify::status", false, move |_args| {
            let _ = addr_clone.do_send(SourceStatusMessage);
            None
        })
        .unwrap();

        let addr_clone = ctx.address();
        src.connect("notify::statistics", false, move |_args| {
            let _ = addr_clone.do_send(SourceStatusMessage);
            None
        })
        .unwrap();

        let src_bin: &gst::Bin = src.downcast_ref().unwrap();

        let addr = ctx.address();
        src_bin.connect_deep_element_added(move |_src, _bin, element| {
            if element.has_property("primary-health", None) {
                let _ = addr.do_send(NewSwitchMessage(element.clone()));
            }

            if element.type_().name() == "GstURISourceBin" {
                let _ = addr.do_send(NewSourceBinMessage(element.clone()));
            }
        });

        debug!("now prerolling");

        self.media = Some(Media {
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

        let addr = ctx.address();
        let id = self.id.clone();
        pipeline.call_async(move |pipeline| {
            if let Err(err) = pipeline.set_state(gst::State::Playing) {
                let _ = addr.do_send(ErrorMessage(format!(
                    "Failed to start source {}: {}",
                    id, err
                )));
            }
        });

        Ok(StateChangeResult::Success)
    }

    /// Unblock a prerolling pipeline
    #[instrument(level = "debug", name = "unblocking", skip(self), fields(id = %self.id))]
    fn unblock(&mut self, ctx: &mut Context<Self>) -> Result<StateChangeResult, Error> {
        let media = self.media.as_ref().unwrap();

        media.src.emit_by_name("unblock", &[]).unwrap();
        self.video_producer.forward();
        self.audio_producer.forward();

        debug!("unblocked, now playing");

        let id_clone = self.id.clone();
        self.monitor_handle = Some(ctx.run_interval(
            std::time::Duration::from_secs(1),
            move |s, _ctx| {
                if let Some(ref media) = s.media {
                    if let Some(ref source_bin) = media.source_bin {
                        let val = source_bin.property("statistics").unwrap();
                        let s = val.get::<gst::Structure>().unwrap();

                        trace!(id = %id_clone, "source statistics: {}", s.to_string());
                    }
                }
            },
        ));

        Ok(StateChangeResult::Success)
    }

    /// A new pad was added, or an existing pad EOS'd
    fn handle_stream_change(&mut self, ctx: &mut Context<Self>, starting: bool) {
        if let Some(ref mut media) = self.media {
            if starting {
                media.n_streams += 1;

                debug!(id = %self.id, n_streams = %media.n_streams, "new active stream");
            } else {
                media.n_streams -= 1;

                debug!(id = %self.id, n_streams = %media.n_streams, "active stream finished");

                if media.n_streams == 0 {
                    self.stop(ctx)
                }
            }
        }
    }

    /// Track the status of a new fallbackswitch
    #[instrument(level = "debug", name = "new-fallbackswitch", skip(self, ctx), fields(id = %self.id))]
    fn monitor_switch(&mut self, ctx: &mut Context<Self>, switch: gst::Element) {
        if let Some(ref mut media) = self.media {
            let addr_clone = ctx.address();
            switch
                .connect("notify::primary-health", false, move |_args| {
                    let _ = addr_clone.do_send(SourceStatusMessage);
                    None
                })
                .unwrap();

            let addr_clone = ctx.address();
            switch
                .connect("notify::fallback-health", false, move |_args| {
                    let _ = addr_clone.do_send(SourceStatusMessage);
                    None
                })
                .unwrap();

            media.switches.push(switch);
        }
    }

    /// Trace the status of the source for monitoring purposes
    #[instrument(level = "trace", name = "new-source-status", skip(self), fields(id = %self.id))]
    fn log_source_status(&mut self) {
        if let Some(ref media) = self.media {
            let value = media.src.property("status").unwrap();
            let status = gst::glib::EnumValue::from_value(&value).expect("Not an enum type");
            trace!("Source status: {}", status.nick());
            trace!(
                "Source statistics: {:?}",
                media.src.property("statistics").unwrap()
            );

            for switch in &media.switches {
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

    #[instrument(level = "debug", skip(self), fields(id = %self.id))]
    fn reinitialize(&mut self) -> Result<StateChangeResult, Error> {
        if let Some(media) = self.media.take() {
            debug!("tearing down previously prerolling pipeline");
            let _ = media.pipeline.set_state(gst::State::Null);

            let audio_appsink: &gst::Element = self.audio_producer.appsink().upcast_ref();
            let video_appsink: &gst::Element = self.video_producer.appsink().upcast_ref();

            media
                .pipeline
                .remove_many(&[audio_appsink, video_appsink])
                .unwrap();

            let _ = media.pipeline_manager.do_send(StopManagerMessage);
        }

        Ok(StateChangeResult::Success)
    }

    #[instrument(level = "debug", skip(self, ctx), fields(id = %self.id))]
    fn stop(&mut self, ctx: &mut Context<Self>) {
        self.stop_schedule(ctx);
        ctx.stop();
    }
}

impl Actor for Source {
    type Context = Context<Self>;

    #[instrument(level = "debug", name = "starting", skip(self, _ctx), fields(id = %self.id))]
    fn started(&mut self, _ctx: &mut Self::Context) {}

    #[instrument(level = "debug", name = "stopping", skip(self, _ctx), fields(id = %self.id))]
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if let Some(media) = self.media.take() {
            let _ = media.pipeline_manager.do_send(StopManagerMessage);
        }

        NodeManager::from_registry().do_send(StoppedMessage {
            id: self.id.clone(),
            video_producer: Some(self.video_producer.clone()),
            audio_producer: Some(self.audio_producer.clone()),
        });
    }
}

impl Schedulable<Self> for Source {
    fn state_machine(&self) -> &StateMachine {
        &self.state_machine
    }

    fn state_machine_mut(&mut self) -> &mut StateMachine {
        &mut self.state_machine
    }

    fn node_id(&self) -> &str {
        &self.id
    }

    fn next_time(&self) -> Option<DateTime<Utc>> {
        match self.state_machine.state {
            State::Initial => self
                .state_machine
                .cue_time
                .map(|cue_time| cue_time - chrono::Duration::seconds(10)),
            State::Starting => self.state_machine.cue_time,
            State::Started => self.state_machine.end_time,
            State::Stopping => None,
            State::Stopped => None,
        }
    }

    #[instrument(level = "debug", skip(self, ctx), fields(id = %self.id))]
    fn transition(
        &mut self,
        ctx: &mut Context<Self>,
        target: State,
    ) -> Result<StateChangeResult, Error> {
        match target {
            State::Initial => self.reinitialize(),
            State::Starting => self.preroll(ctx),
            State::Started => self.unblock(ctx),
            State::Stopping => Ok(StateChangeResult::Skip),
            State::Stopped => {
                self.stop(ctx);
                Ok(StateChangeResult::Success)
            }
        }
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

impl Handler<StartMessage> for Source {
    type Result = MessageResult<StartMessage>;

    fn handle(&mut self, msg: StartMessage, ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(self.start_schedule(ctx, msg.cue_time, msg.end_time))
    }
}

impl Handler<SourceCommandMessage> for Source {
    type Result = MessageResult<SourceCommandMessage>;

    fn handle(&mut self, _msg: SourceCommandMessage, _ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(Ok(()))
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

impl Handler<ErrorMessage> for Source {
    type Result = ();

    fn handle(&mut self, msg: ErrorMessage, ctx: &mut Context<Self>) -> Self::Result {
        error!("Got error message '{}' on source {}", msg.0, self.id,);

        NodeManager::from_registry().do_send(NodeStatusMessage::Error {
            id: self.id.clone(),
            message: msg.0,
        });

        if let Some(media) = &self.media {
            gst::debug_bin_to_dot_file_with_ts(
                &media.pipeline,
                gst::DebugGraphDetails::all(),
                format!("error-source-{}", self.id),
            );
        }

        self.stop(ctx);
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
        if let Some(ref mut media) = self.media {
            media.source_bin = Some(msg.0);
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
        self.stop(ctx);
        Ok(())
    }
}

impl Handler<GetNodeInfoMessage> for Source {
    type Result = Result<NodeInfo, Error>;

    fn handle(&mut self, _msg: GetNodeInfoMessage, _ctx: &mut Context<Self>) -> Self::Result {
        Ok(NodeInfo::Source(SourceInfo {
            uri: self.uri.clone(),
            consumer_slot_ids: self.video_producer.get_consumer_ids(),
            cue_time: self.state_machine.cue_time,
            end_time: self.state_machine.end_time,
            state: self.state_machine.state,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::get_now;
    use crate::utils::tests::*;
    use std::collections::VecDeque;
    use test_env_log::test;

    #[actix_rt::test]
    #[test]
    async fn test_status_after_create() {
        gst::init().unwrap();
        let uri = asset_uri("ball.mp4");

        // Create a valid source
        create_source("test-source", &uri).await.unwrap();

        let info = node_info_unchecked("test-source").await;

        if let NodeInfo::Source(sinfo) = info {
            assert_eq!(sinfo.uri, uri);
            assert!(sinfo.consumer_slot_ids.is_empty());
            assert!(sinfo.cue_time.is_none());
            assert!(sinfo.end_time.is_none());
            assert_eq!(sinfo.state, State::Initial);
        } else {
            panic!("Wrong info type");
        }
    }

    #[actix_rt::test]
    #[test]
    async fn test_start_immediate() {
        gst::init().unwrap();
        let uri = asset_uri("ball.mp4");

        // Expect state to progress to Started with no hiccup
        let listener_addr = register_listener(
            "test-source",
            "test-listener",
            VecDeque::from(vec![State::Starting, State::Started]),
        )
        .await;

        // Create a valid source
        create_source("test-source", &uri).await.unwrap();

        // Start it up immediately
        start_node("test-source", None, None).await.unwrap();

        let progression_result = listener_addr.send(WaitForProgressionMessage).await.unwrap();

        assert!(progression_result.progressed_as_expected);
    }

    #[actix_rt::test]
    #[test]
    async fn test_reschedule() {
        gst::init().unwrap();

        let uri = asset_uri("ball.mp4");

        // Expect state to progress to Started with no hiccup
        let listener_addr = register_listener(
            "test-source",
            "test-listener",
            VecDeque::from(vec![
                State::Starting,
                State::Initial,
                State::Starting,
                State::Started,
            ]),
        )
        .await;

        // Create a valid source
        create_source("test-source", &uri).await.unwrap();

        // "Pause" time, which will cause it to progress immediately on Sleep (eg actix' run_later)
        tokio::time::pause();

        let fut = tokio::time::sleep(std::time::Duration::from_secs(10));

        let cue_time = get_now() + chrono::Duration::seconds(15);

        tracing::info!("Scheduling for {:?}", cue_time);

        // Start it up immediately
        start_node("test-source", Some(cue_time), None)
            .await
            .unwrap();

        // Wait for the node to have prerolled but not started
        fut.await;

        let info = node_info_unchecked("test-source").await;

        if let NodeInfo::Source(sinfo) = info {
            assert_eq!(sinfo.state, State::Starting);
        } else {
            panic!("Wrong info type");
        }

        let cue_time = get_now() + chrono::Duration::seconds(25);

        // Reschedule, node state should reset to Initial
        reschedule_node("test-source", Some(cue_time), None)
            .await
            .unwrap();

        let progression_result = listener_addr.send(WaitForProgressionMessage).await.unwrap();

        assert!(progression_result.progressed_as_expected);
    }
}
