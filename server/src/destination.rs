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
    AddControlPointMessage, ConsumerMessage, GetNodeInfoMessage, NodeManager, NodeStatusMessage,
    RemoveControlPointMessage, ScheduleMessage, StartMessage, StopMessage, StoppedMessage,
};
use crate::utils::{
    make_element, ErrorMessage, PipelineManager, Schedulable, StateChangeResult, StateMachine,
    StopManagerMessage, StreamProducer, WaitForEosMessage,
};

/// Represents a potential connection to a producer
struct ConsumerSlot {
    /// Identifier of the slot
    id: String,
    /// stream producer
    producer: StreamProducer,
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
    video_appsrc: Option<gst_app::AppSrc>,
    /// Audio input to the node
    audio_appsrc: Option<gst_app::AppSrc>,
    /// Optional audio connection point
    audio_slot: Option<ConsumerSlot>,
    /// Optional video connection point
    video_slot: Option<ConsumerSlot>,
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

        self.disconnect_consumers();

        NodeManager::from_registry().do_send(StoppedMessage {
            id: self.id.clone(),
            video_producer: None,
            audio_producer: None,
        });
    }
}

impl Destination {
    /// Create a destination
    #[instrument(level = "debug", name = "creating")]
    pub fn new(id: &str, family: &DestinationFamily, audio: bool, video: bool) -> Self {
        let video_appsrc = if video {
            Some(
                gst::ElementFactory::make(
                    "appsrc",
                    Some(&format!("destination-video-appsrc-{}", id)),
                )
                .unwrap()
                .downcast::<gst_app::AppSrc>()
                .unwrap(),
            )
        } else {
            None
        };

        let audio_appsrc = if audio {
            Some(
                gst::ElementFactory::make(
                    "appsrc",
                    Some(&format!("destination-audio-appsrc-{}", id)),
                )
                .unwrap()
                .downcast::<gst_app::AppSrc>()
                .unwrap(),
            )
        } else {
            None
        };

        for appsrc in [&video_appsrc, &audio_appsrc].iter().copied().flatten() {
            gst_utils::StreamProducer::configure_consumer(appsrc);
        }

        let pipeline = gst::Pipeline::new(None);

        Self {
            id: id.to_string(),
            family: family.clone(),
            pipeline,
            pipeline_manager: None,
            video_appsrc,
            audio_appsrc,
            audio_slot: None,
            video_slot: None,
            state_machine: StateMachine::default(),
        }
    }

    /// Disconnect our consumer slots
    #[instrument(level = "debug", name = "disconnecting consumers", skip(self), fields(id = %self.id))]
    pub fn disconnect_consumers(&mut self) {
        if let Some(slot) = self.audio_slot.take() {
            slot.producer.remove_consumer(&slot.id);
        }

        if let Some(slot) = self.video_slot.take() {
            slot.producer.remove_consumer(&slot.id);
        }
    }

    /// Connect our consumer slots to `StreamProducers`
    #[instrument(level = "debug", name = "connecting consumers", skip(self), fields(id = %self.id))]
    pub fn connect_consumers(&self) -> Result<(), Error> {
        if let Some(ref appsrc) = self.audio_appsrc {
            if let Some(ref slot) = self.audio_slot {
                slot.producer.add_consumer(appsrc, &slot.id);
            } else {
                return Err(anyhow!(
                    "Destination {} must be connected on its input audio slot before starting",
                    self.id,
                ));
            }
        }

        if let Some(ref appsrc) = self.video_appsrc {
            if let Some(ref slot) = self.video_slot {
                slot.producer.add_consumer(appsrc, &slot.id);
            } else {
                return Err(anyhow!(
                    "Destination {} must be connected on its input video slot before starting",
                    self.id,
                ));
            }
        }

        Ok(())
    }

    /// RTMP family
    #[instrument(level = "debug", name = "streaming", skip(self, ctx), fields(id = %self.id))]
    fn start_rtmp_pipeline(
        &mut self,
        ctx: &mut Context<Self>,
        uri: &str,
    ) -> Result<StateChangeResult, Error> {
        let mux = make_element("flvmux", None)?;
        let mux_queue = make_element("queue", None)?;
        let sink = make_element("rtmp2sink", None)?;

        self.pipeline.add_many(&[&mux, &mux_queue, &sink])?;

        sink.set_property("location", uri);

        mux.set_property("streamable", &true);
        mux.set_property("latency", &1000000000u64);

        mux.set_property(
            "start-time-selection",
            gst_base::AggregatorStartTimeSelection::First,
        );

        gst::Element::link_many(&[&mux, &mux_queue, &sink])?;

        if let Some(ref appsrc) = self.video_appsrc {
            let vconv = make_element("videoconvert", None)?;
            let timecodestamper = make_element("timecodestamper", None)?;
            let timeoverlay = make_element("timeoverlay", None)?;
            let venc = make_element("nvh264enc", None).unwrap_or(make_element("x264enc", None)?);
            let vparse = make_element("h264parse", None)?;
            let venc_queue = make_element("queue", None)?;

            self.pipeline.add_many(&[
                appsrc.upcast_ref(),
                &vconv,
                &timecodestamper,
                &timeoverlay,
                &venc,
                &vparse,
                &venc_queue,
            ])?;

            if venc.has_property("tune", None) {
                venc.set_property_from_str("tune", "zerolatency");
            } else if venc.has_property("zerolatency", None) {
                venc.set_property("zerolatency", &true);
            }

            if venc.has_property("key-int-max", None) {
                venc.set_property("key-int-max", &30u32);
            } else if venc.has_property("gop-size", None) {
                venc.set_property("gop-size", &30i32);
            }

            vparse.set_property("config-interval", &-1i32);
            timecodestamper.set_property_from_str("source", "rtc");
            timeoverlay.set_property_from_str("time-mode", "time-code");
            venc_queue.set_properties(&[
                ("max-size-buffers", &0u32),
                ("max-size-bytes", &0u32),
                ("max-size-time", &(3 * gst::ClockTime::SECOND)),
            ]);

            gst::Element::link_many(&[
                appsrc.upcast_ref(),
                &vconv,
                &timecodestamper,
                &timeoverlay,
                &venc,
                &vparse,
                &venc_queue,
                &mux,
            ])?;
        }

        if let Some(ref appsrc) = self.audio_appsrc {
            let aconv = make_element("audioconvert", None)?;
            let aresample = make_element("audioresample", None)?;
            let aenc = make_element("faac", None)?;
            let aenc_queue = make_element("queue", None)?;

            self.pipeline.add_many(&[
                appsrc.upcast_ref(),
                &aconv,
                &aresample,
                &aenc,
                &aenc_queue,
            ])?;

            aenc_queue.set_properties(&[
                ("max-size-buffers", &0u32),
                ("max-size-bytes", &0u32),
                ("max-size-time", &(3 * gst::ClockTime::SECOND)),
            ]);

            gst::Element::link_many(&[
                appsrc.upcast_ref(),
                &aconv,
                &aresample,
                &aenc,
                &aenc_queue,
                &mux,
            ])?;
        }

        self.connect_consumers()?;

        let sink_clone = sink.downgrade();
        let id_clone = self.id.clone();
        ctx.run_interval(std::time::Duration::from_secs(1), move |_s, _ctx| {
            if let Some(sink) = sink_clone.upgrade() {
                let s = sink.property::<gst::Structure>("stats");

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
        let multiqueue = make_element("multiqueue", None)?;
        let sink = make_element("splitmuxsink", None)?;

        self.pipeline.add_many(&[&multiqueue, &sink])?;

        if let Some(max_size_time) = max_size_time {
            sink.set_property(
                "max-size-time",
                (max_size_time as u64) * gst::ClockTime::MSECOND,
            );
            sink.set_property("use-robust-muxing", &true);
            let mux = make_element("qtmux", None)?;
            mux.set_property("reserved-moov-update-period", &gst::ClockTime::SECOND);
            sink.set_property("muxer", &mux);
            let location = base_name.to_owned() + "%05d.mp4";
            sink.set_property("location", &location);
        } else {
            let location = base_name.to_owned() + ".mp4";
            sink.set_property("location", &location);
        }

        if let Some(ref appsrc) = self.video_appsrc {
            let vconv = make_element("videoconvert", None)?;
            let venc = make_element("nvh264enc", None).unwrap_or(make_element("x264enc", None)?);
            let vparse = make_element("h264parse", None)?;

            self.pipeline
                .add_many(&[appsrc.upcast_ref(), &vconv, &venc, &vparse])?;

            gst::Element::link_many(&[appsrc.upcast_ref(), &vconv, &venc, &vparse])?;
            vparse.link_pads(None, &multiqueue, Some("sink_0"))?;
            multiqueue.link_pads(Some("src_0"), &sink, Some("video"))?;
        }

        if let Some(ref appsrc) = self.audio_appsrc {
            let aconv = make_element("audioconvert", None)?;
            let aresample = make_element("audioresample", None)?;
            let aenc = make_element("faac", None)?;

            self.pipeline
                .add_many(&[appsrc.upcast_ref(), &aconv, &aresample, &aenc])?;

            aenc.link_pads(None, &multiqueue, Some("sink_1"))?;
            multiqueue.link_pads(Some("src_1"), &sink, Some("audio_0"))?;
            gst::Element::link_many(&[appsrc.upcast_ref(), &aconv, &aresample, &aenc])?;
        }

        self.connect_consumers()?;

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

    /// LocalPlayback family
    #[instrument(level = "debug", name = "playing on local devices", skip(self, ctx), fields(id = %self.id))]
    fn start_local_playback_pipeline(
        &mut self,
        ctx: &mut Context<Self>,
    ) -> Result<StateChangeResult, Error> {
        if let Some(ref appsrc) = self.video_appsrc {
            let vqueue = make_element("queue", None)?;
            let vconv = make_element("videoconvert", None)?;
            let vsink = make_element("autovideosink", None)?;

            self.pipeline
                .add_many(&[appsrc.upcast_ref(), &vqueue, &vconv, &vsink])?;

            gst::Element::link_many(&[appsrc.upcast_ref(), &vqueue, &vconv, &vsink])?;
        }

        if let Some(ref appsrc) = self.audio_appsrc {
            let aqueue = make_element("queue", None)?;
            let aconv = make_element("audioconvert", None)?;
            let aresample = make_element("audioresample", None)?;
            let asink = make_element("autoaudiosink", None)?;

            self.pipeline
                .add_many(&[appsrc.upcast_ref(), &aqueue, &aconv, &aresample, &asink])?;

            gst::Element::link_many(&[appsrc.upcast_ref(), &aqueue, &aconv, &aresample, &asink])?;
        }

        self.connect_consumers()?;

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
        video_producer: Option<StreamProducer>,
        audio_producer: Option<StreamProducer>,
    ) -> Result<(), Error> {
        let video_slot = if let Some(producer) = video_producer {
            if self.video_slot.is_some() {
                return Err(anyhow!("destination already has a video producer"));
            }

            if self.video_appsrc.is_some() {
                Some(ConsumerSlot {
                    id: link_id.to_string(),
                    producer,
                })
            } else {
                None
            }
        } else {
            None
        };

        let audio_slot = if let Some(producer) = audio_producer {
            if self.audio_slot.is_some() {
                return Err(anyhow!("destination already has an audio producer"));
            }

            if self.audio_appsrc.is_some() {
                Some(ConsumerSlot {
                    id: link_id.to_string(),
                    producer,
                })
            } else {
                None
            }
        } else {
            None
        };

        if audio_slot.is_none() && video_slot.is_none() {
            return Err(anyhow!(
                "destination {} link {} must result in at least one audio / video connection",
                self.id,
                link_id
            ));
        }

        if audio_slot.is_some() {
            self.audio_slot = audio_slot;
        }

        if video_slot.is_some() {
            self.video_slot = video_slot;
        }

        Ok(())
    }

    /// Implement Disconnect command
    #[instrument(level = "debug", name = "disconnecting", skip(self), fields(id = %self.id))]
    fn disconnect(&mut self, link_id: &str) -> Result<(), Error> {
        let mut ret = Err(anyhow!(
            "destination {} has no slot with id {}",
            self.id,
            link_id
        ));

        if let Some(slot) = self.audio_slot.take() {
            if slot.id == link_id {
                slot.producer.remove_consumer(&slot.id);
                ret = Ok(());
            } else {
                self.audio_slot = Some(slot);
            }
        }

        if let Some(slot) = self.video_slot.take() {
            if slot.id == link_id {
                slot.producer.remove_consumer(&slot.id);
                ret = Ok(());
            } else {
                self.audio_slot = Some(slot);
            }
        }

        ret
    }

    /// Wait for EOS to propagate down our pipeline before stopping
    // Returns true if calling code should wait before fully stopping
    #[instrument(level = "debug", name = "checking if waiting for EOS is needed", skip(self, ctx), fields(id = %self.id))]
    fn wait_for_eos(&mut self, ctx: &mut Context<Self>) -> bool {
        match self.state_machine.state {
            State::Initial | State::Stopped => false,
            State::Starting => false,
            _ => {
                self.disconnect_consumers();

                let pipeline_manager = self.pipeline_manager.as_ref().unwrap();

                debug!("waiting for EOS");

                if let Some(ref appsrc) = self.video_appsrc {
                    appsrc.send_event(gst::event::Eos::new());
                }

                if let Some(ref appsrc) = self.audio_appsrc {
                    appsrc.send_event(gst::event::Eos::new());
                }

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
                DestinationFamily::LocalPlayback => self.start_local_playback_pipeline(ctx),
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
                ..
            } => MessageResult(self.connect(&link_id, video_producer, audio_producer)),
            ConsumerMessage::Disconnect { slot_id } => MessageResult(self.disconnect(&slot_id)),
            ConsumerMessage::AddControlPoint { .. } => {
                MessageResult(Err(anyhow!("destination slot cannot be controlled")))
            }
            ConsumerMessage::RemoveControlPoint { .. } => {
                MessageResult(Err(anyhow!("destination slot cannot be controlled")))
            }
        }
    }
}

impl Handler<StartMessage> for Destination {
    type Result = MessageResult<StartMessage>;

    fn handle(&mut self, msg: StartMessage, ctx: &mut Context<Self>) -> Self::Result {
        if self.audio_appsrc.is_some() && self.audio_slot.is_none() {
            return MessageResult(Err(anyhow!(
                "Destination {} must have its audio slot connected before starting",
                self.id
            )));
        }

        if self.video_appsrc.is_some() && self.video_slot.is_none() {
            return MessageResult(Err(anyhow!(
                "Destination {} must have its video slot connected before starting",
                self.id
            )));
        }

        MessageResult(self.start_schedule(ctx, msg.cue_time, msg.end_time))
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
        if self.audio_appsrc.is_some() && self.audio_slot.is_none() {
            return Err(anyhow!(
                "Destination {} must have its audio slot connected before starting",
                self.id
            ));
        }

        if self.video_appsrc.is_some() && self.video_slot.is_none() {
            return Err(anyhow!(
                "Destination {} must have its video slot connected before starting",
                self.id
            ));
        }

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
            audio_slot_id: self.audio_slot.as_ref().map(|slot| slot.id.clone()),
            video_slot_id: self.video_slot.as_ref().map(|slot| slot.id.clone()),
            cue_time: self.state_machine.cue_time,
            end_time: self.state_machine.end_time,
            state: self.state_machine.state,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::tests::*;
    use tempfile::tempdir;
    use test_log::test;

    #[actix_rt::test]
    #[test]
    async fn test_status_after_create() {
        gst::init().unwrap();
        let dir = tempdir().unwrap();

        let base_name = format!("{}/video", dir.path().display());

        create_local_destination("test-destination", &base_name, Some(5000))
            .await
            .unwrap();

        let info = node_info_unchecked("test-destination").await;

        if let NodeInfo::Destination(dinfo) = info {
            assert_eq!(
                dinfo.family,
                DestinationFamily::LocalFile {
                    base_name,
                    max_size_time: Some(5000),
                }
            );
            assert!(dinfo.audio_slot_id.is_none());
            assert!(dinfo.video_slot_id.is_none());
            assert!(dinfo.cue_time.is_none());
            assert!(dinfo.end_time.is_none());
            assert_eq!(dinfo.state, State::Initial);
        } else {
            panic!("Wrong info type");
        }
    }
}

impl Handler<AddControlPointMessage> for Destination {
    type Result = Result<(), Error>;

    fn handle(&mut self, _msg: AddControlPointMessage, _ctx: &mut Context<Self>) -> Self::Result {
        Err(anyhow!("Destination has no property to control"))
    }
}

impl Handler<RemoveControlPointMessage> for Destination {
    type Result = ();

    fn handle(
        &mut self,
        _msg: RemoveControlPointMessage,
        _ctx: &mut Context<Self>,
    ) -> Self::Result {
    }
}
