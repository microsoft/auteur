use actix::prelude::*;
use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use gst::prelude::*;
use std::collections::HashMap;
use tracing::{error, instrument, trace};

use rtmp_switcher_controlling::controller::{MixerCommand, MixerStatus};

use crate::node::{ConsumerMessage, GetProducerMessage, MixerCommandMessage, NodeManager};
use crate::utils::{
    make_element, ErrorMessage, PipelineManager, StopManagerMessage, StreamProducer,
};

struct ConsumerSlot {
    video_producer: StreamProducer,
    audio_producer: StreamProducer,
    video_appsrc: gst_app::AppSrc,
    audio_appsrc: gst_app::AppSrc,

    video_bin: Option<gst::Bin>,
    audio_bin: Option<gst::Bin>,
}

pub struct Mixer {
    id: String,
    cue_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    status: MixerStatus,
    pipeline: gst::Pipeline,
    pipeline_manager: Option<Addr<PipelineManager>>,
    video_producer: StreamProducer,
    audio_producer: StreamProducer,
    consumer_slots: HashMap<String, ConsumerSlot>,
    audio_mixer: Option<gst::Element>,
    video_mixer: Option<gst::Element>,

    state_handle: Option<SpawnHandle>,
}

impl Actor for Mixer {
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

    #[instrument(level = "debug", name = "stopping", skip(self, _ctx), fields(id = %self.id))]
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if let Some(manager) = self.pipeline_manager.take() {
            let _ = manager.do_send(StopManagerMessage);
        }

        for (id, slot) in self.consumer_slots.drain() {
            slot.video_producer.remove_consumer(&id);
            slot.audio_producer.remove_consumer(&id);
        }

        NodeManager::from_registry().do_send(MixerStoppedMessage {
            id: self.id.clone(),
            video_producer: self.video_producer.clone(),
            audio_producer: self.video_producer.clone(),
        });
    }
}

impl Mixer {
    pub fn new(id: &str) -> Self {
        let pipeline = gst::Pipeline::new(None);

        let audio_appsink = gst::ElementFactory::make("appsink", None)
            .unwrap()
            .downcast::<gst_app::AppSink>()
            .unwrap();

        let video_appsink = gst::ElementFactory::make("appsink", None)
            .unwrap()
            .downcast::<gst_app::AppSink>()
            .unwrap();

        pipeline
            .add_many(&[&audio_appsink, &video_appsink])
            .unwrap();

        Self {
            id: id.to_string(),
            cue_time: None,
            end_time: None,
            status: MixerStatus::Initial,
            pipeline,
            pipeline_manager: None,
            audio_producer: StreamProducer::from(&audio_appsink),
            video_producer: StreamProducer::from(&video_appsink),
            consumer_slots: HashMap::new(),
            audio_mixer: None,
            video_mixer: None,
            state_handle: None,
        }
    }

    #[instrument(
        level = "debug",
        name = "connecting",
        skip(pipeline, slot, vmixer, amixer)
    )]
    fn connect_slot(
        pipeline: &gst::Pipeline,
        slot: &mut ConsumerSlot,
        vmixer: &gst::Element,
        amixer: &gst::Element,
        mixer_id: &str,
        id: &str,
    ) -> Result<(), Error> {
        let video_bin = gst::Bin::new(None);
        let audio_bin = gst::Bin::new(None);

        let aconv = make_element("audioconvert", None)?;
        let aresample = make_element("audioconvert", None)?;
        let aqueue = make_element("queue", None)?;
        let vqueue = make_element("queue", None)?;

        let vappsrc_elem: &gst::Element = slot.video_appsrc.upcast_ref();
        let aappsrc_elem: &gst::Element = slot.audio_appsrc.upcast_ref();

        video_bin.add_many(&[vappsrc_elem, &vqueue])?;

        audio_bin.add_many(&[aappsrc_elem, &aconv, &aresample, &aqueue])?;

        pipeline.add_many(&[&video_bin, &audio_bin])?;

        video_bin.sync_state_with_parent()?;
        audio_bin.sync_state_with_parent()?;

        let ghost =
            gst::GhostPad::with_target(Some("src"), &vqueue.static_pad("src").unwrap()).unwrap();
        video_bin.add_pad(&ghost).unwrap();

        let ghost =
            gst::GhostPad::with_target(Some("src"), &aqueue.static_pad("src").unwrap()).unwrap();
        audio_bin.add_pad(&ghost).unwrap();

        let amixer_pad = amixer.request_pad_simple("sink_%u").unwrap();
        let vmixer_pad = vmixer.request_pad_simple("sink_%u").unwrap();

        gst::Element::link_many(&[aappsrc_elem, &aconv, &aresample, &aqueue])?;
        gst::Element::link_many(&[vappsrc_elem, &vqueue])?;

        let srcpad = audio_bin.static_pad("src").unwrap();
        srcpad.link(&amixer_pad).unwrap();

        let srcpad = video_bin.static_pad("src").unwrap();
        srcpad.link(&vmixer_pad).unwrap();

        slot.audio_bin = Some(audio_bin);
        slot.video_bin = Some(video_bin);

        slot.video_producer.add_consumer(&slot.video_appsrc, id);
        slot.audio_producer.add_consumer(&slot.audio_appsrc, id);

        Ok(())
    }

    #[instrument(level = "debug", name = "mixing", skip(self, ctx), fields(id = %self.id))]
    fn start_pipeline(&mut self, ctx: &mut Context<Self>) -> Result<(), Error> {
        let vsrc = make_element("videotestsrc", None)?;
        let vqueue = make_element("queue", None)?;
        let vmixer = make_element("compositor", Some("compositor"))?;
        let vconv = make_element("videoconvert", None)?;
        let vdeinterlace = make_element("deinterlace", None)?;
        let vscale = make_element("videoscale", None)?;
        let vcapsfilter = make_element("capsfilter", None)?;

        let asrc = make_element("audiotestsrc", None)?;
        let aqueue = make_element("queue", None)?;
        let amixer = make_element("audiomixer", Some("audiomixer"))?;
        let acapsfilter = make_element("capsfilter", None)?;

        vsrc.set_property("is-live", &true).unwrap();
        vsrc.set_property_from_str("pattern", "black");
        vmixer.set_property_from_str("background", "black");
        vmixer
            .set_property(
                "start-time-selection",
                &gst_base::AggregatorStartTimeSelection::First,
            )
            .unwrap();

        vcapsfilter
            .set_property(
                "caps",
                &gst::Caps::builder("video/x-raw")
                    .field("width", &1920)
                    .field("height", &1080)
                    .field("framerate", &gst::Fraction::new(30, 1))
                    .field("pixel-aspect-ratio", &gst::Fraction::new(1, 1))
                    .field("format", &"I420")
                    .field("colorimetry", &"bt601")
                    .field("chroma-site", &"jpeg")
                    .field("interlace-mode", &"progressive")
                    .build(),
            )
            .unwrap();
        asrc.set_property("is-live", &true).unwrap();
        asrc.set_property("volume", &0.).unwrap();
        amixer
            .set_property(
                "start-time-selection",
                &gst_base::AggregatorStartTimeSelection::First,
            )
            .unwrap();
        acapsfilter
            .set_property(
                "caps",
                &gst::Caps::builder("audio/x-raw")
                    .field("channels", &2)
                    .field("format", &"S16LE")
                    .field("rate", &44100)
                    .build(),
            )
            .unwrap();

        self.pipeline.add_many(&[
            &vsrc,
            &vqueue,
            &vmixer,
            &vconv,
            &vdeinterlace,
            &vscale,
            &vcapsfilter,
            &asrc,
            &aqueue,
            &amixer,
            &acapsfilter,
        ])?;

        gst::Element::link_many(&[
            &vsrc,
            &vqueue,
            &vmixer,
            &vconv,
            &vdeinterlace,
            &vscale,
            &vcapsfilter,
            self.video_producer.appsink().upcast_ref(),
        ])?;

        gst::Element::link_many(&[
            &asrc,
            &aqueue,
            &amixer,
            &acapsfilter,
            self.audio_producer.appsink().upcast_ref(),
        ])?;

        self.status = MixerStatus::Mixing;

        for (id, slot) in self.consumer_slots.iter_mut() {
            Mixer::connect_slot(&self.pipeline, slot, &vmixer, &amixer, &self.id, id)?;
        }

        self.video_mixer = Some(vmixer);
        self.audio_mixer = Some(amixer);

        let addr = ctx.address().clone();
        let id = self.id.clone();
        self.pipeline.call_async(move |pipeline| {
            if let Err(err) = pipeline.set_state(gst::State::Playing) {
                let _ = addr.do_send(ErrorMessage(format!(
                    "Failed to start mixer {}: {}",
                    id, err
                )));
            }
        });

        self.video_producer.forward();
        self.audio_producer.forward();

        Ok(())
    }

    #[instrument(level = "trace", name = "scheduling", skip(self, ctx), fields(id = %self.id))]
    fn schedule_state(&mut self, ctx: &mut Context<Self>) {
        if let Some(handle) = self.state_handle {
            trace!("cancelling current state scheduling");
            ctx.cancel_future(handle);
        }

        let next_time = match self.status {
            MixerStatus::Initial => self.cue_time,
            MixerStatus::Mixing => self.end_time,
            MixerStatus::Stopped => None,
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
                    MixerStatus::Initial => self.start_pipeline(ctx),
                    MixerStatus::Mixing => {
                        ctx.stop();
                        self.status = MixerStatus::Stopped;
                        Ok(())
                    }
                    MixerStatus::Stopped => Ok(()),
                } {
                    ctx.notify(ErrorMessage(format!(
                        "Failed to change mixer status: {:?}",
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
            MixerStatus::Initial => {
                self.cue_time = Some(cue_time);
                self.end_time = end_time;

                self.schedule_state(ctx);
            }
            _ => {
                return Err(anyhow!("can't start mixer with status {:?}", self.status));
            }
        }

        Ok(())
    }

    #[instrument(level = "debug", name = "connecting", skip(self, video_producer, audio_producer), fields(id = %self.id))]
    fn connect(
        &mut self,
        link_id: &str,
        video_producer: &StreamProducer,
        audio_producer: &StreamProducer,
    ) -> Result<(), Error> {
        if self.consumer_slots.contains_key(link_id) {
            return Err(anyhow!("mixer {} already has link {}", self.id, link_id));
        }

        let video_appsrc = gst::ElementFactory::make("appsrc", None)
            .unwrap()
            .downcast::<gst_app::AppSrc>()
            .unwrap();
        let audio_appsrc = gst::ElementFactory::make("appsrc", None)
            .unwrap()
            .downcast::<gst_app::AppSrc>()
            .unwrap();

        for appsrc in &[&video_appsrc, &audio_appsrc] {
            appsrc.set_format(gst::Format::Time);
            appsrc.set_is_live(true);
            appsrc.set_handle_segment_change(true);
        }

        let mut slot = ConsumerSlot {
            video_producer: video_producer.clone(),
            audio_producer: audio_producer.clone(),
            video_appsrc: video_appsrc.clone(),
            audio_appsrc: audio_appsrc.clone(),
            audio_bin: None,
            video_bin: None,
        };

        if self.status == MixerStatus::Mixing {
            let vmixer = self.video_mixer.clone().unwrap();
            let amixer = self.audio_mixer.clone().unwrap();

            if let Err(err) = Mixer::connect_slot(
                &self.pipeline,
                &mut slot,
                &vmixer,
                &amixer,
                &self.id,
                &link_id,
            ) {
                return Err(err);
            }
        }

        self.consumer_slots.insert(link_id.to_string(), slot);

        Ok(())
    }

    #[instrument(level = "debug", name = "disconnecting", skip(self), fields(id = %self.id))]
    fn disconnect(&mut self, slot_id: &str) -> Result<(), Error> {
        if let Some(slot) = self.consumer_slots.remove(slot_id) {
            slot.video_producer.remove_consumer(slot_id);
            slot.audio_producer.remove_consumer(slot_id);
            if let Some(video_bin) = slot.video_bin {
                let mixer_pad = video_bin.static_pad("src").unwrap().peer().unwrap();

                video_bin.set_locked_state(true);
                video_bin.set_state(gst::State::Null).unwrap();
                self.pipeline.remove(&video_bin).unwrap();

                self.video_mixer
                    .clone()
                    .unwrap()
                    .release_request_pad(&mixer_pad);
            }
            if let Some(audio_bin) = slot.audio_bin {
                let mixer_pad = audio_bin.static_pad("src").unwrap().peer().unwrap();

                audio_bin.set_locked_state(true);
                audio_bin.set_state(gst::State::Null).unwrap();
                self.pipeline.remove(&audio_bin).unwrap();

                self.audio_mixer
                    .clone()
                    .unwrap()
                    .release_request_pad(&mixer_pad);
            }

            Ok(())
        } else {
            Err(anyhow!("mixer {} has no slot with id {}", self.id, slot_id))
        }
    }
}

impl Handler<ConsumerMessage> for Mixer {
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

impl Handler<MixerCommandMessage> for Mixer {
    type Result = MessageResult<MixerCommandMessage>;

    fn handle(&mut self, msg: MixerCommandMessage, ctx: &mut Context<Self>) -> Self::Result {
        match msg.command {
            MixerCommand::Start { cue_time, end_time } => {
                MessageResult(self.start(ctx, cue_time, end_time))
            }
        }
    }
}

#[derive(Debug)]
pub struct MixerStoppedMessage {
    pub id: String,
    pub video_producer: StreamProducer,
    pub audio_producer: StreamProducer,
}

impl Message for MixerStoppedMessage {
    type Result = ();
}

impl Handler<ErrorMessage> for Mixer {
    type Result = ();

    fn handle(&mut self, msg: ErrorMessage, ctx: &mut Context<Self>) -> Self::Result {
        error!("Got error message '{}' on destination {}", msg.0, self.id,);

        ctx.stop();
    }
}

impl Handler<GetProducerMessage> for Mixer {
    type Result = MessageResult<GetProducerMessage>;

    fn handle(&mut self, _msg: GetProducerMessage, _ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(Ok((
            self.video_producer.clone(),
            self.audio_producer.clone(),
        )))
    }
}
