use actix::prelude::*;
use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use gst::prelude::*;
use std::collections::HashMap;
use tracing::{debug, error, info, trace};

use rtmp_switcher_controlling::controller::{MixerCommand, MixerStatus};

use crate::node::{ConsumerMessage, GetProducerMessage, MixerCommandMessage, NodeManager};
use crate::utils::{make_element, ErrorMessage, PipelineManager, StreamProducer};

struct ConsumerSlot {
    video_producer: StreamProducer,
    audio_producer: StreamProducer,
    video_appsrc: gst_app::AppSrc,
    audio_appsrc: gst_app::AppSrc,
    video_pad: Option<gst::Pad>,
    audio_pad: Option<gst::Pad>,
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

    fn started(&mut self, ctx: &mut Self::Context) {
        info!("Started destination {}", self.id);

        self.pipeline_manager = Some(
            PipelineManager::new(self.pipeline.clone(), ctx.address().recipient().clone()).start(),
        );
    }

    /// Called once the mixer is stopped
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        let _ = self.pipeline_manager.take();

        for (id, slot) in self.consumer_slots.drain() {
            slot.video_producer.remove_consumer(&id);
            slot.audio_producer.remove_consumer(&id);
        }

        NodeManager::from_registry().do_send(MixerStoppedMessage {
            id: self.id.clone(),
            video_producer: self.video_producer.clone(),
            audio_producer: self.video_producer.clone(),
        });

        info!("Stopped mixer {}", self.id);
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

        debug!("{} is now mixing", self.id);

        self.status = MixerStatus::Mixing;

        for (id, mut slot) in self.consumer_slots.iter_mut() {
            let aconv = make_element("audioconvert", None)?;
            let aresample = make_element("audioconvert", None)?;
            let aqueue = make_element("queue", None)?;
            let vqueue = make_element("queue", None)?;

            let vappsrc_elem: &gst::Element = slot.video_appsrc.upcast_ref();
            let aappsrc_elem: &gst::Element = slot.audio_appsrc.upcast_ref();

            self.pipeline.add_many(&[
                &aconv,
                &aresample,
                &aqueue,
                &vqueue,
                vappsrc_elem,
                aappsrc_elem,
            ])?;

            aconv.sync_state_with_parent()?;
            aresample.sync_state_with_parent()?;
            aqueue.sync_state_with_parent()?;
            vqueue.sync_state_with_parent()?;
            vappsrc_elem.sync_state_with_parent()?;
            aappsrc_elem.sync_state_with_parent()?;

            let amixer_pad = amixer.request_pad_simple("sink_%u").unwrap();
            let vmixer_pad = vmixer.request_pad_simple("sink_%u").unwrap();

            let aq_srcpad = aqueue.static_pad("src").unwrap();
            let vq_srcpad = vqueue.static_pad("src").unwrap();

            gst::Element::link_many(&[aappsrc_elem, &aconv, &aresample, &aqueue])?;
            gst::Element::link_many(&[vappsrc_elem, &vqueue])?;

            aq_srcpad.link(&amixer_pad).unwrap();
            vq_srcpad.link(&vmixer_pad).unwrap();

            slot.audio_pad = Some(amixer_pad);
            slot.video_pad = Some(vmixer_pad);

            slot.video_producer.add_consumer(&slot.video_appsrc, &id);
            slot.audio_producer.add_consumer(&slot.audio_appsrc, &id);
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

    fn schedule_state(&mut self, ctx: &mut Context<Self>) {
        if let Some(handle) = self.state_handle {
            trace!("{} cancelling current state scheduling", self.id);
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
                trace!("{} not ready to progress to next state", self.id);

                self.state_handle = Some(ctx.run_later(timeout.to_std().unwrap(), |s, ctx| {
                    s.schedule_state(ctx);
                }));
            } else {
                trace!("{} progressing to next state", self.id);
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
            trace!("{} going back to sleep", self.id);
        }
    }

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
}

impl Handler<ConsumerMessage> for Mixer {
    type Result = MessageResult<ConsumerMessage>;

    fn handle(&mut self, msg: ConsumerMessage, _ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            ConsumerMessage::Connect {
                link_id,
                video_producer,
                audio_producer,
            } => {
                if self.consumer_slots.contains_key(&link_id) {
                    return MessageResult(Err(anyhow!(
                        "mixer {} already has link {}",
                        self.id,
                        link_id
                    )));
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

                let (audio_pad, video_pad) = {
                    if self.status == MixerStatus::Mixing {
                        debug!("mixer {} connecting to producers", self.id);
                        let vmixer = self.video_mixer.clone().unwrap();
                        let amixer = self.audio_mixer.clone().unwrap();

                        // TODO: move to function, handle errors
                        let aconv = make_element("audioconvert", None).unwrap();
                        let aresample = make_element("audioresample", None).unwrap();
                        let aqueue = make_element("queue", None).unwrap();
                        let vqueue = make_element("queue", None).unwrap();

                        let vappsrc_elem: &gst::Element = video_appsrc.upcast_ref();
                        let aappsrc_elem: &gst::Element = audio_appsrc.upcast_ref();

                        self.pipeline
                            .add_many(&[
                                &aqueue,
                                &aconv,
                                &aresample,
                                &vqueue,
                                vappsrc_elem,
                                aappsrc_elem,
                            ])
                            .unwrap();

                        aqueue.sync_state_with_parent().unwrap();
                        aconv.sync_state_with_parent().unwrap();
                        aresample.sync_state_with_parent().unwrap();
                        vqueue.sync_state_with_parent().unwrap();
                        vappsrc_elem.sync_state_with_parent().unwrap();
                        aappsrc_elem.sync_state_with_parent().unwrap();

                        let amixer_pad = amixer.request_pad_simple("sink_%u").unwrap();
                        let vmixer_pad = vmixer.request_pad_simple("sink_%u").unwrap();

                        let aq_srcpad = aqueue.static_pad("src").unwrap();
                        let vq_srcpad = vqueue.static_pad("src").unwrap();

                        gst::Element::link_many(&[aappsrc_elem, &aconv, &aresample, &aqueue])
                            .unwrap();
                        gst::Element::link_many(&[vappsrc_elem, &vqueue]).unwrap();

                        aq_srcpad.link(&amixer_pad).unwrap();
                        vq_srcpad.link(&vmixer_pad).unwrap();

                        video_producer.add_consumer(&video_appsrc, &link_id);
                        audio_producer.add_consumer(&audio_appsrc, &link_id);

                        (Some(amixer_pad), Some(vmixer_pad))
                    } else {
                        (None, None)
                    }
                };

                self.consumer_slots.insert(
                    link_id.clone(),
                    ConsumerSlot {
                        video_producer,
                        audio_producer,
                        video_appsrc: video_appsrc.clone(),
                        audio_appsrc: audio_appsrc.clone(),
                        audio_pad,
                        video_pad,
                    },
                );

                MessageResult(Ok(()))
            }
            ConsumerMessage::Disconnect { slot_id } => {
                if let Some(slot) = self.consumer_slots.remove(&slot_id) {
                    slot.video_producer.remove_consumer(&slot_id);
                    slot.audio_producer.remove_consumer(&slot_id);
                    if let Some(video_pad) = slot.video_pad {
                        self.video_mixer
                            .clone()
                            .unwrap()
                            .release_request_pad(&video_pad);
                    }
                    if let Some(audio_pad) = slot.audio_pad {
                        self.audio_mixer
                            .clone()
                            .unwrap()
                            .release_request_pad(&audio_pad);
                    }
                }
                MessageResult(Ok(()))
            }
        }
    }
}

impl Handler<MixerCommandMessage> for Mixer {
    type Result = MessageResult<MixerCommandMessage>;

    fn handle(&mut self, msg: MixerCommandMessage, ctx: &mut Context<Self>) -> Self::Result {
        match msg.command {
            MixerCommand::Start { cue_time, end_time } => {
                // TODO : use result
                let _ = self.start(ctx, cue_time, end_time);
            }
        }
        MessageResult(Ok(()))
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
