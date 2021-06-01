use crate::node::{GetProducerMessage, NodeManager, SourceCommandMessage};
use crate::utils::{make_element, ErrorMessage, PipelineManager, StreamProducer};
use actix::prelude::*;
use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use gst::prelude::*;
use rtmp_switcher_controlling::controller::{SourceCommand, SourceStatus};
use tracing::{debug, error, info, trace};

#[derive(Debug)]
pub struct Source {
    id: String,
    uri: String,
    cue_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    status: SourceStatus,
    n_streams: u32,
    pipeline: gst::Pipeline,
    pipeline_manager: Option<Addr<PipelineManager>>,
    src: Option<gst::Element>,
    audio_producer: StreamProducer,
    video_producer: StreamProducer,

    state_handle: Option<SpawnHandle>,
}

impl Source {
    pub fn new(id: &str, uri: &str) -> Self {
        let pipeline = gst::Pipeline::new(Some(&id.to_string()));

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
            uri: uri.to_string(),
            cue_time: None,
            end_time: None,
            status: SourceStatus::Initial,
            n_streams: 0,
            pipeline,
            pipeline_manager: None,
            src: None,
            audio_producer: StreamProducer::from(&audio_appsink),
            video_producer: StreamProducer::from(&video_appsink),
            state_handle: None,
        }
    }

    fn connect_pad(
        id: String,
        is_video: bool,
        pipeline: &gst::Pipeline,
        pad: &gst::Pad,
        video_producer: &StreamProducer,
        audio_producer: &StreamProducer,
    ) -> Result<gst::Element, Error> {
        if is_video {
            let vscale = make_element("videoscale", None)?;
            let capsfilter = make_element("capsfilter", None)?;
            capsfilter
                .set_property(
                    "caps",
                    &gst::Caps::builder("video/x-raw")
                        .field("width", &1920)
                        .field("height", &1080)
                        .field("pixel-aspect-ratio", &gst::Fraction::new(1, 1))
                        .build(),
                )
                .unwrap();
            pipeline.add_many(&[&vscale, &capsfilter])?;

            let appsink = video_producer.appsink().upcast_ref();

            debug!("{} linking video stream to appsink {:?}", id, appsink);

            vscale.sync_state_with_parent()?;
            capsfilter.sync_state_with_parent()?;

            let sinkpad = vscale.static_pad("sink").unwrap();
            pad.link(&sinkpad)?;
            gst::Element::link_many(&[&vscale, &capsfilter, &appsink])?;

            Ok(appsink.clone())
        } else {
            let audioconvert = make_element("audioconvert", None)?;
            let audioresample = make_element("audioresample", None)?;
            pipeline.add_many(&[&audioconvert, &audioresample])?;

            let appsink = audio_producer.appsink().upcast_ref();

            debug!("{} linking audio stream to appsink {:?}", id, appsink);

            audioconvert.sync_state_with_parent()?;
            audioresample.sync_state_with_parent()?;

            let sinkpad = audioconvert.static_pad("sink").unwrap();
            pad.link(&sinkpad)?;
            gst::Element::link_many(&[&audioconvert, &audioresample, &appsink])?;

            Ok(appsink.clone())
        }
    }

    fn preroll(&mut self, ctx: &mut Context<Self>) -> Result<(), Error> {
        debug!("{} attempting preroll", self.id);

        if self.status != SourceStatus::Initial {
            return Err(anyhow!("can't preroll source in state: {:?}", self.status));
        }

        let src = make_element("fallbacksrc", None)?;
        self.pipeline.add(&src)?;

        src.set_property("uri", &self.uri).unwrap();
        src.set_property("manual-unblock", &true).unwrap();

        let pipeline_clone = self.pipeline.downgrade();
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
                        pad.add_probe(gst::PadProbeType::EVENT_DOWNSTREAM, move |pad, info| {
                            match info.data {
                                Some(gst::PadProbeData::Event(ref ev))
                                    if ev.type_() == gst::EventType::Eos =>
                                {
                                    addr_clone.do_send(StreamMessage {
                                        starting: false,
                                        pad: pad.clone(),
                                        is_video,
                                    });
                                    gst::PadProbeReturn::Drop
                                }
                                _ => gst::PadProbeReturn::Ok,
                            }
                        });

                        addr.do_send(StreamMessage {
                            starting: true,
                            pad: pad.clone(),
                            is_video,
                        });
                    }
                    Err(err) => addr.do_send(ErrorMessage(format!(
                        "Failed to connect source stream: {:?}",
                        err
                    ))),
                }
            }
        });

        debug!("{} is now prerolling", self.id);

        self.status = SourceStatus::Prerolling;
        self.src = Some(src);

        let addr = ctx.address().clone();
        let id = self.id.clone();
        self.pipeline.call_async(move |pipeline| {
            if let Err(err) = pipeline.set_state(gst::State::Playing) {
                let _ = addr.do_send(ErrorMessage(format!(
                    "Failed to start source {}: {}",
                    id, err
                )));
            }
        });

        Ok(())
    }

    fn unblock(&mut self) -> Result<(), Error> {
        debug!("{} attempting unblock", self.id);

        if self.status != SourceStatus::Prerolling {
            return Err(anyhow!("can't play source in state: {:?}", self.status));
        }

        let src = self.src.as_ref().unwrap();

        src.emit_by_name("unblock", &[]).unwrap();
        self.video_producer.forward();
        self.audio_producer.forward();

        debug!("{} is now unblocked", self.id);

        self.status = SourceStatus::Playing;

        Ok(())
    }

    fn schedule_state(&mut self, ctx: &mut Context<Self>) {
        if let Some(handle) = self.state_handle {
            trace!("{} cancelling current state scheduling", self.id);
            ctx.cancel_future(handle);
        }

        let next_time = match self.status {
            SourceStatus::Initial => {
                if let Some(cue_time) = self.cue_time {
                    Some(cue_time - chrono::Duration::seconds(10))
                } else {
                    None
                }
            }
            SourceStatus::Prerolling => self.cue_time,
            SourceStatus::Playing => self.end_time,
            SourceStatus::Stopped => None,
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
                debug!("{} progressing to next state", self.id);
                if let Err(err) = match self.status {
                    SourceStatus::Initial => self.preroll(ctx),
                    SourceStatus::Prerolling => self.unblock(),
                    SourceStatus::Playing => {
                        ctx.stop();
                        self.status = SourceStatus::Stopped;
                        Ok(())
                    }
                    SourceStatus::Stopped => Ok(()),
                } {
                    ctx.notify(ErrorMessage(format!("Failed to preroll source: {:?}", err)));
                } else {
                    self.schedule_state(ctx);
                }
            }
        } else {
            debug!("{} going back to sleep", self.id);
        }
    }

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
            SourceStatus::Initial => {
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
}

impl Actor for Source {
    type Context = Context<Self>;

    /// Called once the source is started.
    fn started(&mut self, ctx: &mut Self::Context) {
        self.pipeline_manager = Some(
            PipelineManager::new(self.pipeline.clone(), ctx.address().recipient().clone()).start(),
        );
    }

    /// Called once the source is stopped
    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        let _ = self.pipeline_manager.take();

        NodeManager::from_registry().do_send(SourceStoppedMessage {
            id: self.id.clone(),
            video_producer: self.video_producer.clone(),
            audio_producer: self.audio_producer.clone(),
        });

        debug!("Stopped source {}", self.id);

        Running::Stop
    }
}

#[derive(Debug)]
struct StreamMessage {
    starting: bool,
    pad: gst::Pad,
    is_video: bool,
}

impl Message for StreamMessage {
    type Result = ();
}

impl Handler<StreamMessage> for Source {
    type Result = ();

    fn handle(&mut self, _msg: StreamMessage, _ctx: &mut Context<Self>) {}
}

impl Handler<SourceCommandMessage> for Source {
    type Result = MessageResult<SourceCommandMessage>;

    fn handle(&mut self, msg: SourceCommandMessage, ctx: &mut Context<Self>) -> Self::Result {
        match msg.command {
            SourceCommand::Play { cue_time, end_time } => {
                let _ = self.play(ctx, cue_time, end_time);
            }
        }
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

        ctx.stop();
    }
}
