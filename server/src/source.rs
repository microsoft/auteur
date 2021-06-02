use crate::node::{GetProducerMessage, NodeManager, SourceCommandMessage};
use crate::utils::{
    make_element, ErrorMessage, PipelineManager, StopManagerMessage, StreamProducer,
};
use actix::prelude::*;
use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use gst::prelude::*;
use rtmp_switcher_controlling::controller::{SourceCommand, SourceStatus};
use tracing::{debug, error, instrument, trace};

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
    switches: Vec<gst::Element>,
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
            // Only used to monitor status
            switches: vec![],
            audio_producer: StreamProducer::from(&audio_appsink),
            video_producer: StreamProducer::from(&video_appsink),
            state_handle: None,
        }
    }

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
            let appsink: &gst::Element = audio_producer.appsink().upcast_ref();

            debug!(appsink = %appsink.name(), "linking audio stream to appsink");

            let sinkpad = appsink.static_pad("sink").unwrap();
            pad.link(&sinkpad)?;

            Ok(appsink.clone())
        }
    }

    #[instrument(level = "debug", name = "prerolling", skip(self, ctx), fields(id = %self.id))]
    fn preroll(&mut self, ctx: &mut Context<Self>) -> Result<(), Error> {
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
        src_bin.connect_element_added(move |_src, element| {
            if element.has_property("primary-health", None) {
                let _ = addr.do_send(NewSwitchMessage(element.clone()));
            }
        });

        debug!("now prerolling");

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

    #[instrument(level = "debug", name = "unblocking", skip(self), fields(id = %self.id))]
    fn unblock(&mut self) -> Result<(), Error> {
        if self.status != SourceStatus::Prerolling {
            return Err(anyhow!("can't play source in state: {:?}", self.status));
        }

        let src = self.src.as_ref().unwrap();

        src.emit_by_name("unblock", &[]).unwrap();
        self.video_producer.forward();
        self.audio_producer.forward();

        debug!("unblocked, now playing");

        self.status = SourceStatus::Playing;

        Ok(())
    }

    #[instrument(level = "trace", name = "scheduling", skip(self, ctx), fields(id = %self.id))]
    fn schedule_state(&mut self, ctx: &mut Context<Self>) {
        if let Some(handle) = self.state_handle {
            trace!("cancelling current state scheduling");
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
                trace!("not ready to progress to next state");

                self.state_handle = Some(ctx.run_later(timeout.to_std().unwrap(), |s, ctx| {
                    s.schedule_state(ctx);
                }));
            } else {
                trace!("progressing to next state");
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
            debug!("going back to sleep");
        }
    }

    #[instrument(level = "trace", name = "cueing", skip(self, ctx), fields(id = %self.id))]
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

    fn handle_stream_change(&mut self, ctx: &mut Context<Self>, starting: bool) {
        if starting {
            self.n_streams += 1;

            debug!(id = %self.id, n_streams = %self.n_streams, "new active stream");
        } else {
            self.n_streams -= 1;

            debug!(id = %self.id, n_streams = %self.n_streams, "active stream finished");

            if self.n_streams == 0 {
                ctx.stop()
            }
        }
    }

    #[instrument(level = "debug", name = "new-fallbackswitch", skip(self, ctx), fields(id = %self.id))]
    fn monitor_switch(&mut self, ctx: &mut Context<Self>, switch: gst::Element) {
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

        self.switches.push(switch);
    }

    #[instrument(level = "trace", name = "new-source-status", skip(self), fields(id = %self.id))]
    fn log_source_status(&mut self) {
        if let Some(ref src) = self.src {
            let value = src.property("status").unwrap();
            let status = gst::glib::EnumValue::from_value(&value).expect("Not an enum type");
            trace!("Source status: {}", status.nick());
            trace!(
                "Source statistics: {:?}",
                src.property("statistics").unwrap()
            );
        }

        for switch in &self.switches {
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

impl Actor for Source {
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

        NodeManager::from_registry().do_send(SourceStoppedMessage {
            id: self.id.clone(),
            video_producer: self.video_producer.clone(),
            audio_producer: self.audio_producer.clone(),
        });
    }
}

#[derive(Debug)]
struct StreamMessage {
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
