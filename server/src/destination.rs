use actix::prelude::*;
use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use gst::prelude::*;
use tracing::{debug, error, instrument, trace};

use rtmp_switcher_controlling::controller::{
    DestinationCommand, DestinationFamily, DestinationStatus,
};

use crate::node::{ConsumerMessage, DestinationCommandMessage, NodeManager};
use crate::utils::{
    make_element, ErrorMessage, PipelineManager, StopManagerMessage, StreamProducer,
};

struct ConsumerSlot {
    id: String,
    video_producer: StreamProducer,
    audio_producer: StreamProducer,
}

pub struct Destination {
    id: String,
    family: DestinationFamily,
    cue_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    status: DestinationStatus,
    pipeline: gst::Pipeline,
    pipeline_manager: Option<Addr<PipelineManager>>,
    video_appsrc: gst_app::AppSrc,
    audio_appsrc: gst_app::AppSrc,
    consumer_slot: Option<ConsumerSlot>,

    state_handle: Option<SpawnHandle>,
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

    #[instrument(level = "debug", name = "stopping", skip(self, _ctx), fields(id = %self.id))]
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if let Some(manager) = self.pipeline_manager.take() {
            let _ = manager.do_send(StopManagerMessage);
        }

        if let Some(slot) = self.consumer_slot.take() {
            slot.video_producer.remove_consumer(&slot.id);
            slot.audio_producer.remove_consumer(&slot.id);
        }

        NodeManager::from_registry().do_send(DestinationStoppedMessage {
            id: self.id.clone(),
        });
    }
}

impl Destination {
    pub fn new(id: &str, family: &DestinationFamily) -> Self {
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

        let pipeline = gst::Pipeline::new(None);

        Self {
            id: id.to_string(),
            family: family.clone(),
            cue_time: None,
            end_time: None,
            status: DestinationStatus::Initial,
            pipeline,
            pipeline_manager: None,
            video_appsrc,
            audio_appsrc,
            consumer_slot: None,
            state_handle: None,
        }
    }

    #[instrument(level = "debug", name = "streaming", skip(self, ctx), fields(id = %self.id))]
    fn start_rtmp_pipeline(&mut self, ctx: &mut Context<Self>, uri: &String) -> Result<(), Error> {
        let vconv = make_element("videoconvert", None)?;
        let timecodestamper = make_element("timecodestamper", None)?;
        let timeoverlay = make_element("timeoverlay", None)?;
        let venc = make_element("x264enc", None)?;
        let vparse = make_element("h264parse", None)?;
        let venc_queue = make_element("queue", None)?;

        let aconv = make_element("audioconvert", None)?;
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
            &aenc,
            &aenc_queue,
            &mux,
            &mux_queue,
            &sink,
        ])?;

        venc.set_property_from_str("tune", "zerolatency");
        venc.set_property("key-int-max", &30u32).unwrap();
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
            &venc_queue,
            &mux,
            &mux_queue,
            &sink,
        ])?;

        gst::Element::link_many(&[
            self.audio_appsrc.upcast_ref(),
            &aconv,
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

        self.status = DestinationStatus::Streaming;

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

    #[instrument(level = "trace", name = "scheduling", skip(self, ctx), fields(id = %self.id))]
    fn schedule_state(&mut self, ctx: &mut Context<Self>) {
        if let Some(handle) = self.state_handle {
            trace!("cancelling current state scheduling");
            ctx.cancel_future(handle);
        }

        let next_time = match self.status {
            DestinationStatus::Initial => self.cue_time,
            DestinationStatus::Streaming => self.end_time,
            DestinationStatus::Stopped => None,
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
                    DestinationStatus::Initial => match self.family.clone() {
                        DestinationFamily::RTMP { uri } => self.start_rtmp_pipeline(ctx, &uri),
                    },
                    DestinationStatus::Streaming => {
                        ctx.stop();
                        self.status = DestinationStatus::Stopped;
                        Ok(())
                    }
                    DestinationStatus::Stopped => Ok(()),
                } {
                    ctx.notify(ErrorMessage(format!("Failed to preroll source: {:?}", err)));
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
            DestinationStatus::Initial => {
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

        if self.status == DestinationStatus::Streaming {
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

impl Handler<DestinationCommandMessage> for Destination {
    type Result = MessageResult<DestinationCommandMessage>;

    fn handle(&mut self, msg: DestinationCommandMessage, ctx: &mut Context<Self>) -> Self::Result {
        match msg.command {
            DestinationCommand::Start { cue_time, end_time } => {
                MessageResult(self.start(ctx, cue_time, end_time))
            }
        }
    }
}

#[derive(Debug)]
pub struct DestinationStoppedMessage {
    pub id: String,
}

impl Message for DestinationStoppedMessage {
    type Result = ();
}

impl Handler<ErrorMessage> for Destination {
    type Result = ();

    fn handle(&mut self, msg: ErrorMessage, ctx: &mut Context<Self>) -> Self::Result {
        error!("Got error message '{}' on destination {}", msg.0, self.id,);

        ctx.stop();
    }
}
