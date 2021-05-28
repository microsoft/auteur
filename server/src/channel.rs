use crate::config::Config;
use actix::{
    prelude::*, Actor, ActorContext, Addr, AsyncContext, Context, Handler, Message, MessageResult,
    SpawnHandle, StreamHandler,
};
use anyhow::{anyhow, Error};
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::prelude::*;
use gst::prelude::*;
use gst::tags::Date;
use log::{debug, error, info, trace};
use priority_queue::PriorityQueue;
use rtmp_switcher_controlling::controller::{
    ChannelInfo, ControllerCommand, SourceInfo, SourceStatus,
};
use std::cmp::{Ordering, Reverse};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug)]
struct Source {
    fallbacksrc: gst::Element,
    src: gst::Element,
}

#[derive(Debug)]
struct CuedSource {
    id: uuid::Uuid,
    uri: String,
    cue_time: DateTime<Utc>,
    end_time: Option<DateTime<Utc>>,
    status: SourceStatus,
    source: Option<Source>,
    n_streams: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum CuedItem {
    Source { id: uuid::Uuid, next: SourceStatus },
}

/// The scheduled actions
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CuedAction {
    next_time: DateTime<Utc>,
    item: CuedItem,
}

impl Ord for CuedAction {
    fn cmp(&self, other: &Self) -> Ordering {
        self.next_time.cmp(&other.next_time)
    }
}

impl PartialOrd for CuedAction {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
struct BusMessage(gst::Message);

impl Message for BusMessage {
    type Result = ();
}

impl StreamHandler<BusMessage> for Channel {
    fn handle(&mut self, msg: BusMessage, _ctx: &mut Context<Self>) {
        use gst::MessageView;

        match msg.0.view() {
            MessageView::Latency(latency) => {
                info!("The latency of the pipeline has changed: {:?}", latency);
                self.pipeline.call_async(|pipeline| {
                    let _ = pipeline.recalculate_latency();
                });
            }
            _ => (),
        }
    }

    fn finished(&mut self, _ctx: &mut Self::Context) {
        debug!("Finished bus messages");
    }
}

#[derive(Debug)]
struct StreamMessage {
    starting: bool,
    mixer: gst::Element,
    mixer_sinkpad: gst::Pad,
    source_id: uuid::Uuid,
}

impl Message for StreamMessage {
    type Result = ();
}

impl Handler<StreamMessage> for Channel {
    type Result = ();

    fn handle(&mut self, msg: StreamMessage, ctx: &mut Context<Self>) {
        self.handle_source_stream_change(
            ctx,
            msg.starting,
            msg.mixer,
            msg.mixer_sinkpad,
            msg.source_id,
        );
    }
}

/// Actor that represents a channel
#[derive(Debug)]
pub struct Channel {
    cfg: Arc<Config>,
    channels: Arc<Mutex<HashMap<uuid::Uuid, Addr<Channel>>>>,
    pub id: uuid::Uuid,
    name: String,
    destination: String,
    cued_actions: PriorityQueue<uuid::Uuid, Reverse<CuedAction>>,
    pipeline: gst::Pipeline,
    schedule_handle: Option<SpawnHandle>,
    sources: HashMap<uuid::Uuid, CuedSource>,
}

fn make(element: &str, name: Option<&str>) -> Result<gst::Element, Error> {
    gst::ElementFactory::make(element, name)
        .map_err(|err| anyhow!("Failed to make element {}: {}", element, err.message))
}

impl Channel {
    pub fn new(
        cfg: Arc<Config>,
        channels: Arc<Mutex<HashMap<uuid::Uuid, Addr<Channel>>>>,
        name: &str,
        destination: &str,
    ) -> Self {
        let id = uuid::Uuid::new_v4();

        Self {
            cfg,
            channels,
            id,
            name: name.to_string(),
            destination: destination.to_string(),
            cued_actions: PriorityQueue::new(),
            pipeline: gst::Pipeline::new(Some(&id.to_string())),
            schedule_handle: None,
            sources: HashMap::new(),
        }
    }

    fn tear_down_source(&mut self, id: uuid::Uuid) -> Result<(), Error> {
        if let Some(source) = self.sources.remove(&id) {
            debug!("Tearing down source {}", id);

            if let Some(gst_source) = source.source {
                // When the source is still linked to the mixer, we want to
                // collect the mixer pads and mute them, finalize the source
                // then remove the now-unlinked mixer pads
                let mut peers = vec![];

                gst_source.src.foreach_src_pad(|_, pad| {
                    if let Some(peer) = pad.peer() {
                        if peer.has_property("alpha", None) {
                            peer.set_property("alpha", &0.).unwrap();
                        } else {
                            peer.set_property("volume", &0.).unwrap();
                        }
                        peers.push(peer);
                    }
                    true
                });

                gst_source.src.set_locked_state(true);
                gst_source.src.set_state(gst::State::Null).unwrap();
                self.pipeline.remove(&gst_source.src).unwrap();

                for peer in &peers {
                    if let Some(mixer) = peer.parent() {
                        let _ = mixer.downcast::<gst::Element>().unwrap().remove_pad(peer);
                    }
                }
            }

            let _ = self.cued_actions.remove(&source.id);

            Ok(())
        } else {
            Err(anyhow!("No source with id {}", id))
        }
    }

    fn handle_source_stream_change(
        &mut self,
        ctx: &mut Context<Self>,
        starting: bool,
        mixer: gst::Element,
        mixer_sinkpad: gst::Pad,
        source_id: uuid::Uuid,
    ) {
        if starting {
            debug!(
                "Stream starting for source with id {}, potentially activating {:?}",
                source_id, mixer_sinkpad
            );

            let source = self.sources.get_mut(&source_id).unwrap();
            if mixer_sinkpad.has_property("alpha", None) {
                if source.status == SourceStatus::Playing {
                    mixer_sinkpad.set_property("alpha", &1.).unwrap();
                } else {
                    mixer_sinkpad.set_property("alpha", &0.).unwrap();
                }
            } else {
                if source.status == SourceStatus::Playing {
                    mixer_sinkpad.set_property("volume", &1.).unwrap();
                } else {
                    mixer_sinkpad.set_property("volume", &0.).unwrap();
                }
            }

            source.n_streams += 1;
            trace!(
                "Source {} now has {} streams ready",
                source.id,
                source.n_streams
            );
        } else {
            if let Some(mut source) = self.sources.get_mut(&source_id) {
                debug!(
                    "Stream stopped for source with id {}, deactivating {:?}",
                    source_id, mixer_sinkpad
                );

                if mixer_sinkpad.has_property("alpha", None) {
                    mixer_sinkpad.set_property("alpha", &0.).unwrap();
                } else {
                    mixer_sinkpad.set_property("volume", &0.).unwrap();
                }

                mixer.remove_pad(&mixer_sinkpad).unwrap();

                source.n_streams -= 1;

                trace!(
                    "Source {} now has {} streams running",
                    source.id,
                    source.n_streams
                );

                if source.n_streams == 0 {
                    let _ = self.tear_down_source(source_id);
                }
            }
        }
    }

    fn play_cued_source(&mut self, ctx: &mut Context<Self>, id: uuid::Uuid) -> Result<(), Error> {
        if let Some(source) = self.sources.get_mut(&id) {
            if let Some(prerolled_source) = source.source.take() {
                prerolled_source
                    .fallbacksrc
                    .emit_by_name("unblock", &[])
                    .unwrap();

                prerolled_source.src.foreach_src_pad(|_, pad| {
                    if let Some(peer) = pad.peer() {
                        if peer.has_property("alpha", None) {
                            peer.set_property("alpha", &1.).unwrap();
                        } else {
                            peer.set_property("volume", &1.).unwrap();
                        }
                    }
                    true
                });

                source.status = SourceStatus::Playing;

                if let Some(end_time) = source.end_time {
                    self.cued_actions.push(
                        source.id,
                        Reverse(CuedAction {
                            next_time: end_time,
                            item: CuedItem::Source {
                                id: source.id,
                                next: SourceStatus::Stopped,
                            },
                        }),
                    );
                }

                source.source = Some(prerolled_source);
            }
        }

        Ok(())
    }

    fn preroll_cued_source(
        &mut self,
        ctx: &mut Context<Self>,
        id: uuid::Uuid,
    ) -> Result<(), Error> {
        if let Some(source) = self.sources.get_mut(&id) {
            let bin = gst::Bin::new(None);
            let src = make("fallbacksrc", None)?;
            let _ = bin.add(&src);

            src.set_property("uri", &source.uri).unwrap();
            src.set_property("manual-unblock", &true).unwrap();

            let pipeline_clone = self.pipeline.downgrade();
            let bin_clone = bin.clone();
            let addr = ctx.address();
            let source_id = source.id;
            src.connect_pad_added(move |src, pad| {
                if let Some(pipeline) = pipeline_clone.upgrade() {
                    let is_video = pad.name() == "video";

                    let (mixer, pad) = {
                        if is_video {
                            // TODO: bubble up errors
                            let vscale = make("videoscale", None).unwrap();
                            let capsfilter = make("capsfilter", None).unwrap();
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
                            bin_clone.add_many(&[&vscale, &capsfilter]).unwrap();
                            vscale.sync_state_with_parent().unwrap();
                            capsfilter.sync_state_with_parent().unwrap();
                            vscale.link(&capsfilter).unwrap();
                            let sinkpad = vscale.static_pad("sink").unwrap();
                            let _ = pad.link(&sinkpad);
                            let ghost = gst::GhostPad::with_target(
                                None,
                                &capsfilter.static_pad("src").unwrap(),
                            )
                            .unwrap();
                            bin_clone.add_pad(&ghost).unwrap();
                            (
                                pipeline.by_name("compositor").unwrap(),
                                ghost.upcast::<gst::Pad>(),
                            )
                        } else {
                            // TODO: bubble up errors
                            let audioconvert = make("audioconvert", None).unwrap();
                            let audioresample = make("audioresample", None).unwrap();
                            bin_clone
                                .add_many(&[&audioconvert, &audioresample])
                                .unwrap();
                            audioconvert.sync_state_with_parent().unwrap();
                            audioresample.sync_state_with_parent().unwrap();
                            audioconvert.link(&audioresample).unwrap();
                            let sinkpad = audioconvert.static_pad("sink").unwrap();
                            let _ = pad.link(&sinkpad);
                            let ghost = gst::GhostPad::with_target(
                                None,
                                &audioresample.static_pad("src").unwrap(),
                            )
                            .unwrap();
                            bin_clone.add_pad(&ghost).unwrap();
                            (
                                pipeline.by_name("audiomixer").unwrap(),
                                ghost.upcast::<gst::Pad>(),
                            )
                        }
                    };

                    let mixer_sinkpad = mixer.request_pad_simple(&"sink_%u").unwrap();

                    let mixer_clone = mixer.clone();
                    let mixer_sinkpad_clone = mixer_sinkpad.clone();
                    let addr_clone = addr.clone();
                    pad.add_probe(
                        gst::PadProbeType::EVENT_DOWNSTREAM,
                        move |pad, info| match info.data {
                            Some(gst::PadProbeData::Event(ref ev))
                                if ev.type_() == gst::EventType::Eos =>
                            {
                                addr_clone.do_send(StreamMessage {
                                    starting: false,
                                    mixer: mixer_clone.clone(),
                                    mixer_sinkpad: mixer_sinkpad_clone.clone(),
                                    source_id,
                                });
                                gst::PadProbeReturn::Drop
                            }
                            _ => gst::PadProbeReturn::Ok,
                        },
                    );

                    // TODO use result
                    error!("Linking {:?} | {:?}", pad, mixer_sinkpad);
                    let _ = pad.link(&mixer_sinkpad);

                    addr.do_send(StreamMessage {
                        starting: true,
                        mixer,
                        mixer_sinkpad,
                        source_id,
                    });
                }
            });

            self.pipeline.add(&bin)?;

            bin.sync_state_with_parent()?;

            source.status = SourceStatus::Prerolling;
            source.source = Some(Source {
                fallbacksrc: src,
                src: bin.upcast(),
            });

            self.cued_actions.push(
                source.id,
                Reverse(CuedAction {
                    next_time: source.cue_time,
                    item: CuedItem::Source {
                        id: source.id,
                        next: SourceStatus::Playing,
                    },
                }),
            );
        }

        Ok(())
    }

    fn schedule(&mut self, ctx: &mut Context<Self>) {
        trace!("Rescheduling");

        if let Some(handle) = self.schedule_handle.take() {
            ctx.cancel_future(handle);
        }

        while let Some(next_action) = {
            if let Some((_, Reverse(first_action))) = self.cued_actions.peek() {
                let now = Utc::now();
                let timeout = first_action.next_time - now;

                trace!(
                    "Now is {:?}, next action time is {:?}",
                    now,
                    first_action.next_time
                );

                if timeout > chrono::Duration::zero() {
                    trace!(
                        "Next action time hasn't come, going back to sleep for {:?}",
                        timeout
                    );
                    self.schedule_handle =
                        Some(ctx.run_later(timeout.to_std().unwrap(), |s, ctx| {
                            trace!("Waking up");
                            s.schedule(ctx);
                        }));
                    None
                } else {
                    trace!("Next action time has come");

                    self.cued_actions.pop()
                }
            } else {
                trace!("No source cued at the time");
                None
            }
        } {
            let next_action = next_action.1 .0;

            match next_action.item {
                CuedItem::Source { id, next } => match next {
                    SourceStatus::Prerolling => {
                        debug!("Next source is ready for preroll {}", id);
                        self.preroll_cued_source(ctx, id).unwrap();
                    }
                    SourceStatus::Playing => {
                        debug!("Next source is ready for playback {}", id);
                        self.play_cued_source(ctx, id).unwrap();
                    }
                    SourceStatus::Stopped => {
                        debug!("Reached end time for source {}", id);
                        let _ = self.tear_down_source(id);
                    }
                    _ => unreachable!(),
                },
            }
        }
    }

    fn add_source(
        &mut self,
        ctx: &mut Context<Self>,
        uri: String,
        cue_time: DateTime<Utc>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<uuid::Uuid, Error> {
        let src_id = uuid::Uuid::new_v4();

        if let Some(end_time) = end_time {
            if end_time <= cue_time {
                return Err(anyhow!("cue_time {} > end_time {}", cue_time, end_time));
            }
        }

        let next_time = cue_time - chrono::Duration::seconds(10);
        self.sources.insert(
            src_id,
            CuedSource {
                id: src_id,
                uri,
                cue_time,
                end_time,
                status: SourceStatus::Initial,
                source: None,
                n_streams: 0,
            },
        );

        self.cued_actions.push(
            src_id,
            Reverse(CuedAction {
                next_time,
                item: CuedItem::Source {
                    id: src_id,
                    next: SourceStatus::Prerolling,
                },
            }),
        );

        self.schedule(ctx);

        Ok(src_id)
    }

    fn modify_source(
        &mut self,
        ctx: &mut Context<Self>,
        source_id: uuid::Uuid,
        cue_time: DateTime<Utc>,
    ) -> Result<(), Error> {
        /*
        if let Some((id, Reverse(mut source))) = self.cued_actions.remove(&source_id) {
            source.cue_time = cue_time;
            source.next_time = Some(cue_time - chrono::Duration::seconds(10));

            if let Some(source) = self.sources.remove(&id) {
                self.tear_down_source(source);
            }

            self.cued_actions.push(id, Reverse(source));

            self.schedule(ctx);

            Ok(())
        } else {
            Err(anyhow!("no source with id {}", source_id))
        }
        */
        Ok(())
    }

    fn remove_source(
        &mut self,
        ctx: &mut Context<Self>,
        source_id: uuid::Uuid,
    ) -> Result<(), Error> {
        self.tear_down_source(source_id)
    }

    fn start_pipeline(&mut self, ctx: &mut Context<Self>) -> Result<(), Error> {
        let vsrc = make("videotestsrc", None)?;
        let vqueue = make("queue", None)?;
        let vmixer = make("compositor", Some("compositor"))?;
        let vident = make("identity", None)?;
        let vconv = make("videoconvert", None)?;
        let vdeinterlace = make("deinterlace", None)?;
        let vscale = make("videoscale", None)?;
        let vcapsfilter = make("capsfilter", None)?;
        let timeoverlay = make("timeoverlay", None)?;
        let venc = make("x264enc", None)?;
        let vparse = make("h264parse", None)?;
        let venc_queue = make("queue", None)?;

        let asrc = make("audiotestsrc", None)?;
        let aqueue = make("queue", None)?;
        let amixer = make("audiomixer", Some("audiomixer"))?;
        let aident = make("identity", None)?;
        let acapsfilter = make("capsfilter", None)?;
        let aenc = make("faac", None)?;
        let aenc_queue = make("queue", None)?;

        let mux = make("flvmux", None)?;
        let mux_queue = make("queue", None)?;
        let sink = make("rtmp2sink", None)?;

        vsrc.set_property("is-live", &true).unwrap();
        vsrc.set_property_from_str("pattern", "black");
        vmixer.set_property_from_str("background", "black");
        vident.set_property("single-segment", &true).unwrap();
        venc.set_property_from_str("tune", "zerolatency");
        venc.set_property("key-int-max", &30u32).unwrap();
        vparse.set_property("config-interval", &-1i32).unwrap();
        sink.set_property("location", &self.destination).unwrap();
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
        aident.set_property("single-segment", &true).unwrap();
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
        mux.set_property("streamable", &true).unwrap();
        mux.set_property("latency", &1000000000u64).unwrap();

        self.pipeline.add_many(&[
            &vsrc,
            &vqueue,
            &vmixer,
            &vident,
            &vconv,
            &vdeinterlace,
            &vscale,
            &vcapsfilter,
            &timeoverlay,
            &venc,
            &vparse,
            &venc_queue,
            &asrc,
            &aqueue,
            &amixer,
            &acapsfilter,
            &aenc,
            &aenc_queue,
            &mux,
            &mux_queue,
            &sink,
        ])?;

        gst::Element::link_many(&[
            &vsrc,
            &vqueue,
            &vmixer,
            &vident,
            &vconv,
            &vdeinterlace,
            &vscale,
            &vcapsfilter,
            &timeoverlay,
            &venc,
            &venc_queue,
            &mux,
            &mux_queue,
            &sink,
        ])?;

        gst::Element::link_many(&[
            &asrc,
            &aqueue,
            &amixer,
            &acapsfilter,
            &aenc,
            &aenc_queue,
            &mux,
        ])?;

        let bus = self.pipeline.bus().expect("Pipeline with no bus");
        let bus_stream = bus.stream();
        Self::add_stream(bus_stream.map(BusMessage), ctx);

        self.pipeline.set_state(gst::State::Playing)?;

        Ok(())
    }
}

impl Actor for Channel {
    type Context = Context<Self>;

    /// Called once the channel is started.
    fn started(&mut self, ctx: &mut Self::Context) {
        if let Err(err) = self.start_pipeline(ctx) {
            error!("Error when starting pipeline: {}", err);
            ctx.stop();
        } else {
            info!("Started channel {}", self.id);
        }
    }

    /// Called once the channel is stopped
    fn stopped(&mut self, ctx: &mut Self::Context) {
        let _ = self.channels.lock().unwrap().remove(&self.id);
        info!("Stopped channel {}", self.id);
    }
}

#[derive(Debug)]
pub struct GetInfoMessage {}

impl Message for GetInfoMessage {
    type Result = ChannelInfo;
}

impl Handler<GetInfoMessage> for Channel {
    type Result = MessageResult<GetInfoMessage>;

    fn handle(&mut self, msg: GetInfoMessage, ctx: &mut Context<Self>) -> Self::Result {
        let mut sources: Vec<SourceInfo> = self
            .sources
            .values()
            .map(|source| SourceInfo {
                id: source.id,
                uri: source.uri.clone(),
                cue_time: source.cue_time,
                end_time: source.end_time,
                status: source.status,
            })
            .collect();

        sources.sort_by(|a, b| a.cue_time.cmp(&b.cue_time));

        MessageResult(ChannelInfo {
            id: self.id,
            name: self.name.to_string(),
            destination: self.destination.to_string(),
            sources,
        })
    }
}

#[derive(Debug)]
pub struct AddSourceMessage {
    pub uri: String,
    pub cue_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
}

impl Message for AddSourceMessage {
    type Result = Result<uuid::Uuid, Error>;
}

impl Handler<AddSourceMessage> for Channel {
    type Result = MessageResult<AddSourceMessage>;

    fn handle(&mut self, msg: AddSourceMessage, ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(self.add_source(ctx, msg.uri, msg.cue_time, msg.end_time))
    }
}

#[derive(Debug)]
pub struct ModifySourceMessage {
    pub id: uuid::Uuid,
    pub cue_time: DateTime<Utc>,
}

impl Message for ModifySourceMessage {
    type Result = Result<(), Error>;
}

impl Handler<ModifySourceMessage> for Channel {
    type Result = MessageResult<ModifySourceMessage>;

    fn handle(&mut self, msg: ModifySourceMessage, ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(self.modify_source(ctx, msg.id, msg.cue_time))
    }
}

#[derive(Debug)]
pub struct RemoveSourceMessage {
    pub id: uuid::Uuid,
}

impl Message for RemoveSourceMessage {
    type Result = Result<(), Error>;
}

impl Handler<RemoveSourceMessage> for Channel {
    type Result = MessageResult<RemoveSourceMessage>;

    fn handle(&mut self, msg: RemoveSourceMessage, ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(self.remove_source(ctx, msg.id))
    }
}
