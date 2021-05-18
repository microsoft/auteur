use crate::config::Config;
use actix::{
    prelude::*, Actor, ActorContext, Addr, AsyncContext, Context, Handler, Message, MessageResult,
    SpawnHandle, StreamHandler,
};
use anyhow::{anyhow, Error};
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::prelude::*;
use gst::prelude::*;
use log::{debug, error, info, trace};
use priority_queue::PriorityQueue;
use rtmp_switcher_controlling::controller::{ChannelInfo, ControllerCommand};
use std::cmp::{Ordering, Reverse};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

/// The scheduled sources
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CuedSource {
    uri: String,
    cue_time: Option<DateTime<Utc>>,
    seq: u64,
}

impl Ord for CuedSource {
    fn cmp(&self, other: &Self) -> Ordering {
        if let Some(cue_time) = self.cue_time {
            if let Some(other_cue_time) = other.cue_time {
                cue_time.cmp(&other_cue_time)
            } else {
                Ordering::Less
            }
        } else {
            // Order sources without a cue time by insertion order
            self.seq.cmp(&other.seq)
        }
    }
}

impl PartialOrd for CuedSource {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
struct Source {
    src: gst::Element,
    n_streams: u32,
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
}

impl Message for StreamMessage {
    type Result = ();
}

impl Handler<StreamMessage> for Channel {
    type Result = ();

    fn handle(&mut self, msg: StreamMessage, ctx: &mut Context<Self>) {
        self.handle_source_stream_change(ctx, msg.starting, msg.mixer, msg.mixer_sinkpad);
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
    sources: PriorityQueue<uuid::Uuid, Reverse<CuedSource>>,
    pipeline: gst::Pipeline,
    schedule_handle: Option<SpawnHandle>,
    source_seq: u64,
    current_source: Option<Source>,
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
            sources: PriorityQueue::new(),
            pipeline: gst::Pipeline::new(Some(&id.to_string())),
            schedule_handle: None,
            source_seq: 0,
            current_source: None,
        }
    }

    fn tear_down_source(&self, source: Source) {
        debug!("Tearing down current_source");

        // When the source is still linked to the mixer, we want to
        // collect the mixer pads and mute them, finalize the source
        // then remove the now-unlinked mixer pads
        let mut peers = vec![];

        source.src.foreach_src_pad(|_, pad| {
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

        source.src.set_locked_state(true);
        source.src.set_state(gst::State::Null).unwrap();
        self.pipeline.remove(&source.src).unwrap();

        for peer in &peers {
            if let Some(mixer) = peer.parent() {
                let _ = mixer.downcast::<gst::Element>().unwrap().remove_pad(peer);
            }
        }
    }

    fn handle_source_stream_change(
        &mut self,
        ctx: &mut Context<Self>,
        starting: bool,
        mixer: gst::Element,
        mixer_sinkpad: gst::Pad,
    ) {
        if starting {
            debug!(
                "Stream starting for current source, activating {:?}",
                mixer_sinkpad
            );

            if let Some(source) = self.current_source.as_mut() {
                source.n_streams += 1;
                trace!(
                    "Current source now has {} streams running",
                    source.n_streams
                );
            }
        } else {
            debug!(
                "Stream stopped for current source, deactivating {:?}",
                mixer_sinkpad
            );

            if mixer_sinkpad.has_property("alpha", None) {
                mixer_sinkpad.set_property("alpha", &0.).unwrap();
            } else {
                mixer_sinkpad.set_property("volume", &0.).unwrap();
            }

            mixer.remove_pad(&mixer_sinkpad).unwrap();

            if let Some(mut source) = self.current_source.take() {
                source.n_streams -= 1;

                trace!(
                    "Current source now has {} streams running",
                    source.n_streams
                );

                if source.n_streams == 0 {
                    self.tear_down_source(source);
                    self.schedule(ctx);
                } else {
                    self.current_source = Some(source);
                }
            }
        }
    }

    fn play_cued_source(
        &mut self,
        ctx: &mut Context<Self>,
        source: CuedSource,
    ) -> Result<(), Error> {
        let bin = gst::Bin::new(None);
        let src = make("fallbacksrc", None)?;
        let _ = bin.add(&src);

        src.set_property("uri", &source.uri).unwrap();

        let pipeline_clone = self.pipeline.downgrade();
        let bin_clone = bin.clone();
        let addr = ctx.address();
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
                });
            }
        });

        self.pipeline.add(&bin)?;

        bin.sync_state_with_parent()?;

        self.current_source = Some(Source {
            src: bin.upcast(),
            n_streams: 0,
        });

        Ok(())
    }

    fn schedule(&mut self, ctx: &mut Context<Self>) {
        trace!("Rescheduling");

        if let Some(handle) = self.schedule_handle.take() {
            ctx.cancel_future(handle);
        }

        if let Some(next_source) = {
            if let Some((_, Reverse(first_source))) = self.sources.peek() {
                if let Some(cue_time) = first_source.cue_time {
                    let now = Utc::now();
                    let timeout = cue_time - now;

                    trace!("Now is {:?}, next source cue time is {:?}", now, cue_time);

                    if timeout > chrono::Duration::zero() {
                        trace!(
                            "Next source's time hasn't come, going back to sleep for {:?}",
                            timeout
                        );
                        self.schedule_handle =
                            Some(ctx.run_later(timeout.to_std().unwrap(), |s, ctx| {
                                trace!("Waking up");
                                s.schedule(ctx);
                            }));
                        None
                    } else {
                        trace!("Next source's time has come");

                        if let Some(source) = self.current_source.take() {
                            trace!("Finalizing current source");
                            self.tear_down_source(source);
                        }
                        self.sources.pop()
                    }
                } else {
                    match self.current_source {
                        Some(_) => {
                            trace!("Current source still active");
                            None
                        }
                        None => {
                            trace!("No source currently playing, dequeuing");
                            self.sources.pop()
                        }
                    }
                }
            } else {
                trace!("No source cued at the time");
                None
            }
        } {
            debug!("Next source is ready for playback {:?}", next_source);
            // TODO use result
            let _ = self.play_cued_source(ctx, next_source.1 .0);

            // We dequeued a source, loop to check the next one, if any
            self.schedule(ctx);
        }
    }

    fn add_source(
        &mut self,
        ctx: &mut Context<Self>,
        uri: String,
        cue_time: Option<DateTime<Utc>>,
    ) -> uuid::Uuid {
        let src_id = uuid::Uuid::new_v4();

        self.sources.push(
            src_id,
            Reverse(CuedSource {
                uri,
                cue_time,
                seq: self.source_seq,
            }),
        );

        self.source_seq += 1;

        self.schedule(ctx);

        src_id
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
        MessageResult(ChannelInfo {
            id: self.id,
            name: self.name.to_string(),
            destination: self.destination.to_string(),
        })
    }
}

#[derive(Debug)]
pub struct AddSourceMessage {
    pub uri: String,
    pub cue_time: Option<DateTime<Utc>>,
}

impl Message for AddSourceMessage {
    type Result = uuid::Uuid;
}

impl Handler<AddSourceMessage> for Channel {
    type Result = MessageResult<AddSourceMessage>;

    fn handle(&mut self, msg: AddSourceMessage, ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(self.add_source(ctx, msg.uri, msg.cue_time))
    }
}
