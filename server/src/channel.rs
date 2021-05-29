use crate::utils::{StreamProducer, make_element};
use actix::{
    Actor, ActorContext, Addr, AsyncContext, Context, Handler, Message, MessageResult, SpawnHandle,
    StreamHandler,
};
use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use futures::prelude::*;
use gst::prelude::*;
use priority_queue::PriorityQueue;
use rtmp_switcher_controlling::controller::{
    ChannelInfo, DestinationFamily, DestinationInfo, DestinationStatus, SourceInfo, SourceStatus,
};
use std::cmp::{Ordering, Reverse};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, info, trace};

// Wrapper around the actual GStreamer elements
#[derive(Debug)]
struct Source {
    fallbacksrc: gst::Element,
    src: gst::Element,
}

// Logical source object, tracks metadata about the source
// and wraps an optional Source, which will only be present
// when the status is either Prerolling or Playing
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
    Source {
        id: uuid::Uuid,
        next: SourceStatus,
    },
    Destination {
        id: uuid::Uuid,
        next: DestinationStatus,
    },
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
struct ConsumerPipeline {
    pipeline: gst::Pipeline,
    video_appsrc: gst_app::AppSrc,
    audio_appsrc: gst_app::AppSrc,
}

// Logical destination object
#[derive(Debug)]
struct CuedDestination {
    id: uuid::Uuid,
    family: DestinationFamily,
    cue_time: DateTime<Utc>,
    end_time: Option<DateTime<Utc>>,
    status: DestinationStatus,
    consumer: Option<ConsumerPipeline>,
}

/// Actor that represents a channel
#[derive(Debug)]
pub struct Channel {
    channels: Arc<Mutex<HashMap<uuid::Uuid, Addr<Channel>>>>,
    pub id: uuid::Uuid,
    name: String,
    cued_actions: PriorityQueue<uuid::Uuid, Reverse<CuedAction>>,
    pipeline: gst::Pipeline,
    schedule_handle: Option<SpawnHandle>,
    sources: HashMap<uuid::Uuid, CuedSource>,
    audio_producer: StreamProducer,
    video_producer: StreamProducer,
    destinations: HashMap<uuid::Uuid, CuedDestination>,
}

impl Channel {
    pub fn new(
        channels: Arc<Mutex<HashMap<uuid::Uuid, Addr<Channel>>>>,
        name: &str,
    ) -> Self {
        let id = uuid::Uuid::new_v4();
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
            channels,
            id,
            name: name.to_string(),
            cued_actions: PriorityQueue::new(),
            pipeline,
            schedule_handle: None,
            sources: HashMap::new(),
            audio_producer: StreamProducer::from(&audio_appsink),
            video_producer: StreamProducer::from(&video_appsink),
            destinations: HashMap::new(),
        }
    }

    fn tear_down_source(&mut self, source: Source) {
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
    }

    fn tear_down_cued_source(&mut self, id: uuid::Uuid) -> Result<(), Error> {
        if let Some(mut source) = self.sources.remove(&id) {
            debug!("Tearing down source {}", id);

            if let Some(gst_source) = source.source.take() {
                self.tear_down_source(gst_source);
            }

            let _ = self.cued_actions.remove(&source.id);

            Ok(())
        } else {
            Err(anyhow!("No source with id {}", id))
        }
    }

    fn handle_source_stream_change(
        &mut self,
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
                    let _ = self.tear_down_cued_source(source_id);
                }
            }
        }
    }

    fn play_cued_source(&mut self, id: uuid::Uuid) -> Result<(), Error> {
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

    fn play_cued_destination(
        &mut self,
        ctx: &mut Context<Self>,
        id: uuid::Uuid,
    ) -> Result<(), Error> {
        if let Some(mut destination) = self.destinations.remove(&id) {
            match destination.family {
                DestinationFamily::RTMP { ref uri } => {
                    destination.consumer = Some(self.start_rtmp_consumer(ctx, uri)?);
                }
            }

            destination.status = DestinationStatus::Streaming;

            if let Some(end_time) = destination.end_time {
                self.cued_actions.push(
                    destination.id,
                    Reverse(CuedAction {
                        next_time: end_time,
                        item: CuedItem::Destination {
                            id: destination.id,
                            next: DestinationStatus::Stopped,
                        },
                    }),
                );
            }

            self.destinations.insert(id, destination);
        }

        Ok(())
    }

    fn tear_down_cued_destination(&mut self, id: uuid::Uuid) -> Result<(), Error> {
        if let Some(mut destination) = self.destinations.remove(&id) {
            debug!("Tearing down destination {}", id);

            if let Some(consumer) = destination.consumer.take() {
                self.video_producer.remove_consumer(&consumer.video_appsrc);
                self.audio_producer.remove_consumer(&consumer.audio_appsrc);
                let _ = consumer.pipeline.set_state(gst::State::Null);
            }

            let _ = self.cued_actions.remove(&destination.id);

            Ok(())
        } else {
            Err(anyhow!("No destination with id {}", id))
        }
    }

    fn preroll_cued_source(
        &mut self,
        ctx: &mut Context<Self>,
        id: uuid::Uuid,
    ) -> Result<(), Error> {
        if let Some(source) = self.sources.get_mut(&id) {
            let bin = gst::Bin::new(None);
            let src = make_element("fallbacksrc", None)?;
            let _ = bin.add(&src);

            src.set_property("uri", &source.uri).unwrap();
            src.set_property("manual-unblock", &true).unwrap();

            let pipeline_clone = self.pipeline.downgrade();
            let bin_clone = bin.clone();
            let addr = ctx.address();
            let source_id = source.id;
            src.connect_pad_added(move |_src, pad| {
                if let Some(pipeline) = pipeline_clone.upgrade() {
                    let is_video = pad.name() == "video";

                    let (mixer, pad) = {
                        if is_video {
                            // TODO: bubble up errors
                            let vscale = make_element("videoscale", None).unwrap();
                            let capsfilter = make_element("capsfilter", None).unwrap();
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
                            let audioconvert = make_element("audioconvert", None).unwrap();
                            let audioresample = make_element("audioresample", None).unwrap();
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
                        move |_pad, info| match info.data {
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
                        self.play_cued_source(id).unwrap();
                    }
                    SourceStatus::Stopped => {
                        debug!("Reached end time for source {}", id);
                        let _ = self.tear_down_cued_source(id);
                    }
                    _ => unreachable!(),
                },
                CuedItem::Destination { id, next } => match next {
                    DestinationStatus::Streaming => {
                        debug!("Next destination is ready for streaming {}", id);
                        self.play_cued_destination(ctx, id).unwrap();
                    }
                    DestinationStatus::Stopped => {
                        debug!("Reached end time for destination {}", id);
                        let _ = self.tear_down_cued_destination(id);
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
        mut source: CuedSource,
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> (Result<(), Error>, CuedSource) {
        if let Some(cue_time) = cue_time {
            if let Some(end_time) = end_time {
                if end_time <= cue_time {
                    return (
                        Err(anyhow!("cue_time {} > end_time {}", cue_time, end_time)),
                        source,
                    );
                }
            } else if let Some(end_time) = source.end_time {
                if end_time <= cue_time {
                    return (
                        Err(anyhow!("cue_time {} > end_time {}", cue_time, end_time)),
                        source,
                    );
                }
            }

            match source.status {
                SourceStatus::Initial => {
                    let _ = self.cued_actions.remove(&source.id);

                    source.cue_time = cue_time;

                    self.cued_actions.push(
                        source.id,
                        Reverse(CuedAction {
                            next_time: cue_time - chrono::Duration::seconds(10),
                            item: CuedItem::Source {
                                id: source.id,
                                next: SourceStatus::Prerolling,
                            },
                        }),
                    );
                }
                SourceStatus::Prerolling => {
                    let _ = self.cued_actions.remove(&source.id);

                    if let Some(gst_source) = source.source.take() {
                        self.tear_down_source(gst_source);
                    }

                    source.cue_time = cue_time;

                    self.cued_actions.push(
                        source.id,
                        Reverse(CuedAction {
                            next_time: cue_time - chrono::Duration::seconds(10),
                            item: CuedItem::Source {
                                id: source.id,
                                next: SourceStatus::Prerolling,
                            },
                        }),
                    );
                }
                SourceStatus::Playing | SourceStatus::Stopped => {
                    return (
                        Err(anyhow!(
                            "Cannot change cue time of source {} as its status is {:?}",
                            source.id,
                            source.status
                        )),
                        source,
                    );
                }
            }
        }

        if let Some(end_time) = end_time {
            if end_time <= source.cue_time {
                return (
                    Err(anyhow!(
                        "cue_time {} > end_time {}",
                        source.cue_time,
                        end_time
                    )),
                    source,
                );
            }

            match source.status {
                SourceStatus::Initial | SourceStatus::Prerolling => {
                    source.end_time = Some(end_time);
                }
                SourceStatus::Playing => {
                    let _ = self.cued_actions.remove(&source.id);

                    source.end_time = Some(end_time);

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
                SourceStatus::Stopped => {
                    return (
                        Err(anyhow!(
                            "Cannot change end time of source {} as its status is {:?}",
                            source.id,
                            source.status
                        )),
                        source,
                    );
                }
            }
        }

        (Ok(()), source)
    }

    fn modify_source_id(
        &mut self,
        ctx: &mut Context<Self>,
        source_id: uuid::Uuid,
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        if let Some(source) = self.sources.remove(&source_id) {
            let (res, source) = self.modify_source(source, cue_time, end_time);

            self.sources.insert(source_id, source);

            self.schedule(ctx);

            res
        } else {
            Err(anyhow!("No source with id {}", source_id))
        }
    }

    fn remove_source(&mut self, source_id: uuid::Uuid) -> Result<(), Error> {
        self.tear_down_cued_source(source_id)
    }

    fn add_destination(
        &mut self,
        ctx: &mut Context<Self>,
        family: DestinationFamily,
        cue_time: DateTime<Utc>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<uuid::Uuid, Error> {
        let destination_id = uuid::Uuid::new_v4();

        if let Some(end_time) = end_time {
            if end_time <= cue_time {
                return Err(anyhow!("cue_time {} > end_time {}", cue_time, end_time));
            }
        }

        self.destinations.insert(
            destination_id,
            CuedDestination {
                id: destination_id,
                family,
                cue_time,
                end_time,
                status: DestinationStatus::Initial,
                consumer: None,
            },
        );

        self.cued_actions.push(
            destination_id,
            Reverse(CuedAction {
                next_time: cue_time,
                item: CuedItem::Destination {
                    id: destination_id,
                    next: DestinationStatus::Streaming,
                },
            }),
        );

        self.schedule(ctx);

        Ok(destination_id)
    }

    fn modify_destination(
        &mut self,
        mut destination: CuedDestination,
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> (Result<(), Error>, CuedDestination) {
        if let Some(cue_time) = cue_time {
            if let Some(end_time) = end_time {
                if end_time <= cue_time {
                    return (
                        Err(anyhow!("cue_time {} > end_time {}", cue_time, end_time)),
                        destination,
                    );
                }
            } else if let Some(end_time) = destination.end_time {
                if end_time <= cue_time {
                    return (
                        Err(anyhow!("cue_time {} > end_time {}", cue_time, end_time)),
                        destination,
                    );
                }
            }

            match destination.status {
                DestinationStatus::Initial => {
                    let _ = self.cued_actions.remove(&destination.id);

                    destination.cue_time = cue_time;

                    self.cued_actions.push(
                        destination.id,
                        Reverse(CuedAction {
                            next_time: cue_time,
                            item: CuedItem::Destination {
                                id: destination.id,
                                next: DestinationStatus::Streaming,
                            },
                        }),
                    );
                }
                DestinationStatus::Streaming | DestinationStatus::Stopped => {
                    return (
                        Err(anyhow!(
                            "Cannot change cue time of destination {} as its status is {:?}",
                            destination.id,
                            destination.status
                        )),
                        destination,
                    );
                }
            }
        }

        if let Some(end_time) = end_time {
            if end_time <= destination.cue_time {
                return (
                    Err(anyhow!(
                        "cue_time {} > end_time {}",
                        destination.cue_time,
                        end_time
                    )),
                    destination,
                );
            }

            match destination.status {
                DestinationStatus::Initial => {
                    destination.end_time = Some(end_time);
                }
                DestinationStatus::Streaming => {
                    let _ = self.cued_actions.remove(&destination.id);

                    destination.end_time = Some(end_time);

                    self.cued_actions.push(
                        destination.id,
                        Reverse(CuedAction {
                            next_time: end_time,
                            item: CuedItem::Destination {
                                id: destination.id,
                                next: DestinationStatus::Stopped,
                            },
                        }),
                    );
                }
                DestinationStatus::Stopped => {
                    return (
                        Err(anyhow!(
                            "Cannot change end time of source {} as its status is {:?}",
                            destination.id,
                            destination.status
                        )),
                        destination,
                    );
                }
            }
        }

        (Ok(()), destination)
    }

    fn modify_destination_id(
        &mut self,
        ctx: &mut Context<Self>,
        destination_id: uuid::Uuid,
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        if let Some(destination) = self.destinations.remove(&destination_id) {
            let (res, destination) = self.modify_destination(destination, cue_time, end_time);

            self.destinations.insert(destination_id, destination);

            self.schedule(ctx);

            res
        } else {
            Err(anyhow!("No destination with id {}", destination_id))
        }
    }

    fn remove_destination(&mut self, destination_id: uuid::Uuid) -> Result<(), Error> {
        self.tear_down_cued_destination(destination_id)
    }

    fn start_rtmp_consumer(
        &mut self,
        ctx: &mut Context<Self>,
        uri: &String,
    ) -> Result<ConsumerPipeline, Error> {
        let pipeline = gst::Pipeline::new(None);

        let video_appsrc = gst::ElementFactory::make("appsrc", None)
            .unwrap()
            .downcast::<gst_app::AppSrc>()
            .unwrap();
        let timecodestamper = make_element("timecodestamper", None)?;
        let timeoverlay = make_element("timeoverlay", None)?;
        let venc = make_element("x264enc", None)?;
        let vparse = make_element("h264parse", None)?;
        let venc_queue = make_element("queue", None)?;

        let audio_appsrc = gst::ElementFactory::make("appsrc", None)
            .unwrap()
            .downcast::<gst_app::AppSrc>()
            .unwrap();
        let aconv = make_element("audioconvert", None)?;
        let aenc = make_element("faac", None)?;
        let aenc_queue = make_element("queue", None)?;

        let mux = make_element("flvmux", None)?;
        let mux_queue = make_element("queue", None)?;
        let sink = make_element("rtmp2sink", None)?;

        pipeline.add_many(&[
            video_appsrc.upcast_ref(),
            &timecodestamper,
            &timeoverlay,
            &venc,
            &vparse,
            &venc_queue,
            audio_appsrc.upcast_ref(),
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
            video_appsrc.upcast_ref(),
            &timecodestamper,
            &timeoverlay,
            &venc,
            &venc_queue,
            &mux,
            &mux_queue,
            &sink,
        ])?;

        gst::Element::link_many(&[audio_appsrc.upcast_ref(), &aconv, &aenc, &aenc_queue, &mux])?;

        let bus = pipeline.bus().expect("Pipeline with no bus");
        let bus_stream = bus.stream();
        Self::add_stream(bus_stream.map(BusMessage), ctx);

        pipeline.use_clock(Some(&gst::SystemClock::obtain()));
        pipeline.set_start_time(gst::CLOCK_TIME_NONE);
        pipeline.set_base_time(gst::ClockTime::from(0));

        self.video_producer.add_consumer(&video_appsrc);
        self.audio_producer.add_consumer(&audio_appsrc);

        pipeline.call_async(move |pipeline| {
            if let Err(_) = pipeline.set_state(gst::State::Playing) {
                error!("Failed to start consumer pipeline");
            }
        });

        Ok(ConsumerPipeline {
            pipeline,
            video_appsrc,
            audio_appsrc,
        })
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

        let bus = self.pipeline.bus().expect("Pipeline with no bus");
        let bus_stream = bus.stream();
        Self::add_stream(bus_stream.map(BusMessage), ctx);

        self.pipeline.use_clock(Some(&gst::SystemClock::obtain()));
        self.pipeline.set_start_time(gst::CLOCK_TIME_NONE);
        self.pipeline.set_base_time(gst::ClockTime::from(0));
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
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        let _ = self.channels.lock().unwrap().remove(&self.id);
        let _ = self.pipeline.set_state(gst::State::Null);
        let sources: Vec<CuedSource> = self.sources.drain().map(|(_, source)| source).collect();
        for mut source in sources {
            if let Some(source) = source.source.take() {
                self.tear_down_source(source);
            }
        }

        let destinations: Vec<CuedDestination> = self
            .destinations
            .drain()
            .map(|(_, destination)| destination)
            .collect();
        for mut destination in destinations {
            if let Some(consumer) = destination.consumer.take() {
                self.video_producer.remove_consumer(&consumer.video_appsrc);
                self.audio_producer.remove_consumer(&consumer.audio_appsrc);
                let _ = consumer.pipeline.set_state(gst::State::Null);
            }
        }

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

    fn handle(&mut self, _msg: GetInfoMessage, _ctx: &mut Context<Self>) -> Self::Result {
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

        let mut destinations: Vec<DestinationInfo> = self
            .destinations
            .values()
            .map(|destination| DestinationInfo {
                id: destination.id,
                family: destination.family.clone(),
                cue_time: destination.cue_time,
                end_time: destination.end_time,
                status: destination.status,
            })
            .collect();

        destinations.sort_by(|a, b| a.cue_time.cmp(&b.cue_time));

        MessageResult(ChannelInfo {
            id: self.id,
            name: self.name.to_string(),
            sources,
            destinations,
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
    pub cue_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
}

impl Message for ModifySourceMessage {
    type Result = Result<(), Error>;
}

impl Handler<ModifySourceMessage> for Channel {
    type Result = MessageResult<ModifySourceMessage>;

    fn handle(&mut self, msg: ModifySourceMessage, ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(self.modify_source_id(ctx, msg.id, msg.cue_time, msg.end_time))
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

    fn handle(&mut self, msg: RemoveSourceMessage, _ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(self.remove_source(msg.id))
    }
}

#[derive(Debug)]
pub struct AddDestinationMessage {
    pub family: DestinationFamily,
    pub cue_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
}

impl Message for AddDestinationMessage {
    type Result = Result<uuid::Uuid, Error>;
}

impl Handler<AddDestinationMessage> for Channel {
    type Result = MessageResult<AddDestinationMessage>;

    fn handle(&mut self, msg: AddDestinationMessage, ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(self.add_destination(ctx, msg.family, msg.cue_time, msg.end_time))
    }
}

#[derive(Debug)]
pub struct ModifyDestinationMessage {
    pub id: uuid::Uuid,
    pub cue_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
}

impl Message for ModifyDestinationMessage {
    type Result = Result<(), Error>;
}

impl Handler<ModifyDestinationMessage> for Channel {
    type Result = MessageResult<ModifyDestinationMessage>;

    fn handle(&mut self, msg: ModifyDestinationMessage, ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(self.modify_destination_id(ctx, msg.id, msg.cue_time, msg.end_time))
    }
}

#[derive(Debug)]
pub struct RemoveDestinationMessage {
    pub id: uuid::Uuid,
}

impl Message for RemoveDestinationMessage {
    type Result = Result<(), Error>;
}

impl Handler<RemoveDestinationMessage> for Channel {
    type Result = MessageResult<RemoveDestinationMessage>;

    fn handle(&mut self, msg: RemoveDestinationMessage, _ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(self.remove_destination(msg.id))
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
            MessageView::Latency(_) => {
                if let Some(src) = msg.0.src() {
                    if &src == self.pipeline.upcast_ref::<gst::Object>()
                        || src.has_as_ancestor(&self.pipeline)
                    {
                        debug!("Channel updated latency");
                        self.pipeline.call_async(|pipeline| {
                            let _ = pipeline.recalculate_latency();
                        });
                    } else if let Some(bin) = src.downcast_ref::<gst::Bin>() {
                        debug!("Consumer pipeline updated latency");
                        bin.call_async(|pipeline| {
                            let _ = pipeline.recalculate_latency();
                        });
                    }
                }
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

    fn handle(&mut self, msg: StreamMessage, _ctx: &mut Context<Self>) {
        self.handle_source_stream_change(msg.starting, msg.mixer, msg.mixer_sinkpad, msg.source_id);
    }
}
