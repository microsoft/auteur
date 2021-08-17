//! A mixer processing node.
//!
//! A mixer can have multiple consumer slots, which will be routed
//! through `compositor` and `audiomixer` elements.

use actix::prelude::*;
use anyhow::{anyhow, Error};
use gst::prelude::*;
use gst_base::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, instrument, trace};

use auteur_controlling::controller::{
    ControlPoint, MixerCommand, MixerConfig, MixerInfo, MixerSlotInfo, NodeInfo, State,
};

use crate::node::{
    ConsumerMessage, GetNodeInfoMessage, GetProducerMessage, MixerCommandMessage, NodeManager,
    NodeStatusMessage, ScheduleMessage, StartMessage, StopMessage, StoppedMessage,
};
use crate::utils::{
    get_now, make_element, ErrorMessage, PipelineManager, PropertyController, Schedulable,
    StateChangeResult, StateMachine, StopManagerMessage, StreamProducer,
};

const DEFAULT_FALLBACK_TIMEOUT: u32 = 500;

/// Represents a connection to a producer
struct ConsumerSlot {
    /// Video producer
    video_producer: StreamProducer,
    /// Audio producer
    audio_producer: StreamProducer,
    /// Video input to `compositor`
    video_appsrc: gst_app::AppSrc,
    /// Audio input to `audiomixer`
    audio_appsrc: gst_app::AppSrc,
    /// Processing elements before `compositor`
    video_bin: Option<gst::Bin>,
    /// Processing elements before `audiomixer`
    audio_bin: Option<gst::Bin>,
    /// Volume of the `audiomixer` pad
    volume: f64,
    /// Used to reconfigure the geometry of the input video stream
    video_capsfilter: Option<gst::Element>,
    /// The video mixer pad
    video_pad: gst::Pad,
    /// The audio mixer pad
    audio_pad: gst::Pad,
}

/// Used from our `compositor::samples_selected` callback
#[derive(Debug)]
pub struct VideoMixingState {
    /// For how long no pad other than our base plate has selected samples
    base_plate_timeout: gst::ClockTime,
    /// Whether our base plate is opaque
    showing_base_plate: bool,
    /// Our slot controllers
    controllers: Option<HashMap<String, PropertyController>>,
    /// The last observed PTS, for interpolating
    last_pts: gst::ClockTime,
}

/// Used from our `audiomixer::samples_selected` callback
#[derive(Debug)]
pub struct AudioMixingState {
    /// Our slot controllers
    controllers: Option<HashMap<String, PropertyController>>,
    /// The last observed PTS, for interpolating
    last_pts: gst::ClockTime,
}

/// The Mixer actor
pub struct Mixer {
    /// Unique identifier
    id: String,
    /// The wrapped pipeline
    pipeline: gst::Pipeline,
    /// A helper for managing the pipeline
    pipeline_manager: Option<Addr<PipelineManager>>,
    /// Output video producer
    video_producer: StreamProducer,
    /// Output audio producer
    audio_producer: StreamProducer,
    /// Input connection points
    consumer_slots: HashMap<String, ConsumerSlot>,
    /// `audiomixer`
    audio_mixer: gst::Element,
    /// `compositor`
    video_mixer: gst::Element,
    /// Mixing geometry and format
    config: MixerConfig,
    /// Used for showing and hiding the base plate, and updating slot video controllers
    video_mixing_state: Arc<Mutex<VideoMixingState>>,
    /// Used for updating slot audio controllers
    audio_mixing_state: Arc<Mutex<AudioMixingState>>,
    /// Optional timeout for showing the base plate
    fallback_timeout: gst::ClockTime,
    /// For controlling the output sample rate
    audio_capsfilter: Option<gst::Element>,
    /// For resizing the base plate
    base_plate_capsfilter: Option<gst::Element>,
    /// For resizing our output video stream
    video_capsfilter: Option<gst::Element>,
    /// Our state machine
    state_machine: StateMachine,
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

    #[instrument(level = "debug", name = "stopped", skip(self, _ctx), fields(id = %self.id))]
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if let Some(manager) = self.pipeline_manager.take() {
            let _ = manager.do_send(StopManagerMessage);
        }

        for (id, slot) in self.consumer_slots.drain() {
            slot.video_producer.remove_consumer(&id);
            slot.audio_producer.remove_consumer(&id);
        }

        NodeManager::from_registry().do_send(StoppedMessage {
            id: self.id.clone(),
            video_producer: Some(self.video_producer.clone()),
            audio_producer: Some(self.video_producer.clone()),
        });
    }
}

impl Mixer {
    /// Create a mixer
    pub fn new(id: &str, config: MixerConfig) -> Self {
        let pipeline = gst::Pipeline::new(None);

        let audio_appsink =
            gst::ElementFactory::make("appsink", Some(&format!("mixer-audio-appsink-{}", id)))
                .unwrap()
                .downcast::<gst_app::AppSink>()
                .unwrap();

        let video_appsink =
            gst::ElementFactory::make("appsink", Some(&format!("mixer-video-appsink-{}", id)))
                .unwrap()
                .downcast::<gst_app::AppSink>()
                .unwrap();

        let audio_mixer = make_element("audiomixer", Some("audiomixer")).unwrap();
        let video_mixer = make_element("compositor", Some("compositor")).unwrap();

        pipeline
            .add_many(&[&audio_appsink, &video_appsink])
            .unwrap();

        let fallback_timeout = config.fallback_timeout.unwrap_or(DEFAULT_FALLBACK_TIMEOUT);

        Self {
            id: id.to_string(),
            pipeline,
            pipeline_manager: None,
            audio_producer: StreamProducer::from(&audio_appsink),
            video_producer: StreamProducer::from(&video_appsink),
            consumer_slots: HashMap::new(),
            audio_mixer,
            video_mixer,
            config,
            video_mixing_state: Arc::new(Mutex::new(VideoMixingState {
                base_plate_timeout: gst::CLOCK_TIME_NONE,
                showing_base_plate: true,
                controllers: Some(HashMap::new()),
                last_pts: gst::CLOCK_TIME_NONE,
            })),
            audio_mixing_state: Arc::new(Mutex::new(AudioMixingState {
                controllers: Some(HashMap::new()),
                last_pts: gst::CLOCK_TIME_NONE,
            })),
            fallback_timeout: fallback_timeout as u64 * gst::MSECOND,
            audio_capsfilter: None,
            video_capsfilter: None,
            base_plate_capsfilter: None,
            state_machine: StateMachine::default(),
        }
    }

    fn parse_slot_config_key(property: &str) -> Result<(bool, &str), Error> {
        let split: Vec<&str> = property.splitn(2, "::").collect();

        match split.len() {
            2 => match split[0] {
                "video" => Ok((true, split[1])),
                "audio" => Ok((false, split[1])),
                _ => Err(anyhow!(
                    "Slot controller property media type must be one of {audio, video}"
                )),
            },
            _ => Err(anyhow!(
                "Slot controller property name must be in form media-type::property-name"
            )),
        }
    }

    /// Connect an input slot to `compositor` and `audiomixer`
    #[instrument(level = "debug", name = "connecting", skip(pipeline, slot))]
    fn connect_slot(
        pipeline: &gst::Pipeline,
        slot: &mut ConsumerSlot,
        mixer_id: &str,
        id: &str,
        config: &MixerConfig,
    ) -> Result<(), Error> {
        let video_bin = gst::Bin::new(None);
        let audio_bin = gst::Bin::new(None);

        let aconv = make_element("audioconvert", None)?;
        let aresample = make_element("audioresample", None)?;
        let acapsfilter = make_element("capsfilter", None)?;
        let aqueue = make_element("queue", None)?;
        let vqueue = make_element("queue", None)?;

        // FIXME: https://gitlab.freedesktop.org/gstreamer/gst-plugins-base/-/merge_requests/1156
        let vconv = make_element("videoconvert", None)?;
        let vscale = make_element("videoscale", None)?;
        let vcapsfilter = make_element("capsfilter", None)?;
        vcapsfilter
            .set_property(
                "caps",
                &gst::Caps::builder("video/x-raw")
                    .field("width", &config.width)
                    .field("height", &config.height)
                    .field("pixel-aspect-ratio", &gst::Fraction::new(1, 1))
                    .build(),
            )
            .unwrap();

        acapsfilter
            .set_property(
                "caps",
                &gst::Caps::builder("audio/x-raw")
                    .field("channels", &2)
                    .field("format", &"S16LE")
                    .field("rate", &96000)
                    .build(),
            )
            .unwrap();

        let vappsrc_elem: &gst::Element = slot.video_appsrc.upcast_ref();
        let aappsrc_elem: &gst::Element = slot.audio_appsrc.upcast_ref();

        video_bin.add_many(&[vappsrc_elem, &vconv, &vscale, &vcapsfilter, &vqueue])?;

        audio_bin.add_many(&[aappsrc_elem, &aconv, &aresample, &acapsfilter, &aqueue])?;

        pipeline.add_many(&[&video_bin, &audio_bin])?;

        video_bin.sync_state_with_parent()?;
        audio_bin.sync_state_with_parent()?;

        let ghost =
            gst::GhostPad::with_target(Some("src"), &vqueue.static_pad("src").unwrap()).unwrap();
        video_bin.add_pad(&ghost).unwrap();

        let ghost =
            gst::GhostPad::with_target(Some("src"), &aqueue.static_pad("src").unwrap()).unwrap();
        audio_bin.add_pad(&ghost).unwrap();

        slot.audio_pad.set_property("volume", &slot.volume).unwrap();

        gst::Element::link_many(&[aappsrc_elem, &aconv, &aresample, &acapsfilter, &aqueue])?;
        gst::Element::link_many(&[vappsrc_elem, &vconv, &vscale, &vcapsfilter, &vqueue])?;

        let srcpad = audio_bin.static_pad("src").unwrap();
        srcpad.link(&slot.audio_pad).unwrap();

        let srcpad = video_bin.static_pad("src").unwrap();
        srcpad.link(&slot.video_pad).unwrap();

        slot.audio_bin = Some(audio_bin);
        slot.video_bin = Some(video_bin);
        slot.video_capsfilter = Some(vcapsfilter);

        slot.video_producer.add_consumer(&slot.video_appsrc, id);
        slot.audio_producer.add_consumer(&slot.audio_appsrc, id);

        Ok(())
    }

    /// Build the base plate. It may be either a live videotestsrc, or an
    /// imagefreeze'd image when a fallback image was specified
    #[instrument(level = "debug", name = "building base plate", skip(self), fields(id = %self.id))]
    fn build_base_plate(&mut self) -> Result<gst::Element, Error> {
        let bin = gst::Bin::new(None);

        let ghost = match self.config.fallback_image.as_ref() {
            Some(path) => {
                let filesrc = make_element("filesrc", None)?;
                let decodebin = make_element("decodebin3", None)?;
                let vconv = make_element("videoconvert", None)?;
                let imagefreeze = make_element("imagefreeze", None)?;

                /* We have to rescale after imagefreeze for now, as we might
                 * need to update the resolution dynamically */
                let vscale = make_element("videoscale", None)?;
                let capsfilter = make_element("capsfilter", None)?;

                filesrc.set_property("location", path).unwrap();
                imagefreeze.set_property("is-live", &true).unwrap();
                capsfilter
                    .set_property(
                        "caps",
                        &gst::Caps::builder("video/x-raw")
                            .field("width", &self.config.width)
                            .field("height", &self.config.height)
                            .field("pixel-aspect-ratio", &gst::Fraction::new(1, 1))
                            .build(),
                    )
                    .unwrap();

                bin.add_many(&[
                    &filesrc,
                    &decodebin,
                    &imagefreeze,
                    &vconv,
                    &vscale,
                    &capsfilter,
                ])?;

                let imagefreeze_clone = imagefreeze.downgrade();
                decodebin.connect_pad_added(move |_bin, pad| {
                    if let Some(imagefreeze) = imagefreeze_clone.upgrade() {
                        let sinkpad = imagefreeze.static_pad("sink").unwrap();
                        pad.link(&sinkpad).unwrap();
                    }
                });

                filesrc.link(&decodebin)?;

                gst::Element::link_many(&[&imagefreeze, &vconv, &vscale, &capsfilter])?;

                self.base_plate_capsfilter = Some(capsfilter.clone());

                gst::GhostPad::with_target(Some("src"), &capsfilter.static_pad("src").unwrap())
                    .unwrap()
            }
            None => {
                let vsrc = make_element("videotestsrc", None)?;
                vsrc.set_property("is-live", &true).unwrap();
                vsrc.set_property_from_str("pattern", "black");

                bin.add(&vsrc)?;

                gst::GhostPad::with_target(Some("src"), &vsrc.static_pad("src").unwrap()).unwrap()
            }
        };

        bin.add_pad(&ghost).unwrap();

        Ok(bin.upcast())
    }

    /// Update slot controllers
    #[instrument(
        name = "Updating audio mixing state",
        level = "trace",
        skip(mixing_state)
    )]
    fn update_audio_mixing_state(
        agg: &gst_base::Aggregator,
        id: &str,
        pts: gst::ClockTime,
        mixing_state: &mut AudioMixingState,
    ) {
        let duration = if mixing_state.last_pts.is_none() {
            gst::CLOCK_TIME_NONE
        } else {
            pts - mixing_state.last_pts
        };

        mixing_state.controllers = Some(Mixer::synchronize_controllers(
            agg,
            id,
            duration,
            &mut mixing_state.controllers.take().unwrap(),
        ));

        mixing_state.last_pts = pts;
    }

    #[instrument(name = "synchronizing controllers", level = "trace", skip(controllers))]
    fn synchronize_controllers(
        agg: &gst_base::Aggregator,
        id: &str,
        duration: gst::ClockTime,
        controllers: &mut HashMap<String, PropertyController>,
    ) -> HashMap<String, PropertyController> {
        let now = get_now();
        let mut updated_controllers = HashMap::new();

        for (id, mut controller) in controllers.drain() {
            if !controller.synchronize(now, duration) {
                updated_controllers.insert(id, controller);
            }
        }

        updated_controllers
    }

    /// Show or hide our base plate, update slot controllers
    #[instrument(
        name = "Updating video mixing state",
        level = "trace",
        skip(mixing_state)
    )]
    fn update_video_mixing_state(
        agg: &gst_base::Aggregator,
        id: &str,
        pts: gst::ClockTime,
        mixing_state: &mut VideoMixingState,
        timeout: gst::ClockTime,
    ) {
        let mut base_plate_only = true;

        let base_plate_pad = agg.static_pad("sink_0").unwrap();

        for pad in agg.sink_pads() {
            if pad == base_plate_pad {
                continue;
            }

            let agg_pad: &gst_base::AggregatorPad = pad.downcast_ref().unwrap();
            if let Some(sample) = agg.peek_next_sample(agg_pad) {
                trace!(pad = %pad.name(), "selected non-base plate sample {:?}", sample);
                base_plate_only = false;
                break;
            }
        }

        if base_plate_only {
            if mixing_state.base_plate_timeout.is_none() {
                mixing_state.base_plate_timeout = pts;
            } else if !mixing_state.showing_base_plate
                && pts - mixing_state.base_plate_timeout > timeout
            {
                debug!("falling back to base plate {:?}", base_plate_pad);
                base_plate_pad.set_property("alpha", &1.0f64).unwrap();
                mixing_state.showing_base_plate = true;
            }
        } else {
            if mixing_state.showing_base_plate {
                debug!("hiding base plate: {:?}", base_plate_pad);
                base_plate_pad.set_property("alpha", &0.0f64).unwrap();
                mixing_state.showing_base_plate = false;
            }
            mixing_state.base_plate_timeout = gst::CLOCK_TIME_NONE;
        }

        let duration = if mixing_state.last_pts.is_none() {
            gst::CLOCK_TIME_NONE
        } else {
            pts - mixing_state.last_pts
        };

        mixing_state.controllers = Some(Mixer::synchronize_controllers(
            agg,
            id,
            duration,
            &mut mixing_state.controllers.take().unwrap(),
        ));

        mixing_state.last_pts = pts;
    }

    /// Start our pipeline when cue_time is reached
    #[instrument(level = "debug", name = "mixing", skip(self, ctx), fields(id = %self.id))]
    fn start_pipeline(&mut self, ctx: &mut Context<Self>) -> Result<StateChangeResult, Error> {
        let vsrc = self.build_base_plate()?;
        let vqueue = make_element("queue", None)?;
        let vcapsfilter = make_element("capsfilter", None)?;

        let asrc = make_element("audiotestsrc", None)?;
        let asrccapsfilter = make_element("capsfilter", None)?;
        let aqueue = make_element("queue", None)?;
        let acapsfilter = make_element("capsfilter", None)?;
        let level = make_element("level", None)?;
        let aresample = make_element("audioresample", None)?;
        let aresamplecapsfilter = make_element("capsfilter", None)?;

        self.video_mixer
            .set_property_from_str("background", "black");
        self.video_mixer
            .set_property(
                "start-time-selection",
                &gst_base::AggregatorStartTimeSelection::First,
            )
            .unwrap();
        self.video_mixer
            .set_property("ignore-inactive-pads", &true)
            .unwrap();

        debug!("stream config: {:?}", self.config);

        vcapsfilter
            .set_property(
                "caps",
                &gst::Caps::builder("video/x-raw")
                    .field("width", &self.config.width)
                    .field("height", &self.config.height)
                    .field("framerate", &gst::Fraction::new(30, 1))
                    .field("pixel-aspect-ratio", &gst::Fraction::new(1, 1))
                    .field("format", &"AYUV")
                    .field("colorimetry", &"bt601")
                    .field("chroma-site", &"jpeg")
                    .field("interlace-mode", &"progressive")
                    .build(),
            )
            .unwrap();
        asrc.set_property("is-live", &true).unwrap();
        asrc.set_property("volume", &0.).unwrap();
        self.audio_mixer
            .set_property(
                "start-time-selection",
                &gst_base::AggregatorStartTimeSelection::First,
            )
            .unwrap();
        self.audio_mixer
            .set_property("ignore-inactive-pads", &true)
            .unwrap();

        // FIXME: audiomixer doesn't deal very well with audio rate changes,
        // for now the solution is to simply pick a very high sample rate
        // (96000 was picked because it is the maximum rate faac supports),
        // and never change that fixed rate in the mixer, simply modulating
        // it downstream according to what the application requires.
        //
        // Alternatively, we could avoid exposing that config switch, and
        // always output 48000, which should be more than enough for anyone
        asrccapsfilter
            .set_property(
                "caps",
                &gst::Caps::builder("audio/x-raw")
                    .field("channels", &2)
                    .field("format", &"S16LE")
                    .field("rate", &96000)
                    .build(),
            )
            .unwrap();

        acapsfilter
            .set_property(
                "caps",
                &gst::Caps::builder("audio/x-raw")
                    .field("channels", &2)
                    .field("format", &"S16LE")
                    .field("rate", &96000)
                    .build(),
            )
            .unwrap();

        aresamplecapsfilter
            .set_property(
                "caps",
                &gst::Caps::builder("audio/x-raw")
                    .field("rate", &self.config.sample_rate)
                    .build(),
            )
            .unwrap();

        self.pipeline.add_many(&[
            &vsrc,
            &vqueue,
            &self.video_mixer,
            &vcapsfilter,
            &asrc,
            &asrccapsfilter,
            &aqueue,
            &self.audio_mixer,
            &acapsfilter,
            &level,
            &aresample,
            &aresamplecapsfilter,
        ])?;

        gst::Element::link_many(&[
            &vsrc,
            &vqueue,
            &self.video_mixer,
            &vcapsfilter,
            self.video_producer.appsink().upcast_ref(),
        ])?;

        gst::Element::link_many(&[
            &asrc,
            &asrccapsfilter,
            &aqueue,
            &self.audio_mixer,
            &acapsfilter,
            &level,
            &aresample,
            &aresamplecapsfilter,
            self.audio_producer.appsink().upcast_ref(),
        ])?;

        for (id, slot) in self.consumer_slots.iter_mut() {
            Mixer::connect_slot(&self.pipeline, slot, &self.id, id, &self.config)?;
        }

        let video_mixing_state = self.video_mixing_state.clone();
        let id = self.id.clone();
        let timeout = self.fallback_timeout;

        self.video_mixer
            .set_property("emit-signals", &true)
            .unwrap();
        self.video_mixer
            .downcast_ref::<gst_base::Aggregator>()
            .unwrap()
            .connect_samples_selected(
                move |agg: &gst_base::Aggregator, _segment, pts, _dts, _duration, _info| {
                    let mut mixing_state = video_mixing_state.lock().unwrap();
                    Mixer::update_video_mixing_state(agg, &id, pts, &mut *mixing_state, timeout);
                },
            );

        let audio_mixing_state = self.audio_mixing_state.clone();
        let id = self.id.clone();

        self.audio_mixer
            .set_property("emit-signals", &true)
            .unwrap();
        self.audio_mixer
            .downcast_ref::<gst_base::Aggregator>()
            .unwrap()
            .connect_samples_selected(
                move |agg: &gst_base::Aggregator, _segment, pts, _dts, _duration, _info| {
                    let mut mixing_state = audio_mixing_state.lock().unwrap();
                    Mixer::update_audio_mixing_state(agg, &id, pts, &mut *mixing_state);
                },
            );

        self.video_capsfilter = Some(vcapsfilter);
        self.audio_capsfilter = Some(aresamplecapsfilter);

        let addr = ctx.address();
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

        Ok(StateChangeResult::Success)
    }

    #[instrument(level = "debug", name = "updating slot volume", skip(self), fields(id = %self.id))]
    fn set_slot_volume(&mut self, slot_id: &str, volume: f64) -> Result<(), Error> {
        if !(0. ..=10.).contains(&volume) {
            return Err(anyhow!("invalid slot volume: {}", volume));
        }

        if let Some(mut slot) = self.consumer_slots.get_mut(slot_id) {
            slot.volume = volume;

            if let Some(ref audio_bin) = slot.audio_bin {
                let mixer_pad = audio_bin.static_pad("src").unwrap().peer().unwrap();
                mixer_pad.set_property("volume", &volume).unwrap();
            }
            Ok(())
        } else {
            Err(anyhow!("mixer {} has no slot with id {}", self.id, slot_id))
        }
    }

    /// Implement UpdateConfig
    #[instrument(level = "debug", name = "updating config", skip(self), fields(id = %self.id))]
    fn update_config(
        &mut self,
        width: Option<i32>,
        height: Option<i32>,
        sample_rate: Option<i32>,
    ) -> Result<(), Error> {
        if let Some(width) = width {
            self.config.width = width;
        }

        if let Some(height) = height {
            self.config.height = height;
        }

        if let Some(sample_rate) = sample_rate {
            self.config.sample_rate = sample_rate;
        }

        // FIXME: do this atomically from selected_samples for tear-free transition
        // once https://gitlab.freedesktop.org/gstreamer/gst-plugins-base/-/merge_requests/1156 is
        // in

        if let Some(capsfilter) = &self.video_capsfilter {
            debug!("updating output resolution");
            capsfilter
                .set_property(
                    "caps",
                    &gst::Caps::builder("video/x-raw")
                        .field("width", &self.config.width)
                        .field("height", &self.config.height)
                        .field("framerate", &gst::Fraction::new(30, 1))
                        .field("pixel-aspect-ratio", &gst::Fraction::new(1, 1))
                        .field("format", &"I420")
                        .field("colorimetry", &"bt601")
                        .field("chroma-site", &"jpeg")
                        .field("interlace-mode", &"progressive")
                        .build(),
                )
                .unwrap();
        }

        if let Some(capsfilter) = &self.base_plate_capsfilter {
            debug!("updating fallback image resolution");
            capsfilter
                .set_property(
                    "caps",
                    &gst::Caps::builder("video/x-raw")
                        .field("width", &self.config.width)
                        .field("height", &self.config.height)
                        .field("pixel-aspect-ratio", &gst::Fraction::new(1, 1))
                        .build(),
                )
                .unwrap();
        }

        for (slot_id, slot) in &self.consumer_slots {
            if let Some(ref capsfilter) = slot.video_capsfilter {
                debug!(slot_id = %slot_id,"updating mixer slot resolution");
                capsfilter
                    .set_property(
                        "caps",
                        &gst::Caps::builder("video/x-raw")
                            .field("width", &self.config.width)
                            .field("height", &self.config.height)
                            .field("pixel-aspect-ratio", &gst::Fraction::new(1, 1))
                            .build(),
                    )
                    .unwrap();
            }
        }

        if let Some(capsfilter) = &self.audio_capsfilter {
            debug!("Updating output audio rate");
            capsfilter
                .set_property(
                    "caps",
                    &gst::Caps::builder("audio/x-raw")
                        .field("rate", &self.config.sample_rate)
                        .build(),
                )
                .unwrap();
        }

        Ok(())
    }

    /// Implement Connect command
    #[instrument(level = "debug", name = "connecting", skip(self, video_producer, audio_producer), fields(id = %self.id))]
    fn connect(
        &mut self,
        link_id: &str,
        video_producer: &StreamProducer,
        audio_producer: &StreamProducer,
        config: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<(), Error> {
        if self.consumer_slots.contains_key(link_id) {
            return Err(anyhow!("mixer {} already has link {}", self.id, link_id));
        }

        let video_pad = self.video_mixer.request_pad_simple("sink_%u").unwrap();
        let audio_pad = self.audio_mixer.request_pad_simple("sink_%u").unwrap();

        if let Some(config) = config {
            for (key, value) in config {
                let (is_video, property) = Mixer::parse_slot_config_key(&key)?;

                let pad = if is_video { &video_pad } else { &audio_pad };

                PropertyController::validate_value(property, pad.upcast_ref(), &value)?;

                debug!("Setting initial slot config {} {}", property, value);

                PropertyController::set_property_from_value(pad.upcast_ref(), property, &value);
            }
        }

        let video_appsrc = gst::ElementFactory::make(
            "appsrc",
            Some(&format!("mixer-slot-video-appsrc-{}", link_id)),
        )
        .unwrap()
        .downcast::<gst_app::AppSrc>()
        .unwrap();
        let audio_appsrc = gst::ElementFactory::make(
            "appsrc",
            Some(&format!("mixer-slot-audio-appsrc-{}", link_id)),
        )
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
            video_appsrc,
            audio_appsrc,
            audio_bin: None,
            video_bin: None,
            volume: 1.0,
            video_capsfilter: None,
            video_pad,
            audio_pad,
        };

        if self.state_machine.state == State::Started {
            if let Err(err) =
                Mixer::connect_slot(&self.pipeline, &mut slot, &self.id, link_id, &self.config)
            {
                return Err(err);
            }
        }

        self.consumer_slots.insert(link_id.to_string(), slot);

        Ok(())
    }

    /// Implement Disconnect command
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

                self.video_mixer.release_request_pad(&mixer_pad);
            }
            if let Some(audio_bin) = slot.audio_bin {
                let mixer_pad = audio_bin.static_pad("src").unwrap().peer().unwrap();

                audio_bin.set_locked_state(true);
                audio_bin.set_state(gst::State::Null).unwrap();
                self.pipeline.remove(&audio_bin).unwrap();

                self.audio_mixer.release_request_pad(&mixer_pad);
            }

            Ok(())
        } else {
            Err(anyhow!("mixer {} has no slot with id {}", self.id, slot_id))
        }
    }

    /// Implement AddControlPoint command for slots
    #[instrument(level = "debug", name = "controlling-slot", skip(self), fields(id = %self.id))]
    fn add_slot_control_point(
        &mut self,
        slot_id: &str,
        property: &str,
        point: ControlPoint,
    ) -> Result<(), Error> {
        if let Some(slot) = self.consumer_slots.get(slot_id) {
            let (is_video, property) = Mixer::parse_slot_config_key(property)?;

            let pad = if is_video {
                slot.video_pad.clone()
            } else {
                slot.audio_pad.clone()
            };

            debug!(slot_id = %slot_id, pad_name = %pad.name(), property = %property, "Upserting controller");

            PropertyController::validate_control_point(property, pad.upcast_ref(), &point)?;

            let id = slot_id.to_owned() + property;

            if is_video {
                let mut mixing_state = self.video_mixing_state.lock().unwrap();

                mixing_state
                    .controllers
                    .as_mut()
                    .unwrap()
                    .entry(id)
                    .or_insert_with(|| PropertyController::new(slot_id, pad.upcast(), property))
                    .push_control_point(point);
            } else {
                let mut mixing_state = self.audio_mixing_state.lock().unwrap();

                mixing_state
                    .controllers
                    .as_mut()
                    .unwrap()
                    .entry(id)
                    .or_insert_with(|| PropertyController::new(slot_id, pad.upcast(), property))
                    .push_control_point(point);
            }

            Ok(())
        } else {
            Err(anyhow!("mixer {} has no slot with id {}", self.id, slot_id))
        }
    }

    /// Implement RemoveControlPoint command for slots
    #[instrument(level = "debug", name = "removing control point", skip(self), fields(id = %self.id))]
    fn remove_slot_control_point(&mut self, controller_id: &str, slot_id: &str, property: &str) {
        let split: Vec<&str> = property.splitn(2, "::").collect();

        let (is_video, property) = match split.len() {
            2 => match split[0] {
                "video" => (true, split[1]),
                "audio" => (false, split[1]),
                _ => {
                    return;
                }
            },
            _ => {
                return;
            }
        };

        let id = slot_id.to_owned() + property;

        if is_video {
            let mut mixing_state = self.video_mixing_state.lock().unwrap();

            if let Some(controller) = mixing_state.controllers.as_mut().unwrap().get_mut(&id) {
                controller.remove_control_point(controller_id);
            }
        } else {
            let mut mixing_state = self.audio_mixing_state.lock().unwrap();

            if let Some(controller) = mixing_state.controllers.as_mut().unwrap().get_mut(&id) {
                controller.remove_control_point(controller_id);
            }
        }
    }

    fn slot_control_points(&self) -> HashMap<String, HashMap<String, Vec<ControlPoint>>> {
        let mut ret = HashMap::new();

        let mixing_state = self.video_mixing_state.lock().unwrap();

        for controller in mixing_state.controllers.as_ref().unwrap().values() {
            ret.entry(controller.controllee_id.clone())
                .or_insert_with(HashMap::new)
                .insert(
                    "video::".to_owned() + &controller.propname,
                    controller.control_points(),
                );
        }

        let mixing_state = self.audio_mixing_state.lock().unwrap();

        for controller in mixing_state.controllers.as_ref().unwrap().values() {
            ret.entry(controller.controllee_id.clone())
                .or_insert_with(HashMap::new)
                .insert(
                    "audio::".to_owned() + &controller.propname,
                    controller.control_points(),
                );
        }

        ret
    }

    #[instrument(level = "debug", skip(self, ctx), fields(id = %self.id))]
    fn stop(&mut self, ctx: &mut Context<Self>) {
        self.stop_schedule(ctx);
        ctx.stop();
    }
}

impl Schedulable<Self> for Mixer {
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
            State::Starting => self.start_pipeline(ctx),
            State::Started => Ok(StateChangeResult::Success),
            State::Stopping => Ok(StateChangeResult::Skip),
            State::Stopped => {
                self.stop(ctx);
                Ok(StateChangeResult::Success)
            }
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
                config,
            } => MessageResult(self.connect(&link_id, &video_producer, &audio_producer, config)),
            ConsumerMessage::Disconnect { slot_id } => MessageResult(self.disconnect(&slot_id)),
            ConsumerMessage::AddControlPoint {
                slot_id,
                property,
                control_point,
            } => MessageResult(self.add_slot_control_point(&slot_id, &property, control_point)),
            ConsumerMessage::RemoveControlPoint {
                controller_id,
                slot_id,
                property,
            } => {
                self.remove_slot_control_point(&controller_id, &slot_id, &property);
                MessageResult(Ok(()))
            }
        }
    }
}

impl Handler<StartMessage> for Mixer {
    type Result = MessageResult<StartMessage>;

    fn handle(&mut self, msg: StartMessage, ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(self.start_schedule(ctx, msg.cue_time, msg.end_time))
    }
}

impl Handler<MixerCommandMessage> for Mixer {
    type Result = MessageResult<MixerCommandMessage>;

    fn handle(&mut self, msg: MixerCommandMessage, _ctx: &mut Context<Self>) -> Self::Result {
        match msg.command {
            MixerCommand::UpdateConfig {
                width,
                height,
                sample_rate,
            } => MessageResult(self.update_config(width, height, sample_rate)),
            MixerCommand::SetSlotVolume { slot_id, volume } => {
                MessageResult(self.set_slot_volume(&slot_id, volume))
            }
        }
    }
}

impl Handler<ErrorMessage> for Mixer {
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
            format!("error-mixer-{}", self.id),
        );

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

impl Handler<ScheduleMessage> for Mixer {
    type Result = Result<(), Error>;

    fn handle(&mut self, msg: ScheduleMessage, ctx: &mut Context<Self>) -> Self::Result {
        self.reschedule(ctx, msg.cue_time, msg.end_time)
    }
}

impl Handler<StopMessage> for Mixer {
    type Result = Result<(), Error>;

    fn handle(&mut self, _msg: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        ctx.stop();
        Ok(())
    }
}

impl Handler<GetNodeInfoMessage> for Mixer {
    type Result = Result<NodeInfo, Error>;

    fn handle(&mut self, _msg: GetNodeInfoMessage, _ctx: &mut Context<Self>) -> Self::Result {
        Ok(NodeInfo::Mixer(MixerInfo {
            width: self.config.width,
            height: self.config.height,
            sample_rate: self.config.sample_rate,
            slots: self
                .consumer_slots
                .iter()
                .map(|(id, slot)| {
                    (
                        id.clone(),
                        MixerSlotInfo {
                            volume: slot.volume,
                        },
                    )
                })
                .collect(),
            consumer_slot_ids: self.video_producer.get_consumer_ids(),
            cue_time: self.state_machine.cue_time,
            end_time: self.state_machine.end_time,
            state: self.state_machine.state,
            slot_control_points: self.slot_control_points(),
        }))
    }
}
