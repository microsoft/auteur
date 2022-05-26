//! A mixer processing node.
//!
//! A mixer can have multiple consumer slots, which will be routed
//! through `compositor` and `audiomixer` elements.

use actix::prelude::*;
use anyhow::{anyhow, Error};
use gst::prelude::*;
use gst_base::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use tracing::{debug, error, instrument, trace};

use auteur_controlling::controller::{ControlPoint, MixerInfo, MixerSlotInfo, NodeInfo, State};

use crate::node::{
    AddControlPointMessage, ConsumerMessage, GetNodeInfoMessage, GetProducerMessage, NodeManager,
    NodeStatusMessage, RemoveControlPointMessage, ScheduleMessage, StartMessage, StopMessage,
    StoppedMessage,
};
use crate::utils::{
    get_now, make_element, ErrorMessage, PipelineManager, PropertyController, Schedulable, Setting,
    SettingController, SettingSpec, StateChangeResult, StateMachine, StopManagerMessage,
    StreamProducer,
};

/// Represents one audio *or* video input connection
struct InputSlot {
    /// The producer we are connected to
    producer: StreamProducer,
    /// Input to the mixer slot
    appsrc: gst_app::AppSrc,
    /// Processing elements before the mixer
    bin: Option<gst::Bin>,
    /// Sink pad of our mixer
    pad: gst::Pad,
}

/// Represents a connection to a producer
struct ConsumerSlot {
    /// Video input slot
    video_slot: Option<InputSlot>,
    /// Audio input slot
    audio_slot: Option<InputSlot>,
    /// Volume of the `audiomixer` pad
    volume: f64,
}

/// Used from our `compositor::samples_selected` callback
#[derive(Debug)]
pub struct VideoMixingState {
    /// For how long no pad other than our base plate has selected samples
    base_plate_timeout: Option<gst::ClockTime>,
    /// Whether our base plate is opaque
    showing_base_plate: bool,
    /// Our slot controllers
    slot_controllers: Option<HashMap<String, PropertyController>>,
    /// Our controllers (width, height, ...)
    mixer_controllers: Option<HashMap<String, SettingController>>,
    /// The last observed PTS, for interpolating
    last_pts: Option<gst::ClockTime>,
    /// For resizing our output video stream
    capsfilter: Option<gst::Element>,
}

/// Used from our `audiomixer::samples_selected` callback
#[derive(Debug)]
pub struct AudioMixingState {
    /// Our slot controllers
    slot_controllers: Option<HashMap<String, PropertyController>>,
    /// The last observed PTS, for interpolating
    last_pts: Option<gst::ClockTime>,
}

/// One audio or video output branch
struct Output {
    /// The producer exposed for this output branch
    producer: StreamProducer,
    /// The mixer for this output branch
    mixer: gst::Element,
    /// ID for internal logging
    id: String,
}

/// Video output branch
struct VideoOutput {
    /// Output elements
    output: Output,
    /// Used for showing and hiding the base plate, and updating slot video controllers
    mixing_state: Arc<Mutex<VideoMixingState>>,
}

/// Audio output branch
struct AudioOutput {
    /// Output elements
    output: Output,
    /// Used for updating slot audio controllers
    mixing_state: Arc<Mutex<AudioMixingState>>,
}

impl VideoOutput {
    /// Create a new video output
    #[instrument(level = "debug", name = "creating")]
    fn new(id: &str) -> Self {
        let id = format!("video-{}", id);

        let appsink = gst::ElementFactory::make("appsink", Some(&format!("mixer-appsink-{}", id)))
            .unwrap()
            .downcast::<gst_app::AppSink>()
            .unwrap();

        let mixer = make_element("compositor", Some("compositor")).unwrap();

        // Reserve our base pad
        let _ = mixer.request_pad_simple("sink_0").unwrap();

        Self {
            output: Output {
                producer: StreamProducer::from(&appsink),
                mixer,
                id,
            },
            mixing_state: Arc::new(Mutex::new(VideoMixingState {
                base_plate_timeout: gst::ClockTime::NONE,
                showing_base_plate: false,
                slot_controllers: Some(HashMap::new()),
                mixer_controllers: Some(HashMap::new()),
                last_pts: gst::ClockTime::NONE,
                capsfilter: None,
            })),
        }
    }

    /// Build the base plate. It may be either a live videotestsrc, or an
    /// imagefreeze'd image when a fallback image was specified
    #[instrument(level = "debug", name = "building base plate", skip(self), fields(id = %self.output.id))]
    fn build_base_plate(
        &mut self,
        width: i32,
        height: i32,
        fallback_image: &str,
    ) -> Result<gst::Element, Error> {
        let bin = gst::Bin::new(None);
        let ghost = match fallback_image {
            "" => {
                let vsrc = make_element("videotestsrc", None)?;
                vsrc.set_property("is-live", &true);
                vsrc.set_property_from_str("pattern", "black");

                bin.add(&vsrc)?;

                gst::GhostPad::with_target(Some("src"), &vsrc.static_pad("src").unwrap()).unwrap()
            }
            _ => {
                let filesrc = make_element("filesrc", None)?;
                let decodebin = make_element("decodebin3", None)?;
                let vconv = make_element("videoconvert", None)?;
                let imagefreeze = make_element("imagefreeze", None)?;

                filesrc.set_property("location", fallback_image);
                imagefreeze.set_property("is-live", &true);

                bin.add_many(&[&filesrc, &decodebin, &imagefreeze, &vconv])?;

                let imagefreeze_clone = imagefreeze.downgrade();
                decodebin.connect_pad_added(move |_bin, pad| {
                    if let Some(imagefreeze) = imagefreeze_clone.upgrade() {
                        let sinkpad = imagefreeze.static_pad("sink").unwrap();
                        pad.link(&sinkpad).unwrap();
                    }
                });

                filesrc.link(&decodebin)?;

                gst::GhostPad::with_target(Some("src"), &imagefreeze.static_pad("src").unwrap())
                    .unwrap()
            }
        };

        bin.add_pad(&ghost).unwrap();

        Ok(bin.upcast())
    }

    #[instrument(
        name = "synchronizing mixer controllers",
        level = "trace",
        skip(controllers)
    )]
    fn synchronize_mixer_controllers(
        agg: &gst_base::Aggregator,
        base_plate_pad: &gst::Pad,
        id: &str,
        duration: Option<gst::ClockTime>,
        controllers: &mut HashMap<String, SettingController>,
        capsfilter: &Option<gst::Element>,
    ) -> HashMap<String, SettingController> {
        let now = get_now();
        let mut updated_controllers = HashMap::new();
        let mut caps = capsfilter
            .as_ref()
            .map(|capsfilter| capsfilter.property::<gst::Caps>("caps"));

        for (id, mut controller) in controllers.drain() {
            let setting = controller.setting.clone();

            if !controller.synchronize(now, duration) {
                updated_controllers.insert(id.clone(), controller);
            }
            if let Some(ref mut caps) = caps {
                if id == "width" {
                    let width = setting.lock().unwrap().as_i32().unwrap();
                    caps.make_mut().set_simple(&[("width", &width)]);
                    base_plate_pad.set_property("width", &width);
                } else if id == "height" {
                    let height = setting.lock().unwrap().as_i32().unwrap();
                    caps.make_mut().set_simple(&[("height", &height)]);
                    base_plate_pad.set_property("height", &height);
                }
            }
        }

        if let Some(capsfilter) = capsfilter {
            capsfilter.set_property("caps", &caps.unwrap());
        }

        updated_controllers
    }

    /// Show or hide our base plate, update slot controllers
    #[instrument(
        name = "Updating video mixing state",
        level = "trace",
        skip(mixing_state)
    )]
    fn update_mixing_state(
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
                mixing_state.base_plate_timeout = Some(pts);
            } else if !mixing_state.showing_base_plate
                && pts - mixing_state.base_plate_timeout.unwrap() > timeout
            {
                debug!("falling back to base plate {:?}", base_plate_pad);
                base_plate_pad.set_property("alpha", &1.0f64);
                mixing_state.showing_base_plate = true;
            }
        } else {
            if mixing_state.showing_base_plate {
                debug!("hiding base plate: {:?}", base_plate_pad);
                base_plate_pad.set_property("alpha", &0.0f64);
                mixing_state.showing_base_plate = false;
            }
            mixing_state.base_plate_timeout = gst::ClockTime::NONE;
        }

        let duration = if mixing_state.last_pts.is_none() {
            gst::ClockTime::NONE
        } else {
            Some(pts - mixing_state.last_pts.unwrap())
        };

        mixing_state.slot_controllers = Some(Mixer::synchronize_slot_controllers(
            agg,
            id,
            duration,
            &mut mixing_state.slot_controllers.take().unwrap(),
        ));

        mixing_state.mixer_controllers = Some(Self::synchronize_mixer_controllers(
            agg,
            &base_plate_pad,
            id,
            duration,
            &mut mixing_state.mixer_controllers.take().unwrap(),
            &mixing_state.capsfilter,
        ));

        mixing_state.last_pts = Some(pts);
    }

    /// Fill the pipeline for this output branch
    #[instrument(
        name = "Starting video mixing output branch",
        level = "debug",
        skip(self),
        fields(id = %self.output.id)
    )]
    fn start(
        &mut self,
        pipeline: &gst::Pipeline,
        width: i32,
        height: i32,
        fallback_image: &str,
        timeout: i32,
    ) -> Result<(), Error> {
        let vsrc = self.build_base_plate(width, height, fallback_image)?;
        let vqueue = make_element("queue", None)?;
        let vcapsfilter = make_element("capsfilter", None)?;

        self.output
            .mixer
            .set_property_from_str("background", "black");
        self.output.mixer.set_property(
            "start-time-selection",
            &gst_base::AggregatorStartTimeSelection::First,
        );
        self.output
            .mixer
            .set_property("ignore-inactive-pads", &true);

        vcapsfilter.set_property(
            "caps",
            &gst::Caps::builder("video/x-raw")
                .field("width", &width)
                .field("height", &height)
                .field("framerate", &gst::Fraction::new(30, 1))
                .field("pixel-aspect-ratio", &gst::Fraction::new(1, 1))
                .field("format", &"AYUV")
                .field("colorimetry", &"bt601")
                .field("chroma-site", &"jpeg")
                .field("interlace-mode", &"progressive")
                .build(),
        );

        pipeline.add_many(&[&vsrc, &vqueue, &self.output.mixer, &vcapsfilter])?;

        gst::Element::link_many(&[&vsrc, &vqueue])?;

        vqueue
            .static_pad("src")
            .unwrap()
            .link(&self.output.mixer.static_pad("sink_0").unwrap())?;

        gst::Element::link_many(&[
            &self.output.mixer,
            &vcapsfilter,
            self.output.producer.appsink().upcast_ref(),
        ])?;

        let base_plate_pad = self.output.mixer.static_pad("sink_0").unwrap();

        base_plate_pad.set_property("alpha", &0.0f64);
        base_plate_pad.set_property("width", &width);
        base_plate_pad.set_property("height", &height);
        base_plate_pad.set_property_from_str("sizing-policy", "keep-aspect-ratio");

        let mixing_state = self.mixing_state.clone();
        mixing_state.lock().unwrap().capsfilter = Some(vcapsfilter);
        let id = self.output.id.clone();
        let timeout = timeout as u64 * gst::ClockTime::MSECOND;

        self.output.mixer.set_property("emit-signals", &true);
        self.output
            .mixer
            .downcast_ref::<gst_base::Aggregator>()
            .unwrap()
            .connect_samples_selected(
                move |agg: &gst_base::Aggregator, _segment, pts, _dts, _duration, _info| {
                    let mut mixing_state = mixing_state.lock().unwrap();
                    Self::update_mixing_state(agg, &id, pts.unwrap(), &mut *mixing_state, timeout);
                },
            );

        Ok(())
    }
}

impl AudioOutput {
    /// Create a new audio output
    #[instrument(level = "debug", name = "creating")]
    fn new(id: &str) -> Self {
        let id = format!("audio-{}", id);
        let appsink = gst::ElementFactory::make("appsink", Some(&format!("mixer-appsink-{}", id)))
            .unwrap()
            .downcast::<gst_app::AppSink>()
            .unwrap();

        let mixer = make_element("audiomixer", Some("audiomixer")).unwrap();

        // Reserve our base pad
        let _ = mixer.request_pad_simple("sink_0").unwrap();

        Self {
            output: Output {
                producer: StreamProducer::from(&appsink),
                mixer,
                id,
            },
            mixing_state: Arc::new(Mutex::new(AudioMixingState {
                slot_controllers: Some(HashMap::new()),
                last_pts: gst::ClockTime::NONE,
            })),
        }
    }

    /// Update slot controllers
    #[instrument(name = "Updating mixing state", level = "trace", skip(mixing_state))]
    fn update_mixing_state(
        agg: &gst_base::Aggregator,
        id: &str,
        pts: gst::ClockTime,
        mixing_state: &mut AudioMixingState,
    ) {
        let duration = if mixing_state.last_pts.is_none() {
            gst::ClockTime::NONE
        } else {
            Some(pts - mixing_state.last_pts.unwrap())
        };

        mixing_state.slot_controllers = Some(Mixer::synchronize_slot_controllers(
            agg,
            id,
            duration,
            &mut mixing_state.slot_controllers.take().unwrap(),
        ));

        mixing_state.last_pts = Some(pts);
    }

    /// Fill the pipeline for this output branch
    #[instrument(
        name = "Starting audio mixing output branch",
        level = "debug",
        skip(self),
        fields(id = %self.output.id)
    )]
    fn start(&mut self, pipeline: &gst::Pipeline, sample_rate: i32) -> Result<(), Error> {
        let asrc = make_element("audiotestsrc", None)?;
        let asrccapsfilter = make_element("capsfilter", None)?;
        let aqueue = make_element("queue", None)?;
        let acapsfilter = make_element("capsfilter", None)?;
        let level = make_element("level", None)?;
        let aresample = make_element("audioresample", None)?;
        let aresamplecapsfilter = make_element("capsfilter", None)?;

        asrc.set_property("is-live", true);
        asrc.set_property("volume", 0f64);
        self.output.mixer.set_property(
            "start-time-selection",
            &gst_base::AggregatorStartTimeSelection::First,
        );
        self.output
            .mixer
            .set_property("ignore-inactive-pads", &true);

        asrccapsfilter.set_property(
            "caps",
            &gst::Caps::builder("audio/x-raw")
                .field("channels", 2i32)
                .field("format", &"S16LE")
                .field("rate", &sample_rate)
                .build(),
        );

        acapsfilter.set_property(
            "caps",
            &gst::Caps::builder("audio/x-raw")
                .field("channels", 2i32)
                .field("format", &"S16LE")
                .field("rate", &sample_rate)
                .build(),
        );

        aresamplecapsfilter.set_property(
            "caps",
            &gst::Caps::builder("audio/x-raw")
                .field("rate", &sample_rate)
                .build(),
        );

        pipeline.add_many(&[
            &asrc,
            &asrccapsfilter,
            &aqueue,
            &self.output.mixer,
            &acapsfilter,
            &level,
            &aresample,
            &aresamplecapsfilter,
        ])?;

        gst::Element::link_many(&[&asrc, &asrccapsfilter, &aqueue])?;

        aqueue
            .static_pad("src")
            .unwrap()
            .link(&self.output.mixer.static_pad("sink_0").unwrap())?;

        gst::Element::link_many(&[
            &self.output.mixer,
            &acapsfilter,
            &level,
            &aresample,
            &aresamplecapsfilter,
            self.output.producer.appsink().upcast_ref(),
        ])?;

        let mixing_state = self.mixing_state.clone();
        let id = self.output.id.clone();

        self.output.mixer.set_property("emit-signals", &true);
        self.output
            .mixer
            .downcast_ref::<gst_base::Aggregator>()
            .unwrap()
            .connect_samples_selected(
                move |agg: &gst_base::Aggregator, _segment, pts, _dts, _duration, _info| {
                    let mut mixing_state = mixing_state.lock().unwrap();
                    Self::update_mixing_state(agg, &id, pts.unwrap(), &mut *mixing_state);
                },
            );

        Ok(())
    }
}

/// The Mixer actor
pub struct Mixer {
    /// Unique identifier
    id: String,
    /// The wrapped pipeline
    pipeline: gst::Pipeline,
    /// A helper for managing the pipeline
    pipeline_manager: Option<Addr<PipelineManager>>,
    video_output: Option<VideoOutput>,
    audio_output: Option<AudioOutput>,
    /// Input connection points
    consumer_slots: HashMap<String, ConsumerSlot>,
    /// Our state machine
    state_machine: StateMachine,
    /// Our output settings
    settings: HashMap<String, Arc<Mutex<Setting>>>,
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
            if let Some(slot) = slot.video_slot {
                slot.producer.remove_consumer(&id);
            }

            if let Some(slot) = slot.audio_slot {
                slot.producer.remove_consumer(&id);
            }
        }

        NodeManager::from_registry().do_send(StoppedMessage {
            id: self.id.clone(),
            video_producer: self
                .video_output
                .as_ref()
                .map(|o| o.output.producer.clone()),
            audio_producer: self
                .audio_output
                .as_ref()
                .map(|o| o.output.producer.clone()),
        });
    }
}

impl Mixer {
    /// TODO: potentially use this for inspectability?
    fn create_settings() -> HashMap<String, Arc<Mutex<Setting>>> {
        let mut settings = HashMap::new();

        settings.insert(
            "width".to_string(),
            Arc::new(Mutex::new(Setting {
                name: "width".to_string(),
                spec: SettingSpec::I32 {
                    min: 1,
                    max: 2147483647,
                    current: 1920,
                },
                controllable: true,
            })),
        );

        settings.insert(
            "height".to_string(),
            Arc::new(Mutex::new(Setting {
                name: "height".to_string(),
                spec: SettingSpec::I32 {
                    min: 1,
                    max: 2147483647,
                    current: 1920,
                },
                controllable: true,
            })),
        );

        settings.insert(
            "sample-rate".to_string(),
            Arc::new(Mutex::new(Setting {
                name: "height".to_string(),
                spec: SettingSpec::I32 {
                    min: 1,
                    max: 2147483647,
                    current: 48000,
                },
                controllable: false,
            })),
        );

        settings.insert(
            "fallback-image".to_string(),
            Arc::new(Mutex::new(Setting {
                name: "fallback-image".to_string(),
                spec: SettingSpec::Str { current: "".into() },
                controllable: false,
            })),
        );

        settings.insert(
            "fallback-timeout".to_string(),
            Arc::new(Mutex::new(Setting {
                name: "fallback-timeout".to_string(),
                spec: SettingSpec::I32 {
                    min: 0,
                    max: 2147483647,
                    current: 500,
                },
                controllable: true,
            })),
        );

        settings
    }

    fn setting(&self, name: &str) -> Option<MutexGuard<Setting>> {
        self.settings
            .get(name)
            .map(|setting| setting.lock().unwrap())
    }

    fn settings(&self) -> HashMap<String, serde_json::Value> {
        self.settings
            .iter()
            .map(|(id, setting)| (id.clone(), setting.lock().unwrap().as_value()))
            .collect()
    }

    fn control_points(&self) -> HashMap<String, Vec<ControlPoint>> {
        if let Some(ref output) = self.video_output {
            let mixing_state = output.mixing_state.lock().unwrap();

            mixing_state
                .mixer_controllers
                .as_ref()
                .unwrap()
                .iter()
                .map(|(id, controller)| (id.clone(), controller.control_points()))
                .collect()
        } else {
            HashMap::new()
        }
    }

    /// Create a mixer
    pub fn new(
        id: &str,
        config: Option<HashMap<String, serde_json::Value>>,
        audio: bool,
        video: bool,
    ) -> Result<Self, Error> {
        let pipeline = gst::Pipeline::new(None);

        let audio_output = if audio {
            let output = AudioOutput::new(id);

            pipeline
                .add(
                    output
                        .output
                        .producer
                        .appsink()
                        .upcast_ref::<gst::Element>(),
                )
                .unwrap();

            Some(output)
        } else {
            None
        };

        let video_output = if video {
            let output = VideoOutput::new(id);

            pipeline
                .add(
                    output
                        .output
                        .producer
                        .appsink()
                        .upcast_ref::<gst::Element>(),
                )
                .unwrap();

            Some(output)
        } else {
            None
        };

        let mut mixer_settings = Mixer::create_settings();

        if let Some(config) = config {
            for (key, value) in config {
                if let Some(setting) = mixer_settings.get_mut(&key) {
                    let mut setting = setting.lock().unwrap();
                    SettingController::validate_value(&setting, &value)?;
                    SettingController::set_from_value(&mut setting, &value);
                } else {
                    return Err(anyhow!("No setting with name {} on mixers", key));
                }
            }
        }

        Ok(Self {
            id: id.to_string(),
            pipeline,
            pipeline_manager: None,
            audio_output,
            video_output,
            consumer_slots: HashMap::new(),
            state_machine: StateMachine::default(),
            settings: mixer_settings,
        })
    }

    fn parse_slot_config_key(property: &str) -> Result<(bool, &str), Error> {
        let split: Vec<&str> = property.splitn(2, "::").collect();

        match split.len() {
            2 => match split[0] {
                "video" => Ok((true, split[1])),
                "audio" => Ok((false, split[1])),
                _ => Err(anyhow!(
                    "Slot controller property media type must be one of [audio, video]"
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
        width: i32,
        height: i32,
        sample_rate: i32,
    ) -> Result<(), Error> {
        let volume = slot.volume;

        if let Some(ref mut slot) = slot.video_slot {
            let bin = gst::Bin::new(None);
            let queue = make_element("queue", None)?;
            let appsrc_elem: &gst::Element = slot.appsrc.upcast_ref();

            bin.add_many(&[appsrc_elem, &queue])?;
            pipeline.add(&bin)?;
            bin.sync_state_with_parent()?;

            let ghost =
                gst::GhostPad::with_target(Some("src"), &queue.static_pad("src").unwrap()).unwrap();
            bin.add_pad(&ghost).unwrap();

            gst::Element::link_many(&[appsrc_elem, &queue])?;
            let srcpad = bin.static_pad("src").unwrap();
            srcpad.link(&slot.pad).unwrap();

            slot.bin = Some(bin);

            slot.producer.add_consumer(&slot.appsrc, id);
        }

        if let Some(ref mut slot) = slot.audio_slot {
            let bin = gst::Bin::new(None);

            let conv = make_element("audioconvert", None)?;
            let resample = make_element("audioresample", None)?;
            let capsfilter = make_element("capsfilter", None)?;
            let queue = make_element("queue", None)?;
            let appsrc_elem: &gst::Element = slot.appsrc.upcast_ref();

            capsfilter.set_property(
                "caps",
                &gst::Caps::builder("audio/x-raw")
                    .field("channels", 2i32)
                    .field("format", &"S16LE")
                    .field("rate", &sample_rate)
                    .build(),
            );

            bin.add_many(&[appsrc_elem, &conv, &resample, &capsfilter, &queue])?;
            pipeline.add(&bin)?;
            bin.sync_state_with_parent()?;

            let ghost =
                gst::GhostPad::with_target(Some("src"), &queue.static_pad("src").unwrap()).unwrap();
            bin.add_pad(&ghost).unwrap();

            slot.pad.set_property("volume", &volume);

            gst::Element::link_many(&[appsrc_elem, &conv, &resample, &capsfilter, &queue])?;
            let srcpad = bin.static_pad("src").unwrap();
            srcpad.link(&slot.pad).unwrap();

            slot.bin = Some(bin);

            slot.producer.add_consumer(&slot.appsrc, id);
        }

        Ok(())
    }

    #[instrument(
        name = "synchronizing slot controllers",
        level = "trace",
        skip(controllers)
    )]
    fn synchronize_slot_controllers(
        agg: &gst_base::Aggregator,
        id: &str,
        duration: Option<gst::ClockTime>,
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

    /// Start our pipeline when cue_time is reached
    #[instrument(level = "debug", name = "mixing", skip(self, ctx), fields(id = %self.id))]
    fn start_pipeline(&mut self, ctx: &mut Context<Self>) -> Result<StateChangeResult, Error> {
        let width = self.setting("width").unwrap().as_i32().unwrap();
        let height = self.setting("height").unwrap().as_i32().unwrap();
        let sample_rate = self.setting("sample-rate").unwrap().as_i32().unwrap();
        let fallback_image = self
            .setting("fallback-image")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let timeout = self.setting("fallback-timeout").unwrap().as_i32().unwrap();

        if let Some(ref mut output) = self.video_output {
            output.start(&self.pipeline, width, height, &fallback_image, timeout)?;
        }

        if let Some(ref mut output) = self.audio_output {
            output.start(&self.pipeline, sample_rate)?;
        }

        for (id, slot) in self.consumer_slots.iter_mut() {
            Mixer::connect_slot(
                &self.pipeline,
                slot,
                &self.id,
                id,
                width,
                height,
                sample_rate,
            )?;
        }

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

        if let Some(ref output) = self.video_output {
            output.output.producer.forward();
        }

        if let Some(ref output) = self.audio_output {
            output.output.producer.forward();
        }

        Ok(StateChangeResult::Success)
    }

    /// Implement Connect command
    #[instrument(level = "debug", name = "connecting", skip(self, video_producer, audio_producer), fields(id = %self.id))]
    fn connect(
        &mut self,
        link_id: &str,
        video_producer: Option<StreamProducer>,
        audio_producer: Option<StreamProducer>,
        config: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<(), Error> {
        if self.consumer_slots.contains_key(link_id) {
            return Err(anyhow!("mixer {} already has link {}", self.id, link_id));
        }

        let video_slot = if let Some(producer) = video_producer {
            self.video_output.as_ref().map(|output| InputSlot {
                producer,
                appsrc: gst::ElementFactory::make(
                    "appsrc",
                    Some(&format!("mixer-slot-video-appsrc-{}", link_id)),
                )
                .unwrap()
                .downcast::<gst_app::AppSrc>()
                .unwrap(),
                bin: None,
                pad: output.output.mixer.request_pad_simple("sink_%u").unwrap(),
            })
        } else {
            None
        };

        let audio_slot = if let Some(producer) = audio_producer {
            self.audio_output.as_ref().map(|output| InputSlot {
                producer,
                appsrc: gst::ElementFactory::make(
                    "appsrc",
                    Some(&format!("mixer-slot-audio-appsrc-{}", link_id)),
                )
                .unwrap()
                .downcast::<gst_app::AppSrc>()
                .unwrap(),
                bin: None,
                pad: output.output.mixer.request_pad_simple("sink_%u").unwrap(),
            })
        } else {
            None
        };

        if audio_slot.is_none() && video_slot.is_none() {
            return Err(anyhow!(
                "mixer {} link {} must result in at least one audio / video connection",
                self.id,
                link_id
            ));
        }

        if let Some(config) = config {
            for (key, value) in config {
                let (is_video, property) = Mixer::parse_slot_config_key(&key)?;

                let slot = if is_video { &video_slot } else { &audio_slot };

                if let Some(slot) = slot {
                    PropertyController::validate_value(property, slot.pad.upcast_ref(), &value)?;

                    debug!("Setting initial slot config {} {}", property, value);

                    PropertyController::set_property_from_value(
                        slot.pad.upcast_ref(),
                        property,
                        &value,
                    );
                }
            }
        }

        for slot in [&video_slot, &audio_slot].iter().copied().flatten() {
            gst_utils::StreamProducer::configure_consumer(&slot.appsrc);
        }

        let mut slot = ConsumerSlot {
            video_slot,
            audio_slot,
            volume: 1.0,
        };

        if self.state_machine.state == State::Started {
            let width = self.setting("width").unwrap().as_i32().unwrap();
            let height = self.setting("height").unwrap().as_i32().unwrap();
            let sample_rate = self.setting("sample-rate").unwrap().as_i32().unwrap();

            if let Err(err) = Mixer::connect_slot(
                &self.pipeline,
                &mut slot,
                &self.id,
                link_id,
                width,
                height,
                sample_rate,
            ) {
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
            for slot in [&slot.video_slot, &slot.audio_slot]
                .iter()
                .copied()
                .flatten()
            {
                slot.producer.remove_consumer(slot_id);

                if let Some(ref bin) = slot.bin {
                    let mixer_pad = bin.static_pad("src").unwrap().peer().unwrap();
                    let mixer = mixer_pad
                        .parent()
                        .unwrap()
                        .downcast::<gst::Element>()
                        .unwrap();

                    bin.set_locked_state(true);
                    bin.set_state(gst::State::Null).unwrap();
                    self.pipeline.remove(bin).unwrap();

                    mixer.release_request_pad(&mixer_pad);
                }
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

            if let Some(slot) = if is_video {
                &slot.video_slot
            } else {
                &slot.audio_slot
            } {
                debug!(slot_id = %slot_id, pad_name = %slot.pad.name(), property = %property, "Upserting controller");

                PropertyController::validate_control_point(
                    property,
                    slot.pad.upcast_ref(),
                    &point,
                )?;

                let id = slot_id.to_owned() + property;

                if is_video {
                    if let Some(ref output) = self.video_output {
                        let mut mixing_state = output.mixing_state.lock().unwrap();

                        mixing_state
                            .slot_controllers
                            .as_mut()
                            .unwrap()
                            .entry(id)
                            .or_insert_with(|| {
                                PropertyController::new(
                                    slot_id,
                                    slot.pad.clone().upcast(),
                                    property,
                                )
                            })
                            .push_control_point(point);
                    }
                } else if let Some(ref output) = self.audio_output {
                    let mut mixing_state = output.mixing_state.lock().unwrap();

                    mixing_state
                        .slot_controllers
                        .as_mut()
                        .unwrap()
                        .entry(id)
                        .or_insert_with(|| {
                            PropertyController::new(slot_id, slot.pad.clone().upcast(), property)
                        })
                        .push_control_point(point);
                }
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
            if let Some(ref output) = self.video_output {
                let mut mixing_state = output.mixing_state.lock().unwrap();

                if let Some(controller) =
                    mixing_state.slot_controllers.as_mut().unwrap().get_mut(&id)
                {
                    controller.remove_control_point(controller_id);
                }
            }
        } else if let Some(ref output) = self.audio_output {
            let mut mixing_state = output.mixing_state.lock().unwrap();

            if let Some(controller) = mixing_state.slot_controllers.as_mut().unwrap().get_mut(&id) {
                controller.remove_control_point(controller_id);
            }
        }
    }

    fn slot_control_points(&self) -> HashMap<String, HashMap<String, Vec<ControlPoint>>> {
        let mut ret = HashMap::new();

        if let Some(ref output) = self.video_output {
            let mixing_state = output.mixing_state.lock().unwrap();

            for controller in mixing_state.slot_controllers.as_ref().unwrap().values() {
                ret.entry(controller.controllee_id.clone())
                    .or_insert_with(HashMap::new)
                    .insert(
                        "video::".to_owned() + &controller.propname,
                        controller.control_points(),
                    );
            }
        }

        if let Some(ref output) = self.audio_output {
            let mixing_state = output.mixing_state.lock().unwrap();

            for controller in mixing_state.slot_controllers.as_ref().unwrap().values() {
                ret.entry(controller.controllee_id.clone())
                    .or_insert_with(HashMap::new)
                    .insert(
                        "audio::".to_owned() + &controller.propname,
                        controller.control_points(),
                    );
            }
        }

        ret
    }

    fn slot_settings(&self) -> HashMap<String, HashMap<String, serde_json::Value>> {
        let mut ret = HashMap::new();

        for (id, slot) in &self.consumer_slots {
            let mut properties: HashMap<String, serde_json::Value> = HashMap::new();

            if let Some(ref slot) = slot.video_slot {
                properties.extend(PropertyController::properties(
                    slot.pad.upcast_ref(),
                    "video::",
                ));
            }

            if let Some(ref slot) = slot.audio_slot {
                properties.extend(PropertyController::properties(
                    slot.pad.upcast_ref(),
                    "audio::",
                ));
            }

            ret.insert(id.clone(), properties);
        }

        ret
    }

    /// Implement AddControlPoint command for the mixer
    #[instrument(level = "debug", name = "controlling", skip(self), fields(id = %self.id))]
    fn add_control_point(&mut self, property: String, point: ControlPoint) -> Result<(), Error> {
        if let Some(setting) = self.settings.get(&property) {
            if let Some(ref output) = self.video_output {
                SettingController::validate_control_point(&setting.lock().unwrap(), &point)?;
                let mut mixing_state = output.mixing_state.lock().unwrap();

                mixing_state
                    .mixer_controllers
                    .as_mut()
                    .unwrap()
                    .entry(property)
                    .or_insert_with(|| SettingController::new(&self.id, setting.clone()))
                    .push_control_point(point);
            }

            Ok(())
        } else {
            Err(anyhow!(
                "mixer {} has no setting with name {}",
                self.id,
                property
            ))
        }
    }

    /// Implement RemoveControlPoint command for the mixer
    #[instrument(level = "debug", name = "removing control point", skip(self), fields(id = %self.id))]
    fn remove_control_point(&mut self, controller_id: &str, property: &str) {
        if let Some(ref output) = self.video_output {
            let mut mixing_state = output.mixing_state.lock().unwrap();

            if let Some(controller) = mixing_state
                .slot_controllers
                .as_mut()
                .unwrap()
                .get_mut(property)
            {
                controller.remove_control_point(controller_id);
            }
        }
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
            } => MessageResult(self.connect(&link_id, video_producer, audio_producer, config)),
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
            self.video_output
                .as_ref()
                .map(|o| o.output.producer.clone()),
            self.audio_output
                .as_ref()
                .map(|o| o.output.producer.clone()),
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
            video_consumer_slot_ids: self
                .video_output
                .as_ref()
                .map(|o| o.output.producer.get_consumer_ids()),
            audio_consumer_slot_ids: self
                .audio_output
                .as_ref()
                .map(|o| o.output.producer.get_consumer_ids()),
            cue_time: self.state_machine.cue_time,
            end_time: self.state_machine.end_time,
            state: self.state_machine.state,
            settings: self.settings(),
            control_points: self.control_points(),
            slot_settings: self.slot_settings(),
            slot_control_points: self.slot_control_points(),
        }))
    }
}

impl Handler<AddControlPointMessage> for Mixer {
    type Result = Result<(), Error>;

    fn handle(&mut self, msg: AddControlPointMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.add_control_point(msg.property, msg.control_point)
    }
}

impl Handler<RemoveControlPointMessage> for Mixer {
    type Result = ();

    fn handle(&mut self, msg: RemoveControlPointMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.remove_control_point(&msg.controller_id, &msg.property)
    }
}
