//! Data interface between nodes

use gst::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::debug;

#[derive(Debug)]
struct StreamProducerInner {
    producer: gst_utils::StreamProducer,
    links: HashMap<String, gst_utils::ConsumptionLink>,
}

#[derive(Debug, Clone)]
/// Wrapper around `gst_utils::StreamProducer`, additionally keeping
/// track of consumer ids
pub struct StreamProducer {
    inner: Arc<Mutex<StreamProducerInner>>,
}

impl StreamProducer {
    /// Add an appsrc to dispatch data to
    pub fn add_consumer(&self, consumer: &gst_app::AppSrc, consumer_id: &str) {
        let mut inner = self.inner.lock().unwrap();

        let link = inner.producer.add_consumer(consumer).unwrap();

        inner.links.insert(consumer_id.to_string(), link);
    }

    /// Remove a consumer appsrc by id
    pub fn remove_consumer(&self, consumer_id: &str) {
        if self
            .inner
            .lock()
            .unwrap()
            .links
            .remove(consumer_id)
            .is_some()
        {
            debug!(appsink = %self.appsink().name(), consumer_id = %consumer_id, "Removed consumer");
        } else {
            debug!(appsink = %self.appsink().name(), consumer_id = %consumer_id, "Consumer not found");
        }
    }

    /// Stop discarding data samples and start forwarding them to the consumers.
    ///
    /// This is useful for example for prerolling live sources.
    pub fn forward(&self) {
        self.inner.lock().unwrap().producer.forward();
    }

    /// Get the GStreamer `appsink` wrapped by this producer
    pub fn appsink(&self) -> gst_app::AppSink {
        self.inner.lock().unwrap().producer.appsink().clone()
    }

    /// Get the unique identifiers of all the consumers currently connected
    /// to this producer
    ///
    /// This is useful for disconnecting those automatically when the parent node
    /// stops
    pub fn get_consumer_ids(&self) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .links
            .keys()
            .map(|id| id.to_string())
            .collect()
    }
}

impl<'a> From<&'a gst_app::AppSink> for StreamProducer {
    fn from(appsink: &'a gst_app::AppSink) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StreamProducerInner {
                producer: gst_utils::StreamProducer::from(appsink),
                links: HashMap::new(),
            })),
        }
    }
}
