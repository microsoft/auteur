use crate::config::Config;
use actix::{Actor, Context, Handler, Message, MessageResult};
use rtmp_switcher_controlling::controller::ChannelInfo;
use std::sync::Arc;

/// Actor that represents a channel
#[derive(Debug)]
pub struct Channel {
    cfg: Arc<Config>,
    pub id: uuid::Uuid,
    name: String,
    destination: String,
}

impl Channel {
    pub fn new(cfg: Arc<Config>, name: &str, destination: &str) -> Self {
        Self {
            cfg,
            id: uuid::Uuid::new_v4(),
            name: name.to_string(),
            destination: destination.to_string(),
        }
    }
}

impl Actor for Channel {
    type Context = Context<Self>;
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
