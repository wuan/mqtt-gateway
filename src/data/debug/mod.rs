use crate::config::Target;
use crate::data::CheckMessage;
use log::{info, warn};
use paho_mqtt::Message;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

pub struct DebugLogger {}

impl DebugLogger {
    pub(crate) fn new() -> Self {
        DebugLogger {}
    }
}

impl CheckMessage for DebugLogger {
    fn check_message(&mut self, msg: &Message) {
        let topic = msg.topic();
        let payload = msg.payload_str();

        info!("'{}' with {}", topic, payload);
    }
}

pub fn create_logger(targets: Vec<Target>) -> (Arc<Mutex<dyn CheckMessage>>, Vec<JoinHandle<()>>) {
    if targets.len() > 0 {
        warn!("debug type has targets defined: {:?}", &targets);
    }

    (Arc::new(Mutex::new(DebugLogger::new())), Vec::new())
}
