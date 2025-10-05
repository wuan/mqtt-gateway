
use crate::config::Target;
use crate::data::CheckMessage;
use log::{info, warn};
use paho_mqtt::Message;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::JoinHandle;

pub struct DebugLogger {
    checked_count: AtomicU64,
}

impl DebugLogger {
    pub(crate) fn new() -> Self {
        DebugLogger {
            checked_count: AtomicU64::new(0),
        }
    }
}

impl CheckMessage for DebugLogger {
    fn check_message(&mut self, msg: &Message) {
        let topic = msg.topic();
        let payload = msg.payload_str();

        self.checked_count.fetch_add(1, Ordering::SeqCst);

        info!("'{}' with {}", topic, payload);
    }

    fn checked_count(&self) -> u64 {
        self.checked_count.load(Ordering::SeqCst)
    }
}

pub fn create_logger(targets: Vec<Target>) -> anyhow::Result<(Arc<Mutex<dyn CheckMessage>>, Vec<JoinHandle<()>>)> {
    if targets.len() > 0 {
        warn!("debug type has targets defined: {:?}", &targets);
    }

    Ok((Arc::new(Mutex::new(DebugLogger::new())), Vec::new()))
}
