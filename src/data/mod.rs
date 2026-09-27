use crate::Number;
use log::warn;
use paho_mqtt::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

pub(crate) mod debug;
pub(crate) mod klimalogger;
pub(crate) mod opendtu;
pub(crate) mod openmqttgateway;
pub(crate) mod shelly;

/// A per-source message handler shared with the receiver.
pub(crate) type Logger = Arc<Mutex<dyn CheckMessage>>;
/// Writer threads spawned by a source's targets.
pub(crate) type LoggerHandles = Vec<JoinHandle<()>>;
/// Returned by every source's `create_logger`.
pub(crate) type LoggerResult = (Logger, LoggerHandles);

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LogEvent {
    pub measurement: String,
    pub timestamp: i64,
    pub tags: HashMap<String, String>,
    pub fields: HashMap<String, Number>,
}

impl LogEvent {
    pub(crate) fn new_value_from_ref(
        measurement: String,
        timestamp: i64,
        tags: HashMap<&str, &str>,
        value: Number,
    ) -> Self {
        let mut fields = HashMap::new();
        fields.insert("value", value);
        Self::new_from_ref(measurement, timestamp, tags, fields)
    }

    pub(crate) fn new_from_ref(
        measurement: String,
        timestamp: i64,
        tags: HashMap<&str, &str>,
        fields: HashMap<&str, Number>,
    ) -> Self {
        let tags = tags
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let fields = fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();

        Self::new(measurement, timestamp, tags, fields)
    }

    pub(crate) fn new(
        measurement: String,
        timestamp: i64,
        tags: HashMap<String, String>,
        fields: HashMap<String, Number>,
    ) -> Self {
        Self {
            measurement,
            timestamp,
            tags,
            fields,
        }
    }
}

pub trait CheckMessage {
    fn check_message(&mut self, msg: &Message);

    #[cfg(test)]
    fn checked_count(&self) -> u64;

    #[cfg(test)]
    fn drop_all(&mut self);
}

/// Forward an event to every target without blocking the receiving thread.
///
/// The target channels are bounded; if a writer cannot keep up we drop the
/// event instead of blocking (and potentially deadlocking) the MQTT receive
/// loop. A disconnected channel must never panic the process either.
pub(crate) fn send_event(txs: &[SyncSender<LogEvent>], event: &LogEvent) {
    for tx in txs {
        match tx.try_send(event.clone()) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                report_drop("target channel is full", &event.measurement);
            }
            Err(TrySendError::Disconnected(_)) => {
                report_drop("target channel is disconnected", &event.measurement);
            }
        }
    }
}

/// Log a dropped event, throttled to avoid flooding the log.
fn report_drop(reason: &str, measurement: &str) {
    let count = crate::metrics::increment(&crate::metrics::EVENTS_DROPPED);
    if crate::metrics::should_log(count, DROP_LOG_EVERY) {
        warn!(
            "dropping event for measurement '{}': {} ({} event(s) dropped so far)",
            measurement, reason, count
        );
    }
}

/// Log every drop only up to this many, then every Nth.
const DROP_LOG_EVERY: u64 = 1000;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{sync_channel, TryRecvError};

    fn event() -> LogEvent {
        LogEvent::new_value_from_ref("measurement".to_string(), 0, HashMap::new(), Number::Int(1))
    }

    #[test]
    fn test_send_event_disconnected_does_not_panic() {
        let (tx, rx) = sync_channel(1);
        drop(rx);

        send_event(&[tx], &event());
    }

    #[test]
    fn test_send_event_full_does_not_block_or_panic() {
        let (tx, rx) = sync_channel(1);

        send_event(std::slice::from_ref(&tx), &event());
        // The channel is now full; this must be dropped rather than blocking.
        send_event(std::slice::from_ref(&tx), &event());

        assert!(rx.try_recv().is_ok());
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn test_send_event_forwards_to_all_targets() {
        let (tx1, rx1) = sync_channel(1);
        let (tx2, rx2) = sync_channel(1);

        send_event(&[tx1, tx2], &event());

        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }
}
