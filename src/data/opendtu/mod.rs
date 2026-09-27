use std::borrow::Cow;
use std::sync::mpsc::SyncSender;

use crate::config::Target;
use crate::data::{send_event, CheckMessage, LogEvent, LoggerResult};
use crate::target::create_targets;
use crate::Number;
use crate::Shutdown;
use anyhow::Result;
use chrono::Datelike;
use log::{debug, trace, warn};
use paho_mqtt::Message;
use std::sync::{Arc, Mutex};

struct Data {
    timestamp: i64,
    device: String,
    component: String,
    string: Option<String>,
    field: String,
    value: f64,
}

pub struct OpenDTULogger {
    txs: Vec<SyncSender<Arc<LogEvent>>>,
    parser: OpenDTUParser,
}

impl OpenDTULogger {
    pub(crate) fn new(txs: Vec<SyncSender<Arc<LogEvent>>>) -> Self {
        OpenDTULogger {
            txs,
            parser: OpenDTUParser::new(),
        }
    }
}

impl CheckMessage for OpenDTULogger {
    fn check_message(&mut self, msg: &Message) {
        let result1 = match self.parser.parse(msg) {
            Ok(result) => result,
            Err(error) => {
                warn!(
                    "OpenDTU parse error: {} on '{}' (topic: {})",
                    error,
                    msg.payload_str(),
                    msg.topic()
                );
                return;
            }
        };
        if let Some(data) = result1 {
            let timestamp = match chrono::DateTime::from_timestamp(data.timestamp, 0) {
                Some(timestamp) => timestamp,
                None => {
                    warn!(
                        "OpenDTU: invalid timestamp {} on '{}'",
                        data.timestamp,
                        msg.topic()
                    );
                    return;
                }
            };
            let month_string = timestamp.month().to_string();
            let year_string = timestamp.year().to_string();
            let year_month_string = format!("{:04}-{:02}", timestamp.year(), timestamp.month());

            let mut tags: Vec<(&str, &str)> = vec![
                ("device", &data.device),
                ("component", &data.component),
                ("month", &month_string),
                ("year", &year_string),
                ("year_month", &year_month_string),
            ];

            if let Some(ref string) = data.string {
                tags.push(("string", string));
            }

            let log_event = LogEvent::new_value_from_ref(
                data.field,
                data.timestamp,
                tags.into_iter().collect(),
                Number::Float(data.value),
            );
            send_event(&self.txs, Arc::new(log_event));
        }
    }

    #[cfg(test)]
    fn checked_count(&self) -> u64 {
        0
    }

    #[cfg(test)]
    fn drop_all(&mut self) {
        self.txs.clear();
    }
}

struct OpenDTUParser {
    timestamp: Option<i64>,
}

impl OpenDTUParser {
    pub fn new() -> Self {
        OpenDTUParser { timestamp: None }
    }

    fn parse(&mut self, msg: &Message) -> Result<Option<Data>> {
        let mut data: Option<Data> = None;

        let mut split = msg.topic().split("/");
        let _ = split.next();
        let section = split.next();
        let element = split.next();
        if let (Some(section), Some(element)) = (section, element) {
            let field = split.next();
            if let Some(field) = field {
                match element {
                    "0" => {
                        if let Some(timestamp) = self.timestamp {
                            data = Self::map_inverter(msg, section, field, timestamp);
                        }
                    }
                    "device" => {
                        // ignore device global data
                        trace!("  device: {:}: {:?}", field, msg.payload_str())
                    }
                    "status" => {
                        if field == "last_update" {
                            self.timestamp = Some(msg.payload_str().parse::<i64>()?);
                        } else {
                            // ignore other status data
                            trace!("  status: {:}: {:?}", field, msg.payload_str());
                        }
                    }
                    _ => {
                        let payload = msg.payload_str();
                        if !payload.is_empty() {
                            if let Some(timestamp) = self.timestamp {
                                data =
                                    Self::map_string(section, element, field, payload, timestamp);
                            }
                        }
                    }
                }
            } else {
                // global options -> ignore for now
                trace!(" global {:}.{:}: {:?}", section, element, msg.payload_str())
            }
        }

        Ok(data)
    }

    fn map_string(
        section: &str,
        element: &str,
        field: &str,
        payload: Cow<str>,
        timestamp: i64,
    ) -> Option<Data> {
        let value = match payload.parse() {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    "OpenDTU: cannot parse {} string {}:{} value {:?}: {}",
                    section, element, field, payload, error
                );
                return None;
            }
        };
        debug!(
            "OpenDTU {} string {:}: {:}: {:?}",
            section, element, field, payload
        );
        Some(Data {
            timestamp,
            device: String::from(section),
            component: String::from("string"),
            string: Some(String::from(element)),
            field: String::from(field),
            value,
        })
    }

    fn map_inverter(msg: &Message, section: &str, field: &str, timestamp: i64) -> Option<Data> {
        let value = match msg.payload_str().parse() {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    "OpenDTU: cannot parse {} inverter {} value {:?}: {}",
                    section,
                    field,
                    msg.payload_str(),
                    error
                );
                return None;
            }
        };
        debug!(
            "OpenDTU {} inverter: {:}: {:?}",
            section,
            field,
            msg.payload_str()
        );
        Some(Data {
            timestamp,
            device: String::from(section),
            component: String::from("inverter"),
            field: String::from(field),
            value,
            string: None,
        })
    }
}

pub fn create_logger(targets: Vec<Target>, shutdown: Shutdown) -> Result<LoggerResult> {
    let (txs, handles) = create_targets(targets, shutdown)?;

    Ok((Arc::new(Mutex::new(OpenDTULogger::new(txs))), handles))
}

#[cfg(test)]
mod tests {
    use paho_mqtt::QOS_1;
    use std::sync::mpsc::sync_channel;

    use super::*;

    #[test]
    fn test_check_message_non_numeric_payload_does_not_panic() {
        let (tx, rx) = sync_channel(100);
        let mut logger = OpenDTULogger::new(vec![tx]);

        // Prime the cached timestamp.
        logger.check_message(&Message::new(
            "solar/114190641177/status/last_update",
            "1701271852",
            QOS_1,
        ));
        // A non-numeric value must be skipped, not panic.
        logger.check_message(&Message::new(
            "solar/114190641177/0/powerdc",
            "not-a-number",
            QOS_1,
        ));

        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_check_message_invalid_timestamp_does_not_panic() {
        let (tx, rx) = sync_channel(100);
        let mut logger = OpenDTULogger::new(vec![tx]);

        // A timestamp far outside chrono's range must be rejected, not panic.
        logger.check_message(&Message::new(
            "solar/114190641177/status/last_update",
            "999999999999999999",
            QOS_1,
        ));
        logger.check_message(&Message::new("solar/114190641177/0/powerdc", "0.6", QOS_1));

        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_parse_timestamp_returns_none() -> Result<()> {
        let mut parser = OpenDTUParser::new();
        let message = Message::new("solar/114190641177/status/last_update", "1701271852", QOS_1);
        let result = parser.parse(&message)?;

        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn test_parse_inverter_information() -> Result<()> {
        let mut parser = OpenDTUParser::new();
        let message = Message::new("solar/114190641177/status/last_update", "1701271852", QOS_1);
        let _ = parser.parse(&message)?;

        let message_2 = Message::new("solar/114190641177/0/powerdc", "0.6", QOS_1);
        let result = parser.parse(&message_2)?.unwrap();

        assert_eq!(result.timestamp, 1701271852);
        assert_eq!(result.field, "powerdc");
        assert_eq!(result.device, "114190641177");
        assert_eq!(result.component, "inverter");
        assert!(result.string.is_none());
        assert_eq!(result.value, 0.6);

        Ok(())
    }

    #[test]
    fn test_parse_string_information() -> Result<()> {
        let mut parser = OpenDTUParser::new();
        let message_1 = Message::new("solar/114190641177/status/last_update", "1701271852", QOS_1);
        let _ = parser.parse(&message_1)?;

        let message_2 = Message::new("solar/114190641177/1/voltage", "14.1", QOS_1);
        let result = parser.parse(&message_2)?.unwrap();

        assert_eq!(result.timestamp, 1701271852);
        assert_eq!(result.field, "voltage");
        assert_eq!(result.device, "114190641177");
        assert_eq!(result.component, "string");
        assert_eq!(result.string.unwrap(), "1");
        assert_eq!(result.value, 14.1);

        Ok(())
    }

    #[test]
    fn test_create_logger() -> Result<()> {
        let targets = vec![Target::Debug {}];
        let (logger, mut handles) = create_logger(targets, crate::Shutdown::new())?;

        assert!(logger.lock().unwrap().checked_count() == 0);
        assert_eq!(handles.len(), 1);

        logger.lock().unwrap().drop_all();
        if let Some(handle) = handles.pop() {
            handle
                .join()
                .map_err(|e| anyhow::anyhow!("Thread panicked: {:?}", e))?;
        }

        Ok(())
    }
}
