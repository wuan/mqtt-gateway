use std::borrow::Cow;
use std::sync::mpsc::SyncSender;

use crate::config::Target;
use crate::data::{CheckMessage, LogEvent};
use crate::target::create_targets;
use crate::Number;
use anyhow::Result;
use chrono::Datelike;
use log::{debug, trace};
use paho_mqtt::Message;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

struct Data {
    timestamp: i64,
    device: String,
    component: String,
    string: Option<String>,
    field: String,
    value: f64,
}

pub struct OpenDTULogger {
    txs: Vec<SyncSender<LogEvent>>,
    parser: OpenDTUParser,
}

impl OpenDTULogger {
    pub(crate) fn new(txs: Vec<SyncSender<LogEvent>>) -> Self {
        OpenDTULogger {
            txs,
            parser: OpenDTUParser::new(),
        }
    }
}

impl CheckMessage for OpenDTULogger {
    fn check_message(&mut self, msg: &Message) {
        let result1 = self.parser.parse(msg).unwrap();
        if let Some(data) = result1 {
            let timestamp = chrono::DateTime::from_timestamp(data.timestamp, 0)
                .expect("failed to convert timestamp");
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
            for tx in &self.txs {
                tx.send(log_event.clone()).expect("failed to send");
            }
        }
    }

    fn checked_count(&self) -> u64 {
        0
    }

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
                        if payload.len() > 0 {
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
            value: payload.parse().unwrap(),
        })
    }

    fn map_inverter(msg: &Message, section: &str, field: &str, timestamp: i64) -> Option<Data> {
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
            value: msg.payload_str().parse().unwrap(),
            string: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use paho_mqtt::QOS_1;

    use super::*;

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
        let (logger, mut handles) = create_logger(targets)?;

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

pub fn create_logger(
    targets: Vec<Target>,
) -> Result<(Arc<Mutex<dyn CheckMessage>>, Vec<JoinHandle<()>>)> {
    let (txs, handles) = create_targets(targets)?;

    Ok((Arc::new(Mutex::new(OpenDTULogger::new(txs))), handles))
}
