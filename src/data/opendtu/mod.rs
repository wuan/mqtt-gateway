use std::sync::mpsc::SyncSender;

use anyhow::Result;
use chrono::Datelike;
use influxdb::Timestamp::Seconds;
use influxdb::WriteQuery;
use paho_mqtt::Message;

use crate::data::CheckMessage;

struct Data {
    timestamp: i64,
    device: String,
    component: String,
    string: Option<String>,
    field: String,
    value: f64,
}

pub struct OpenDTULogger {
    txs: Vec<SyncSender<WriteQuery>>,
    parser: OpenDTUParser,
}

impl OpenDTULogger {
    pub(crate) fn new(txs: Vec<SyncSender<WriteQuery>>) -> Self {
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
            let month_string = format!("{:04}-{:02}", timestamp.year(), timestamp.month());
            let mut write_query = WriteQuery::new(Seconds(data.timestamp as u128), data.field)
                .add_tag("device", data.device)
                .add_tag("component", data.component)
                .add_field("value", data.value)
                .add_tag("month", timestamp.month())
                .add_tag("year", timestamp.year())
                .add_tag("year_month", month_string);

            write_query = if let Some(string) = data.string {
                write_query.add_tag("string", string)
            } else {
                write_query
            };
            for tx in &self.txs {
                tx.send(write_query.clone()).expect("failed to send");
            }
        }
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
                            println!(
                                "OpenDTU {} inverter: {:}: {:?}",
                                section,
                                field,
                                msg.payload_str()
                            );
                            data = Some(Data {
                                timestamp,
                                device: String::from(section),
                                component: String::from("inverter"),
                                field: String::from(field),
                                value: msg.payload_str().parse().unwrap(),
                                string: None,
                            });
                        }
                    }
                    "device" => {
                        // ignore device global data
                        // println!("  device: {:}: {:?}", field, msg.payload_str())
                    }
                    "status" => {
                        if field == "last_update" {
                            self.timestamp = Some(msg.payload_str().parse::<i64>()?);
                        } else {
                            // ignore other status data
                            // println!("  status: {:}: {:?}", field, msg.payload_str());
                        }
                    }
                    _ => {
                        let payload = msg.payload_str();
                        if payload.len() > 0 {
                            if let Some(timestamp) = self.timestamp {
                                if false {
                                    println!(
                                        "OpenDTU {} string {:}: {:}: {:?}",
                                        section, element, field, payload
                                    );
                                }
                                data = Some(Data {
                                    timestamp,
                                    device: String::from(section),
                                    component: String::from("string"),
                                    string: Some(String::from(element)),
                                    field: String::from(field),
                                    value: payload.parse().unwrap(),
                                });
                            }
                        }
                    }
                }
            } else {
                // global options -> ignore for now
                // println!(" global {:}.{:}: {:?}", section, element, msg.payload_str())
            }
        }

        return Ok(data);
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
}
