use std::collections::HashMap;
use std::sync::mpsc::SyncSender;

use crate::config::Target;
use crate::data::{CheckMessage, LogEvent};
use crate::target::create_targets;
use crate::Number;
use anyhow::Result;
use log::warn;
use paho_mqtt::Message;
use serde_json::{Map, Value};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

struct Data {
    fields: HashMap<String, Number>,
    tags: HashMap<String, String>,
}

pub struct OpenMqttGatewayLogger {
    txs: Vec<SyncSender<LogEvent>>,
    parser: OpenMqttGatewayParser,
}

impl OpenMqttGatewayLogger {
    pub(crate) fn new(txs: Vec<SyncSender<LogEvent>>) -> Self {
        OpenMqttGatewayLogger {
            txs,
            parser: OpenMqttGatewayParser::new(),
        }
    }
}

impl CheckMessage for OpenMqttGatewayLogger {
    fn check_message(&mut self, msg: &Message) {
        let data = self.parser.parse(msg).unwrap();
        if let Some(data) = data {
            let timestamp = chrono::offset::Utc::now();

            let log_event = LogEvent::new(
                "btle".to_string(),
                timestamp.timestamp(),
                data.tags
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                data.fields
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            );
            for tx in &self.txs {
                tx.send(log_event.clone()).expect("failed to send");
            }
        }
    }

    fn checked_count(&self) -> u64 {
        0
    }
}

fn parse_json(payload: &str) -> Result<Map<String, Value>> {
    let parsed: Value = serde_json::from_str(payload)?;
    let obj: Map<String, Value> = parsed.as_object().unwrap().clone();
    Ok(obj)
}

struct OpenMqttGatewayParser {}

impl OpenMqttGatewayParser {
    pub fn new() -> Self {
        OpenMqttGatewayParser {}
    }

    fn parse(&mut self, msg: &Message) -> Result<Option<Data>> {
        let mut data: Option<Data> = None;

        let mut split = msg.topic().split("/");
        let _ = split.next();
        let gateway_id = split.next();
        let channel = split.next();
        let device_id = split.next();

        if let (Some(gateway_id), Some(channel), Some(device_id)) = (gateway_id, channel, device_id)
        {
            if channel == "BTtoMQTT" {
                let mut result = parse_json(&msg.payload_str())?;

                let _ = result.remove("id");

                let mut fields = HashMap::new();
                let mut tags = HashMap::new();
                tags.insert(String::from("device"), String::from(device_id));
                tags.insert(String::from("gateway"), String::from(gateway_id));

                let base_tag_count = tags.len();

                for (key, value) in result {
                    match value {
                        Value::Number(number) => {
                            let number_value = if number.is_f64() {
                                Number::Float(number.as_f64().unwrap())
                            } else {
                                Number::Int(number.as_i64().unwrap())
                            };
                            fields.insert(key, number_value);
                        }
                        Value::String(value) => {
                            tags.insert(key, value);
                        }
                        _ => {
                            warn!("unhandled entry {}: {:?}", key, value);
                        }
                    }
                }

                if fields.contains_key("rssi") && fields.len() == 1 && tags.len() == base_tag_count
                {
                    tags.insert(String::from("type"), String::from("NONE"));
                } else if !tags.contains_key("type") {
                    tags.insert(String::from("type"), String::from("UNKN"));
                }
                if fields.len() > 0 {
                    data = Some(Data { fields, tags });
                } else {
                    warn!("skip without fields {:?}", tags)
                }
            }
        }
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use paho_mqtt::QOS_1;

    use super::*;

    #[test]
    fn test_parse() -> Result<()> {
        let mut parser = OpenMqttGatewayParser::new();
        let message = Message::new("blegateway/D12331654712/BTtoMQTT/283146C17616", "{\"id\":\"28:31:46:C1:76:16\",\"name\":\"DHS\",\"rssi\":-92,\"brand\":\"Oras\",\"model\":\"Hydractiva Digital\",\"model_id\":\"ADHS\",\"type\":\"ENRG\",\"session\":67,\"seconds\":115,\"litres\":9.1,\"tempc\":12,\"tempf\":53.6,\"energy\":0.03}", QOS_1);
        let result = parser.parse(&message)?;

        assert!(result.is_some());
        let data = result.unwrap();
        let fields = data.fields;
        assert_eq!(*fields.get("rssi").unwrap(), Number::Int(-92));
        assert_eq!(*fields.get("seconds").unwrap(), Number::Int(115));
        let tags = data.tags;
        assert_eq!(tags.get("device").unwrap(), "283146C17616");
        assert_eq!(tags.get("gateway").unwrap(), "D12331654712");
        assert_eq!(tags.get("name").unwrap(), "DHS");

        Ok(())
    }

    #[test]
    fn test_parse_none_type() -> Result<()> {
        let mut parser = OpenMqttGatewayParser::new();
        let message = Message::new(
            "blegateway/D12331654712/BTtoMQTT/283146C17616",
            "{\"id\":\"28:31:46:C1:76:16\",\"rssi\":-92}",
            QOS_1,
        );
        let result = parser.parse(&message)?;

        assert!(result.is_some());
        let data = result.unwrap();
        let tags = data.tags;
        assert_eq!(tags.get("device").unwrap(), "283146C17616");
        assert_eq!(tags.get("gateway").unwrap(), "D12331654712");
        assert_eq!(tags.get("type").unwrap(), "NONE");

        Ok(())
    }

    #[test]
    fn test_parse_missing_fields() -> Result<()> {
        let mut parser = OpenMqttGatewayParser::new();
        let message = Message::new(
            "blegateway/D12331654712/BTtoMQTT/283146C17616",
            "{\"id\":\"28:31:46:C1:76:16\"}",
            QOS_1,
        );
        let result = parser.parse(&message)?;

        assert!(result.is_none());

        Ok(())
    }

    #[test]
    fn test_parse_unknown_type() -> Result<()> {
        let mut parser = OpenMqttGatewayParser::new();
        let message = Message::new(
            "blegateway/D12331654712/BTtoMQTT/283146C17616",
            "{\"id\":\"28:31:46:C1:76:16\",\"rssi\":-92,\"name\":\"foo\"}",
            QOS_1,
        );
        let result = parser.parse(&message)?;

        assert!(result.is_some());
        let data = result.unwrap();
        let tags = data.tags;
        assert_eq!(tags.get("device").unwrap(), "283146C17616");
        assert_eq!(tags.get("gateway").unwrap(), "D12331654712");
        assert_eq!(tags.get("type").unwrap(), "UNKN");

        Ok(())
    }
}

pub fn create_logger(
    targets: Vec<Target>,
) -> Result<(Arc<Mutex<dyn CheckMessage>>, Vec<JoinHandle<()>>)> {
    let (txs, handles) = create_targets(targets)?;

    Ok((
        Arc::new(Mutex::new(OpenMqttGatewayLogger::new(txs))),
        handles,
    ))
}
