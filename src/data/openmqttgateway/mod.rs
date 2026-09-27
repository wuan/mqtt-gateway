use std::collections::HashMap;
use std::sync::mpsc::SyncSender;

use crate::config::Target;
use crate::data::{send_event, CheckMessage, LogEvent, LoggerResult};
use crate::target::create_targets;
use crate::Number;
use crate::Shutdown;
use anyhow::{anyhow, Result};
use log::warn;
use paho_mqtt::Message;
use serde_json::{Map, Value};
use std::sync::{Arc, Mutex};

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
        let data = match self.parser.parse(msg) {
            Ok(data) => data,
            Err(error) => {
                warn!(
                    "OpenMQTTGateway parse error: {} on '{}' (topic: {})",
                    error,
                    msg.payload_str(),
                    msg.topic()
                );
                return;
            }
        };
        if let Some(data) = data {
            let timestamp = chrono::offset::Utc::now();

            let log_event = LogEvent::new(
                "btle".to_string(),
                timestamp.timestamp(),
                data.tags
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                data.fields.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            );
            send_event(&self.txs, &log_event);
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

fn parse_json(payload: &str) -> Result<Map<String, Value>> {
    let parsed: Value = serde_json::from_str(payload)?;
    let obj: Map<String, Value> = parsed
        .as_object()
        .ok_or_else(|| anyhow!("expected a JSON object, got: {parsed}"))?
        .clone();
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
                    Self::convert_value(&mut fields, &mut tags, key, value);
                }

                if fields.contains_key("rssi") && fields.len() == 1 && tags.len() == base_tag_count
                {
                    tags.insert(String::from("type"), String::from("NONE"));
                } else if !tags.contains_key("type") {
                    tags.insert(String::from("type"), String::from("UNKN"));
                }
                if !fields.is_empty() {
                    data = Some(Data { fields, tags });
                } else {
                    warn!("skip without fields {:?}", tags)
                }
            }
        }
        Ok(data)
    }

    fn convert_value(
        fields: &mut HashMap<String, Number>,
        tags: &mut HashMap<String, String>,
        key: String,
        value: Value,
    ) {
        match value {
            Value::Number(number) => {
                Self::convert_number(fields, key, number);
            }
            Value::String(value) => {
                tags.insert(key, value);
            }
            Value::Bool(value) => {
                fields.insert(key, Number::Int(value as i64));
            }
            _ => {
                warn!("unhandled entry {}: {:?}", key, value);
            }
        }
    }

    fn convert_number(
        fields: &mut HashMap<String, Number>,
        key: String,
        number: serde_json::Number,
    ) {
        let number_value = if let Some(value) = number.as_i64() {
            Number::Int(value)
        } else if let Some(value) = number.as_u64() {
            Number::UInt(value)
        } else if let Some(value) = number.as_f64() {
            Number::Float(value)
        } else {
            warn!("OpenMQTTGateway: unsupported number value {}", number);
            return;
        };
        fields.insert(key, number_value);
    }
}

pub fn create_logger(targets: Vec<Target>, shutdown: Shutdown) -> Result<LoggerResult> {
    let (txs, handles) = create_targets(targets, shutdown)?;

    Ok((
        Arc::new(Mutex::new(OpenMqttGatewayLogger::new(txs))),
        handles,
    ))
}

#[cfg(test)]
mod tests {
    use paho_mqtt::QOS_1;
    use std::sync::mpsc::sync_channel;

    use super::*;

    #[test]
    fn test_parse_non_object_payload_is_error() {
        let mut parser = OpenMqttGatewayParser::new();
        let message = Message::new(
            "blegateway/D12331654712/BTtoMQTT/283146C17616",
            "[1,2,3]",
            QOS_1,
        );

        assert!(parser.parse(&message).is_err());
    }

    #[test]
    fn test_parse_huge_unsigned_number_keeps_precision() {
        let mut parser = OpenMqttGatewayParser::new();
        let message = Message::new(
            "blegateway/D12331654712/BTtoMQTT/283146C17616",
            "{\"value\": 18446744073709551615}",
            QOS_1,
        );

        let data = parser.parse(&message).unwrap().unwrap();
        assert_eq!(data.fields.get("value"), Some(&Number::UInt(u64::MAX)));
    }

    #[test]
    fn test_parse_boolean_field() {
        let mut parser = OpenMqttGatewayParser::new();
        let message = Message::new(
            "blegateway/D12331654712/BTtoMQTT/283146C17616",
            "{\"power\": true, \"rssi\": -70}",
            QOS_1,
        );

        let data = parser.parse(&message).unwrap().unwrap();
        assert_eq!(data.fields.get("power"), Some(&Number::Int(1)));
    }

    #[test]
    fn test_check_message_malformed_payload_does_not_panic() {
        let (tx, rx) = sync_channel(100);
        let mut logger = OpenMqttGatewayLogger::new(vec![tx]);

        let message = Message::new(
            "blegateway/D12331654712/BTtoMQTT/283146C17616",
            "not json at all",
            QOS_1,
        );
        logger.check_message(&message);

        assert!(rx.try_recv().is_err());
    }

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
