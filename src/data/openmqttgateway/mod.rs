use std::collections::HashMap;
use std::sync::mpsc::SyncSender;

use crate::config::Target;
use crate::data::CheckMessage;
use crate::target::influx;
use crate::target::influx::InfluxConfig;
use anyhow::Result;
use influxdb::Timestamp::Seconds;
use influxdb::WriteQuery;
use log::warn;
use paho_mqtt::Message;
use serde_json::{Map, Number, Value};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

struct Data {
    fields: HashMap<String, Number>,
    tags: HashMap<String, String>,
}

pub struct OpenMqttGatewayLogger {
    txs: Vec<SyncSender<WriteQuery>>,
    parser: OpenMqttGatewayParser,
}

impl OpenMqttGatewayLogger {
    pub(crate) fn new(txs: Vec<SyncSender<WriteQuery>>) -> Self {
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

            let mut write_query = WriteQuery::new(Seconds(timestamp.timestamp() as u128), "btle");
            for (key, value) in data.fields {
                write_query = write_query.add_field(key, value.as_f64());
            }
            for (key, value) in data.tags {
                write_query = write_query.add_tag(key, value);
            }
            for tx in &self.txs {
                tx.send(write_query.clone()).expect("failed to send");
            }
        }
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
                for (key, value) in result {
                    match value {
                        Value::Number(value) => {
                            fields.insert(key, value);
                        }
                        Value::String(value) => {
                            tags.insert(key, value);
                        }
                        _ => {
                            warn!("unhandled entry {}: {:?}", key, value);
                        }
                    }
                }
                data = Some(Data { fields, tags });
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
        assert_eq!(fields.get("rssi").unwrap().as_f64().unwrap(), -92f64);
        assert_eq!(fields.get("seconds").unwrap().as_f64().unwrap(), 115f64);
        let tags = data.tags;
        assert_eq!(tags.get("device").unwrap(), "283146C17616");
        assert_eq!(tags.get("gateway").unwrap(), "D12331654712");
        assert_eq!(tags.get("name").unwrap(), "DHS");

        Ok(())
    }
}

pub fn create_logger(targets: Vec<Target>) -> (Arc<Mutex<dyn CheckMessage>>, Vec<JoinHandle<()>>) {
    let mut txs: Vec<SyncSender<WriteQuery>> = Vec::new();
    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    for target in targets {
        let (tx, handle) = match target {
            Target::InfluxDB {
                url,
                database,
                user,
                password,
            } => influx::spawn_influxdb_writer(
                InfluxConfig::new(url, database, user, password),
                std::convert::identity,
            ),
            Target::Postgresql { .. } => {
                panic!("Postgresql not supported for open");
            }
        };
        txs.push(tx);
        handles.push(handle);
    }

    let logger = OpenMqttGatewayLogger::new(txs);

    (Arc::new(Mutex::new(logger)), handles)
}
