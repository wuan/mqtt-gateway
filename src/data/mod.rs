use crate::Number;
use paho_mqtt::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub(crate) mod debug;
pub(crate) mod klimalogger;
pub(crate) mod opendtu;
pub(crate) mod openmqttgateway;
pub(crate) mod shelly;

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

    fn checked_count(&self) -> u64;

    fn drop_all(&mut self);
}
