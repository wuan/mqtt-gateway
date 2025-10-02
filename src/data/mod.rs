use crate::Number;
use paho_mqtt::Message;
use serde::{Deserialize, Serialize};

pub(crate) mod debug;
pub(crate) mod klimalogger;
pub(crate) mod opendtu;
pub(crate) mod openmqttgateway;
pub(crate) mod shelly;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LogEvent {
    pub measurement: String,
    pub timestamp: i64,
    pub tags: Vec<(String, String)>,
    pub fields: Vec<(String, Number)>,
}

impl LogEvent {
    pub(crate) fn new_value_from_ref(
        measurement: String,
        timestamp: i64,
        tags: Vec<(&str, &str)>,
        value: Number,
    ) -> Self {
        Self::new_from_ref(measurement, timestamp, tags, vec![("value", value)])
    }

    pub(crate) fn new_from_ref(
        measurement: String,
        timestamp: i64,
        tags: Vec<(&str, &str)>,
        fields: Vec<(&str, Number)>,
    ) -> Self {
        let tags: Vec<(String, String)> = tags
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let fields: Vec<(String, Number)> = fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();

        Self::new(measurement, timestamp, tags, fields)
    }

    pub(crate) fn new(
        measurement: String,
        timestamp: i64,
        tags: Vec<(String, String)>,
        fields: Vec<(String, Number)>,
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
}
