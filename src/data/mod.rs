use paho_mqtt::Message;
use serde::{Deserialize, Serialize};

pub(crate) mod klimalogger;
pub(crate) mod opendtu;
pub(crate) mod shelly;

#[derive(Debug, Deserialize, Serialize)]
pub struct LogEvent {
    host: String,
    location: String,
    #[serde(rename = "type")]
    measurement_type: String,
    unit: String,
    sensor: String,
    calculated: bool,
    time: String,
    value: f64,
}

pub trait CheckMessage {
    fn check_message(&mut self, msg: &Message);
}
