pub(crate) mod parse;
mod opendtu;
pub(crate) mod klimalogger;

use serde::{Deserialize, Serialize};

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
