use std::fmt;
use std::sync::mpsc::SyncSender;

use chrono::{DateTime, Utc};
use influxdb::{Timestamp, WriteQuery};
use paho_mqtt::Message;
use serde::{Deserialize, Serialize};

use crate::data::CheckMessage;
use crate::SensorReading;

#[derive(Serialize, Deserialize, Clone)]
pub struct Data {
    #[serde(rename = "time")]
    pub(crate) timestamp: i32,
    pub(crate) value: f32,
    pub(crate) unit: String,
    pub(crate) sensor: String,
    pub(crate) calculated: bool,
}

impl fmt::Debug for Data {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {} (@{}, {}{})", self.value, self.unit, self.timestamp, self.sensor, if self.calculated { ", C" } else { "" })
    }
}

pub struct Sensorlogger {
    tx: SyncSender<WriteQuery>,
    ts_tx: SyncSender<SensorReading>,
}

impl Sensorlogger {
    pub(crate) fn new(tx: SyncSender<WriteQuery>, ts_tx: SyncSender<SensorReading>) -> Self {
        Sensorlogger { tx, ts_tx }
    }
}

impl CheckMessage for Sensorlogger {
    fn check_message(&self, msg: &Message) {
        let mut split = msg.topic().split("/");
        let location = split.nth(1).unwrap();
        let measurement = split.next().unwrap();

        let result = parse(&msg).unwrap();

        if let Some(result) = result {
            println!("{} \"{}\": {:?}", location, measurement, &result);

            let timestamp = Timestamp::Seconds(result.timestamp as u128);
            let write_query = WriteQuery::new(timestamp, "data")
                .add_tag("type", measurement.to_string())
                .add_tag("location", location.to_string())
                .add_tag("sensor", result.sensor.to_string())
                .add_tag("calculated", result.calculated)
                .add_field("value", result.value);
            let write_query = if result.unit != "" {
                write_query.add_tag("unit", result.unit.to_string())
            } else {
                write_query
            };
            self.tx.send(write_query).expect("failed to send");

            let naive_date_time = chrono::NaiveDateTime::from_timestamp_opt(result.timestamp as i64, 0).expect("failed to convert timestamp");
            let date_time = DateTime::<Utc>::from_utc(naive_date_time, Utc);

            let sensor_reading = SensorReading {
                measurement: measurement.to_string(),
                time: date_time,
                location: location.to_string(),
                sensor: result.sensor.to_string(),
                value: result.value,
                unit: result.unit.to_string(),
                calculated: result.calculated,
            };
            self.ts_tx.send(sensor_reading).expect("failed to send");
        }
    }
}

pub fn resend() {}

pub fn parse(msg: &Message) -> Result<Option<Data>, &'static str> {
    let data = serde_json::from_slice::<Data>(msg.payload()).map_err(|error| {
        eprintln!("{:?}", error);
        "could not deserialize JSON"
    })?;
    Ok(Some(data.clone()))
}

#[cfg(test)]
mod tests {
    use paho_mqtt::QOS_1;

    use super::*;

    #[test]
    fn test_parse() -> Result<(), &'static str> {
        let topic = "klimalogger";
        let payload = "{\"location\": \"Kinderzimmer 1\", \"type\": \"temperature\", \"unit\": \"\u{00b0C}\", \"sensor\": \"BME680\", \"calculated\": false, \"time\": 1701292592, \"value\": 19.45}";

        let message = Message::new(topic, payload, QOS_1);
        let data = parse(&message)?.unwrap();

        assert_eq!(data.timestamp, 1701292592);
        assert_eq!(data.sensor, "BME680");

        Ok(())
    }

    #[test]
    fn test_parse_error() -> Result<(), &'static str> {
        let topic = "klimalogger";
        let payload = "{\"host\": \"dana\", \"location\": \"Kinderzimmer 1\", \"type\": \"temperature\", \"unit\": \"\u{00b0C}\", \"sensor\": \"BME680\", \"calculated\": false, \"time\": \"foo\", \"value\": 19.45}";

        let message = Message::new(topic, payload, QOS_1);
        let error = parse(&message).err().unwrap();

        assert_eq!(error, "could not deserialize JSON");

        Ok(())
    }
}