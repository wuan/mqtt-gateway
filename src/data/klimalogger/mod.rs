use std::fmt;
use std::sync::mpsc::SyncSender;

use chrono::{DateTime, Utc};
use paho_mqtt::Message;
use serde::{Deserialize, Serialize};

use crate::data::CheckMessage;
use crate::SensorReading;

#[derive(Serialize, Deserialize, Clone)]
pub struct Data {
    #[serde(rename = "time")]
    pub(crate) timestamp: i32,
    pub(crate) value: f32,
    pub(crate) sensor: String,
}

impl fmt::Debug for Data {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} (@{}, {})", self.value, self.timestamp, self.sensor)
    }
}

pub struct SensorLogger {
    txs: Vec::<SyncSender<SensorReading>>,
}

impl SensorLogger {
    pub(crate) fn new(tx: Vec::<SyncSender<SensorReading>>) -> Self {
        SensorLogger { txs: tx }
    }

    fn convert_timestamp(timestamp: i64) -> DateTime<Utc> {
        let naive_date_time = chrono::NaiveDateTime::from_timestamp_opt(timestamp, 0).expect("failed to convert timestamp");
        DateTime::<Utc>::from_naive_utc_and_offset(naive_date_time, Utc)
    }
}

impl CheckMessage for SensorLogger {
    fn check_message(&mut self, msg: &Message) {
        let mut split = msg.topic().split("/");

        let location = split.nth(1);
        let measurement = split.next();
        let result = parse(&msg);
        if let (Some(location), Some(measurement), Ok(result)) = (
            location, measurement, result.clone()) {
            if let Some(result) = result {
                let date_time = Self::convert_timestamp(result.timestamp as i64);

                let now = chrono::offset::Utc::now();
                let difference = now - date_time;

                let has_high_time_offset = difference.num_seconds() > 10;
                println!("Sensor {} \"{}\": {:?} {:.2}s{}", location, measurement, &result,
                         difference.num_milliseconds() as f32 / 1000.0,
                         if has_high_time_offset { " *** HIGH TIME OFFSET ***" } else { "" });

                if has_high_time_offset {
                    return;
                }

                let sensor_reading = SensorReading {
                    measurement: measurement.to_string(),
                    time: date_time,
                    location: location.to_string(),
                    sensor: result.sensor.to_string(),
                    value: result.value,
                };

                for tx in &self.txs {
                    tx.send(sensor_reading.clone()).expect("failed to send");
                }
            }
        } else {
            println!("FAILED: {:?}, {:?}, {:?}", location, measurement, result);
        }
    }
}

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