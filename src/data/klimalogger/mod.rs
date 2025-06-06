use std::fmt;
use std::sync::mpsc::SyncSender;

use crate::config::Target;
use crate::data::CheckMessage;
use crate::target::influx;
use crate::target::influx::InfluxConfig;
use crate::target::postgres::PostgresConfig;
use crate::{target, SensorReading};
use anyhow::Result;
use chrono::{DateTime, Utc};
use influxdb::{Timestamp, WriteQuery};
use log::{debug, warn};
use paho_mqtt::Message;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

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
    txs: Vec<SyncSender<SensorReading>>,
}

impl SensorLogger {
    pub(crate) fn new(tx: Vec<SyncSender<SensorReading>>) -> Self {
        SensorLogger { txs: tx }
    }

    fn convert_timestamp(timestamp: i64) -> DateTime<Utc> {
        chrono::DateTime::from_timestamp(timestamp, 0).expect("failed to convert timestamp")
    }
}

const MAX_TIME_OFFSET_SECONDS: i64 = 60;

impl CheckMessage for SensorLogger {
    fn check_message(&mut self, msg: &Message) {
        let mut split = msg.topic().split("/");

        let location = split.nth(1);
        let measurement = split.next();
        let result = parse(msg);
        if let (Some(location), Some(measurement), Ok(result)) = (location, measurement, &result) {
            let date_time = Self::convert_timestamp(result.timestamp as i64);

            let now = chrono::offset::Utc::now();
            let difference = now - date_time;

            let log_message = format!(
                "Sensor {} \"{}\": {:?} {:.2}s",
                location,
                measurement,
                &result,
                difference.num_milliseconds() as f32 / 1000.0,
            );

            if difference.num_seconds() > MAX_TIME_OFFSET_SECONDS {
                warn!("*** HIGH TIME OFFSET *** {} : {} - {}", log_message, now, date_time);
                return;
            }

            debug!("{}", log_message);
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
        } else {
            warn!("FAILED: {:?}, {:?}, {:?}", location, measurement, &result);
        }
    }
}

pub fn parse(msg: &Message) -> Result<Data> {
    Ok(serde_json::from_slice::<Data>(msg.payload())?)
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::sync_channel;
    use std::thread;

    use paho_mqtt::QOS_1;

    use super::*;

    #[test]
    fn test_parse() -> Result<()> {
        let topic = "klimalogger";
        let payload = "{\"foo\": \"ignored\", \"sensor\": \"BME680\", \"time\": 1701292592, \"value\": 19.45}";

        let message = Message::new(topic, payload, QOS_1);
        let data = parse(&message)?;

        assert_eq!(data.timestamp, 1701292592);
        assert_eq!(data.sensor, "BME680");

        Ok(())
    }

    #[test]
    fn test_parse_error() -> Result<()> {
        let topic = "klimalogger";
        let payload = "{\"sensor\": \"BME680\", \"time\": \"foo\", \"value\": 19.45}";

        let message = Message::new(topic, payload, QOS_1);
        let error = parse(&message).err().unwrap();

        assert_eq!(
            error.to_string(),
            "invalid type: string \"foo\", expected i32 at line 1 column 34"
        );

        Ok(())
    }

    #[test]
    fn test_check_message() -> Result<()> {
        let topic = "klimalogger/location/temperature";
        let now = chrono::offset::Utc::now();
        let payload = format!(
            "{{\"sensor\": \"BME680\", \"time\": {}, \"value\": 19.45}}",
            now.timestamp()
        );

        let (tx, rx) = sync_channel(100);

        let mut logger = SensorLogger::new(vec![tx]);
        let message = Message::new(topic, payload, QOS_1);
        thread::spawn(move || {
            logger.check_message(&message);
        });

        let result = rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();

        assert_eq!(result.location, "location");
        assert_eq!(result.measurement, "temperature");
        assert_eq!(result.sensor, "BME680");

        Ok(())
    }

    #[test]
    fn test_check_message_handles_outdated_value() -> Result<()> {
        let topic = "klimalogger/location/temperature";
        let payload = "{{\"sensor\": \"BME680\", \"time\": 1701292592, \"value\": 19.45}}";

        let (tx, rx) = sync_channel(100);

        let mut logger = SensorLogger::new(vec![tx]);
        let message = Message::new(topic, payload, QOS_1);
        thread::spawn(move || {
            logger.check_message(&message);
        });

        let result = rx.recv_timeout(std::time::Duration::from_secs(1));

        assert!(result.is_err());

        Ok(())
    }
}

pub fn create_logger(targets: Vec<Target>) -> (Arc<Mutex<dyn CheckMessage>>, Vec<JoinHandle<()>>) {
    let mut txs: Vec<SyncSender<SensorReading>> = Vec::new();
    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    for target in targets {
        let (tx, handle) = match target {
            Target::InfluxDB {
                url,
                database,
                user,
                password,
            } => {
                fn mapper(result: SensorReading) -> WriteQuery {
                    let timestamp = Timestamp::Seconds(result.time.timestamp() as u128);
                    WriteQuery::new(timestamp, result.measurement.to_string())
                        .add_tag("location", result.location.to_string())
                        .add_tag("sensor", result.sensor.to_string())
                        .add_field("value", result.value)
                }

                influx::spawn_influxdb_writer(
                    InfluxConfig::new(url, database, user, password),
                    mapper,
                )
            }
            Target::Postgresql {
                host,
                port,
                user,
                password,
                database,
            } => target::postgres::spawn_postgres_writer(PostgresConfig::new(
                host, port, user, password, database,
            )),
        };
        txs.push(tx);
        handles.push(handle);
    }

    (Arc::new(Mutex::new(SensorLogger::new(txs))), handles)
}
