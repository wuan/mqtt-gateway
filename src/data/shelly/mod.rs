mod data;

use std::fmt::Debug;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, LazyLock, Mutex};

use crate::config::Target;
use crate::data::{shelly, CheckMessage, LogEvent};
use crate::target::influx;
use crate::target::influx::InfluxConfig;
use crate::Number;
use anyhow::Result;
use data::{CoverData, SwitchData};
use log::{debug, warn};
use paho_mqtt::Message;
use regex::Regex;
use serde::Deserialize;
use std::thread::JoinHandle;

pub trait Timestamped {
    fn timestamp(&self) -> Option<i64>;
}

pub trait Typenamed {
    fn type_name(&self) -> &str;
}

pub struct ShellyLogger {
    txs: Vec<SyncSender<LogEvent>>,
}

impl ShellyLogger {
    pub(crate) fn new(txs: Vec<SyncSender<LogEvent>>) -> Self {
        ShellyLogger { txs }
    }
}

pub fn parse<'a, T: Deserialize<'a> + Clone>(msg: &'a Message) -> Result<T> {
    Ok(serde_json::from_slice::<T>(msg.payload())?)
}

type WriteTypeMapper<T> = fn(&T) -> Option<Number>;

const SWITCH_FIELDS: &[(&str, WriteTypeMapper<SwitchData>, &str)] = &[
    (
        "output",
        |data: &SwitchData| Some(Number::Int(data.output as i64)),
        "bool",
    ),
    (
        "power",
        |data: &SwitchData| data.power.map(Number::Float),
        "W",
    ),
    (
        "current",
        |data: &SwitchData| data.current.map(Number::Float),
        "A",
    ),
    (
        "voltage",
        |data: &SwitchData| data.voltage.map(Number::Float),
        "V",
    ),
    (
        "total_energy",
        |data: &SwitchData| Some(Number::Float(data.energy.total)),
        "Wh",
    ),
    (
        "temperature",
        |data: &SwitchData| Some(Number::Float(data.temperature.t_celsius)),
        "°C",
    ),
];

const COVER_FIELDS: &[(&str, WriteTypeMapper<CoverData>, &str)] = &[
    (
        "position",
        |data: &CoverData| data.position.map(Number::Int),
        "%",
    ),
    (
        "power",
        |data: &CoverData| data.power.map(Number::Float),
        "W",
    ),
    (
        "current",
        |data: &CoverData| data.current.map(Number::Float),
        "A",
    ),
    (
        "voltage",
        |data: &CoverData| data.voltage.map(Number::Float),
        "V",
    ),
    (
        "total_energy",
        |data: &CoverData| Some(Number::Float(data.energy.total)),
        "Wh",
    ),
    (
        "temperature",
        |data: &CoverData| Some(Number::Float(data.temperature.t_celsius)),
        "°C",
    ),
];

static SWITCH_REGEX: LazyLock<Regex, fn() -> Regex> =
    LazyLock::new(|| Regex::new("/status/switch:.").unwrap());
static COVER_REGEX: LazyLock<Regex, fn() -> Regex> =
    LazyLock::new(|| Regex::new("/status/cover:.").unwrap());

impl CheckMessage for ShellyLogger {
    fn check_message(&mut self, msg: &Message) {
        let topic = msg.topic();
        if SWITCH_REGEX.is_match(topic) {
            handle_message(msg, &self.txs, SWITCH_FIELDS);
        } else if COVER_REGEX.is_match(topic) {
            handle_message(msg, &self.txs, COVER_FIELDS);
        }
    }
}

fn handle_message<'a, T: Deserialize<'a> + Clone + Debug + Timestamped + Typenamed>(
    msg: &'a Message,
    txs: &Vec<SyncSender<LogEvent>>,
    fields: &[(&str, WriteTypeMapper<T>, &str)],
) {
    let location = msg.topic().split("/").nth(1).unwrap();
    let channel = msg.topic().split(":").last().unwrap();
    let parse_result = shelly::parse(msg);
    if parse_result.is_err() {
        warn!(
            "Shelly parse error: {:?} on '{}' (topic: {})",
            parse_result.err(),
            msg.payload_str(),
            msg.topic()
        );
        return;
    }
    let result: Option<T> = parse_result.unwrap();
    if let Some(data) = result {
        debug!("Shelly {}:{}: {:?}", location, channel, data);

        if let Some(minute_ts) = data.timestamp() {
            for (measurement, value, unit) in fields {
                if let Some(result) = value(&data) {
                    let tags = vec![
                        ("location", location),
                        ("channel", channel),
                        ("sensor", "shelly"),
                        ("type", data.type_name()),
                        ("unit", unit),
                    ];
                    let log_event = LogEvent::new_value_from_ref(
                        measurement.to_string(),
                        minute_ts,
                        tags.into_iter().collect(),
                        result,
                    );

                    for tx in txs {
                        tx.send(log_event.clone()).expect("failed to send");
                    }
                }
            }
        } else {
            warn!("{} no timestamp {:?}", msg.topic(), msg.payload_str());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paho_mqtt::QOS_1;
    use std::collections::HashMap;
    use std::sync::mpsc::{sync_channel, Receiver};
    use std::time::Duration;

    fn next(rx: &Receiver<LogEvent>) -> Result<LogEvent> {
        let result = rx.recv_timeout(Duration::from_micros(100))?;
        Ok(result)
    }

    struct EventAssert {
        tags: HashMap<String, String>,
    }

    impl EventAssert {
        fn new(tags: Vec<(&str, &str)>) -> Self {
            Self {
                tags: to_map(tags)
            }
        }

        fn assert(&self, log_event: LogEvent, measurement: &str, tags: Vec<(&str, &str)>, number: Number) {
            let mut expected = self.tags.clone();
            expected.extend(to_map(tags));

            for (tag, value) in &expected {
                assert_eq!(log_event.tags.get(tag).unwrap(), value);
            }
            assert_eq!(log_event.measurement, measurement);
            assert_eq!(log_event.fields.get("value").unwrap(), &number);
        }
    }

    fn to_map(tags: Vec<(&str, &str)>) -> HashMap<String, String> {
        tags.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }


    #[test]
    fn test_handle_switch_message() -> Result<()> {
        let (tx, rx) = sync_channel(100);
        let txs = vec![tx];

        let mut logger = ShellyLogger::new(txs);

        let message = Message::new(
            "shellies/loo-fan/status/switch:1",
            "{\"id\":0, \"source\":\"timer\", \"output\":false, \
            \"apower\":0.0, \"voltage\":226.5, \"current\":3.1, \
            \"aenergy\":{\"total\":1094.865,\"by_minute\":[0.000,0.000,0.000],\
            \"minute_ts\":1703415907},\"temperature\":{\"tC\":36.4, \"tF\":97.5}}",
            QOS_1,
        );
        logger.check_message(&message);

        let event_assert = EventAssert::new(
            vec![("location", "loo-fan"), ("sensor", "shelly"), ("channel", "1") ],
        );
        event_assert.assert(next(&rx)?, "output", vec![("unit", "bool")], Number::Int(0));
        event_assert.assert(next(&rx)?, "power", vec![("unit", "W")], Number::Float(0f64));
        event_assert.assert(next(&rx)?, "current", vec![("unit", "A")], Number::Float(3.1));
        event_assert.assert(next(&rx)?, "voltage", vec![("unit", "V")], Number::Float(226.5));
        event_assert.assert(next(&rx)?, "total_energy", vec![("unit", "Wh")], Number::Float(1094.865));
        event_assert.assert(next(&rx)?, "temperature", vec![("unit", "°C")], Number::Float(36.40));

        assert!(next(&rx).is_err());
        Ok(())
    }

    #[test]
    fn test_handle_curtain_message() -> Result<()> {
        let (tx, rx) = sync_channel(100);
        let txs = vec![tx];

        let mut logger = ShellyLogger::new(txs);

        let message = Message::new(
            "shellies/bedroom-curtain/status/cover:0",
            "{\"id\":0, \"source\":\"limit_switch\", \"state\":\"open\",\
                \"apower\":0.0,\"voltage\":231.7,\"current\":0.500,\"pf\":0.00,\"freq\":50.0,\
                \"aenergy\":{\"total\":3.143,\"by_minute\":[0.000,0.000,97.712],\
                \"minute_ts\":1703414519},\"temperature\":{\"tC\":30.7, \"tF\":87.3},\
                \"pos_control\":true,\"last_direction\":\"open\",\"current_pos\":100}",
            QOS_1,
        );
        logger.check_message(&message);

        let event_assert = EventAssert::new(
            vec![("sensor", "shelly"), ("location", "bedroom-curtain"), ("type", "cover"), ("channel", "0")],
        );
        event_assert.assert(next(&rx)?, "position", vec![("unit", "%")], Number::Int(100));
        event_assert.assert(next(&rx)?, "power", vec![("unit", "W")], Number::Float(0f64));
        event_assert.assert(next(&rx)?, "current", vec![("unit", "A")], Number::Float(0.5));
        event_assert.assert(next(&rx)?, "voltage", vec![("unit", "V")], Number::Float(231.7));
        event_assert.assert(next(&rx)?, "total_energy", vec![("unit", "Wh")], Number::Float(3.143));
        event_assert.assert(next(&rx)?, "temperature", vec![("unit", "°C")], Number::Float(30.7));
        assert!(next(&rx).is_err());

        Ok(())
    }

    #[test]
    fn test_handle_message_with_parse_error() -> Result<()> {
        let (tx, rx) = sync_channel(100);
        let txs = vec![tx];

        let mut logger = ShellyLogger::new(txs);

        let message = Message::new(
            "shellies/bedroom-curtain/status/cover:0",
            "{\"id\":0, \"source\":\"limit_switch\", \"state\":\"open\",\
                \"apower\":0.0}",
            QOS_1,
        );
        logger.check_message(&message);

        assert!(next(&rx).is_err());

        Ok(())
    }

    #[test]
    fn test_parse_switch_status() -> Result<()> {
        let message = Message::new("shellies/loo-fan/status/switch:0", "{\"id\":0, \"source\":\"timer\", \"output\":false, \"apower\":0.0, \"voltage\":226.5, \"current\":3.1, \"aenergy\":{\"total\":1094.865,\"by_minute\":[0.000,0.000,0.000],\"minute_ts\":1703415907},\"temperature\":{\"tC\":36.4, \"tF\":97.5}}", QOS_1);
        let result: SwitchData = parse(&message)?;

        assert_eq!(result.output, false);
        assert_eq!(result.power, Some(0.0));
        assert_eq!(result.voltage, Some(226.5));
        assert_eq!(result.current, Some(3.1));
        assert_eq!(result.energy.total, 1094.865);
        assert_eq!(result.temperature.t_celsius, 36.4);

        Ok(())
    }

    #[test]
    fn test_parse_cover_status() -> Result<()> {
        let message = Message::new("shellies/bedroom-curtain/status/cover:0", "{\"id\":0, \"source\":\"limit_switch\", \"state\":\"open\",\"apower\":0.0,\"voltage\":231.7,\"current\":0.500,\"pf\":0.00,\"freq\":50.0,\"aenergy\":{\"total\":3.143,\"by_minute\":[0.000,0.000,97.712],\"minute_ts\":1703414519},\"temperature\":{\"tC\":30.7, \"tF\":87.3},\"pos_control\":true,\"last_direction\":\"open\",\"current_pos\":100}", QOS_1);
        let result: CoverData = parse(&message)?;

        assert_eq!(result.position, Some(100));
        assert_eq!(result.power, Some(0.0));
        assert_eq!(result.voltage, Some(231.7));
        assert_eq!(result.current, Some(0.5));
        assert_eq!(result.energy.total, 3.143);
        assert_eq!(result.temperature.t_celsius, 30.7);
        assert_eq!(result.energy.minute_ts.unwrap(), 1703414519);

        Ok(())
    }

    #[test]
    fn test_parse_cover_status_without_timestamp() -> Result<()> {
        let message = Message::new("shellies/bedroom-curtain/status/cover:0", "{\"id\":0, \"source\":\"limit_switch\", \"state\":\"open\",\"apower\":0.0,\"voltage\":231.7,\"current\":0.500,\"pf\":0.00,\"freq\":50.0,\"aenergy\":{\"total\":3.143,\"by_minute\":[0.000,0.000,97.712]},\"temperature\":{\"tC\":30.7, \"tF\":87.3},\"pos_control\":true,\"last_direction\":\"open\",\"current_pos\":100}", QOS_1);
        let result: CoverData = parse(&message)?;

        assert!(result.energy.minute_ts.is_none());

        Ok(())
    }

    #[test]
    fn test_parse_cover_status_without_position() -> Result<()> {
        let message = Message::new("shellies/bedroom-curtain/status/cover:0", "{\"id\":0, \"source\":\"limit_switch\", \"state\":\"open\",\"apower\":0.0,\"voltage\":231.7,\"current\":0.500,\"pf\":0.00,\"freq\":50.0,\"aenergy\":{\"total\":3.143,\"by_minute\":[0.000,0.000,97.712],\"minute_ts\":1703414519},\"temperature\":{\"tC\":30.7, \"tF\":87.3},\"pos_control\":true,\"last_direction\":\"open\"}", QOS_1);
        let result: CoverData = parse(&message)?;

        assert!(result.position.is_none());

        Ok(())
    }
}

pub fn create_logger(
    targets: Vec<Target>,
) -> anyhow::Result<(Arc<Mutex<dyn CheckMessage>>, Vec<JoinHandle<()>>)> {
    let mut txs: Vec<SyncSender<LogEvent>> = Vec::new();
    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    for target in targets {
        let (tx, handle) = match target {
            Target::InfluxDB {
                url,
                database,
                user,
                password,
            } => influx::spawn_influxdb_writer(
                InfluxConfig::new(url, database, user, password)?,
                std::convert::identity,
            ),
            Target::Postgresql { .. } => {
                panic!("Postgresql not supported for shelly");
            }
        };
        txs.push(tx);
        handles.push(handle);
    }

    Ok((Arc::new(Mutex::new(ShellyLogger::new(txs))), handles))
}
