mod data;

use crate::config::Target;
use crate::data::{shelly, CheckMessage, LogEvent};
use crate::target::create_targets;
use crate::Number;
use anyhow::Result;
use data::{CoverData, SwitchData};
use log::{debug, warn};
use paho_mqtt::Message;
use regex::Regex;
use serde::Deserialize;
use std::fmt::Debug;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, LazyLock, Mutex};
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

    #[cfg(test)]
    fn checked_count(&self) -> u64 {
        0
    }

    #[cfg(test)]
    fn drop_all(&mut self) {
        self.txs.clear();
    }
}

fn handle_message<'a, T: Deserialize<'a> + Clone + Debug + Timestamped + Typenamed>(
    msg: &'a Message,
    txs: &Vec<SyncSender<LogEvent>>,
    fields: &[(&str, WriteTypeMapper<T>, &str)],
) {
    let location = msg.topic().split("/").nth(1).unwrap();
    let channel = msg.topic().split(":").last().unwrap();
    let parse_result: Result<Option<T>> = shelly::parse(msg);
    match parse_result {
        Ok(result) => {
            if let Some(data) = result {
                debug!("Shelly {}:{}: {:?}", location, channel, data);

                if let Some(minute_ts) = data.timestamp() {
                    convert_measurements(txs, fields, location, channel, &data, minute_ts);
                } else {
                    warn!("{} no timestamp {:?}", msg.topic(), msg.payload_str());
                }
            }
        }
        Err(value) => {
            warn!(
                "Shelly parse error: {:?} on '{}' (topic: {})",
                value.to_string(),
                msg.payload_str(),
                msg.topic()
            );
            return;
        }
    }
}

fn convert_measurements<T: Clone + Debug + Timestamped + Typenamed>(
    txs: &Vec<SyncSender<LogEvent>>,
    fields: &[(&str, WriteTypeMapper<T>, &str)],
    location: &str,
    channel: &str,
    data: &T,
    minute_ts: i64,
) {
    for (measurement, value, unit) in fields {
        if let Some(result) = value(data) {
            for tx in txs {
                tx.send(create_event(
                    location,
                    channel,
                    data,
                    minute_ts,
                    measurement,
                    unit,
                    result,
                ))
                .expect("failed to send");
            }
        }
    }
}

fn create_event<T: Clone + Debug + Timestamped + Typenamed>(
    location: &str,
    channel: &str,
    data: &T,
    minute_ts: i64,
    measurement: &str,
    unit: &str,
    result: Number,
) -> LogEvent {
    let tags = vec![
        ("location", location),
        ("channel", channel),
        ("sensor", "shelly"),
        ("type", data.type_name()),
        ("unit", unit),
    ];
    LogEvent::new_value_from_ref(
        measurement.to_string(),
        minute_ts,
        tags.into_iter().collect(),
        result,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::LevelFilter;
    use paho_mqtt::QOS_1;
    use std::collections::HashMap;
    use std::io;
    use std::io::Write;
    use std::sync::mpsc::{sync_channel, Receiver};
    use std::time::Duration;

    struct WriteAdapter(Arc<Mutex<Vec<u8>>>);

    impl Write for WriteAdapter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.0.lock().unwrap().flush()
        }
    }

    fn next(rx: &Receiver<LogEvent>) -> Result<LogEvent> {
        let result = rx.recv_timeout(Duration::from_micros(100))?;
        Ok(result)
    }

    struct EventAssert {
        tags: HashMap<String, String>,
    }

    impl EventAssert {
        fn new(tags: Vec<(&str, &str)>) -> Self {
            Self { tags: to_map(tags) }
        }

        fn assert(
            &self,
            log_event: LogEvent,
            measurement: &str,
            tags: Vec<(&str, &str)>,
            number: Number,
        ) {
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
        tags.into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
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

        let event_assert = EventAssert::new(vec![
            ("location", "loo-fan"),
            ("sensor", "shelly"),
            ("channel", "1"),
        ]);
        event_assert.assert(next(&rx)?, "output", vec![("unit", "bool")], Number::Int(0));
        event_assert.assert(
            next(&rx)?,
            "power",
            vec![("unit", "W")],
            Number::Float(0f64),
        );
        event_assert.assert(
            next(&rx)?,
            "current",
            vec![("unit", "A")],
            Number::Float(3.1),
        );
        event_assert.assert(
            next(&rx)?,
            "voltage",
            vec![("unit", "V")],
            Number::Float(226.5),
        );
        event_assert.assert(
            next(&rx)?,
            "total_energy",
            vec![("unit", "Wh")],
            Number::Float(1094.865),
        );
        event_assert.assert(
            next(&rx)?,
            "temperature",
            vec![("unit", "°C")],
            Number::Float(36.40),
        );

        assert!(next(&rx).is_err());
        Ok(())
    }

    const COVER_PAYLOAD: &'static str =
        "{\"id\":0, \"source\":\"limit_switch\", \"state\":\"open\",\
                \"apower\":0.0,\"voltage\":231.7,\"current\":0.500,\"pf\":0.00,\"freq\":50.0,\
                \"aenergy\":{\"total\":3.143,\"by_minute\":[0.000,0.000,97.712],\
                \"minute_ts\":1703414519},\"temperature\":{\"tC\":30.7, \"tF\":87.3},\
                \"pos_control\":true,\"last_direction\":\"open\",\"current_pos\":100}";

    #[test]
    fn test_handle_curtain_message() -> Result<()> {
        let (tx, rx) = sync_channel(100);
        let txs = vec![tx];

        let mut logger = ShellyLogger::new(txs);

        let message = Message::new(
            "shellies/bedroom-curtain/status/cover:0",
            COVER_PAYLOAD,
            QOS_1,
        );
        logger.check_message(&message);

        let event_assert = EventAssert::new(vec![
            ("sensor", "shelly"),
            ("location", "bedroom-curtain"),
            ("type", "cover"),
            ("channel", "0"),
        ]);
        event_assert.assert(
            next(&rx)?,
            "position",
            vec![("unit", "%")],
            Number::Int(100),
        );
        event_assert.assert(
            next(&rx)?,
            "power",
            vec![("unit", "W")],
            Number::Float(0f64),
        );
        event_assert.assert(
            next(&rx)?,
            "current",
            vec![("unit", "A")],
            Number::Float(0.5),
        );
        event_assert.assert(
            next(&rx)?,
            "voltage",
            vec![("unit", "V")],
            Number::Float(231.7),
        );
        event_assert.assert(
            next(&rx)?,
            "total_energy",
            vec![("unit", "Wh")],
            Number::Float(3.143),
        );
        event_assert.assert(
            next(&rx)?,
            "temperature",
            vec![("unit", "°C")],
            Number::Float(30.7),
        );
        assert!(next(&rx).is_err());

        Ok(())
    }

    #[test]
    fn test_handle_message_with_parse_error() -> Result<()> {
        let log_buffer = Arc::new(Mutex::new(Vec::new()));
        let buffer_clone = log_buffer.clone();

        let _ = env_logger::builder()
            .filter(None, log::LevelFilter::Off)
            .filter_module("mqtt_gateway::data::shelly", LevelFilter::Warn)
            .format_timestamp(None)
            .target(env_logger::Target::Pipe(Box::new(WriteAdapter(
                buffer_clone,
            ))))
            .is_test(true)
            .try_init();

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

        assert_eq!(
            String::from_utf8_lossy(&log_buffer.lock().unwrap().as_slice()),
            "[WARN  mqtt_gateway::data::shelly] Shelly parse error: \"missing field `aenergy` at line 1 column 62\" on '{\"id\":0, \"source\":\"limit_switch\", \"state\":\"open\",\"apower\":0.0}' (topic: shellies/bedroom-curtain/status/cover:0)\n"
        );

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

    #[test]
    fn test_create_logger() -> Result<()> {
        let targets = vec![Target::Debug {}];
        let (logger, mut handles) = create_logger(targets)?;

        assert!(logger.lock().unwrap().checked_count() == 0);
        assert_eq!(handles.len(), 1);

        logger.lock().unwrap().drop_all();
        if let Some(handle) = handles.pop() {
            handle
                .join()
                .map_err(|e| anyhow::anyhow!("Thread panicked: {:?}", e))?;
        }

        Ok(())
    }
}

pub fn create_logger(
    targets: Vec<Target>,
) -> Result<(Arc<Mutex<dyn CheckMessage>>, Vec<JoinHandle<()>>)> {
    let (txs, handles) = create_targets(targets)?;

    Ok((Arc::new(Mutex::new(ShellyLogger::new(txs))), handles))
}
