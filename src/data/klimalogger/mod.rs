use std::fmt;
use std::sync::mpsc::SyncSender;

use crate::config::{Pattern, Target, ValueFormat};
use crate::data::{send_event, CheckMessage, LogEvent, LoggerResult};
use crate::target::create_targets;
use crate::Number;
use crate::Shutdown;
use anyhow::Result;
use chrono::{DateTime, Utc};
use log::{debug, warn};
use paho_mqtt::Message;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Serialize, Deserialize, Clone)]
pub struct Data {
    #[serde(rename = "time")]
    pub(crate) timestamp: i64,
    pub(crate) value: f64,
    pub(crate) sensor: String,
}

impl fmt::Debug for Data {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} (@{}, {})", self.value, self.timestamp, self.sensor)
    }
}

pub struct SensorLogger {
    txs: Vec<SyncSender<Arc<LogEvent>>>,
    patterns: Vec<Pattern>,
}

impl SensorLogger {
    pub(crate) fn new(tx: Vec<SyncSender<Arc<LogEvent>>>, patterns: Vec<Pattern>) -> Self {
        SensorLogger { txs: tx, patterns }
    }

    fn convert_timestamp(timestamp: i64) -> Option<DateTime<Utc>> {
        chrono::DateTime::from_timestamp(timestamp, 0)
    }

    /// Decode the default JSON payload `{ sensor, time, value }`.
    ///
    /// A payload that is not JSON at all is expected on a shared source prefix,
    /// so it is ignored with a debug log. Valid JSON that does not match the
    /// expected shape is warned about.
    fn handle_json(&self, msg: &Message) {
        let result = match parse(msg) {
            Ok(result) => result,
            Err(error) => {
                if is_non_json(&error) {
                    debug!(
                        "ignoring non-JSON payload on topic '{}': {}",
                        msg.topic(),
                        error
                    );
                } else {
                    warn!("FAILED: parsing '{}': {}", msg.topic(), error);
                }
                return;
            }
        };

        let mut split = msg.topic().split("/");

        let location = split.nth(1);
        let measurement = split.next();
        let (location, measurement) = match (location, measurement) {
            (Some(location), Some(measurement)) => (location, measurement),
            _ => {
                warn!(
                    "FAILED: topic '{}' has no location/measurement",
                    msg.topic()
                );
                return;
            }
        };

        let date_time = match Self::convert_timestamp(result.timestamp) {
            Some(date_time) => date_time,
            None => {
                warn!(
                    "invalid timestamp {} from {}",
                    result.timestamp,
                    msg.topic()
                );
                return;
            }
        };

        let now = chrono::offset::Utc::now();
        let difference = now - date_time;

        let log_message = format!(
            "Sensor {} \"{}\": {:?} {:.2}s",
            location,
            measurement,
            result,
            difference.num_milliseconds() as f32 / 1000.0,
        );

        if difference.num_seconds().abs() > MAX_TIME_OFFSET_SECONDS {
            warn!(
                "*** HIGH TIME OFFSET *** {} : {} - {}",
                log_message, now, date_time
            );
            return;
        }

        debug!("{}", log_message);

        let log_event = LogEvent::new_value_from_ref(
            measurement.to_string(),
            date_time.timestamp(),
            vec![("location", location), ("sensor", &result.sensor)]
                .into_iter()
                .collect(),
            Number::Float(result.value),
        );

        send_event(&self.txs, Arc::new(log_event));
    }

    /// Emit an event for a bare scalar payload selected by `pattern`.
    ///
    /// Splits the topic into its location and measurement segments, warns and
    /// drops when either is missing, and otherwise logs the match and sends the
    /// event stamped with the receive time and tagged with `location` and a
    /// `sensor` tag (the pattern's value or the location default).
    fn handle_scalar(&self, msg: &Message, pattern: &Pattern, kind: &str, value: Number) {
        let mut split = msg.topic().split("/");

        let location = split.nth(1);
        let measurement = split.next();
        let (location, measurement) = match (location, measurement) {
            (Some(location), Some(measurement)) => (location, measurement),
            _ => {
                warn!(
                    "sensor {}: topic '{}' has no location/measurement",
                    kind,
                    msg.topic()
                );
                return;
            }
        };

        let sensor = pattern.sensor.as_deref().unwrap_or(location);
        debug!(
            "sensor {}: pattern '{}' matched topic '{}': {:?} ({}/{})",
            kind,
            pattern.topic,
            msg.topic(),
            value,
            location,
            measurement,
        );
        let log_event = LogEvent::new_value_from_ref(
            measurement.to_string(),
            chrono::offset::Utc::now().timestamp(),
            vec![("location", location), ("sensor", sensor)]
                .into_iter()
                .collect(),
            value,
        );

        send_event(&self.txs, Arc::new(log_event));
    }

    /// Decode a bare floating point payload selected by a `float` pattern.
    ///
    /// The payload carries no timestamp, so the event is stamped with the
    /// receive time and the freshness check is skipped.
    fn handle_float(&self, msg: &Message, pattern: &Pattern) {
        let value = match msg.payload_str().parse::<f64>() {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    "sensor float: cannot parse '{}' as number (topic: {}): {}",
                    msg.payload_str(),
                    msg.topic(),
                    error
                );
                return;
            }
        };

        self.handle_scalar(msg, pattern, "float", Number::Float(value));
    }

    /// Decode a bare integer payload selected by an `int` pattern.
    ///
    /// The payload carries no timestamp, so the event is stamped with the
    /// receive time and the freshness check is skipped. Only strict integers are
    /// accepted; a value with a fraction or exponent is dropped.
    fn handle_int(&self, msg: &Message, pattern: &Pattern) {
        let value = match msg.payload_str().parse::<i64>() {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    "sensor int: cannot parse '{}' as integer (topic: {}): {}",
                    msg.payload_str(),
                    msg.topic(),
                    error
                );
                return;
            }
        };

        self.handle_scalar(msg, pattern, "int", Number::Int(value));
    }
}

/// Whether `filter` (an MQTT topic filter where `+` matches a single level and
/// `#` matches the remaining levels) matches `topic`.
fn topic_filter_matches(filter: &str, topic: &str) -> bool {
    let mut filter_levels = filter.split('/');
    let mut topic_levels = topic.split('/');

    loop {
        match (filter_levels.next(), topic_levels.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => {}
            (Some(level), Some(topic_level)) if level == topic_level => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

const MAX_TIME_OFFSET_SECONDS: i64 = 60;

pub fn create_logger(
    targets: Vec<Target>,
    shutdown: Shutdown,
    patterns: Vec<Pattern>,
) -> Result<LoggerResult> {
    let (txs, handles) = create_targets(targets, shutdown)?;

    Ok((
        Arc::new(Mutex::new(SensorLogger::new(txs, patterns))),
        handles,
    ))
}

impl CheckMessage for SensorLogger {
    fn check_message(&mut self, msg: &Message) {
        if let Some(pattern) = self
            .patterns
            .iter()
            .find(|pattern| topic_filter_matches(&pattern.topic, msg.topic()))
        {
            debug!(
                "sensor pattern '{}' matched topic '{}' with format {:?}",
                pattern.topic,
                msg.topic(),
                pattern.format,
            );

            match pattern.format {
                ValueFormat::Float => {
                    self.handle_float(msg, pattern);
                    return;
                }
                ValueFormat::Int => {
                    self.handle_int(msg, pattern);
                    return;
                }
                ValueFormat::Json => {}
            }
        } else if !self.patterns.is_empty() {
            debug!(
                "no sensor pattern matched topic '{}'; using JSON decoding",
                msg.topic()
            );
        }

        self.handle_json(msg);
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

/// Whether a decode failure means the payload is not JSON at all, as opposed to
/// valid JSON that does not match the `{ sensor, time, value }` shape.
fn is_non_json(error: &serde_json::Error) -> bool {
    matches!(
        error.classify(),
        serde_json::error::Category::Syntax | serde_json::error::Category::Eof
    )
}

pub fn parse(msg: &Message) -> Result<Data, serde_json::Error> {
    serde_json::from_slice::<Data>(msg.payload())
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::sync_channel;
    use std::thread;

    use paho_mqtt::QOS_1;

    use super::*;
    use crate::config::{Pattern, ValueFormat};

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
            "invalid type: string \"foo\", expected i64 at line 1 column 34"
        );

        Ok(())
    }

    #[test]
    fn test_is_non_json() {
        let syntax = parse(&Message::new("klimalogger", "heating", QOS_1)).unwrap_err();
        assert!(is_non_json(&syntax));

        let wrong_shape = parse(&Message::new("klimalogger", "42", QOS_1)).unwrap_err();
        assert!(!is_non_json(&wrong_shape));
    }

    #[test]
    fn test_check_message_ignores_non_json_payload() {
        let (tx, rx) = sync_channel(100);
        let mut logger = SensorLogger::new(vec![tx], vec![]);

        logger.check_message(&Message::new(
            "klimalogger/location/temperature",
            "heating",
            QOS_1,
        ));

        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_check_message_drops_wrong_shape_json() {
        let (tx, rx) = sync_channel(100);
        let mut logger = SensorLogger::new(vec![tx], vec![]);

        logger.check_message(&Message::new(
            "klimalogger/location/temperature",
            "{\"a\":1}",
            QOS_1,
        ));

        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_check_message() -> Result<()> {
        let topic = "klimalogger/location/temperature";
        let timestamp = chrono::offset::Utc::now().timestamp();
        let payload = format!(
            "{{\"sensor\": \"BME680\", \"time\": {}, \"value\": 19.45}}",
            timestamp
        );
        let (tx, rx) = sync_channel(100);
        let mut logger = SensorLogger::new(vec![tx], vec![]);
        let message = Message::new(topic, payload, QOS_1);
        logger.check_message(&message);

        let result = rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();

        assert_eq!(result.measurement, "temperature");
        assert_eq!(result.tags.get("location").unwrap(), "location");
        assert_eq!(result.tags.get("sensor").unwrap(), "BME680");
        assert_eq!(result.timestamp, timestamp);

        Ok(())
    }

    #[test]
    fn test_check_message_handles_outdated_value() -> Result<()> {
        let topic = "klimalogger/location/temperature";
        let payload = "{{\"sensor\": \"BME680\", \"time\": 1701292592, \"value\": 19.45}}";

        let (tx, rx) = sync_channel(100);

        let mut logger = SensorLogger::new(vec![tx], vec![]);
        let message = Message::new(topic, payload, QOS_1);
        thread::spawn(move || {
            logger.check_message(&message);
        });

        let result = rx.recv_timeout(std::time::Duration::from_secs(1));

        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_check_message_handles_future_timestamp() -> Result<()> {
        let topic = "klimalogger/location/temperature";
        let future_timestamp = chrono::offset::Utc::now().timestamp() + 3600;
        let payload = format!(
            "{{\"sensor\": \"BME680\", \"time\": {}, \"value\": 19.45}}",
            future_timestamp
        );

        let (tx, rx) = sync_channel(100);
        let mut logger = SensorLogger::new(vec![tx], vec![]);
        logger.check_message(&Message::new(topic, payload, QOS_1));

        assert!(rx.try_recv().is_err());

        Ok(())
    }

    #[test]
    fn test_check_message_handles_overflow_timestamp() -> Result<()> {
        let topic = "klimalogger/location/temperature";
        let payload = "{\"sensor\": \"BME680\", \"time\": 9223372036854775807, \"value\": 19.45}";

        let (tx, rx) = sync_channel(100);
        let mut logger = SensorLogger::new(vec![tx], vec![]);
        logger.check_message(&Message::new(topic, payload, QOS_1));

        assert!(rx.try_recv().is_err());

        Ok(())
    }

    #[test]
    fn test_create_logger() -> Result<()> {
        let targets = vec![Target::Debug {}];
        let (logger, mut handles) = create_logger(targets, crate::Shutdown::new(), vec![])?;

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

    fn float_pattern(topic: &str, sensor: Option<&str>) -> Pattern {
        Pattern {
            topic: topic.to_string(),
            format: ValueFormat::Float,
            sensor: sensor.map(str::to_string),
        }
    }

    fn int_pattern(topic: &str, sensor: Option<&str>) -> Pattern {
        Pattern {
            topic: topic.to_string(),
            format: ValueFormat::Int,
            sensor: sensor.map(str::to_string),
        }
    }

    #[test]
    fn test_topic_filter_matches() {
        assert!(topic_filter_matches(
            "sensors/+/temperature",
            "sensors/kitchen/temperature"
        ));
        assert!(topic_filter_matches(
            "sensors/#",
            "sensors/kitchen/temperature"
        ));
        assert!(topic_filter_matches("#", "anything/at/all"));
        assert!(topic_filter_matches(
            "sensors/kitchen/temperature",
            "sensors/kitchen/temperature"
        ));

        assert!(!topic_filter_matches(
            "sensors/+/temperature",
            "sensors/kitchen/humidity"
        ));
        assert!(!topic_filter_matches(
            "sensors/+",
            "sensors/kitchen/temperature"
        ));
        assert!(!topic_filter_matches("sensors/+", "sensors"));
        assert!(!topic_filter_matches(
            "sensors/kitchen",
            "sensors/kitchen/temperature"
        ));
    }

    #[test]
    fn test_check_message_parses_float_pattern() -> Result<()> {
        let (tx, rx) = sync_channel(100);
        let patterns = vec![float_pattern("sensors/+/temperature", Some("raw"))];
        let mut logger = SensorLogger::new(vec![tx], patterns);

        logger.check_message(&Message::new("sensors/kitchen/temperature", "19.45", QOS_1));

        let event = rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(event.measurement, "temperature");
        assert_eq!(event.tags.get("location").unwrap(), "kitchen");
        assert_eq!(event.tags.get("sensor").unwrap(), "raw");
        assert_eq!(event.fields.get("value").unwrap(), &Number::Float(19.45));

        Ok(())
    }

    #[test]
    fn test_check_message_parses_int_pattern() -> Result<()> {
        let (tx, rx) = sync_channel(100);
        let patterns = vec![int_pattern("sensors/+/count", Some("raw"))];
        let mut logger = SensorLogger::new(vec![tx], patterns);

        let before = chrono::offset::Utc::now().timestamp();
        logger.check_message(&Message::new("sensors/kitchen/count", "42", QOS_1));

        let event = rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(event.measurement, "count");
        assert_eq!(event.tags.get("location").unwrap(), "kitchen");
        assert_eq!(event.tags.get("sensor").unwrap(), "raw");
        assert_eq!(event.fields.get("value").unwrap(), &Number::Int(42));
        assert!(event.timestamp >= before);
        assert!(event.timestamp <= chrono::offset::Utc::now().timestamp());

        Ok(())
    }

    #[test]
    fn test_check_message_int_pattern_accepts_negative() -> Result<()> {
        let (tx, rx) = sync_channel(100);
        let patterns = vec![int_pattern("sensors/+/count", None)];
        let mut logger = SensorLogger::new(vec![tx], patterns);

        logger.check_message(&Message::new("sensors/kitchen/count", "-3", QOS_1));

        let event = rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(event.fields.get("value").unwrap(), &Number::Int(-3));

        Ok(())
    }

    #[test]
    fn test_check_message_int_pattern_drops_non_integers() {
        for payload in ["19.0", "1.9e1", "not-a-number"] {
            let (tx, rx) = sync_channel(100);
            let patterns = vec![int_pattern("sensors/+/count", None)];
            let mut logger = SensorLogger::new(vec![tx], patterns);

            logger.check_message(&Message::new("sensors/kitchen/count", payload, QOS_1));

            assert!(
                rx.try_recv().is_err(),
                "expected payload '{payload}' to be dropped"
            );
        }
    }

    #[test]
    fn test_check_message_int_pattern_without_location_is_dropped() {
        let (tx, rx) = sync_channel(100);
        // A topic with no location or measurement segment.
        let patterns = vec![int_pattern("#", None)];
        let mut logger = SensorLogger::new(vec![tx], patterns);

        logger.check_message(&Message::new("count", "42", QOS_1));

        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_check_message_int_pattern_defaults_sensor_to_location() -> Result<()> {
        let (tx, rx) = sync_channel(100);
        let patterns = vec![int_pattern("sensors/+/count", None)];
        let mut logger = SensorLogger::new(vec![tx], patterns);

        logger.check_message(&Message::new("sensors/kitchen/count", "42", QOS_1));

        let event = rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(event.tags.get("sensor").unwrap(), "kitchen");

        Ok(())
    }

    #[test]
    fn test_check_message_unmatched_int_topic_uses_json() -> Result<()> {
        let (tx, rx) = sync_channel(100);
        // The pattern selects only count, so temperature keeps the JSON path.
        let patterns = vec![int_pattern("sensors/+/count", Some("raw"))];
        let mut logger = SensorLogger::new(vec![tx], patterns);

        let timestamp = chrono::offset::Utc::now().timestamp();
        let payload = format!(
            "{{\"sensor\": \"BME680\", \"time\": {}, \"value\": 42.0}}",
            timestamp
        );
        logger.check_message(&Message::new("sensors/kitchen/temperature", payload, QOS_1));

        let event = rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(event.measurement, "temperature");
        assert_eq!(event.tags.get("sensor").unwrap(), "BME680");

        Ok(())
    }

    #[test]
    fn test_check_message_float_pattern_defaults_sensor_to_location() -> Result<()> {
        let (tx, rx) = sync_channel(100);
        let patterns = vec![float_pattern("sensors/+/temperature", None)];
        let mut logger = SensorLogger::new(vec![tx], patterns);

        logger.check_message(&Message::new("sensors/kitchen/temperature", "19.45", QOS_1));

        let event = rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(event.tags.get("sensor").unwrap(), "kitchen");

        Ok(())
    }

    #[test]
    fn test_check_message_unmatched_float_topic_uses_json() -> Result<()> {
        let (tx, rx) = sync_channel(100);
        // The pattern selects only temperature, so humidity keeps the JSON path.
        let patterns = vec![float_pattern("sensors/+/temperature", Some("raw"))];
        let mut logger = SensorLogger::new(vec![tx], patterns);

        let timestamp = chrono::offset::Utc::now().timestamp();
        let payload = format!(
            "{{\"sensor\": \"BME680\", \"time\": {}, \"value\": 42.0}}",
            timestamp
        );
        logger.check_message(&Message::new("sensors/kitchen/humidity", payload, QOS_1));

        let event = rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(event.measurement, "humidity");
        assert_eq!(event.tags.get("sensor").unwrap(), "BME680");

        Ok(())
    }

    #[test]
    fn test_check_message_float_parse_error_does_not_panic() {
        let (tx, rx) = sync_channel(100);
        let patterns = vec![float_pattern("sensors/+/temperature", None)];
        let mut logger = SensorLogger::new(vec![tx], patterns);

        logger.check_message(&Message::new(
            "sensors/kitchen/temperature",
            "not-a-number",
            QOS_1,
        ));

        assert!(rx.try_recv().is_err());
    }
}
