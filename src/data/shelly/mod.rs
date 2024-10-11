use std::fmt;
use std::fmt::Debug;
use std::sync::mpsc::SyncSender;
use std::sync::LazyLock;

use anyhow::Result;
use influxdb::{Timestamp, WriteQuery};
use paho_mqtt::Message;
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::data::{shelly, CheckMessage};
use crate::WriteType;

pub trait Timestamped {
    fn timestamp(&self) -> Option<i64>;
}

pub trait Typenamed {
    fn type_name(&self) -> &str;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SwitchData {
    pub(crate) output: bool,
    #[serde(rename = "apower")]
    pub(crate) power: Option<f32>,
    pub(crate) voltage: Option<f32>,
    pub(crate) current: Option<f32>,
    #[serde(rename = "aenergy")]
    pub(crate) energy: EnergyData,
    pub(crate) temperature: TemperatureData,
}

impl Timestamped for SwitchData {
    fn timestamp(&self) -> Option<i64> {
        self.energy.minute_ts
    }
}

impl Typenamed for SwitchData {
    fn type_name(&self) -> &str {
        return "switch";
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CoverData {
    #[serde(rename = "current_pos")]
    pub(crate) position: Option<i32>,
    #[serde(rename = "apower")]
    pub(crate) power: Option<f32>,
    pub(crate) voltage: Option<f32>,
    pub(crate) current: Option<f32>,
    #[serde(rename = "aenergy")]
    pub(crate) energy: EnergyData,
    pub(crate) temperature: TemperatureData,
}

impl Timestamped for CoverData {
    fn timestamp(&self) -> Option<i64> {
        self.energy.minute_ts
    }
}

impl Typenamed for CoverData {
    fn type_name(&self) -> &str {
        return "cover";
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct EnergyData {
    pub(crate) total: f32,
    pub(crate) minute_ts: Option<i64>,
}

impl fmt::Debug for EnergyData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} Wh", self.total)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TemperatureData {
    #[serde(rename = "tC")]
    pub(crate) t_celsius: f32,
}

impl fmt::Debug for TemperatureData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} °C", self.t_celsius)
    }
}

pub struct ShellyLogger {
    txs: Vec<SyncSender<WriteQuery>>,
}

impl ShellyLogger {
    pub(crate) fn new(txs: Vec<SyncSender<WriteQuery>>) -> Self {
        ShellyLogger { txs }
    }
}

pub fn parse<'a, T: Deserialize<'a> + Clone>(msg: &'a Message) -> Result<T> {
    Ok(serde_json::from_slice::<T>(msg.payload())?)
}

const SWITCH_FIELDS: &[(&str, fn(data: &SwitchData) -> Option<WriteType>, &str)] = &[
    (
        "output",
        |data: &SwitchData| Some(WriteType::Int(data.output as i32)),
        "bool",
    ),
    (
        "power",
        |data: &SwitchData| data.power.map(|value| WriteType::Float(value)),
        "W",
    ),
    (
        "current",
        |data: &SwitchData| data.current.map(|value| WriteType::Float(value)),
        "A",
    ),
    (
        "voltage",
        |data: &SwitchData| data.voltage.map(|value| WriteType::Float(value)),
        "V",
    ),
    (
        "total_energy",
        |data: &SwitchData| Some(WriteType::Float(data.energy.total)),
        "Wh",
    ),
    (
        "temperature",
        |data: &SwitchData| Some(WriteType::Float(data.temperature.t_celsius)),
        "°C",
    ),
];

const COVER_FIELDS: &[(&str, fn(data: &CoverData) -> Option<WriteType>, &str)] = &[
    (
        "position",
        |data: &CoverData| data.position.map(|value| WriteType::Int(value)),
        "%",
    ),
    (
        "power",
        |data: &CoverData| data.power.map(|value| WriteType::Float(value)),
        "W",
    ),
    (
        "current",
        |data: &CoverData| data.current.map(|value| WriteType::Float(value)),
        "A",
    ),
    (
        "voltage",
        |data: &CoverData| data.voltage.map(|value| WriteType::Float(value)),
        "V",
    ),
    (
        "total_energy",
        |data: &CoverData| Some(WriteType::Float(data.energy.total)),
        "Wh",
    ),
    (
        "temperature",
        |data: &CoverData| Some(WriteType::Float(data.temperature.t_celsius)),
        "°C",
    ),
];

static SWITCH_REGEX: LazyLock<Regex, fn() -> Regex> = LazyLock::new(|| Regex::new("/status/switch:.").unwrap());
static COVER_REGEX: LazyLock<Regex, fn() -> Regex> = LazyLock::new(|| Regex::new("/status/cover:.").unwrap());

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
    txs: &Vec<SyncSender<WriteQuery>>,
    fields: &[(&str, fn(&T) -> Option<WriteType>, &str)],
) {
    let location = msg.topic().split("/").nth(1).unwrap();
    let channel = msg.topic().split(":").last().unwrap();
    let result: Option<T> = shelly::parse(&msg).unwrap();
    if let Some(data) = result {
        println!("Shelly {}:{}: {:?}", location, channel, data);

        if let Some(minute_ts) = data.timestamp() {
            let timestamp = Timestamp::Seconds(minute_ts as u128);
            for (measurement, value, unit) in fields {
                let query = WriteQuery::new(timestamp, *measurement);
                if let Some(result) = value(&data) {
                    let query = match result {
                        WriteType::Int(i) => query.add_field("value", i),
                        WriteType::Float(f) => query.add_field("value", f),
                    };

                    let query = query
                        .add_tag("location", location)
                        .add_tag("channel", channel)
                        .add_tag("sensor", "shelly")
                        .add_tag("type", data.type_name())
                        .add_tag("unit", unit);

                    for tx in txs {
                        tx.send(query.clone()).expect("failed to send");
                    }
                }
            }
        } else {
            println!("{} no timestamp {:?}", msg.topic(), msg.payload_str());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb::Query;
    use paho_mqtt::QOS_1;
    use std::sync::mpsc::sync_channel;
    use std::time::Duration;

    #[test]
    fn test_handle_switch_message() -> Result<()> {
        let (tx, rx) = sync_channel(100);
        let txs = vec![tx];

        let mut logger = ShellyLogger::new(txs);

        let message = Message::new("shellies/loo-fan/status/switch:1", "{\"id\":0, \"source\":\"timer\", \"output\":false, \"apower\":0.0, \"voltage\":226.5, \"current\":3.1, \"aenergy\":{\"total\":1094.865,\"by_minute\":[0.000,0.000,0.000],\"minute_ts\":1703415907},\"temperature\":{\"tC\":36.4, \"tF\":97.5}}", QOS_1);
        logger.check_message(&message);

        let result = rx.recv()?.build()?.get();
        assert!(result.starts_with("output,location=loo-fan,channel=1,sensor=shelly,type=switch,unit=bool value=0i "));

        let result = rx.recv()?.build()?.get();
        assert!(result.starts_with("power,location=loo-fan,channel=1,sensor=shelly,type=switch,unit=W value=0 "));

        let result = rx.recv()?.build()?.get();
        assert!(result.starts_with("current,location=loo-fan,channel=1,sensor=shelly,type=switch,unit=A value=3.0999"));

        let result = rx.recv()?.build()?.get();
        assert!(result.starts_with("voltage,location=loo-fan,channel=1,sensor=shelly,type=switch,unit=V value=226.5 "));

        let result = rx.recv()?.build()?.get();
        assert!(result.starts_with("total_energy,location=loo-fan,channel=1,sensor=shelly,type=switch,unit=Wh value=1094.86"));

        let result = rx.recv()?.build()?.get();
        assert!(result.starts_with("temperature,location=loo-fan,channel=1,sensor=shelly,type=switch,unit=°C value=36.40"));

        let result = rx.recv_timeout(Duration::from_micros(100));
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_handle_curtain_message() -> Result<()> {
        let (tx, rx) = sync_channel(100);
        let txs = vec![tx];

        let mut logger = ShellyLogger::new(txs);

        let message = Message::new("shellies/bedroom-curtain/status/cover:0", "{\"id\":0, \"source\":\"limit_switch\", \"state\":\"open\",\"apower\":0.0,\"voltage\":231.7,\"current\":0.500,\"pf\":0.00,\"freq\":50.0,\"aenergy\":{\"total\":3.143,\"by_minute\":[0.000,0.000,97.712],\"minute_ts\":1703414519},\"temperature\":{\"tC\":30.7, \"tF\":87.3},\"pos_control\":true,\"last_direction\":\"open\",\"current_pos\":100}", QOS_1);
        logger.check_message(&message);

        let result = rx.recv()?.build()?.get();
        assert!(result.starts_with("position,location=bedroom-curtain,channel=0,sensor=shelly,type=cover,unit=% value=100i "));

        let result = rx.recv()?.build()?.get();
        assert!(result.starts_with("power,location=bedroom-curtain,channel=0,sensor=shelly,type=cover,unit=W value=0 "));

        let result = rx.recv()?.build()?.get();
        assert!(result.starts_with("current,location=bedroom-curtain,channel=0,sensor=shelly,type=cover,unit=A value=0.5 "));

        let result = rx.recv()?.build()?.get();
        assert!(result.starts_with("voltage,location=bedroom-curtain,channel=0,sensor=shelly,type=cover,unit=V value=231.6999"));

        let result = rx.recv()?.build()?.get();
        assert!(result.starts_with("total_energy,location=bedroom-curtain,channel=0,sensor=shelly,type=cover,unit=Wh value=3.14"));

        let result = rx.recv()?.build()?.get();
        assert!(result.starts_with("temperature,location=bedroom-curtain,channel=0,sensor=shelly,type=cover,unit=°C value=30.7"));

        let result = rx.recv_timeout(Duration::from_micros(100));
        assert!(result.is_err());

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
