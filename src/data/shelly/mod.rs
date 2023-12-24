use std::fmt;
use paho_mqtt::Message;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SwitchData {
    pub(crate) output: bool,
    #[serde(rename = "apower")]
    pub(crate) power: f32,
    pub(crate) voltage: f32,
    pub(crate) current: f32,
    #[serde(rename = "aenergy")]
    pub(crate) energy: EnergyData,
    pub(crate) temperature: TemperatureData,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CoverData {
    #[serde(rename = "current_pos")]
    pub(crate) position: i32,
    #[serde(rename = "apower")]
    pub(crate) power: f32,
    pub(crate) voltage: f32,
    pub(crate) current: f32,
    #[serde(rename = "aenergy")]
    pub(crate) energy: EnergyData,
    pub(crate) temperature: TemperatureData,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct EnergyData {
    pub(crate) total: f32,
    pub(crate) minute_ts: i64,
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

pub fn parse<'a, T: Deserialize<'a> + Clone>(msg: &'a Message) -> Result<Option<T>, &'static str> {
    let data = serde_json::from_slice::<T>(msg.payload()).map_err(|error| {
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
    fn test_parse_switch_status() -> Result<(), &'static str> {
        let message = Message::new("shellies/loo-fan/status/switch:0", "{\"id\":0, \"source\":\"timer\", \"output\":false, \"apower\":0.0, \"voltage\":226.5, \"current\":3.1, \"aenergy\":{\"total\":1094.865,\"by_minute\":[0.000,0.000,0.000],\"minute_ts\":1703415907},\"temperature\":{\"tC\":36.4, \"tF\":97.5}}", QOS_1);
        let result: SwitchData = parse(&message)?.unwrap();

        assert_eq!(result.output, false);
        assert_eq!(result.power, 0.0);
        assert_eq!(result.voltage, 226.5);
        assert_eq!(result.current, 3.1);
        assert_eq!(result.energy.total, 1094.865);
        assert_eq!(result.temperature.t_celsius, 36.4);

        Ok(())
    }

    #[test]
    fn test_parse_cover_status() -> Result<(), &'static str> {
        let message = Message::new("shellies/bedroom-curtain/status/cover:0", "{\"id\":0, \"source\":\"limit_switch\", \"state\":\"open\",\"apower\":0.0,\"voltage\":231.7,\"current\":0.500,\"pf\":0.00,\"freq\":50.0,\"aenergy\":{\"total\":3.143,\"by_minute\":[0.000,0.000,97.712],\"minute_ts\":1703414519},\"temperature\":{\"tC\":30.7, \"tF\":87.3},\"pos_control\":true,\"last_direction\":\"open\",\"current_pos\":100}", QOS_1);
        let result: CoverData = parse(&message)?.unwrap();

        assert_eq!(result.position, 100);
        assert_eq!(result.power, 0.0);
        assert_eq!(result.voltage, 231.7);
        assert_eq!(result.current, 0.5);
        assert_eq!(result.energy.total, 3.143);
        assert_eq!(result.temperature.t_celsius, 30.7);

        Ok(())
    }
}