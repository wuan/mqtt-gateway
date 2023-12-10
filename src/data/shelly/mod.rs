use paho_mqtt::Message;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Data {
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
pub struct EnergyData {
    pub(crate) total: f32,
    pub(crate) minute_ts: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TemperatureData {
    #[serde(rename = "tC")]
    pub(crate) t_C: f32,
}

pub fn parse(msg: &Message) -> Result<Option<Data>, &'static str> {
    let data = serde_json::from_slice::<Data>(msg.payload()).map_err(|error| {
        eprintln!("{:?}", error);
        "could not deserialize JSON"
    })?;
    Ok(Some(data.clone()))
}