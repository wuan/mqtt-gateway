use crate::data::shelly::{Timestamped, Typenamed};
use serde::{Deserialize, Serialize};
use std::fmt;

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
        "switch"
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
        "cover"
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_switch_data_debug() {
        let energy = EnergyData {
            total: 10.0,
            minute_ts: Some(1627848123),
        };
        let temperature = TemperatureData { t_celsius: 25.0 };
        let switch_data = SwitchData {
            output: true,
            power: Some(100.0),
            voltage: Some(220.0),
            current: Some(0.45),
            energy,
            temperature,
        };

        assert_eq!(format!("{:?}", switch_data.energy), "10 Wh");
        assert_eq!(format!("{:?}", switch_data.temperature), "25 °C");
    }

    #[test]
    fn test_cover_data_debug() {
        let energy = EnergyData {
            total: 20.0,
            minute_ts: Some(1627848124),
        };
        let temperature = TemperatureData { t_celsius: 26.0 };
        let cover_data = CoverData {
            position: Some(50),
            power: Some(110.0),
            voltage: Some(230.0),
            current: Some(0.50),
            energy,
            temperature,
        };

        assert_eq!(format!("{:?}", cover_data.energy), "20 Wh");
        assert_eq!(format!("{:?}", cover_data.temperature), "26 °C");
    }

    #[test]
    fn test_switch_data_timestamp() {
        let energy = EnergyData {
            total: 10.0,
            minute_ts: Some(1627848123),
        };
        let switch_data = SwitchData {
            output: true,
            power: Some(100.0),
            voltage: Some(220.0),
            current: Some(0.45),
            energy,
            temperature: TemperatureData { t_celsius: 25.0 },
        };

        assert_eq!(switch_data.timestamp(), Some(1627848123));
    }

    #[test]
    fn test_cover_data_timestamp() {
        let energy = EnergyData {
            total: 20.0,
            minute_ts: Some(1627848124),
        };
        let cover_data = CoverData {
            position: Some(50),
            power: Some(110.0),
            voltage: Some(230.0),
            current: Some(0.50),
            energy,
            temperature: TemperatureData { t_celsius: 26.0 },
        };

        assert_eq!(cover_data.timestamp(), Some(1627848124));
    }

    #[test]
    fn test_switch_data_typename() {
        let switch_data = SwitchData {
            output: true,
            power: Some(100.0),
            voltage: Some(220.0),
            current: Some(0.45),
            energy: EnergyData {
                total: 10.0,
                minute_ts: Some(1627848123),
            },
            temperature: TemperatureData { t_celsius: 25.0 },
        };

        assert_eq!(switch_data.type_name(), "switch");
    }

    #[test]
    fn test_cover_data_typename() {
        let cover_data = CoverData {
            position: Some(50),
            power: Some(110.0),
            voltage: Some(230.0),
            current: Some(0.50),
            energy: EnergyData {
                total: 20.0,
                minute_ts: Some(1627848124),
            },
            temperature: TemperatureData { t_celsius: 26.0 },
        };

        assert_eq!(cover_data.type_name(), "cover");
    }
}
