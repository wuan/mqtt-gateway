use crate::core::receiver::Receiver;
use crate::core::sources::Sources;
use source::mqtt::MqttClientDefault;
use chrono::{DateTime, Utc};
use log::debug;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use serial_test::serial;
use std::fmt::Debug;
use std::path::Path;
use std::{env, fs};

mod config;
mod data;
mod core;
mod source;
mod target;

#[derive(Debug, Clone)]
pub struct SensorReading {
    pub measurement: String,
    pub time: DateTime<Utc>,
    pub location: String,
    pub sensor: String,
    pub value: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Number {
    Int(i64),
    Float(f64),
}

fn main() -> anyhow::Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info")
    }
    // Initialize the logger from the environment
    env_logger::init();

    let config_file_path = determine_config_file_path();

    let config_string = fs::read_to_string(config_file_path).expect("failed to read config file");
    let config: config::Config =
        serde_yaml_ng::from_str(&config_string).expect("failed to parse config file");

    debug!("config: {:?}", config);

    let mqtt_client = source::mqtt::create_mqtt_client(config.mqtt_url, config.mqtt_client_id);

    let receiver = Receiver::new(
        Box::new(MqttClientDefault::new(mqtt_client)),
        Sources::new(config.sources),
    );
    receiver.listen()
}

fn determine_config_file_path() -> String {
    let config_file_name = "config.yml";
    let config_locations = ["./", "./config"];

    let mut config_file_path: Option<String> = None;

    for config_location in config_locations {
        let path = Path::new(config_location);
        let tmp_config_file_path = path.join(Path::new(config_file_name));
        if tmp_config_file_path.exists() && tmp_config_file_path.is_file() {
            config_file_path = Some(String::from(tmp_config_file_path.to_str().unwrap()));
            break;
        }
    }

    if config_file_path.is_none() {
        panic!("ERROR: no configuration file found");
    }

    config_file_path.unwrap()
}

#[cfg(test)]
#[serial]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    #[should_panic]
    fn test_determine_config_file_path_no_file() {
        let temp_dir = tempdir().unwrap();
        let current_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        determine_config_file_path();

        std::env::set_current_dir(current_dir).unwrap();
        let _ = temp_dir.close();
    }

    #[test]
    fn test_determine_config_file_path_root() -> std::io::Result<()> {
        let temp_dir = tempdir().expect("failed to create temp dir");
        std::env::set_current_dir(temp_dir.path()).unwrap();

        let config_path = temp_dir.path().join("config.yml");
        {
            let mut file = File::create(&config_path).unwrap();
            file.write_all(b"test config").unwrap();
        }

        let result = determine_config_file_path();
        assert!(result.ends_with("config.yml"));

        temp_dir.close()
    }

    #[test]
    fn test_determine_config_file_path_config_dir() -> std::io::Result<()> {
        let temp_dir = tempdir().expect("failed to create temp dir");
        env::set_current_dir(temp_dir.path())?;

        fs::create_dir("config")?;
        let config_path = temp_dir.path().join("config").join("config.yml");
        {
            let mut file = File::create(&config_path)?;
            file.write_all(b"test config")?;
        }

        let result = determine_config_file_path();
        print!("result: {}", result);
        assert!(result.ends_with("config/config.yml"));

        temp_dir.close()
    }
}
