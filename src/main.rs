use crate::domain::receiver::Receiver;
use crate::domain::sources::Sources;
use crate::domain::MqttClientDefault;
use anyhow::Context;
use chrono::{DateTime, Utc};
use log::debug;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use serial_test::serial;
use std::path::Path;
use std::{env, fs};

mod config;
mod data;
mod domain;
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

    let config_file_path = determine_config_file_path()
        .context("Failed to find configuration file in ./ or ./config/")?;

    let config_string = fs::read_to_string(&config_file_path)
        .with_context(|| format!("Failed to read config file: {}", config_file_path))?;
    let config: config::Config = serde_yaml_ng::from_str(&config_string)
        .with_context(|| format!("Failed to parse config file: {}", config_file_path))?;

    debug!("config: {:?}", config);

    let mqtt_client = source::mqtt::create_mqtt_client(&config.mqtt_url, &config.mqtt_client_id)?;

    let receiver = Receiver::new(
        Box::new(MqttClientDefault::new(mqtt_client)),
        Sources::new(config.sources),
    );
    receiver.listen()
}

fn determine_config_file_path() -> anyhow::Result<String> {
    let config_file_name = "config.yml";
    let config_locations = ["./", "./config"];

    for config_location in config_locations {
        let path = Path::new(config_location);
        let tmp_config_file_path = path.join(Path::new(config_file_name));
        if tmp_config_file_path.exists() && tmp_config_file_path.is_file() {
            return Ok(String::from(
                tmp_config_file_path.to_str().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Invalid config file path: {}",
                        tmp_config_file_path.display()
                    )
                })?,
            ));
        }
    }

    Err(anyhow::anyhow!("No configuration file found in ./ or ./config/"))
}

#[cfg(test)]
#[serial]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_determine_config_file_path_no_file() {
        // Change to a directory that doesn't have config.yml
        let temp_dir = tempdir().unwrap();
        let current_dir = std::env::current_dir().unwrap();
        
        // Set current dir to temp dir - this should not have config.yml
        std::env::set_current_dir(temp_dir.path()).expect("failed to set current dir");

        let result = determine_config_file_path();
        assert!(result.is_err());

        // Restore current dir before dropping temp_dir
        std::env::set_current_dir(&current_dir).expect("failed to restore current dir");
        drop(temp_dir);
    }

    #[test]
    fn test_determine_config_file_path_root() -> std::io::Result<()> {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let current_dir = std::env::current_dir()?;
        std::env::set_current_dir(temp_dir.path())?;

        let config_path = temp_dir.path().join("config.yml");
        {
            let mut file = File::create(&config_path).unwrap();
            file.write_all(b"test config").unwrap();
        }

        let result = determine_config_file_path().expect("failed to determine config path");
        assert!(result.ends_with("config.yml"));

        std::env::set_current_dir(&current_dir)?;
        temp_dir.close()
    }

    #[test]
    fn test_determine_config_file_path_config_dir() -> std::io::Result<()> {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let current_dir = std::env::current_dir()?;
        env::set_current_dir(temp_dir.path())?;

        fs::create_dir("config")?;
        let config_path = temp_dir.path().join("config").join("config.yml");
        {
            let mut file = File::create(&config_path)?;
            file.write_all(b"test config")?;
        }

        let result = determine_config_file_path().expect("failed to determine config path");
        print!("result: {}", result);
        assert!(result.ends_with("config/config.yml"));

        std::env::set_current_dir(&current_dir)?;
        temp_dir.close()
    }
}
