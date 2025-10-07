use crate::domain::receiver::Receiver;
use crate::domain::sources::Sources;
use crate::domain::MqttClientDefault;
use chrono::{DateTime, Utc};
use log::{debug, error};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::path::Path;
use std::process::exit;
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
        error!("ERROR: no configuration file found");
        exit(10);
    }

    config_file_path.unwrap()
}
