use crate::config::SourceType;
use crate::data::{debug, openmqttgateway, CheckMessage};
use chrono::{DateTime, Utc};
use data::{klimalogger, opendtu, shelly};
use futures::{executor::block_on, stream::StreamExt};
use log::{debug, error, info, trace, warn};
use paho_mqtt as mqtt;
use paho_mqtt::QOS_1;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::Path;
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::{env, fs, time::Duration};

mod config;
mod data;
mod source;
mod target;

#[derive(Debug, Clone)]
pub struct SensorReading {
    pub measurement: String,
    pub time: DateTime<Utc>,
    pub location: String,
    pub sensor: String,
    pub value: f32,
}

pub enum WriteType {
    Int(i32),
    Float(f32),
}

fn main() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info")
    }
    // Initialize the logger from the environment
    env_logger::init();

    let config_file_path = determine_config_file_path();

    let config_string = fs::read_to_string(config_file_path).expect("failed to read config file");
    let config: config::Config =
        serde_yml::from_str(&config_string).expect("failed to parse config file");

    debug!("config: {:?}", config);

    let mut handler_map: HashMap<String, Arc<Mutex<dyn CheckMessage>>> = HashMap::new();
    let mut handles: Vec<JoinHandle<()>> = Vec::new();
    let mut topics: Vec<String> = Vec::new();
    let mut qoss: Vec<i32> = Vec::new();

    for source in config.sources {
        let targets = source.targets.unwrap_or_default();
        let (logger, mut source_handles) = match source.source_type {
            SourceType::Shelly => shelly::create_logger(targets),
            SourceType::Sensor => klimalogger::create_logger(targets),
            SourceType::OpenDTU => opendtu::create_logger(targets),
            SourceType::OpenMqttGateway => openmqttgateway::create_logger(targets),
            SourceType::Debug => debug::create_logger(targets),
        };
        handler_map.insert(source.prefix.clone(), logger);
        handles.append(&mut source_handles);

        topics.push(format!("{}/#", source.prefix));
        qoss.push(QOS_1);
    }

    let mut mqtt_client = source::mqtt::create_mqtt_client(config.mqtt_url, config.mqtt_client_id);

    if let Err(err) = block_on(async {
        // Get message stream before connecting.
        let mut strm = mqtt_client.get_stream(None);

        let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
            .keep_alive_interval(Duration::from_secs(30))
            .clean_session(true)
            .automatic_reconnect(Duration::from_secs(1), Duration::from_secs(300))
            .finalize();

        mqtt_client.connect(conn_opts).await?;

        info!("Subscribing to topics: {:?}", &topics);
        mqtt_client.subscribe_many(&topics, &qoss).await?;

        info!("Waiting for messages ...");

        while let Some(msg_opt) = strm.next().await {
            if let Some(msg) = msg_opt {
                let prefix = msg.topic().split("/").next().unwrap();
                trace!("received from {} - {}", msg.topic(), msg.payload_str());

                let handler = handler_map.get(prefix);
                if let Some(handler) = handler {
                    handler.lock().unwrap().check_message(&msg);
                } else {
                    warn!("unhandled prefix {} from topic {}", prefix, msg.topic());
                }
            } else {
                // A "None" means we were disconnected. Try to reconnect...
                warn!(
                    "Lost connection. Attempting reconnect. {:?}",
                    mqtt_client.is_connected()
                );
                while let Err(err) = mqtt_client.reconnect().await {
                    warn!("Error reconnecting: {}", err);
                    // For tokio use: tokio::time::delay_for()
                    async_std::task::sleep(Duration::from_millis(1000)).await;
                }
            }
        }

        for handle in handles {
            handle.join().expect("failed to join influx writer thread");
        }

        // Explicit return type for the async block
        Ok::<(), mqtt::Error>(())
    }) {
        error!("{}", err);
    }
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
