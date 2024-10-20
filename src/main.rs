use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::{fs, time::Duration};

use crate::config::SourceType;
use crate::data::CheckMessage;
use chrono::{DateTime, Utc};
use data::{klimalogger, opendtu, shelly};
use futures::{executor::block_on, stream::StreamExt};
use paho_mqtt as mqtt;
use paho_mqtt::QOS_1;
mod config;
mod data;
mod target;
mod source;

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
    // Initialize the logger from the environment
    env_logger::init();

    let config_string = fs::read_to_string("config.yml").expect("failed to read config file");
    let config: config::Config =
        serde_yml::from_str(&config_string).expect("failed to parse config file");

    println!("config: {:?}", config);

    let mut handler_map: HashMap<String, Arc<Mutex<dyn CheckMessage>>> = HashMap::new();
    let mut handles: Vec<JoinHandle<()>> = Vec::new();
    let mut topics: Vec<String> = Vec::new();
    let mut qoss: Vec<i32> = Vec::new();

    for source in config.sources {
        let (logger, mut source_handles) = match source.source_type {
            SourceType::Shelly => shelly::create_logger(source.targets),
            SourceType::Sensor => klimalogger::create_logger(source.targets),
            SourceType::OpenDTU => opendtu::create_logger(source.targets),
        };
        handler_map.insert(source.prefix.clone(), logger);
        handles.append(&mut source_handles);

        topics.push(format!("{}/#", source.prefix));
        qoss.push(QOS_1);
    }

    let mut mqtt_client = source::mqtt::create_mqtt_client(config.mqtt_url, config.mqtt_client_id);

    if let Err(err) = block_on(async {
        // Get message stream before connecting.
        let mut strm = mqtt_client.get_stream(25);

        let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
            .keep_alive_interval(Duration::from_secs(30))
            .clean_session(false)
            .automatic_reconnect(Duration::from_secs(1), Duration::from_secs(300))
            .finalize();

        mqtt_client.connect(conn_opts).await?;

        println!("Subscribing to topics: {:?}", &topics);
        mqtt_client.subscribe_many(&topics, &qoss).await?;

        println!("Waiting for messages...");

        while let Some(msg_opt) = strm.next().await {
            if let Some(msg) = msg_opt {
                let prefix = msg.topic().split("/").next().unwrap();

                let handler = handler_map.get(prefix);
                if let Some(handler) = handler {
                    handler.lock().unwrap().check_message(&msg);
                } else {
                    println!("unhandled prefix {} from topic {}", prefix, msg.topic());
                }
            } else {
                // A "None" means we were disconnected. Try to reconnect...
                println!(
                    "Lost connection. Attempting reconnect. {:?}",
                    mqtt_client.is_connected()
                );
                while let Err(err) = mqtt_client.reconnect().await {
                    println!("Error reconnecting: {}", err);
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
        eprintln!("{}", err);
    }
}

