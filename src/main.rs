use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::{fs, process, time::Duration};

use crate::config::{SourceType, Target};
use crate::data::klimalogger::SensorLogger;
use crate::data::opendtu::OpenDTULogger;
use crate::data::shelly::ShellyLogger;
use crate::data::CheckMessage;
use crate::target::influx::InfluxConfig;
use crate::target::postgres::PostgresConfig;
use chrono::{DateTime, Utc};
use futures::{executor::block_on, stream::StreamExt};
use influxdb::{Timestamp, WriteQuery};
use paho_mqtt as mqtt;
use paho_mqtt::QOS_1;
use target::influx;

mod config;
mod data;
mod target;

#[derive(Debug, Clone)]
struct SensorReading {
    measurement: String,
    time: DateTime<Utc>,
    location: String,
    sensor: String,
    value: f32,
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
        serde_yaml::from_str(&config_string).expect("failed to parse config file");

    println!("config: {:?}", config);

    let mut handler_map: HashMap<String, Arc<Mutex<dyn CheckMessage>>> = HashMap::new();
    let mut handles: Vec<JoinHandle<()>> = Vec::new();
    let mut topics: Vec<String> = Vec::new();
    let mut qoss: Vec<i32> = Vec::new();

    for source in config.sources {
        let (logger, mut source_handles) = match source.source_type {
            SourceType::Shelly => create_shelly_logger(source.targets),
            SourceType::Sensor => create_sensor_logger(source.targets),
            SourceType::OpenDTU => create_opendtu_logger(source.targets),
        };
        handler_map.insert(source.prefix.clone(), logger);
        handles.append(&mut source_handles);

        topics.push(format!("{}/#", source.prefix));
        qoss.push(QOS_1);
    }

    let host = config.mqtt_url;

    println!("Connecting to the MQTT server at '{}'...", host);

    let create_opts = mqtt::CreateOptionsBuilder::new_v3()
        .server_uri(host)
        .client_id("sensors_gateway")
        .finalize();

    let mut mqtt_client = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
        println!("Error creating the client: {:?}", e);
        process::exit(1);
    });

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
        mqtt_client.subscribe_many(&*topics, &*qoss).await?;

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

fn create_shelly_logger(
    targets: Vec<Target>,
) -> (Arc<Mutex<dyn CheckMessage>>, Vec<JoinHandle<()>>) {
    let mut txs: Vec<SyncSender<WriteQuery>> = Vec::new();
    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    for target in targets {
        let (tx, handle) = match target {
            Target::InfluxDB {
                url,
                database,
                user,
                password,
            } => influx::spawn_influxdb_writer(
                InfluxConfig::new(url, database, user, password),
                std::convert::identity,
            ),
            Target::Postgresql { .. } => {
                panic!("Postgresql not supported for shelly");
            }
        };
        txs.push(tx);
        handles.push(handle);
    }

    (Arc::new(Mutex::new(ShellyLogger::new(txs))), handles)
}

fn create_sensor_logger(
    targets: Vec<Target>,
) -> (Arc<Mutex<dyn CheckMessage>>, Vec<JoinHandle<()>>) {
    let mut txs: Vec<SyncSender<SensorReading>> = Vec::new();
    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    for target in targets {
        let (tx, handle) = match target {
            Target::InfluxDB {
                url,
                database,
                user,
                password,
            } => {
                fn mapper(result: SensorReading) -> WriteQuery {
                    let timestamp = Timestamp::Seconds(result.time.timestamp() as u128);
                    WriteQuery::new(timestamp, result.measurement.to_string())
                        .add_tag("location", result.location.to_string())
                        .add_tag("sensor", result.sensor.to_string())
                        .add_field("value", result.value)
                }

                influx::spawn_influxdb_writer(
                    InfluxConfig::new(url, database, user, password),
                    mapper,
                )
            }
            Target::Postgresql {
                host,
                port,
                user,
                password,
                database,
            } => target::postgres::spawn_postgres_writer(PostgresConfig::new(
                host, port, user, password, database,
            )),
        };
        txs.push(tx);
        handles.push(handle);
    }

    (Arc::new(Mutex::new(SensorLogger::new(txs))), handles)
}

fn create_opendtu_logger(
    targets: Vec<Target>,
) -> (Arc<Mutex<dyn CheckMessage>>, Vec<JoinHandle<()>>) {
    let mut txs: Vec<SyncSender<WriteQuery>> = Vec::new();
    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    for target in targets {
        let (tx, handle) = match target {
            Target::InfluxDB {
                url,
                database,
                user,
                password,
            } => influx::spawn_influxdb_writer(
                InfluxConfig::new(url, database, user, password),
                std::convert::identity,
            ),
            Target::Postgresql { .. } => {
                panic!("Postgresql not supported for opendtu");
            }
        };
        txs.push(tx);
        handles.push(handle);
    }

    let logger = OpenDTULogger::new(txs);

    (Arc::new(Mutex::new(logger)), handles)
}
