use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::{fs, process, thread, time::Duration};

use chrono::{DateTime, Utc};
use futures::{executor::block_on, stream::StreamExt};
use influxdb::{Client, Timestamp, WriteQuery};
use paho_mqtt as mqtt;
use paho_mqtt::QOS_1;
use postgres::{Config, NoTls};

use crate::config::{SourceType, Target};
use crate::data::klimalogger::SensorLogger;
use crate::data::opendtu::OpenDTULogger;
use crate::data::shelly::ShellyLogger;
use crate::data::CheckMessage;

mod config;
mod data;

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
            } => spawn_influxdb_writer(
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

struct InfluxConfig {
    url: String,
    database: String,
    user: Option<String>,
    password: Option<String>,
}

impl InfluxConfig {
    fn new(url: String, database: String, user: Option<String>, password: Option<String>) -> Self {
        Self {
            url,
            database,
            user,
            password,
        }
    }
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

                spawn_influxdb_writer(InfluxConfig::new(url, database, user, password), mapper)
            }
            Target::Postgresql {
                host,
                port,
                user,
                password,
                database,
            } => {
                let (tx, rx) = sync_channel(100);

                let mut db_config = postgres::Config::new();
                let _ = db_config
                    .host(&host)
                    .port(port)
                    .user(&user)
                    .password(password)
                    .dbname(&database);

                (
                    tx,
                    thread::spawn(move || {
                        println!("starting postgres writer");
                        start_postgres_writer(rx, db_config);
                    }),
                )
            }
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
            } => spawn_influxdb_writer(
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

fn spawn_influxdb_writer<T: Send + 'static>(
    config: InfluxConfig,
    mapper: fn(T) -> WriteQuery,
) -> (SyncSender<T>, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);

    (
        tx,
        thread::spawn(move || {
            println!(
                "starting influx writer {} {}",
                &config.url, &config.database
            );

            influxdb_writer(rx, config, mapper);
        }),
    )
}

fn influxdb_writer<T>(
    rx: Receiver<T>,
    influx_config: InfluxConfig,
    query_mapper: fn(T) -> WriteQuery,
) {
    let influx_url = influx_config.url.clone();
    let influx_database = influx_config.database.clone();

    let mut influx_client = Client::new(influx_config.url, influx_config.database);
    influx_client =
        if let (Some(user), Some(password)) = (influx_config.user, influx_config.password) {
            influx_client.with_auth(user, password)
        } else {
            influx_client
        };

    block_on(async move {
        println!(
            "starting influx writer async {} {}",
            &influx_url, &influx_database
        );

        loop {
            let result = rx.recv();
            let data = match result {
                Ok(query) => query,
                Err(error) => {
                    println!("error receiving query: {:?}", error);
                    break;
                }
            };
            let query = query_mapper(data);
            let result = influx_client.query(query).await;
            match result {
                Ok(_) => {}
                Err(error) => {
                    panic!(
                        "#### Error writing to influx: {} {}: {:?}",
                        &influx_url, &influx_database, error
                    );
                }
            }
        }
        println!("exiting influx writer async");
    });

    println!("exiting influx writer");
}

fn start_postgres_writer(rx: Receiver<SensorReading>, config: Config) {
    let mut client = config
        .connect(NoTls)
        .expect("failed to connect to postgres");

    block_on(async move {
        println!("starting postgres writer async");

        loop {
            let result = rx.recv();
            let query = match result {
                Ok(query) => query,
                Err(error) => {
                    println!("error receiving query: {:?}", error);
                    break;
                }
            };

            let statement = format!(
                "insert into \"{}\" (time, location, sensor, value) values ($1, $2, $3, $4);",
                query.measurement
            );
            let x = client.execute(
                &statement,
                &[&query.time, &query.location, &query.sensor, &query.value],
            );

            match x {
                Ok(_) => {}
                Err(error) => {
                    eprintln!(
                        "#### Error writing to postgres: {} {:?}",
                        query.measurement, error
                    );
                }
            }
        }
        println!("exiting influx writer async");
    });

    println!("exiting influx writer");
}
