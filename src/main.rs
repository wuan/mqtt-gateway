use crate::config::{Source, SourceType};
use crate::data::{debug, openmqttgateway, CheckMessage};
use chrono::{DateTime, Utc};
use data::{klimalogger, opendtu, shelly};
use futures::stream::StreamExt;
use log::{debug, error, info, trace, warn};
use paho_mqtt as mqtt;
use paho_mqtt::{AsyncClient, Message, ServerResponse, QOS_1};
use smol::Timer;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::Path;
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::{env, fs, time::Duration};
use tokio::task::JoinHandle;

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

struct Sources {
    handler_map: HashMap<String, Arc<Mutex<dyn CheckMessage>>>,
    handles: Vec<JoinHandle<()>>,
    topics: Vec<String>,
    qoss: Vec<i32>,
}

impl Sources {
    fn new(sources: Vec<Source>) -> Self {
        let mut handler_map: HashMap<String, Arc<Mutex<dyn CheckMessage>>> = HashMap::new();
        let mut handles: Vec<JoinHandle<()>> = Vec::new();
        let mut topics: Vec<String> = Vec::new();
        let mut qoss: Vec<i32> = Vec::new();

        for source in sources {
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

        Self {
            handler_map,
            handles,
            topics,
            qoss,
        }
    }

    pub(crate) async fn subscribe(
        &self,
        mqtt_client: &MqttClient,
    ) -> anyhow::Result<ServerResponse> {
        info!("Subscribing to topics: {:?}", &self.topics);
        mqtt_client
            .subscribe_many(&self.topics, &self.qoss)
            .await
            .map_err(anyhow::Error::from)
    }

    pub(crate) async fn handle(&self, msg: Message) {
        let prefix = msg.topic().split("/").next().unwrap();
        trace!("received from {} - {}", msg.topic(), msg.payload_str());

        let handler = self.handler_map.get(prefix);
        if let Some(handler) = handler {
            handler.lock().unwrap().check_message(&msg);
        } else {
            warn!("unhandled prefix {} from topic {}", prefix, msg.topic());
        }
    }

    pub(crate) async fn shutdown(self) {
        for handle in self.handles {
            handle.await.expect("failed to join influx writer thread");
        }
    }
}

struct MqttClient {
    mqtt_client: AsyncClient,
}

impl MqttClient {
    pub(crate) async fn reconnect(&self) -> anyhow::Result<ServerResponse> {
        self.mqtt_client.reconnect().await.map_err(anyhow::Error::from)
    }
}

impl MqttClient {
    pub(crate) fn is_connected(&self) -> bool {
        self.mqtt_client.is_connected()
    }
}

impl MqttClient {
    pub(crate) async fn subscribe_many(
        &self,
        topics: &Vec<String>,
        qoss: &Vec<i32>,
    ) -> anyhow::Result<ServerResponse> {
        self.mqtt_client
            .subscribe_many(topics, qoss)
            .await
            .map_err(anyhow::Error::from)
    }
}

impl MqttClient {
    pub(crate) async fn create(&mut self) -> anyhow::Result<Stream> {
        let strm = self.mqtt_client.get_stream(None);

        self.connect().await?;

        Ok(Stream::new(strm))
    }
}

struct Stream {
    stream: async_channel::Receiver<Option<Message>>,
}

impl Stream {
    fn new(stream: async_channel::Receiver<Option<Message>>) -> Self {
        Self { stream }
    }

    async fn next(&mut self) -> Option<Option<Message>> {
        self.stream.next().await
    }
}


impl MqttClient {
    fn new(mqtt_client: AsyncClient) -> Self {
        Self { mqtt_client }
    }

    async fn connect(&self) -> anyhow::Result<ServerResponse> {
        let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
            .keep_alive_interval(Duration::from_secs(30))
            .clean_session(true)
            .automatic_reconnect(Duration::from_secs(1), Duration::from_secs(300))
            .finalize();

        self.mqtt_client
            .connect(conn_opts)
            .await
            .map_err(anyhow::Error::from)
    }
}

struct Receiver {
    mqtt_client: MqttClient,
    sources: Sources,
}

impl Receiver {
    fn new(mqtt_client: MqttClient, sources: Sources) -> Self {
        Self {
            mqtt_client,
            sources,
        }
    }

    pub(crate) async fn listen(mut self) -> anyhow::Result<()> {
        let mut stream = self.mqtt_client.create().await?;
        self.sources.subscribe(&self.mqtt_client).await?;

        info!("Waiting for messages ...");

        while let Some(msg_opt) = stream.next().await {
            if let Some(msg) = msg_opt {
                self.sources.handle(msg).await;
            } else {
                self.handle_error().await;
            }
        }

        self.sources.shutdown().await;
        Ok(())
    }

    async fn handle_error(&mut self) {
        warn!(
            "Lost connection. Attempting reconnect. {:?}",
            self.mqtt_client.is_connected()
        );
        while let Err(err) = self.mqtt_client.reconnect().await {
            warn!("Error reconnecting: {}", err);
            Timer::after(Duration::from_secs(1)).await;
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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

    let mqtt_client = source::mqtt::create_mqtt_client(config.mqtt_url, config.mqtt_client_id);

    let receiver = Receiver::new(MqttClient::new(mqtt_client), Sources::new(config.sources));
    receiver.listen().await
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
