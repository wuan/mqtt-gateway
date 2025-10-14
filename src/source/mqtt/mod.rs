use log::{error, info};
use paho_mqtt as mqtt;
use std::process;
use std::time::Duration;
use paho_mqtt::{Client, Message, ServerResponse};
use crate::core::{SourceClient, Stream};

pub fn create_mqtt_client(mqtt_url: String, mqtt_client_id: String) -> mqtt::Client {
    info!("Connecting to the MQTT server at '{}'...", mqtt_url);

    let create_opts = mqtt::CreateOptionsBuilder::new_v3()
        .server_uri(mqtt_url)
        .client_id(mqtt_client_id)
        .finalize();

    mqtt::Client::new(create_opts).unwrap_or_else(|e| {
        error!("Error creating the client: {:?}", e);
        process::exit(1);
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_mqtt_client_success() {
        let client = create_mqtt_client("bad_url".to_string(), "test_client".to_string());

        assert_eq!(client.client_id(), "test_client");
    }

    #[test]
    fn test_create_mqtt_client_connect_failure() {
        let client = create_mqtt_client("bad_url".to_string(), "test_client".to_string());

        let result = client.connect(None);

        assert!(result.is_err());
    }
}

pub(crate) struct MqttClientDefault {
    mqtt_client: Client,
}

impl MqttClientDefault {
    pub(crate) fn new(mqtt_client: Client) -> Self {
        Self { mqtt_client }
    }
}

impl SourceClient for MqttClientDefault {
    fn connect(&self) -> anyhow::Result<ServerResponse> {
        let conn_opts = mqtt::ConnectOptionsBuilder::new_v3()
            .keep_alive_interval(Duration::from_secs(30))
            .clean_session(false)
            .finalize();

        self.mqtt_client
            .connect(conn_opts)
            .map_err(anyhow::Error::from)
    }

    fn subscribe_many(
        &self,
        topics: &Vec<String>,
        qoss: &Vec<i32>,
    ) -> anyhow::Result<ServerResponse> {
        self.mqtt_client
            .subscribe_many(topics, qoss)
            .map_err(anyhow::Error::from)
    }

    fn create(&mut self) -> anyhow::Result<Box<dyn Stream>> {
        let receiver = self.mqtt_client.start_consuming();

        self.connect()?;

        Ok(Box::new(StreamDefault::new(receiver)))
    }

    fn reconnect(&self) -> anyhow::Result<ServerResponse> {
        self.mqtt_client.reconnect().map_err(anyhow::Error::from)
    }
}

pub(crate) struct StreamDefault {
    receiver: mqtt::Receiver<Option<Message>>,
}

impl StreamDefault {
    fn new(stream: mqtt::Receiver<Option<Message>>) -> Self {
        Self { receiver: stream }
    }
}

impl Stream for StreamDefault {
    fn next(&mut self) -> anyhow::Result<Option<Message>> {
        self.receiver.recv().map_err(anyhow::Error::from)
    }
}