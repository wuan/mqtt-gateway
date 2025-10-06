use anyhow::Result;
#[cfg(test)]
use mockall::automock;
use paho_mqtt as mqtt;
use paho_mqtt::{Client, Message, ServerResponse};
use std::time::Duration;

pub(crate) mod receiver;
pub(crate) mod sources;

#[cfg_attr(test, automock)]
pub(crate) trait MqttClient {
    fn connect(&self) -> anyhow::Result<ServerResponse>;
    fn subscribe_many(
        &self,
        topics: &Vec<String>,
        qoss: &Vec<i32>,
    ) -> anyhow::Result<ServerResponse>;
    fn create(&mut self) -> anyhow::Result<Box<dyn Stream>>;
    fn reconnect(&self) -> anyhow::Result<ServerResponse>;
}

pub(crate) struct MqttClientDefault {
    mqtt_client: Client,
}

impl MqttClientDefault {
    pub(crate) fn new(mqtt_client: Client) -> Self {
        Self { mqtt_client }
    }
}

impl MqttClient for MqttClientDefault {
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

#[cfg_attr(test, automock)]
pub(crate) trait Stream {
    fn next(&mut self) -> Result<Option<Message>>;
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
    fn next(&mut self) -> Result<Option<Message>> {
        self.receiver.recv().map_err(anyhow::Error::from)
    }
}
