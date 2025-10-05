use paho_mqtt::{AsyncClient, Message, ServerResponse};
use std::time::Duration;
use async_trait::async_trait;
#[cfg(test)]
use mockall::automock;
use paho_mqtt as mqtt;
use smol::stream::StreamExt;

pub(crate) mod receiver;
pub(crate) mod sources;


#[cfg_attr(test, automock)]
#[async_trait]
pub(crate) trait MqttClient {
    async fn connect(&self) -> anyhow::Result<ServerResponse>;
    async fn subscribe_many(&self, topics: &Vec<String>, qoss: &Vec<i32>) -> anyhow::Result<ServerResponse>;
    async fn create(&mut self) -> anyhow::Result<Stream>;
    fn is_connected(&self) -> bool;
    async fn reconnect(&self) -> anyhow::Result<ServerResponse>;
}

pub(crate) struct MqttClientDefault {
    mqtt_client: AsyncClient,
}

impl MqttClientDefault {
    pub(crate) fn new(mqtt_client: AsyncClient) -> Self {
        Self { mqtt_client }
    }
}

#[async_trait]
impl MqttClient for MqttClientDefault {
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

    async fn subscribe_many(
        &self,
        topics: &Vec<String>,
        qoss: &Vec<i32>,
    ) -> anyhow::Result<ServerResponse> {
        self.mqtt_client
            .subscribe_many(topics, qoss)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn create(&mut self) -> anyhow::Result<Stream> {
        let strm = self.mqtt_client.get_stream(None);

        self.connect().await?;

        Ok(Stream::new(strm))
    }

    fn is_connected(&self) -> bool {
        self.mqtt_client.is_connected()
    }

    async fn reconnect(&self) -> anyhow::Result<ServerResponse> {
        self.mqtt_client
            .reconnect()
            .await
            .map_err(anyhow::Error::from)
    }
}

pub(crate) struct Stream {
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
