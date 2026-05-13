use anyhow::Result;
#[cfg(test)]
use mockall::automock;
use paho_mqtt as mqtt;
use paho_mqtt::{Client, Message, ServerResponse, SyncReceiver};
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
    receiver: SyncReceiver<Option<Message>>,
}

impl StreamDefault {
    fn new(stream: SyncReceiver<Option<Message>>) -> Self {
        Self { receiver: stream }
    }
}

impl Stream for StreamDefault {
    fn next(&mut self) -> Result<Option<Message>> {
        self.receiver.recv().map_err(anyhow::Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paho_mqtt::Message;

    #[test]
    fn test_mock_mqtt_client_connect() {
        let mut mock = MockMqttClient::new();
        mock.expect_connect()
            .times(1)
            .returning(|| Ok(ServerResponse::new()));

        let result = mock.connect();
        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_mqtt_client_subscribe_many() {
        let mut mock = MockMqttClient::new();
        let topics = vec!["topic1".to_string(), "topic2".to_string()];
        let qoss = vec![1, 1];
        let topics_clone = topics.clone();
        let qoss_clone = qoss.clone();

        mock.expect_subscribe_many()
            .withf(move |t, q| t == &topics_clone && q == &qoss_clone)
            .times(1)
            .returning(|_, _| Ok(ServerResponse::new()));

        let result = mock.subscribe_many(&topics, &qoss);
        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_mqtt_client_reconnect() {
        let mut mock = MockMqttClient::new();
        mock.expect_reconnect()
            .times(1)
            .returning(|| Ok(ServerResponse::new()));

        let result = mock.reconnect();
        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_stream_next() {
        let mut mock = MockStream::new();
        let msg = Message::new("topic", "payload", 1);
        let msg_clone = msg.clone();

        mock.expect_next()
            .times(1)
            .returning(move || Ok(Some(msg_clone.clone())));

        let result = mock.next();
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_mock_stream_next_none() {
        let mut mock = MockStream::new();

        mock.expect_next()
            .times(1)
            .returning(|| Ok(None));

        let result = mock.next();
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_mock_stream_next_error() {
        let mut mock = MockStream::new();

        mock.expect_next()
            .times(1)
            .returning(|| Err(anyhow::anyhow!("test error")));

        let result = mock.next();
        assert!(result.is_err());
    }
}
