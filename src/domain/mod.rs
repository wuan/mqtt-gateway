use anyhow::Result;
#[cfg(test)]
use mockall::automock;
use paho_mqtt as mqtt;
use paho_mqtt::sync_channel::RecvTimeoutError;
use paho_mqtt::{Client, Message, ServerResponse, SslOptionsBuilder, SyncReceiver};
use std::time::Duration;

pub(crate) mod receiver;
pub(crate) mod sources;

#[cfg_attr(test, automock)]
pub(crate) trait MqttClient {
    fn connect(&self) -> anyhow::Result<ServerResponse>;
    fn subscribe_many(&self, topics: &[String], qoss: &[i32]) -> anyhow::Result<ServerResponse>;
    fn create(&mut self) -> anyhow::Result<Box<dyn Stream>>;
    fn reconnect(&self) -> anyhow::Result<ServerResponse>;
}

pub(crate) struct MqttClientDefault {
    mqtt_client: Client,
    username: Option<String>,
    password: Option<String>,
    tls: bool,
}

impl MqttClientDefault {
    pub(crate) fn new(
        mqtt_client: Client,
        username: Option<String>,
        password: Option<String>,
        tls: bool,
    ) -> Self {
        Self {
            mqtt_client,
            username,
            password,
            tls,
        }
    }
}

/// Build the MQTT connect options, applying credentials and TLS when requested.
fn connect_options(
    username: Option<&str>,
    password: Option<&str>,
    tls: bool,
) -> mqtt::ConnectOptions {
    let mut builder = mqtt::ConnectOptionsBuilder::new_v3();
    builder
        .keep_alive_interval(Duration::from_secs(30))
        .clean_session(false);

    if tls {
        builder.ssl_options(
            SslOptionsBuilder::new()
                .enable_server_cert_auth(true)
                .verify(true)
                .finalize(),
        );
    }
    if let Some(username) = username {
        builder.user_name(username);
    }
    if let Some(password) = password {
        builder.password(password);
    }

    builder.finalize()
}

impl MqttClient for MqttClientDefault {
    fn connect(&self) -> anyhow::Result<ServerResponse> {
        let conn_opts =
            connect_options(self.username.as_deref(), self.password.as_deref(), self.tls);

        self.mqtt_client
            .connect(conn_opts)
            .map_err(anyhow::Error::from)
    }

    fn subscribe_many(&self, topics: &[String], qoss: &[i32]) -> anyhow::Result<ServerResponse> {
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

/// Outcome of polling the MQTT stream.
#[derive(Debug)]
pub(crate) enum StreamEvent {
    Message(Message),
    /// No message arrived before the timeout elapsed.
    Timeout,
    /// The connection to the broker was lost.
    Disconnected,
}

#[cfg_attr(test, automock)]
pub(crate) trait Stream {
    fn next(&mut self, timeout: Duration) -> Result<StreamEvent>;
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
    fn next(&mut self, timeout: Duration) -> Result<StreamEvent> {
        match self.receiver.recv_timeout(timeout) {
            Ok(Some(message)) => Ok(StreamEvent::Message(message)),
            // paho signals a lost connection by yielding `None`.
            Ok(None) => Ok(StreamEvent::Disconnected),
            Err(RecvTimeoutError::Timeout) => Ok(StreamEvent::Timeout),
            Err(RecvTimeoutError::Disconnected) => Ok(StreamEvent::Disconnected),
        }
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
            .withf(move |t, q| t == topics_clone && q == qoss_clone)
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
    fn test_mock_stream_next_message() {
        let mut mock = MockStream::new();
        let msg = Message::new("topic", "payload", 1);
        let msg_clone = msg.clone();

        mock.expect_next()
            .times(1)
            .returning(move |_| Ok(StreamEvent::Message(msg_clone.clone())));

        let result = mock.next(Duration::from_millis(1));
        assert!(matches!(result, Ok(StreamEvent::Message(_))));
    }

    #[test]
    fn test_mock_stream_next_timeout() {
        let mut mock = MockStream::new();

        mock.expect_next()
            .times(1)
            .returning(|_| Ok(StreamEvent::Timeout));

        let result = mock.next(Duration::from_millis(1));
        assert!(matches!(result, Ok(StreamEvent::Timeout)));
    }

    #[test]
    fn test_mock_stream_next_disconnected() {
        let mut mock = MockStream::new();

        mock.expect_next()
            .times(1)
            .returning(|_| Ok(StreamEvent::Disconnected));

        let result = mock.next(Duration::from_millis(1));
        assert!(matches!(result, Ok(StreamEvent::Disconnected)));
    }

    #[test]
    fn test_mock_stream_next_error() {
        let mut mock = MockStream::new();

        mock.expect_next()
            .times(1)
            .returning(|_| Err(anyhow::anyhow!("test error")));

        let result = mock.next(Duration::from_millis(1));
        assert!(result.is_err());
    }

    #[test]
    fn test_connect_options_builds_for_all_combinations() {
        // The options carry credentials/TLS into the paho builder; there are no
        // public getters on `ConnectOptions`, so at least exercise every path.
        let _ = connect_options(None, None, false);
        let _ = connect_options(Some("user"), None, false);
        let _ = connect_options(None, Some("secret"), true);
        let _ = connect_options(Some("user"), Some("secret"), true);
    }
}
