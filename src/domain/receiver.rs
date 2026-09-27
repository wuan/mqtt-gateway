use crate::domain::sources::Sources;
use crate::domain::{MqttClient, StreamEvent};
use crate::Shutdown;
use log::{info, warn};
use std::thread;
use std::time::Duration;

/// Delay between MQTT reconnect attempts.
const DEFAULT_RECONNECT_DELAY: Duration = Duration::from_secs(5);
/// How long to wait for a message before re-checking the shutdown flag.
const MQTT_POLL_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) struct Receiver {
    mqtt_client: Box<dyn MqttClient>,
    sources: Sources,
    shutdown: Shutdown,
    reconnect_delay: Duration,
}

impl Receiver {
    pub(crate) fn new(
        mqtt_client: Box<dyn MqttClient>,
        sources: Sources,
        shutdown: Shutdown,
    ) -> Self {
        Self {
            mqtt_client,
            sources,
            shutdown,
            reconnect_delay: DEFAULT_RECONNECT_DELAY,
        }
    }

    pub(crate) fn listen(mut self) -> anyhow::Result<()> {
        let mut stream = self.mqtt_client.create()?;
        self.sources.subscribe(self.mqtt_client.as_ref())?;

        info!("Waiting for messages ...");

        while !self.shutdown.is_requested() {
            // Poll with a timeout so a pending shutdown is observed even when
            // no messages are arriving.
            match stream.next(MQTT_POLL_INTERVAL) {
                Ok(StreamEvent::Message(msg)) => self.sources.handle(msg),
                Ok(StreamEvent::Timeout) => continue,
                // Connection lost - try to reconnect.
                Ok(StreamEvent::Disconnected) => self.handle_error(),
                Err(err) => {
                    warn!("Error reading from MQTT stream: {}", err);
                    // On error from stream, break out of loop
                    break;
                }
            }
        }

        info!("Shutdown requested, cleaning up...");
        self.sources.shutdown();

        info!("Exiting receiver");
        Ok(())
    }

    fn handle_error(&mut self) {
        warn!("MQTT: lost connection -> Attempting reconnect");
        let mut failures = 0;
        while !self.shutdown.is_requested() {
            match self.mqtt_client.reconnect() {
                Ok(_) => {
                    let reconnects = crate::metrics::increment(&crate::metrics::MQTT_RECONNECTS);
                    info!(
                        "MQTT: reconnected after {} failed attempt(s) ({} reconnect(s) so far)",
                        failures, reconnects
                    );
                    // Re-subscribe in case the broker dropped the session.
                    if let Err(err) = self.sources.subscribe(self.mqtt_client.as_ref()) {
                        warn!("MQTT: failed to re-subscribe after reconnect: {}", err);
                    }
                    return;
                }
                Err(err) => {
                    if self.shutdown.is_requested() {
                        warn!("MQTT: shutdown requested during reconnect, aborting");
                        return;
                    }
                    failures += 1;
                    warn!("MQTT: error reconnecting (attempt {}): {}", failures, err);
                    thread::sleep(self.reconnect_delay);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::sources::tests::sources;
    use crate::domain::MockMqttClient;
    use anyhow::Error;
    use log::LevelFilter;
    use mockall::predicate::*;
    use paho_mqtt::ServerResponse;

    #[test]
    fn test_receiver_reconnect() -> anyhow::Result<()> {
        let mut mqtt_client = Box::new(crate::domain::MockMqttClient::new());
        mqtt_client.expect_reconnect().times(2).returning({
            let mut calls = 0;
            move || {
                calls += 1;
                if calls == 1 {
                    Err(anyhow::Error::msg("Connection failed"))
                } else {
                    Ok(ServerResponse::default())
                }
            }
        });
        mqtt_client
            .expect_subscribe_many()
            .times(1)
            .returning(|_, _| Ok(ServerResponse::default()));

        let sources = sources();
        let mut receiver = Receiver::new(mqtt_client, sources, crate::Shutdown::new());
        receiver.reconnect_delay = Duration::ZERO;

        receiver.handle_error();
        Ok(())
    }

    #[test]
    fn test_listen() {
        let mqtt_client = mock_mqtt_client("bar/baz");
        let sources = sources();
        let handler_ref = sources.get_handler("bar").unwrap().clone();
        let receiver = Receiver::new(mqtt_client, sources, crate::Shutdown::new());

        let result = receiver.listen();

        assert!(result.is_ok());
        assert_eq!(handler_ref.lock().unwrap().checked_count(), 1);
    }

    #[test]
    fn test_listen_no_matches() {
        let mqtt_client = mock_mqtt_client("test/test");
        let sources = sources();
        let handler_ref = sources.get_handler("bar").unwrap().clone();
        let receiver = Receiver::new(mqtt_client, sources, crate::Shutdown::new());

        let result = receiver.listen();

        assert!(result.is_ok());
        assert_eq!(handler_ref.lock().unwrap().checked_count(), 0);
    }

    fn mock_mqtt_client(topic: &str) -> Box<MockMqttClient> {
        let mut mqtt_client = Box::new(crate::domain::MockMqttClient::new());
        let topic_owned = topic.to_string(); // Clone the topic string to ensure ownership
        mqtt_client.expect_create().times(1).returning(move || {
            let mut stream = Box::new(crate::domain::MockStream::new());
            let topic_clone = topic_owned.clone(); // Clone again for the inner closure
            stream.expect_next().times(1).returning(move |_| {
                Ok(StreamEvent::Message(paho_mqtt::Message::new(
                    &topic_clone,
                    "test payload",
                    0,
                )))
            });
            stream
                .expect_next()
                .times(1)
                .returning(|_| anyhow::Result::Err(Error::msg("test error")));
            Ok(stream)
        });

        mqtt_client
            .expect_subscribe_many()
            .times(1)
            .with(
                function(|topics: &[String]| topics[0] == "bar/#"),
                function(|qoss: &[i32]| qoss[0] == 1),
            )
            .returning(|_, _| Ok(ServerResponse::default()));

        mqtt_client
    }

    #[test]
    fn test_listen_with_error() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Info)
            .is_test(true)
            .try_init();
        let mut mqtt_client = Box::new(crate::domain::MockMqttClient::new());
        mqtt_client.expect_create().times(1).returning(|| {
            let mut stream = Box::new(crate::domain::MockStream::new());
            stream
                .expect_next()
                .times(1)
                .returning(|_| Ok(StreamEvent::Disconnected));
            stream
                .expect_next()
                .times(1)
                .returning(|_| Err(Error::msg("test error")));
            Ok(stream)
        });

        mqtt_client
            .expect_subscribe_many()
            .times(2)
            .with(
                function(|topics: &[String]| topics[0] == "bar/#"),
                function(|qoss: &[i32]| qoss[0] == 1),
            )
            .returning(|_, _| Ok(ServerResponse::default()));
        mqtt_client
            .expect_reconnect()
            .times(1)
            .returning(|| Ok(ServerResponse::default()));

        let sources = sources();
        let receiver = Receiver::new(mqtt_client, sources, crate::Shutdown::new());

        let result = receiver.listen();

        assert!(result.is_ok());
    }
}
