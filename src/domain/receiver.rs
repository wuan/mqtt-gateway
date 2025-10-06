use crate::domain::sources::Sources;
use crate::domain::MqttClient;
use log::{info, warn};
use std::thread;
use std::time::Duration;

pub(crate) struct Receiver {
    mqtt_client: Box<dyn MqttClient>,
    sources: Sources,
}

impl Receiver {
    pub(crate) fn new(mqtt_client: Box<dyn MqttClient>, sources: Sources) -> Self {
        Self {
            mqtt_client,
            sources,
        }
    }

    pub(crate) fn listen(mut self) -> anyhow::Result<()> {
        let mut stream = self.mqtt_client.create()?;
        self.sources.subscribe(&self.mqtt_client)?;

        info!("Waiting for messages ...");

        while let Ok(msg_opt) = stream.next() {
            if let Some(msg) = msg_opt {
                self.sources.handle(msg);
            } else {
                self.handle_error();
            }
        }

        self.sources.shutdown();

        info!("Exiting receiver");
        Ok(())
    }

    fn handle_error(&mut self) {
        warn!("MQTT: lost connection -> Attempting reconnect");
        while let Err(err) = self.mqtt_client.reconnect() {
            warn!("MQTT: error reconnecting: {}", err);
            thread::sleep(Duration::from_secs(5));
        }
        info!("MQTT: reconnected")
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
        mqtt_client.expect_reconnect().times(2).returning(|| {
            static mut CALLED: bool = false;
            unsafe {
                if !CALLED {
                    CALLED = true;
                    Err(anyhow::Error::msg("Connection failed"))
                } else {
                    Ok(ServerResponse::default())
                }
            }
        });

        let sources = sources();
        let mut receiver = Receiver::new(mqtt_client, sources);

        receiver.handle_error();
        Ok(())
    }

    #[test]
    fn test_listen() {
        let mqtt_client = mock_mqtt_client("bar/baz");
        let sources = sources();
        let handler_ref = sources.get_handler("bar").unwrap().clone();
        let receiver = Receiver::new(mqtt_client, sources);

        let result = receiver.listen();

        assert!(result.is_ok());
        assert_eq!(handler_ref.lock().unwrap().checked_count(), 1);
    }

    #[test]
    fn test_listen_no_matches() {
        let mqtt_client = mock_mqtt_client("test/test");
        let sources = sources();
        let handler_ref = sources.get_handler("bar").unwrap().clone();
        let receiver = Receiver::new(mqtt_client, sources);

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
            stream.expect_next().times(1).returning(move || {
                Ok(Some(paho_mqtt::Message::new(
                    &topic_clone,
                    "test payload",
                    0,
                )))
            });
            stream
                .expect_next()
                .times(1)
                .returning(|| anyhow::Result::Err(Error::msg("test error")));
            Ok(stream)
        });

        mqtt_client
            .expect_subscribe_many()
            .times(1)
            .with(
                function(|topics: &Vec<String>| topics[0] == "bar/#"),
                function(|qoss: &Vec<i32>| qoss[0] == 1),
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
            stream.expect_next().times(1).returning(|| Ok(None));
            stream
                .expect_next()
                .times(1)
                .returning(|| Err(Error::msg("test error")));
            Ok(stream)
        });

        mqtt_client
            .expect_subscribe_many()
            .times(1)
            .with(
                function(|topics: &Vec<String>| topics[0] == "bar/#"),
                function(|qoss: &Vec<i32>| qoss[0] == 1),
            )
            .returning(|_, _| Ok(ServerResponse::default()));
        mqtt_client
            .expect_reconnect()
            .times(1)
            .returning(|| Ok(ServerResponse::default()));

        let sources = sources();
        let receiver = Receiver::new(mqtt_client, sources);

        let result = receiver.listen();

        assert!(result.is_ok());
    }
}
