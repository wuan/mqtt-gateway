use crate::domain::sources::Sources;
use crate::domain::MqttClient;
use log::{info, warn};
use smol::Timer;
use std::time::Duration;

pub(crate) struct Receiver {
    mqtt_client: Box<dyn MqttClient>,
    sources: Sources,
}

#[cfg(test)]
mod tests {
    use super::*;
    use paho_mqtt::ServerResponse;
    use crate::domain::sources::tests::sources;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn test_receiver_reconnect() {
        init();
        let mut mqtt_client = Box::new(crate::domain::MockMqttClient::new());
        mqtt_client.expect_reconnect()
            .times(2)
            .returning(|| {
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

        receiver.handle_error().await;
    }
    
    #[tokio::test]
    async fn test_listen() {
        let stream = Box::new(crate::domain::MockStream::new());
        let mut mqtt_client = Box::new(crate::domain::MockMqttClient::new());
        mqtt_client.expect_create()
            .times(1)
            .returning(|| Ok(stream) );

        let sources = sources();
        let receiver = Receiver::new(mqtt_client, sources);

        let result = receiver.listen().await;
        assert!(result.is_ok());
    }
}

impl Receiver {
    pub(crate) fn new(mqtt_client: Box<dyn MqttClient>, sources: Sources) -> Self {
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

