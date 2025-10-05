use crate::domain::sources::Sources;
use crate::domain::MqttClient;
use log::{info, warn};
use smol::Timer;
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
