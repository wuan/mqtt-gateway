use crate::config::{Source, SourceType};
use crate::data::{debug, klimalogger, opendtu, openmqttgateway, shelly, CheckMessage};
#[cfg(test)]
use crate::domain::MockMqttClient;
use crate::domain::MqttClient;
use crate::Shutdown;
use anyhow::Context;
use log::{error, info, trace, warn};
use paho_mqtt::{Message, ServerResponse, QOS_1};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

pub(crate) struct Sources {
    handler_map: HashMap<String, Arc<Mutex<dyn CheckMessage>>>,
    handles: Vec<JoinHandle<()>>,
    topics: Vec<String>,
    qoss: Vec<i32>,
}

impl Sources {
    pub(crate) fn new(sources: Vec<Source>, shutdown: Shutdown) -> anyhow::Result<Self> {
        let mut handler_map: HashMap<String, Arc<Mutex<dyn CheckMessage>>> = HashMap::new();
        let mut handles: Vec<JoinHandle<()>> = Vec::new();
        let mut topics: Vec<String> = Vec::new();
        let mut qoss: Vec<i32> = Vec::new();

        for source in sources {
            let targets = source.targets.unwrap_or_default();
            let (logger, mut source_handles) = match source.source_type {
                SourceType::Shelly => shelly::create_logger(targets, shutdown.clone()),
                SourceType::Sensor => klimalogger::create_logger(targets, shutdown.clone()),
                SourceType::OpenDTU => opendtu::create_logger(targets, shutdown.clone()),
                SourceType::OpenMqttGateway => {
                    openmqttgateway::create_logger(targets, shutdown.clone())
                }
                SourceType::Debug => debug::create_logger(targets, shutdown.clone()),
            }
            .with_context(|| format!("Failed to create logger for source '{}'", source.name))?;

            if handler_map.contains_key(&source.prefix) {
                warn!(
                    "duplicate source prefix '{}' - the later source overrides the earlier one",
                    source.prefix
                );
            }
            handler_map.insert(source.prefix.clone(), logger);
            handles.append(&mut source_handles);

            topics.push(format!("{}/#", source.prefix));
            qoss.push(QOS_1);
        }

        Ok(Self {
            handler_map,
            handles,
            topics,
            qoss,
        })
    }

    pub(crate) fn subscribe(&self, mqtt_client: &dyn MqttClient) -> anyhow::Result<ServerResponse> {
        info!("Subscribing to topics: {:?}", self.topics);
        mqtt_client.subscribe_many(&self.topics, &self.qoss)
    }

    pub(crate) fn handle(&self, msg: Message) {
        let prefix = msg.topic().split('/').next().unwrap_or_default();
        trace!("received from {} - {}", msg.topic(), msg.payload_str());

        let handler = self.get_handler(prefix);
        if let Some(handler) = handler {
            match handler.lock() {
                Ok(mut handler) => handler.check_message(&msg),
                Err(_) => error!(
                    "handler for prefix '{}' is poisoned, dropping message on '{}'",
                    prefix,
                    msg.topic()
                ),
            }
        } else {
            warn!("unhandled prefix {} from topic {}", prefix, msg.topic());
        }
    }

    pub(crate) fn get_handler(&self, prefix: &str) -> Option<&Arc<Mutex<dyn CheckMessage>>> {
        self.handler_map.get(prefix)
    }

    pub(crate) fn shutdown(self) {
        for handle in self.handles {
            if handle.join().is_err() {
                error!("target writer thread panicked during shutdown");
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::SourceType;

    #[test]
    fn test_sources_creation() {
        let sources = sources();

        assert_eq!(sources.topics.len(), 1);
        assert_eq!(sources.qoss.len(), 1);
        assert_eq!(sources.topics[0], "bar/#");
        assert_eq!(sources.qoss[0], QOS_1);
    }

    #[test]
    fn test_subscribe() {
        let sources = sources();

        let mut mock_client = Box::new(MockMqttClient::new());
        mock_client
            .expect_subscribe_many()
            .times(1)
            .returning(|_, _| Ok(ServerResponse::new()));

        let client: &dyn MqttClient = mock_client.as_ref();
        let result = sources.subscribe(client);

        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_message() {
        let sources = sources();
        let message = Message::new("test/topic", "payload", QOS_1);

        sources.handle(message);
    }

    #[test]
    fn test_shutdown() {
        let sources = sources();

        sources.shutdown();
    }

    pub(crate) fn sources() -> Sources {
        let sources = vec![Source {
            name: "foo".to_string(),
            prefix: "bar".to_string(),
            source_type: SourceType::Debug,
            targets: None,
        }];

        Sources::new(sources, crate::Shutdown::new()).expect("failed to create sources")
    }
}
