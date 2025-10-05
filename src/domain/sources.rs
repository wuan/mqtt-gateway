use crate::config::{Source, SourceType};
use crate::data::{debug, klimalogger, opendtu, openmqttgateway, shelly, CheckMessage};
use crate::domain::MqttClient;
#[cfg(test)]
use crate::domain::MockMqttClient;
use log::{info, trace, warn};
use paho_mqtt::{Message, ServerResponse, QOS_1};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

pub(crate) struct Sources {
    handler_map: HashMap<String, Arc<Mutex<dyn CheckMessage>>>,
    handles: Vec<JoinHandle<()>>,
    topics: Vec<String>,
    qoss: Vec<i32>,
}

impl Sources {
    pub(crate) fn new(sources: Vec<Source>) -> Self {
        let mut handler_map: HashMap<String, Arc<Mutex<dyn CheckMessage>>> = HashMap::new();
        let mut handles: Vec<JoinHandle<()>> = Vec::new();
        let mut topics: Vec<String> = Vec::new();
        let mut qoss: Vec<i32> = Vec::new();

        for source in sources {
            let targets = source.targets.unwrap_or_default();
            let (logger, mut source_handles) = match source.source_type {
                SourceType::Shelly => shelly::create_logger(targets),
                SourceType::Sensor => klimalogger::create_logger(targets),
                SourceType::OpenDTU => opendtu::create_logger(targets),
                SourceType::OpenMqttGateway => openmqttgateway::create_logger(targets),
                SourceType::Debug => debug::create_logger(targets),
            };
            handler_map.insert(source.prefix.clone(), logger);
            handles.append(&mut source_handles);

            topics.push(format!("{}/#", source.prefix));
            qoss.push(QOS_1);
        }

        Self {
            handler_map,
            handles,
            topics,
            qoss,
        }
    }

    pub(crate) async fn subscribe(
        &self,
        mqtt_client: &Box<dyn MqttClient>,
    ) -> anyhow::Result<ServerResponse> {
        info!("Subscribing to topics: {:?}", &self.topics);
        mqtt_client
            .subscribe_many(&self.topics, &self.qoss)
            .await
            .map_err(anyhow::Error::from)
    }

    pub(crate) async fn handle(&self, msg: Message) {
        let prefix = msg.topic().split("/").next().unwrap();
        trace!("received from {} - {}", msg.topic(), msg.payload_str());

        let handler = self.handler_map.get(prefix);
        if let Some(handler) = handler {
            handler.lock().unwrap().check_message(&msg);
        } else {
            warn!("unhandled prefix {} from topic {}", prefix, msg.topic());
        }
    }

    pub(crate) async fn shutdown(self) {
        for handle in self.handles {
            handle.await.expect("failed to join influx writer thread");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SourceType;


    #[tokio::test]
    async fn test_sources_creation() {
        let sources = vec![Source {
            name: "foo".to_string(),
            prefix: "test".to_string(),
            source_type: SourceType::Debug,
            targets: None,
        }];

        let sources = Sources::new(sources);
        assert_eq!(sources.topics.len(), 1);
        assert_eq!(sources.qoss.len(), 1);
        assert_eq!(sources.topics[0], "test/#");
        assert_eq!(sources.qoss[0], QOS_1);
    }

    #[tokio::test]
    async fn test_subscribe() {
        let sources = vec![Source {
            name: "foo".to_string(),
            prefix: "test".to_string(),
            source_type: SourceType::Debug,
            targets: None,
        }];

        let sources = Sources::new(sources);

        let mut mock_client = Box::new(MockMqttClient::new());
        mock_client.expect_subscribe_many()
            .times(1)
            .returning(|_, _| Ok(ServerResponse::new()) );
        
        
        let result = sources.subscribe(&(mock_client as Box<dyn MqttClient>)).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_message() {
        let sources = vec![Source {
            name: "foo".to_string(),
            prefix: "test".to_string(),
            source_type: SourceType::Debug,
            targets: None,
        }];

        let sources = Sources::new(sources);
        let message = Message::new("test/topic", "payload", QOS_1);
        sources.handle(message).await;
    }

    #[tokio::test]
    async fn test_shutdown() {
        let sources = vec![Source {
            name: "foo".to_string(),
            prefix: "test".to_string(),
            source_type: SourceType::Debug,
            targets: None,
        }];

        let sources = Sources::new(sources);
        sources.shutdown().await;
    }
}


