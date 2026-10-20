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
        trace!("received from {} - {}", msg.topic(), msg.payload_str());

        match self.matching_prefix(msg.topic()) {
            Some(prefix) => {
                if let Some(handler) = self.get_handler(prefix) {
                    match handler.lock() {
                        Ok(mut handler) => handler.check_message(&msg),
                        Err(_) => error!(
                            "handler for prefix '{}' is poisoned, dropping message on '{}'",
                            prefix,
                            msg.topic()
                        ),
                    }
                }
            }
            None => warn!("unhandled prefix from topic {}", msg.topic()),
        }
    }

    /// The configured prefix that matches `topic` on a topic-segment boundary,
    /// choosing the longest one, or `None` when no configured prefix matches.
    ///
    /// A prefix matches when the topic equals it or continues with a `/`, so
    /// `home/shelly` matches `home/shelly/dev/status` but not `home/shellybox`.
    pub(crate) fn matching_prefix(&self, topic: &str) -> Option<&str> {
        self.handler_map
            .keys()
            .filter(|prefix| matches_topic_prefix(topic, prefix))
            .max_by_key(|prefix| prefix.len())
            .map(String::as_str)
    }

    pub(crate) fn get_handler(&self, prefix: &str) -> Option<&Arc<Mutex<dyn CheckMessage>>> {
        self.handler_map.get(prefix)
    }

    pub(crate) fn shutdown(self) {
        // The source handlers own the target `Sender`s. Drop them before
        // joining the writer threads so a writer that only keeps running
        // because of a live sender observes the closed channel and exits;
        // otherwise `join` could block forever.
        let Self {
            handler_map,
            handles,
            ..
        } = self;
        drop(handler_map);

        for handle in handles {
            if handle.join().is_err() {
                error!("target writer thread panicked during shutdown");
            }
        }
    }
}

/// Whether `prefix` matches `topic` at a topic-segment boundary: the topic is
/// exactly the prefix or continues it with a `/` separator.
fn matches_topic_prefix(topic: &str, prefix: &str) -> bool {
    topic == prefix
        || topic
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::SourceType;
    use std::sync::mpsc::SyncSender;
    use std::thread;

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

    /// A handler that owns a target sender, mirroring how the real handlers
    /// keep their targets' channels open.
    struct SenderHandler {
        _tx: SyncSender<()>,
    }

    impl CheckMessage for SenderHandler {
        fn check_message(&mut self, _msg: &Message) {}

        #[cfg(test)]
        fn checked_count(&self) -> u64 {
            0
        }

        #[cfg(test)]
        fn drop_all(&mut self) {}
    }

    #[test]
    fn test_shutdown_releases_handlers_before_joining_writers() {
        // The writer exits only once every sender is dropped and the channel
        // closes, so `shutdown` hangs unless it releases the handler first.
        let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);
        let handle = thread::spawn(move || while rx.recv().is_ok() {});

        let mut handler_map: HashMap<String, Arc<Mutex<dyn CheckMessage>>> = HashMap::new();
        handler_map.insert(
            "bar".to_string(),
            Arc::new(Mutex::new(SenderHandler { _tx: tx })),
        );

        let sources = Sources {
            handler_map,
            handles: vec![handle],
            topics: Vec::new(),
            qoss: Vec::new(),
        };

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

    fn sources_with_prefixes(prefixes: &[&str]) -> Sources {
        let sources = prefixes
            .iter()
            .enumerate()
            .map(|(index, prefix)| Source {
                name: format!("source-{index}"),
                prefix: (*prefix).to_string(),
                source_type: SourceType::Debug,
                targets: None,
            })
            .collect();

        Sources::new(sources, crate::Shutdown::new()).expect("failed to create sources")
    }

    /// A handler that records the topics it receives, so a test can tell which
    /// configured prefix a received message was routed to.
    struct SpyHandler {
        received: Arc<Mutex<Vec<String>>>,
    }

    impl CheckMessage for SpyHandler {
        fn check_message(&mut self, msg: &Message) {
            self.received
                .lock()
                .expect("spy lock poisoned")
                .push(msg.topic().to_string());
        }

        fn checked_count(&self) -> u64 {
            self.received.lock().map_or(0, |seen| seen.len() as u64)
        }

        fn drop_all(&mut self) {
            if let Ok(mut seen) = self.received.lock() {
                seen.clear();
            }
        }
    }

    type Spies = HashMap<String, Arc<Mutex<Vec<String>>>>;

    fn sources_with_spies(prefixes: &[&str]) -> (Sources, Spies) {
        let mut handler_map: HashMap<String, Arc<Mutex<dyn CheckMessage>>> = HashMap::new();
        let mut spies: Spies = HashMap::new();

        for prefix in prefixes {
            let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            spies.insert((*prefix).to_string(), Arc::clone(&received));
            let handler: Arc<Mutex<dyn CheckMessage>> =
                Arc::new(Mutex::new(SpyHandler { received }));
            handler_map.insert((*prefix).to_string(), handler);
        }

        let sources = Sources {
            handler_map,
            handles: Vec::new(),
            topics: Vec::new(),
            qoss: Vec::new(),
        };

        (sources, spies)
    }

    #[test]
    fn test_matching_prefix_uses_longest_segment_boundary_match() {
        let sources = sources_with_prefixes(&["home", "home/shelly"]);

        assert_eq!(
            sources.matching_prefix("home/shelly/dev/status"),
            Some("home/shelly")
        );
        assert_eq!(sources.matching_prefix("home/other/state"), Some("home"));
    }

    #[test]
    fn test_matching_prefix_routes_a_multi_segment_prefix() {
        let sources = sources_with_prefixes(&["home/shelly"]);

        assert_eq!(
            sources.matching_prefix("home/shelly/dev/status/switch:0"),
            Some("home/shelly")
        );
    }

    #[test]
    fn test_matching_prefix_matches_the_exact_topic() {
        let sources = sources_with_prefixes(&["home/shelly"]);

        assert_eq!(sources.matching_prefix("home/shelly"), Some("home/shelly"));
    }

    #[test]
    fn test_matching_prefix_does_not_match_a_partial_segment() {
        let sources = sources_with_prefixes(&["home/shelly"]);

        assert_eq!(sources.matching_prefix("home/shellybox/state"), None);
    }

    #[test]
    fn test_matching_prefix_keeps_single_segment_behavior() {
        let sources = sources_with_prefixes(&["shellies"]);

        assert_eq!(
            sources.matching_prefix("shellies/device/relay/0"),
            Some("shellies")
        );
    }

    #[test]
    fn test_matching_prefix_is_none_for_an_unrelated_topic() {
        let sources = sources_with_prefixes(&["home"]);

        assert_eq!(sources.matching_prefix("unknown/topic"), None);
    }

    #[test]
    fn test_handle_delivers_to_the_longest_matching_prefix() {
        let (sources, spies) = sources_with_spies(&["home", "home/shelly"]);

        sources.handle(Message::new("home/shelly/dev/status/switch:0", "on", QOS_1));

        assert_eq!(
            *spies["home/shelly"].lock().unwrap(),
            vec!["home/shelly/dev/status/switch:0".to_string()]
        );
        assert!(spies["home"].lock().unwrap().is_empty());
    }

    #[test]
    fn test_handle_delivers_to_the_shorter_prefix_subtree() {
        let (sources, spies) = sources_with_spies(&["home", "home/shelly"]);

        sources.handle(Message::new("home/other/state", "x", QOS_1));

        assert_eq!(
            *spies["home"].lock().unwrap(),
            vec!["home/other/state".to_string()]
        );
        assert!(spies["home/shelly"].lock().unwrap().is_empty());
    }

    #[test]
    fn test_handle_drops_a_topic_that_only_matches_a_partial_segment() {
        let (sources, spies) = sources_with_spies(&["home/shelly"]);

        sources.handle(Message::new("home/shellybox/state", "x", QOS_1));

        assert!(spies["home/shelly"].lock().unwrap().is_empty());
    }
}
