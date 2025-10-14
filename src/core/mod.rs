use anyhow::Result;
#[cfg(test)]
use mockall::automock;
use paho_mqtt::{Message, ServerResponse};
pub(crate) mod receiver;
pub(crate) mod sources;

#[cfg_attr(test, automock)]
pub(crate) trait Stream {
    fn next(&mut self) -> Result<Option<Message>>;
}

#[cfg_attr(test, automock)]
pub(crate) trait SourceClient {
    fn connect(&self) -> anyhow::Result<ServerResponse>;
    fn subscribe_many(
        &self,
        topics: &Vec<String>,
        qoss: &Vec<i32>,
    ) -> anyhow::Result<ServerResponse>;
    fn create(&mut self) -> anyhow::Result<Box<dyn Stream>>;
    fn reconnect(&self) -> anyhow::Result<ServerResponse>;
}