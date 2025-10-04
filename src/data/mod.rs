use paho_mqtt::Message;

pub(crate) mod debug;
pub(crate) mod klimalogger;
pub(crate) mod opendtu;
pub(crate) mod openmqttgateway;
pub(crate) mod shelly;

pub trait CheckMessage {
    fn check_message(&mut self, msg: &Message);
}
