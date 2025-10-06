use log::{info, warn};
use std::fmt::Debug;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread;
use std::thread::JoinHandle;

pub fn spawn_debug_logger<T: Debug + Send + 'static>() -> (SyncSender<T>, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);

    (
        tx,
        thread::spawn(move || {
            info!("starting debug writer",);

            debug_writer(rx)
        }),
    )
}
fn debug_writer<T: Debug>(rx: Receiver<T>) {
    info!("starting debug writer async");

    loop {
        let result = rx.recv();
        let data = match result {
            Ok(query) => query,
            Err(error) => {
                warn!("error receiving query: {:?}", error);
                break;
            }
        };
        info!("query: {:?}", data);
    }
    info!("exiting influx writer");
}
