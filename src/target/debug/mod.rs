use crate::is_shutdown_requested;
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
            info!("starting debug writer");

            debug_writer(rx)
        }),
    )
}
fn debug_writer<T: Debug>(rx: Receiver<T>) {
    info!("starting debug writer async");

    loop {
        // Check for shutdown request
        if is_shutdown_requested() {
            info!("Debug: shutdown requested, exiting writer");
            break;
        }

        let result = rx.recv_timeout(std::time::Duration::from_secs(1));
        let data = match result {
            Ok(query) => query,
            Err(error) => {
                match error {
                    std::sync::mpsc::RecvTimeoutError::Timeout => continue,
                    std::sync::mpsc::RecvTimeoutError::Disconnected => {
                        warn!("Debug: channel disconnected");
                        break;
                    }
                }
            }
        };
        info!("query: {:?}", data);
    }
    info!("exiting debug writer");
}
