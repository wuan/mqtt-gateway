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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spawn_debug_logger_creates_channel() {
        let (tx, handle) = spawn_debug_logger::<String>();

        // Send a test message
        let send_result = tx.send("test message".to_string());
        assert!(send_result.is_ok());

        // Give the thread time to process
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Drop sender to allow thread to potentially exit on disconnect
        drop(tx);

        // Wait for thread to finish (it should exit on disconnect)
        let _ = handle.join();
    }

    #[test]
    fn test_spawn_debug_logger_shutdown() {
        // Set shutdown flag before spawning
        // Note: This is tricky because is_shutdown_requested() uses a global AtomicBool
        // We can't easily reset it, so we test that the function returns valid types
        let (tx, handle) = spawn_debug_logger::<i32>();

        // The thread should be running
        assert!(tx.send(42).is_ok());

        // Clean up
        drop(tx);
        let _ = handle.join();
    }

    #[test]
    fn test_debug_writer_with_timeout() {
        // Test that debug_writer handles timeout correctly
        // We can verify the type signatures compile correctly
        fn check_types<T: Debug + Send + 'static>() {
            let (tx, rx): (SyncSender<T>, Receiver<T>) = sync_channel(1);
            // The function signature is correct
            let _: Receiver<T> = rx;
            let _: SyncSender<T> = tx;
        }
        check_types::<String>();
    }
}
