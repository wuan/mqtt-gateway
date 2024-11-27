use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread;
use std::thread::JoinHandle;

pub fn spawn_debug_logger<T: Send + 'static>() -> (SyncSender<T>, JoinHandle<()>) {
    spawn_debug_logger_internal()
}

fn spawn_debug_logger_internal<T: Send + 'static>() -> (SyncSender<T>, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);

    (
        tx,
        thread::spawn(move || {
            info!(
                "starting influx writer {} {}",
                &influx_config.url, &influx_config.database
            );

            debug_writer(rx, influx_client, influx_config, query_mapper)
        }),
    )
}

fn debug_writer<T>(rx: Receiver<T>) {
    block_on(async move {
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
            let query = query_mapper(data);
            let result = influx_client.query(query).await;
            match result {
                Ok(_) => {}
                Err(error) => {
                    panic!(
                        "#### Error writing to influx: {} {}: {:?}",
                        &influx_config.url, &influx_config.database, error
                    );
                }
            }
        }
        info!("exiting influx writer async");
    });

    info!("exiting influx writer");
}
