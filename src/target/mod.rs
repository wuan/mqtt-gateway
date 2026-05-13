use crate::config::Target;
use crate::data::LogEvent;
use crate::target;
use crate::target::debug::spawn_debug_logger;
use crate::target::influx::InfluxConfig;
use crate::target::postgres::PostgresConfig;
use std::sync::mpsc::SyncSender;
use std::thread::JoinHandle;

pub(crate) mod influx;
pub(crate) mod postgres;

pub(crate) mod debug;

pub fn create_targets(
    targets: Vec<Target>,
) -> anyhow::Result<(Vec<SyncSender<LogEvent>>, Vec<JoinHandle<()>>)> {
    let mut txs: Vec<SyncSender<LogEvent>> = Vec::new();
    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    for target in targets {
        let (tx, handle) = match target {
            Target::InfluxDB {
                url,
                database,
                user,
                password,
                token,
            } => influx::spawn_influxdb_writer(InfluxConfig::new(
                url, database, user, password, token,
            ))?,
            Target::Postgresql {
                host,
                port,
                user,
                password,
                database,
            } => target::postgres::spawn_postgres_writer(PostgresConfig::new(
                host, port, user, password, database,
            ))?,
            Target::Debug {} => spawn_debug_logger(),
        };
        txs.push(tx);
        handles.push(handle);
    }
    Ok((txs, handles))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Target;

    #[test]
    fn test_create_targets_empty() {
        let targets = vec![];
        let result = create_targets(targets);
        assert!(result.is_ok());
        let (txs, handles) = result.unwrap();
        assert_eq!(txs.len(), 0);
        assert_eq!(handles.len(), 0);
    }

    #[test]
    fn test_create_targets_debug() {
        let targets = vec![Target::Debug {}];
        let result = create_targets(targets);
        assert!(result.is_ok());
        let (txs, handles) = result.unwrap();
        assert_eq!(txs.len(), 1);
        assert_eq!(handles.len(), 1);

        // Clean up
        drop(txs);
        for handle in handles {
            let _ = handle.join();
        }
    }

    #[test]
    fn test_create_targets_multiple() {
        let targets = vec![
            Target::Debug {},
            Target::Debug {},
        ];
        let result = create_targets(targets);
        assert!(result.is_ok());
        let (txs, handles) = result.unwrap();
        assert_eq!(txs.len(), 2);
        assert_eq!(handles.len(), 2);

        // Clean up
        drop(txs);
        for handle in handles {
            let _ = handle.join();
        }
    }
}
