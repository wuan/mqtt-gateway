use crate::config::Target;
use crate::data::LogEvent;
use crate::target;
use crate::target::influx::InfluxConfig;
use crate::target::postgres::PostgresConfig;
use std::sync::mpsc::SyncSender;
use tokio::task::JoinHandle;

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
            )),
            Target::Postgresql {
                host,
                port,
                user,
                password,
                database,
            } => target::postgres::spawn_postgres_writer(PostgresConfig::new(
                host, port, user, password, database,
            )),
        };
        txs.push(tx);
        handles.push(handle);
    }
    Ok((txs, handles))
}
