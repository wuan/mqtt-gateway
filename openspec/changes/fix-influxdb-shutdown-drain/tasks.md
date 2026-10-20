# Tasks

## 1. Drain the channel on shutdown

- [ ] 1.1 Add a `drain(&mut self, rx: &Receiver<Arc<LogEvent>>)` method to `Writer` in `src/target/influx/mod.rs` that maps every buffered event with `try_recv()` into the pending batch and then calls `flush()`, and verify it with a unit test that sends three events, calls `drain`, and asserts a single write containing three points
- [ ] 1.2 Call `writer.drain(&rx)` in the shutdown branch of `influxdb_writer` in place of the bare `writer.flush()`, and verify it with a test that pre-requests shutdown with events still buffered in the channel and asserts every buffered event is written before the writer exits
- [ ] 1.3 Call `writer.drain(&rx)` in the `Disconnected` branch (keeping the existing warning log) so both stop paths share one drain, and verify the existing `test_spawn_influxdb_writer_closing_after_sending` and `test_spawn_influxdb_writer_closing_without_sending_something` tests still pass
- [ ] 1.4 Verify a shutdown with no buffered events and no pending batch performs no write, using a unit test with a mock client that expects zero writes

## 2. Integration verification

- [ ] 2.1 Add a writer-level test that sends several distinct valid events, requests shutdown, joins the writer thread, and asserts the mock client received every event that was buffered (no event dropped)
- [ ] 2.2 Run `cargo test` and confirm the InfluxDB and shutdown-related tests pass
- [ ] 2.3 Run `cargo fmt -- --check` and `cargo clippy --all-targets -- -D warnings` and confirm both are clean

## Workflow follow-up

- Archive the change after the project's review requirements are satisfied.
