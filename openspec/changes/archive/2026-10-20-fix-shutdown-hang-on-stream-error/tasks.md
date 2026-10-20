# Tasks

## 1. Release target senders before joining

- [x] 1.1 Add a unit test in `src/domain/sources.rs` that builds `Sources` whose handler holds a target sender and whose writer thread exits only when the channel closes, and confirm the test blocks (or times out) on `shutdown()` before the fix
- [x] 1.2 Change `Sources::shutdown` to drop the handler map before joining the writer threads and verify the new test and `cargo test domain::sources` pass

## 2. Report a stream failure as fatal

- [x] 2.1 Change `Receiver::listen` to request shutdown, shut the sources down, and return the MQTT stream error when the stream ends in an error, and verify a unit test asserts `listen` returns `Err` for that case
- [x] 2.2 Update the existing receiver tests whose mock stream ends in an error (`test_listen`, `test_listen_no_matches`, `test_listen_with_error`) to assert the error return and verify `cargo test domain::receiver` passes

## 3. Integration verification

- [x] 3.1 Run `cargo test` across the crate and confirm every test passes
- [x] 3.2 Run `cargo fmt -- --check` and `cargo clippy --all-targets -- -D warnings` and confirm both are clean

## Workflow follow-up

- [x] Archive the change after the project's review requirements are satisfied.
