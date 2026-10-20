# Tasks

## 1. Per-device timestamp storage

- [x] 1.1 Replace `OpenDTUParser`'s single cached timestamp with a per-section store (for example `HashMap<String, i64>`) and record each `status/last_update` against the message's own section, verified by a unit test where two sections keep independent timestamps
- [x] 1.2 Stamp inverter readings with the timestamp recorded for the reading's section and verify it with a unit test where a reading for device `A` uses `A`'s timestamp after device `B` published a different `status/last_update`
- [x] 1.3 Stamp string readings the same way and verify it with a unit test where a string reading uses its own device's timestamp
- [x] 1.4 Keep dropping a reading whose section has no recorded timestamp and verify it with a unit test that a reading for an unseen device emits no `Data`

## 2. Integration verification

- [x] 2.1 Add a logger-level test that primes two devices with different `status/last_update` values, sends a reading for each, and asserts each emitted event carries its own device's timestamp
- [x] 2.2 Run `cargo test` and confirm the OpenDTU tests pass
- [x] 2.3 Run `cargo fmt -- --check` and `cargo clippy --all-targets -- -D warnings` and confirm both are clean

## Workflow follow-up

- Archive the change after the project's review requirements are satisfied.
