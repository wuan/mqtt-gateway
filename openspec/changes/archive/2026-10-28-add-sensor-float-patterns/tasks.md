# Tasks

## 1. Configuration schema

- [x] 1.1 Add the `ValueFormat` enum (`json` default, `float`) and the `Pattern` struct (`topic`, `format`, optional `sensor`) in `src/config/mod.rs`; verify the deserialization tests for a float pattern and for a pattern that defaults to JSON pass
- [x] 1.2 Add an optional `patterns` list to `Source` and update the existing `Source` literals in config and source tests; verify the existing configuration tests still pass
- [x] 1.3 Add `validate_patterns` and call it from `Config::validate` to reject empty topics, empty segments, and a non-final `#`, naming the source; verify the rejection and acceptance tests pass

## 2. Sensor parsing

- [x] 2.1 Add a `topic_filter_matches` helper for MQTT `+`/`#` filters in `src/data/klimalogger/mod.rs`; verify its unit tests cover single-level, multi-level, exact, and non-matching cases
- [x] 2.2 Store the patterns on `SensorLogger` and dispatch to float or JSON decoding in `check_message`, moving the current logic to `handle_json`; verify the existing JSON sensor tests still pass
- [x] 2.3 Implement `handle_float` to parse a bare number, stamp the receive time, and tag `location` plus `sensor` (pattern value or location default), dropping non-numeric payloads and topics without location/measurement with a warning; verify the float, default-sensor, and parse-error tests pass
- [x] 2.4 Verify that a topic matching no pattern still uses JSON decoding with a test

## 3. Wiring

- [x] 3.1 Pass a source's `patterns` into `klimalogger::create_logger` from `src/domain/sources.rs` and update the remaining `create_logger` call sites; verify `cargo test` passes

## 4. Documentation and integration

- [x] 4.1 Document the `patterns` field and its semantics in the README configuration notes and add a commented example to `config.yml`; verify the documented example matches the implemented topic-filter and default behavior
- [x] 4.2 Run `cargo test`, `cargo clippy --all-targets`, and `cargo fmt --check` and confirm all pass

## Workflow follow-up

- Archive the change after implementation review is complete.
- Verify the archived result updates the `configuration` spec and creates the `sensor-source` spec.
