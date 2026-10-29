# Tasks

## 1. Classify sensor JSON decode failures

- [x] 1.1 Change `parse` in `src/data/klimalogger/mod.rs` to return `Result<Data, serde_json::Error>` and add a private `is_non_json(error)` helper that reports `true` for `serde_json::error::Category::Syntax` and `Category::Eof`; verify with a unit test that a syntax error (e.g. `heating`) is non-JSON while a `Category::Data` error (e.g. `{"sensor":"BME680","time":"foo","value":19.45}`) is not
- [x] 1.2 Rework `handle_json` to parse first: `debug!` and drop non-JSON payloads, keep warning for valid-JSON-wrong-shape payloads and for topics without a location/measurement segment; verify with tests that a non-JSON payload on a sensor topic emits no event and that a valid JSON wrong-shape payload also emits no event

## 2. Full verification

- [x] 2.1 Run `cargo test` and confirm the new and existing `klimalogger` tests pass
- [x] 2.2 Run `cargo fmt -- --check` and `cargo clippy --all-targets -- -D warnings` and confirm both pass

## Workflow follow-up

- Archive the change after review, updating `openspec/specs/sensor-source/spec.md` with the delta.
