# Tasks

## 1. Configuration

- [x] 1.1 Add the `Int` variant (`#[serde(rename = "int")]`) to `ValueFormat` and update the `Pattern::sensor` doc comment to cover `float` and `int` patterns in `src/config/mod.rs`; verify a deserialization test for a pattern with `format: "int"` passes and the pattern-defaults-to-JSON test still passes

## 2. Sensor parser

- [x] 2.1 Extract the shared scalar emission path (split topic into location/measurement, warn and drop when missing, log the match, build and send the `LogEvent`) and refactor `handle_float` onto it; verify the existing float pattern tests still pass unchanged
- [x] 2.2 Add `handle_int`, parsing the payload with `str::parse::<i64>()` and emitting `Number::Int`, and dispatch `ValueFormat::Int` to it in `check_message` (keeping `Json` falling through to `handle_json`); verify a unit test that a payload `42` on a matching `int` pattern emits an event with value `Number::Int(42)`, the `location`/`sensor` tags, and the receive-time timestamp
- [x] 2.3 Verify an `int` pattern drops non-integer payloads (`19.0`, `1.9e1`, `not-a-number`) and topics without a location/measurement segment do not panic and emit no event (tests asserting no event and a warning)
- [x] 2.4 Verify the `int` pattern's `sensor` tag overrides the location default and that a non-matching topic still uses JSON decoding, with tests

## 3. Documentation

- [x] 3.1 Document the `int` format in the README configuration notes and add a commented `int` pattern to the `config.yml` example; verify the documented example matches the implemented strict-integer and default-`sensor` behavior

## 4. Verification

- [x] 4.1 Run `cargo test`, `cargo clippy -- -D warnings`, and `cargo fmt --check`, and verify the full suite passes with no new warnings
