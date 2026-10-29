# Design

## Context

See `proposal.md` - Why. `SensorLogger::handle_json`
(`src/data/klimalogger/mod.rs`) is the fallback decode for a sensor message
that no pattern selects. It calls `parse`, which wraps
`serde_json::from_slice::<Data>` in `anyhow`, and on any failure logs
`warn!("FAILED: {:?}, {:?}, {:?}", location, measurement, result)`. Because the
`anyhow` wrapper erases the underlying `serde_json::Error`, the caller cannot
tell a payload that is not JSON at all from valid JSON that does not match the
`{ sensor, time, value }` schema, so both warn.

A `sensor` source subscribes to a topic prefix that can carry unrelated
messages, so the "not JSON at all" case is expected traffic, not an error.

## Goals / Non-Goals

**Goals:**

- Distinguish "payload is not valid JSON" from "payload is valid JSON with the
  wrong shape" in the default sensor decode path.
- Ignore the former with a debug log, keep warning on the latter.

**Non-Goals:**

- No change to the topic shape check (missing location/measurement), the
  timestamp freshness check, or the pattern-based `float`/`int` paths.
- No new configuration option and no change to which payloads produce events.

## Decisions

### Classify on `serde_json::Error` instead of `anyhow`

Change `parse` to return `Result<Data, serde_json::Error>` so the caller keeps
the concrete error and can call `serde_json::Error::classify()`. Treat
`Category::Syntax` and `Category::Eof` (the payload is not JSON, or is truncated
JSON such as `expected value at line 1 column 1`) as "not JSON"; treat
`Category::Data` (`invalid type ... expected struct Data`) as "wrong shape" and
keep warning. Alternative considered: string-match the error message. Rejected
because it is brittle; `classify()` is the supported API. Alternative
considered: return a custom enum from `parse`. Rejected as more code for the
same result.

`anyhow::Result` is `Result<T, E = Error>`, so `Result<Data, serde_json::Error>`
still compiles with the module's existing `use anyhow::Result` and existing
callers/tests that `?` the error into `anyhow` keep working.

### Reorder `handle_json` so parse comes first

Parse the payload before splitting the topic, then branch:

- `Err(error)` with a non-JSON category → `debug!` the topic and error, return.
- `Err(error)` otherwise → `warn!` as before.
- `Ok(result)` → existing topic split and emission, warning when the topic lacks
  a location/measurement segment.

This keeps a single "not JSON" decision point and leaves the successful path
unchanged. Alternative considered: keep the `(location, measurement, result)`
tuple and match all three. Rejected because the non-JSON case would still warn
when the topic also happens to be malformed, which is not the intent.

### Debug, not trace, for ignored payloads

The message stays visible when debug logging is on but is out of the default
`warn` output. `debug!` is already imported in the module.

## Risks / Trade-offs

- [A producer that emits valid JSON with a typo'd field keeps warning] →
  Intended: a wrong-shape payload signals a real producer/config problem, so it
  stays a warning.
- [A payload that is truncated JSON is classified as not-JSON and ignored] →
  `Category::Eof` is grouped with syntax errors deliberately; a payload that is
  not a complete JSON value cannot be a sensor reading either way.
- [Changing `parse`'s error type could break callers] → `parse` is only used
  inside this module (the `handle_json` path and two unit tests), all of which
  compile against the new signature.

## Migration Plan

No migration. Behavior changes only for payloads that are not valid JSON and
that never emitted an event; rollback is reverting the code change.
