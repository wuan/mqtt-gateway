# Design

## Context

See `proposal.md` - Why. The sensor source already has a pattern mechanism that
selects a `format` per topic filter (`src/config/mod.rs`, `ValueFormat`) and
`SensorLogger::check_message` dispatches a matching pattern to `handle_float` or
falls through to `handle_json` (`src/data/klimalogger/mod.rs`). Emitted values
use `Number` (`src/main.rs`), an untagged enum with `Int(i64)` and `Float(f64)`
variants that both InfluxDB and PostgreSQL targets already handle.

## Goals / Non-Goals

**Goals:**

- Add an `int` pattern format that parses a bare signed integer payload and
  emits it as `Number::Int`.
- Reuse the existing pattern matching, tagging, and receive-time rules so `int`
  behaves exactly like `float` apart from the parse type and stored variant.
- Keep the change small and avoid duplicating the scalar handling logic.

**Non-Goals:**

- No change to the JSON payload format or its timestamp freshness check.
- No coercion between formats (an `int` pattern does not accept `19.0`).
- No new target behavior or schema changes.

## Decisions

### `ValueFormat::Int` as a new enum variant

Add `Int` to `ValueFormat` with `#[serde(rename = "int")]`, alongside `Json`
(default) and `Float`. `serde` already rejects unknown `format` values, so no new
configuration validation is required. Alternative considered: infer the stored
`Number` variant from the payload shape on the existing `float` path. Rejected
because it hides the operator's intent and makes `Number::Int` vs `Number::Float`
depend on payload contents rather than configuration.

### Strict integer parsing, no coercion

Parse with `str::parse::<i64>()`. This accepts `42`, `-3`, and `+7` and rejects
`19.0`, `1.9e1`, and non-numeric payloads, matching the chosen behavior. Parse
the payload string exactly as the `float` path does (no trimming) so both scalar
formats stay consistent; a payload that fails to parse is dropped with a warning
and no event, mirroring `handle_float`.

### Share the scalar emission path

`handle_float` and the new `handle_int` differ only in the parse type/`Number`
variant and the log wording. Extract a private helper that performs the common
work - split the topic into location/measurement, warn and drop when either is
missing, log the match, and build/send the `LogEvent` with the given `Number`.
Each format keeps a thin wrapper that parses its payload and reports a
parse-specific warning. This keeps the warning text specific while removing the
duplicated event construction. Alternative considered: one helper with a
`match format` inside. Also acceptable, but separate wrappers keep the parse
error messages and the `format` log label explicit.

### Dispatch by format in `check_message`

Replace the `if pattern.format == ValueFormat::Float` check with a `match` that
routes `Float` and `Int` to their handlers and lets `Json` fall through to
`handle_json` (the current behavior for a matched JSON pattern).

## Risks / Trade-offs

- [Trailing newline or whitespace in integer payloads, common from some
  firmware, fails strict parsing] → Consistent with the existing `float`
  behavior; operators can already use a `float` pattern when a device adds
  noise. Tightening or trimming both formats would be a separate change.
- [A device switches a topic between whole and fractional values] → The operator
  picks the format per topic; a fractional value on an `int` pattern is dropped
  with a warning rather than silently rounded.
- [Sharing the emission helper could change `float` output] → Covered by the
  existing float pattern tests, which must keep passing unchanged.

## Migration Plan

No migration. `format` defaults to `json` and existing `json`/`float`
configurations are unaffected. Rollback is reverting the code change; the `int`
format is additive and no data is rewritten.
