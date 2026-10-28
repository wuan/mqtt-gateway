# Design

## Context

See `proposal.md` - Why. The `sensor` source is implemented in
`src/data/klimalogger/mod.rs` as a `CheckMessage` handler. `Sources::new` in
`src/domain/sources.rs` builds one handler per configured source and routes a
message to it by topic prefix. Every handler ultimately emits a shared
`LogEvent { measurement, timestamp, tags, fields }` (`src/data/mod.rs`), so the
new decoding only has to produce that shape. Configuration is parsed by
`src/config/mod.rs`, where `Source` already carries an optional `targets` list
and `Config::validate` rejects invalid prefixes up front.

The OpenDTU source (`src/data/opendtu/mod.rs`) already parses bare numeric
payloads, so plain scalars are an established pattern in the codebase.

## Goals / Non-Goals

**Goals:**

- Let a `sensor` source opt specific topics into plain-float decoding while
  keeping the JSON `{ sensor, time, value }` encoding as the default.
- Reuse the existing `LogEvent` output and InfluxDB/PostgreSQL targets.
- Reject malformed pattern configuration at startup, consistent with prefix
  validation.

**Non-Goals:**

- No new source type and no change to `SourceType`.
- No value formats beyond `json` and `float`.
- No regex topic matching and no timestamp extraction from a topic segment or a
  side-channel time topic.

## Decisions

### Patterns live on the existing `sensor` source

Add an optional `patterns` list to `Source` (`src/config/mod.rs`), each entry
holding a `topic` filter, a `format`, and an optional `sensor` name, and store
the list on `SensorLogger`. Alternatives considered: a dedicated `pattern`
source type (more config surface, duplicates routing/targets), and a single
`format` field for the whole source (cannot mix JSON and float topics under one
prefix), and silent auto-detection (ambiguous, hides misconfiguration). The
per-source list keeps configuration explicit and backward compatible: an absent
`patterns` list means "all JSON", exactly today's behavior.

### Topic selection uses MQTT wildcards, not regex

Pattern topics use `+` (one level) and `#` (remaining levels), matched by a small
`topic_filter_matches` helper in the sensor module. This mirrors how the broker
and the prefix routing already think about topics, keeps matching cheap and
allocation-free on the receive path, and avoids growing the `regex` surface.
The multi-level wildcard is only valid as the final segment, enforced both by the
matcher and by validation.

### First matching pattern wins

Patterns are evaluated in declaration order and the first match is applied;
unmatched topics fall back to JSON. This is predictable and lets an operator
carve out float topics from a broader JSON prefix.

### Plain floats are stamped with receive time

A bare number carries no timestamp, so float events use `Utc::now()` (the same
choice as the `openmqttgateway` source) and skip the ±60s freshness check, which
only makes sense for embedded timestamps. Alternatives (a cached time topic like
OpenDTU, or a timestamp topic segment) were rejected as out of scope.

### The `sensor` tag is configurable per pattern

Float events carry `location` from the topic's location segment and `sensor` from
the pattern's optional `sensor` field, defaulting to the location segment. This
keeps the PostgreSQL `location/sensor/value` schema satisfied without forcing a
device name when one is not known.

## Risks / Trade-offs

- **Patterns on a non-sensor source are parsed but ignored** → Document that
  `patterns` is consumed only by `sensor` sources; a follow-up can reject it for
  other types in `Config::validate` if it causes confusion.
- **First-match-wins makes order significant** → Documented; validation keeps
  individual filters well-formed so only intent, not validity, depends on order.
- **Receive-time stamping is subject to clock skew** → Accepted; it matches the
  existing `openmqttgateway` behavior, and float sources have no better time
  available.
- **`+`/`#` are allowed in pattern topics while the prefix forbids them** → These
  are different concepts (subscription prefix vs. per-message filter); validation
  treats them separately and the README notes the distinction.

## Migration Plan

No migration required. `patterns` is optional and defaults to an empty list, so
existing configurations continue to decode every message as JSON. Rollback is a
plain revert: removing the field restores the prior behavior with no data
migration.

## Open Questions

None.
