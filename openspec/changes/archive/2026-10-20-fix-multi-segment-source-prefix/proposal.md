# Proposal

## Why

A configured source prefix that contains `/` is subscribed as `<prefix>/#`, but
incoming messages are routed by the first topic segment only. Every message for
such a source is logged as `unhandled prefix ...` and dropped, and configuration
validation accepts the prefix, so the data loss is silent (issue #409).

## What Changes

- Route each incoming MQTT message to the configured source whose prefix is the
  longest prefix of the topic that ends on a topic-segment boundary, instead of
  routing by the first topic segment only.
- Preserve current behavior for single-segment prefixes: the longest match
  reduces to the existing routing when no multi-segment prefix matches.
- **BREAKING**: reject source prefixes that can never be routed
  deterministically at configuration-validation time - empty prefixes, prefixes
  with a leading or trailing `/`, and prefixes containing the MQTT wildcards `+`
  or `#`. A previously accepted invalid configuration now fails fast at startup
  instead of silently dropping messages.
- Document the accepted source-prefix format in the README configuration notes.

## Capabilities

### New Capabilities

- `mqtt-source-routing`: how the gateway matches received MQTT topics to the
  configured sources that subscribe to them, and which source prefixes the
  configuration accepts.

### Modified Capabilities

<!-- None: the project has no existing specs yet. -->

## Impact

- `src/domain/sources.rs` - routing of received messages to source handlers.
- `src/config/mod.rs` - source-prefix validation.
- `README.md` - configuration notes for `prefix`.
- Unit/integration tests around `Sources` and `Config::validate`.
