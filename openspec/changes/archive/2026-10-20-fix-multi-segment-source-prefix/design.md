# Design

## Context

`Sources` stores handlers in a `HashMap` keyed by the full configured prefix and
subscribes to `<prefix>/#`. `Sources::handle` currently looks up
`msg.topic().split('/').next()`, so a prefix containing `/` is never found. See
proposal.md - Why for the motivation.

## Goals / Non-Goals

**Goals:**

- Route messages to hierarchical (`/`-containing) prefixes without changing how
  existing single-segment prefixes behave.
- Fail fast at configuration load for prefixes that cannot be routed
  deterministically.

**Non-Goals:**

- Supporting MQTT wildcards (`+`, `#`) inside a configured prefix.
- Changing the subscription strategy (`<prefix>/#`, QoS 1) or adding per-source
  topic filters.
- Changing duplicate-prefix handling (later source still overrides earlier with
  a warning).

## Decisions

**Decision: route by longest matching prefix instead of rejecting `/` in
validation.** Longest-prefix matching is backward compatible, matches MQTT's
hierarchical topic model, and actually recovers the data that is currently
dropped. Rejecting `/` (the issue's other suggestion) is simpler but rejects a
natural configuration and leaves the gateway unable to serve hierarchical
prefixes at all; it is recorded here as the considered alternative.

**Decision: match on segment boundaries, not a raw `starts_with`.** A prefix
matches only when the topic equals the prefix or the next character in the topic
is `/`. This prevents `home/shelly` from swallowing `home/shellybox/...` and
keeps the longest-match result deterministic.

**Decision: validate only malformed prefixes (empty, leading/trailing `/`,
wildcards).** These forms cannot be routed or subscribed reliably and today
start up silently broken. A prefix containing `/` is valid under this change, so
it is not rejected.

**Decision: keep the subscription and handler map shape.** Match against the
configured prefixes at dispatch time; the map stays keyed by prefix. No new
index is needed at the sizes involved.

## Risks / Trade-offs

- [Overlapping prefixes make the "correct" handler ambiguous] -> Longest match
  is the deterministic rule and is covered by dedicated scenarios.
- [Rejecting previously accepted invalid configs breaks startup] -> The configs
  were already dropping all of that source's data; failing fast with a clear
  message is the safer outcome and is called out as breaking in the proposal.
- [Dispatch now scans configured prefixes per message] -> The source count is
  small; matching is O(prefixes) string comparisons and can be revisited only if
  profiling shows it matters.

## Migration Plan

No data migration. Operators with a `/`-containing prefix keep their
configuration and start receiving data. Operators with an empty, wildcard, or
boundary-slash prefix must correct the prefix before upgrade; the validation
error names the offending source.
