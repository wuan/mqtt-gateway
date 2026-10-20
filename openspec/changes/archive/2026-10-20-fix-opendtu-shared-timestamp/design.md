# Design

## Context

`OpenDTUParser` (in `src/data/opendtu/mod.rs`) parses each `solar/<section>/...`
topic by splitting the topic into `section` (the device serial), `element`, and
`field`. A `status/last_update` message stores the parsed value in a single
parser-wide `timestamp: Option<i64>`; every subsequent `0/...` (inverter) or
`<string>/...` reading is stamped from that field. See proposal.md - Why for the
motivation.

The parser instance is owned by `OpenDTULogger` and reused for every message of
the source, so the single field is shared across all devices.

## Goals / Non-Goals

**Goals:**

- Make each device's readings carry that device's own `status/last_update`
  timestamp regardless of the order in which devices publish.

**Non-Goals:**

- Changing the topic grammar, the emitted tags, the emitted measurement names,
  or the target write path.
- Adding timestamps to the ignored `solar/<dtu|ac|dc>/...` global sections.
- Expiring or aging out timestamps, or any persistence of the cache.

## Decisions

**Decision: store the timestamp in a `HashMap<String, i64>` keyed by section.**
This is the fix suggested by issue #407, keeps the lookup local to `parse`, and
directly reflects the fact that the timestamp is a per-device attribute. A
`panic`-free `entry`/`insert` on the section string is enough. The alternative -
keeping a single timestamp and additionally remembering which device it belongs
to - was rejected because it fails as soon as two devices interleave their
`last_update` and readings, which is exactly the reported scenario.

**Decision: look the timestamp up by the message's own `section` at stamp
time.** `parse` already knows the section before dispatching to `map_inverter`
or `map_string`, so it passes the section's value (if any) into the mapping
functions instead of reading a shared field.

**Decision: keep dropping readings with no known timestamp.** Preserving the
existing "no timestamp yet, emit nothing" behavior keeps this change scoped to
the shared-timestamp bug. Emitting such readings with a wall-clock timestamp
would be a separate behavioral change.

## Risks / Trade-offs

- [Map grows with every distinct section string observed] -> The subscription is
  bounded by the configured source prefix and entries are small; the number of
  real devices is fixed, so growth is negligible. No eviction is added.
- [A device that changes serial keeps a stale entry] -> Stale entries are never
  read and only waste a small amount of memory; a stale entry cannot stamp
  another device's reading.

## Migration Plan

No data migration. After upgrade, each inverter is stamped with its own
`last_update`, so newly written points land at the correct time. Points already
written with the wrong timestamp are not rewritten.
