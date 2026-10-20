# Proposal

## Why

When reading from the MQTT stream fails, `Receiver::listen` breaks out of its
loop without requesting shutdown, and `Sources::shutdown` then joins the target
writer threads while the source handlers - which own the target `Sender`s - are
still alive. The writers never observe the shutdown flag or a closed channel, so
`join` blocks forever and the process hangs. Even if it did exit, `listen`
returns `Ok(())`, so the process reports success and an on-failure restart
policy would never restart it (issue #406).

## What Changes

- Release the source handlers' target `Sender`s before joining the writer
  threads, so a writer that only keeps running because of those senders observes
  the closed channel and exits.
- Request shutdown when the MQTT stream read fails, so the target writers stop
  promptly.
- Return an error from the receiver's listen loop when the MQTT stream read
  fails, so the process exits with a non-zero status instead of reporting
  success.
- Preserve the existing graceful shutdown on SIGINT/SIGTERM and the
  reconnect-and-continue behavior for a lost connection.

## Capabilities

### New Capabilities

- `mqtt-receiver`: how the gateway consumes messages from the MQTT broker,
  including how its receive loop stops, reports a failed stream, reconnects after
  a lost connection, and joins its target writers.

### Modified Capabilities

<!-- None: source routing and target behavior are unchanged. -->

## Impact

- `src/domain/receiver.rs` - stop and error handling in the listen loop.
- `src/domain/sources.rs` - ordering in `Sources::shutdown`.
- Tests in `src/domain/receiver.rs` and `src/domain/sources.rs`.
- No configuration, dependency, API, or database change.
