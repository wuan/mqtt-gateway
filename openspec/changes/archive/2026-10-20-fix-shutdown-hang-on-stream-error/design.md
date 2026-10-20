# Design

## Context

`Receiver::listen` owns the `Sources` collection and a `Shutdown` flag. `Sources`
keeps the per-source message handlers in `handler_map` and the writer threads in
`handles`; each handler owns the target `Sender`s created for its source, and
`Sources::shutdown(self)` joins `handles`. Target writers (InfluxDB, PostgreSQL,
debug) exit when their channel disconnects or when the shutdown flag is set. See
proposal.md - Why for the reported failure.

## Goals / Non-Goals

**Goals:**

- Make shutdown after an MQTT stream failure terminate instead of hanging.
- Make a stream read failure produce a non-zero process exit status.
- Keep graceful shutdown and reconnect behavior unchanged.

**Non-Goals:**

- Changing how sources are routed, how targets flush, or how reconnection
  works.
- Adding durable buffering, restart policies, or new configuration.
- Changing the reconnect delay or the receive poll interval.

## Decisions

**Decision: release the handlers before joining in `Sources::shutdown`.** The
senders live in `handler_map`, so shutdown destructures `Sources` and drops the
handler map before iterating `handles`. This removes the root cause - a
`join` that can never complete because its writer is kept alive by a sender the
same struct still holds - and it fixes the ordering for every caller, not just
the error path. Considered alternative: request shutdown first and rely on the
shutdown flag. That also unblocks the writers, but it leaves the ordering trap in
place and would not help another caller that joins without requesting shutdown.

**Decision: return the stream error from `listen` and let `main` propagate it.**
`main` already calls `receiver.listen()?`, and `main() -> anyhow::Result<()>`
exits non-zero when it returns `Err`, so returning the error is enough. On the
error path `listen` requests shutdown and then shuts the sources down, so writers
stop promptly regardless of whether they wake on the flag or on channel close.
Considered alternative: keep returning `Ok(())` and report the failure only by
logging. That fixes the hang but not the exit status, so an on-failure restart
policy would still not trigger.

**Decision: keep the `Disconnected` event recoverable.** Only an `Err` from the
stream is fatal; a lost connection still runs the reconnect loop. `StreamDefault`
maps both a closed receiver and an empty poll to `Disconnected`, so this
distinction is available at the call site and is preserved.

**Decision: update the receiver tests that use an erroring stream.** The test
streams end in an error to break the loop. Under the new contract those tests
assert an error return instead of success; the handler-count assertions are
unchanged.

## Risks / Trade-offs

- [Dropping the handlers closes target channels on every shutdown] -> The
  writers already handle `Disconnected` by flushing or draining their queue, so
  queued events are still written; releasing the senders first makes that path,
  not the flag path, the likely one.
- [A stream error now exits the process] -> That is the intended behavior: an
  unexpected read failure is not recoverable by the current loop, and exiting
  non-zero lets an external supervisor restart it.
- [Requesting shutdown in the error path could race with the sender drop] ->
  Both paths flush pending events, so either branch is safe and the writers exit
  within one poll interval.

## Migration Plan

No migration. Behavior-only change with no configuration, API, or data change;
rollback is reverting the commit.
