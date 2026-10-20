# Spec Delta

## Purpose

Defines how the gateway consumes MQTT messages: how its receive loop stops,
reports a failed stream, reconnects after a lost connection, and joins the target
writer threads without blocking shutdown.

## ADDED Requirements

### Requirement: An MQTT stream read failure is fatal

The gateway SHALL stop consuming and report an error when reading from the MQTT
stream fails, so the process exits with a non-zero status instead of reporting
success.

#### Scenario: Stream read error exits with a failure

- **WHEN** the MQTT stream returns a read error
- **THEN** the receiver stops and the process exits with a non-zero status

#### Scenario: A lost connection is not treated as fatal

- **WHEN** the stream reports a lost connection rather than a read error
- **THEN** the receiver attempts to reconnect and keeps consuming instead of exiting

### Requirement: Shutdown joins the target writers without blocking

When the receiver stops, the gateway SHALL join every target writer thread and
complete shutdown even when no shutdown signal was requested and no further
message is received.

#### Scenario: Shutdown completes after a stream failure

- **WHEN** the MQTT stream read fails and the receiver stops
- **THEN** the gateway joins the target writer threads and finishes shutting down instead of blocking indefinitely

#### Scenario: A writer exits when its channel closes

- **WHEN** the source handlers that own a target's senders are released without a shutdown request
- **THEN** the writer thread exits instead of waiting for another event

### Requirement: Graceful shutdown is preserved

The gateway SHALL, on SIGINT or SIGTERM, stop consuming and shut down cleanly
with a successful exit status.

#### Scenario: Signal triggers a clean shutdown

- **WHEN** a SIGINT or SIGTERM is received while the receiver is running
- **THEN** the receiver stops consuming, the target writer threads are joined, and the process exits successfully
