# opendtu-source Specification

## Purpose
Defines how the gateway parses OpenDTU `solar/<device>/...` topics into
timestamped measurements, including how each device's `status/last_update`
timestamp is tracked and applied to that device's readings.

## Requirements

### Requirement: Device-specific timestamps

The gateway SHALL associate each observed OpenDTU `status/last_update` value
with the device section that published it, and SHALL stamp every inverter and
string reading with the timestamp recorded for that reading's own device
section.

#### Scenario: Readings use their own device's timestamp

- **WHEN** device `A` has published `status/last_update` `1701271852` and device `B` has published `status/last_update` `1701300000`
- **THEN** a reading for `A` is stamped `1701271852` and a reading for `B` is stamped `1701300000`

#### Scenario: One device's update does not restamp another device

- **WHEN** device `A` has published `status/last_update` and later device `B` publishes its own `status/last_update`
- **THEN** a subsequent reading for `A` is still stamped with `A`'s timestamp

#### Scenario: A newer update replaces only that device's timestamp

- **WHEN** device `A` publishes `status/last_update` `1701271852` and later publishes `status/last_update` `1701300000`
- **THEN** subsequent readings for `A` are stamped `1701300000` while other devices keep their own timestamps

#### Scenario: String readings share their device's timestamp

- **WHEN** device `A` has published a `status/last_update` timestamp and a string reading arrives on `solar/A/1/voltage`
- **THEN** the string reading is stamped with `A`'s timestamp

### Requirement: Readings without a known timestamp are dropped

The gateway SHALL NOT emit an event for an inverter or string reading whose
device section has no observed `status/last_update` timestamp.

#### Scenario: Reading before any update is dropped

- **WHEN** a reading arrives for a device that has not yet published `status/last_update`
- **THEN** no event is emitted for that reading

#### Scenario: A device without a timestamp does not affect others

- **WHEN** device `A` has a known timestamp, device `B` has none, and a reading arrives for `B`
- **THEN** `B`'s reading emits no event while `A`'s later readings still emit events stamped with `A`'s timestamp
