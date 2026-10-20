# mqtt-source-routing Specification

## Purpose
Routes each MQTT message delivered to a subscribed source topic to the source
handler that owns the topic, so no configured source silently drops its data.

## Requirements

### Requirement: Longest matching source prefix

The gateway SHALL deliver each received MQTT message to the configured source
whose prefix is the longest prefix of the message topic that ends on a topic
separator or equals the whole topic.

#### Scenario: Multi-segment prefix is routed

- **WHEN** a source with prefix `home/shelly` is configured and a message arrives on `home/shelly/dev/status/switch:0`
- **THEN** the `home/shelly` handler receives the message

#### Scenario: Longest prefix wins for nested prefixes

- **WHEN** sources with prefixes `home` and `home/shelly` are configured and a message arrives on `home/shelly/dev/status`
- **THEN** the `home/shelly` handler receives the message and the `home` handler does not

#### Scenario: Shorter prefix matches its own subtree

- **WHEN** sources with prefixes `home` and `home/shelly` are configured and a message arrives on `home/other/state`
- **THEN** the `home` handler receives the message

#### Scenario: Single-segment prefix keeps current behavior

- **WHEN** a source with prefix `shellies` is configured and a message arrives on `shellies/device/relay/0`
- **THEN** the `shellies` handler receives the message

#### Scenario: Prefix does not match a partial segment

- **WHEN** a source with prefix `home/shelly` is configured and a message arrives on `home/shellybox/state`
- **THEN** the message is not delivered to the `home/shelly` handler

### Requirement: Unmatched messages are reported and dropped

The gateway SHALL drop a received message that matches no configured source
prefix and SHALL log it as an unhandled topic.

#### Scenario: No configured prefix matches

- **WHEN** no configured prefix matches the message topic `unknown/topic`
- **THEN** the message is dropped and an "unhandled prefix" warning is logged for the topic

### Requirement: Source prefixes are validated

The configuration SHALL reject a source prefix that is empty, starts or ends
with `/`, or contains the MQTT wildcards `+` or `#`.

#### Scenario: Empty prefix is rejected

- **WHEN** a source declares an empty `prefix`
- **THEN** configuration validation fails with a message naming the source

#### Scenario: Wildcard prefix is rejected

- **WHEN** a source declares a `prefix` containing `+` or `#`
- **THEN** configuration validation fails with a message naming the source

#### Scenario: Boundary slash prefix is rejected

- **WHEN** a source declares a `prefix` with a leading or trailing `/` such as `home/` or `/home`
- **THEN** configuration validation fails with a message naming the source

#### Scenario: Multi-segment prefix is accepted

- **WHEN** a source declares the prefix `home/shelly`
- **THEN** configuration validation succeeds
