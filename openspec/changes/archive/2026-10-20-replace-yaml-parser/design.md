# Design

## Context

See `proposal.md` for motivation. Current state that shapes the approach:

- YAML enters the program through exactly one API, `serde_yaml_ng::from_str`, at
  `src/main.rs:90` and five test call sites in `src/config/mod.rs`.
- `${VAR_NAME}` expansion (`src/config/mod.rs:15`) runs on the raw text *before*
  parsing, so the parser never sees placeholders.
- The `Config`/`Source`/`Target` model uses serde's internally-tagged
  (`#[serde(tag = "type")]`) and untagged (`#[serde(untagged)] Number`) enums;
  no YAML anchors, multi-document streams, or custom tags are used.
- The project has no existing specs, so this change also establishes the
  `configuration` capability contract.

## Goals / Non-Goals

**Goals:**
- Remove the dependency on the archived `unsafe-libyaml` backend and clear
  RUSTSEC-2024-0449 from the dependency graph.
- Use a pure-Rust, actively maintained YAML deserializer.
- Change no observable configuration behavior.

**Non-Goals:**
- Changing the `config.yml` schema, discovery order, or validation rules.
- Switching config format away from YAML.
- Fixing the unrelated `rustls` advisories that also fail `cargo deny`.

## Decisions

### Use `serde-saphyr` (pure Rust) instead of a `serde_yaml` fork

`serde-saphyr` 2.0.0 parses via `granit-parser` (pure Rust), forbids `unsafe`,
is fuzz-tested and panic-free, and is the alternative named by RUSTSEC-2024-0449.
It is actively maintained (releases through October 2026).

Alternatives considered:
- `yaml_serde` (official YAML-org `serde_yaml` fork): true drop-in, but still
  parses through C libyaml (via `libyaml-rs`), so it does not eliminate the C
  backend — rejected given the stated goal of pure Rust.
- `serde_norway`: a fork on `unsafe-libyaml-norway`, stale since 2024-12 —
  rejected.
- `noyalib`: pure Rust but 0.0.x and young — rejected as higher risk than
  `serde-saphyr`.

### Depend on the deserializer surface only

Use `serde-saphyr = { version = "2", default-features = false, features = ["deserialize"] }`.
The gateway never serializes YAML, so `serialize` and `comments` (in `default`)
are dropped, narrowing the compiled dependency graph.

### Keep `${VAR}` expansion in the existing pre-parse step

Do not enable serde-saphyr's `properties` feature. Expansion stays in
`expand_env` on the raw string, preserving the current "undefined variable is an
error" behavior and avoiding two competing interpolation mechanisms.

### Keep the configuration model unchanged

Only the crate path in the six `from_str` calls changes. The serde attributes on
`Config`/`Source`/`Target`/`Number` stay as-is; serde-level features are the
deserializer's responsibility.

## Risks / Trade-offs

- **Behavioral differences from a from-scratch parser** (malformed input, odd
  scalars, error messages) → covered by the existing config test suite plus a
  parse of a real `config.yml`; keep `tests` green before merging.
- **Internally-tagged / untagged enum quirks** → upstream has closed issues
  confirming internally-tagged enums work and fixing untagged bugs; the one open
  untagged issue (leading-zero numeric `0755`) does not apply to this config.
- **MSRV 1.89 for serde-saphyr** → CI and local toolchains are on stable 1.98;
  no `rust-version` pin is added by this change.
- **Error type changes** → serde-saphyr's error implements `Display + Error`, so
  the existing `anyhow` `.with_context(...)` calls keep working; only message
  text differs.
- **`cargo deny` stays red on rustls** → explicitly out of scope; noted so the
  YAML fix is not mistaken for a fully green advisories check.

## Migration Plan

Single dependency swap; revert is `git revert` of the change. `Cargo.lock` is
regenerated, removing `serde_yaml_ng`/`unsafe-libyaml`. No data or deployment
migration: existing `config.yml` files keep working.
