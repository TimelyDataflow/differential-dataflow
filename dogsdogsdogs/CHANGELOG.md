# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.23.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.22.0...differential-dogs3-v0.23.0) - 2026-04-13

### Changed

- Update to timely 0.29, tracking scope ownership and lifetime changes ([#714](https://github.com/TimelyDataflow/differential-dataflow/pull/714), [#718](https://github.com/TimelyDataflow/differential-dataflow/pull/718), [#720](https://github.com/TimelyDataflow/differential-dataflow/pull/720))
- `PrefixExtender`, half-join, and calculus traits now parameterized by `'scope` lifetime and `T: Timestamp` instead of `G: Scope` ([#714](https://github.com/TimelyDataflow/differential-dataflow/pull/714), [#718](https://github.com/TimelyDataflow/differential-dataflow/pull/718))

## [0.21.2](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.21.1...differential-dogs3-v0.21.2) - 2026-04-02

### Other

- Consolidate storage types to avoid double allocations
- Re-order stages and improve session batching
- Less quadratic half_join

## [0.21.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.20.0...differential-dogs3-v0.21.0) - 2026-03-25

### Other

- update Cargo.toml dependencies

## [0.20.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.19.1...differential-dogs3-v0.20.0) - 2026-03-05

### Other

- Update github workflows ([#673](https://github.com/TimelyDataflow/differential-dataflow/pull/673))
- Set MSRV to 1.86 ([#672](https://github.com/TimelyDataflow/differential-dataflow/pull/672))
- Differential corrected atop candidate timely 0.27 ([#671](https://github.com/TimelyDataflow/differential-dataflow/pull/671))
- Test mdbook like a crate ([#669](https://github.com/TimelyDataflow/differential-dataflow/pull/669))
- Migrate Join logic away from traits ([#668](https://github.com/TimelyDataflow/differential-dataflow/pull/668))
- More `VecCollection` demotion ([#667](https://github.com/TimelyDataflow/differential-dataflow/pull/667))
- Deprioritize the `Vec` container ([#664](https://github.com/TimelyDataflow/differential-dataflow/pull/664))

## [0.19.1](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.19.0...differential-dogs3-v0.19.1) - 2026-02-06

### Other

- update Cargo.toml dependencies

## [0.18.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.17.0...differential-dogs3-v0.18.0) - 2025-10-23

### Other

- Updates for timely 0.25 ([#647](https://github.com/TimelyDataflow/differential-dataflow/pull/647))
- Extract `VecCollection` from `Collection` ([#651](https://github.com/TimelyDataflow/differential-dataflow/pull/651))
- Correct some clippy nit ([#649](https://github.com/TimelyDataflow/differential-dataflow/pull/649))

## [0.17.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.16.2...differential-dogs3-v0.17.0) - 2025-09-15

### Other

- update Cargo.toml dependencies

## [0.16.2](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.16.1...differential-dogs3-v0.16.2) - 2025-08-28

### Other

- update Cargo.toml dependencies

## [0.16.1](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.16.0...differential-dogs3-v0.16.1) - 2025-08-16

### Other

- update Cargo.toml dependencies

## [0.15.4](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.1.14...differential-dogs3-v0.15.4) - 2025-08-05

### Other

- Remove `BatchContainer::borrow_as()` ([#628](https://github.com/TimelyDataflow/differential-dataflow/pull/628))
- `Layout` extension trait ([#627](https://github.com/TimelyDataflow/differential-dataflow/pull/627))
- Remove `IntoOwned` (phase 1) ([#624](https://github.com/TimelyDataflow/differential-dataflow/pull/624))
- Extract inner function from half-join ([#619](https://github.com/TimelyDataflow/differential-dataflow/pull/619))

## [0.1.14](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.1.13...differential-dogs3-v0.1.14) - 2025-06-24

### Other

- Modernize many where constraints ([#610](https://github.com/TimelyDataflow/differential-dataflow/pull/610))

## [0.1.13](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.1.12...differential-dogs3-v0.1.13) - 2025-05-19

### Other

- updated the following local packages: differential-dataflow

## [0.1.12](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.1.11...differential-dogs3-v0.1.12) - 2025-05-09

### Other

- updated the following local packages: differential-dataflow

## [0.1.11](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.1.10...differential-dogs3-v0.1.11) - 2025-05-09

### Other

- update Cargo.toml dependencies

## [0.1.10](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.1.9...differential-dogs3-v0.1.10) - 2025-04-06

### Other

- updated the following local packages: differential-dataflow

## [0.1.9](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.1.8...differential-dogs3-v0.1.9) - 2025-03-28

### Other

- updated the following local packages: differential-dataflow

## [0.1.8](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.1.7...differential-dogs3-v0.1.8) - 2025-03-28

### Other

- update Cargo.toml dependencies

## [0.1.7](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.1.6...differential-dogs3-v0.1.7) - 2025-02-28

### Other

- Move differential crate from . to directory ([#575](https://github.com/TimelyDataflow/differential-dataflow/pull/575))

## [0.1.6](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.1.5...differential-dogs3-v0.1.6) - 2025-02-12

### Other

- update Cargo.toml dependencies

## [0.1.5](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.1.4...differential-dogs3-v0.1.5) - 2025-01-24

### Other

- updated the following local packages: differential-dataflow

## [0.1.4](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.1.3...differential-dogs3-v0.1.4) - 2025-01-23

### Other

- update Cargo.toml dependencies

## [0.1.3](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.1.2...differential-dogs3-v0.1.3) - 2025-01-09

### Other

- update Cargo.toml dependencies

## [0.1.2](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.1.1...differential-dogs3-v0.1.2) - 2024-12-18

### Other

- update Cargo.toml dependencies

## [0.1.1](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dogs3-v0.1.0...differential-dogs3-v0.1.1) - 2024-11-11

### Other

- Changes to track timely's [#597](https://github.com/TimelyDataflow/differential-dataflow/pull/597) ([#538](https://github.com/TimelyDataflow/differential-dataflow/pull/538))

## [0.1.0](https://github.com/TimelyDataflow/differential-dataflow/releases/tag/differential-dogs3-v0.1.0) - 2024-10-29

Changelog bankruptcy declared.
