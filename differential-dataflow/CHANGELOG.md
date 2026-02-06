# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.19.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.18.0...differential-dataflow-v0.19.0) - 2026-02-06

### Other

- Back Pointstamp with SmallVec ([#661](https://github.com/TimelyDataflow/differential-dataflow/pull/661))
- Correct chainless lower bound determination ([#660](https://github.com/TimelyDataflow/differential-dataflow/pull/660))
- Exchange columns more efficiently ([#656](https://github.com/TimelyDataflow/differential-dataflow/pull/656))
- fix broken example link ([#654](https://github.com/TimelyDataflow/differential-dataflow/pull/654))

## [0.18.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.17.0...differential-dataflow-v0.18.0) - 2025-10-23

### Other

- Updates for timely 0.25 ([#647](https://github.com/TimelyDataflow/differential-dataflow/pull/647))
- Replace IndexMap with BTreeMap ([#652](https://github.com/TimelyDataflow/differential-dataflow/pull/652))
- Update columnar; introduce mimalloc
- Extract `VecCollection` from `Collection` ([#651](https://github.com/TimelyDataflow/differential-dataflow/pull/651))
- Introduce traits for collection containers ([#650](https://github.com/TimelyDataflow/differential-dataflow/pull/650))
- Correct some clippy nit ([#649](https://github.com/TimelyDataflow/differential-dataflow/pull/649))
- Batcher implementation that has no opinions about chains, and columnar. ([#626](https://github.com/TimelyDataflow/differential-dataflow/pull/626))

## [0.17.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.16.2...differential-dataflow-v0.17.0) - 2025-09-15

### Other

- Adjust Differential to recent Timely changes ([#643](https://github.com/TimelyDataflow/differential-dataflow/pull/643))
- Remove `Filter` and `Freeze` wrappers ([#644](https://github.com/TimelyDataflow/differential-dataflow/pull/644))

## [0.16.2](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.16.1...differential-dataflow-v0.16.2) - 2025-08-28

### Other

- update Cargo.toml dependencies

## [0.16.1](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.16.0...differential-dataflow-v0.16.1) - 2025-08-16

### Other

- update Cargo.toml dependencies

## [0.16.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.15.4...differential-dataflow-v0.16.0) - 2025-08-07

### Other

- Simplify type arguments to reduce, make Upds fields pub ([#632](https://github.com/TimelyDataflow/differential-dataflow/pull/632))

## [0.15.4](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.15.3...differential-dataflow-v0.15.4) - 2025-08-05

### Other

- Inline consolidate fast-path ([#629](https://github.com/TimelyDataflow/differential-dataflow/pull/629))
- Remove `BatchContainer::borrow_as()` ([#628](https://github.com/TimelyDataflow/differential-dataflow/pull/628))
- `Layout` extension trait ([#627](https://github.com/TimelyDataflow/differential-dataflow/pull/627))
- Remove `IntoOwned` (phase 1) ([#624](https://github.com/TimelyDataflow/differential-dataflow/pull/624))
- Remove redundant bounds ([#623](https://github.com/TimelyDataflow/differential-dataflow/pull/623))
- Update comment
- Use trie abstractions for batch implementations ([#616](https://github.com/TimelyDataflow/differential-dataflow/pull/616))
- Bump columnar to 0.8.0 ([#620](https://github.com/TimelyDataflow/differential-dataflow/pull/620))
- Use BTreeMap to avoid sorting ([#615](https://github.com/TimelyDataflow/differential-dataflow/pull/615))
- Respect singleton counts in merge effort ([#614](https://github.com/TimelyDataflow/differential-dataflow/pull/614))
- Demonstrate `Columnar` batch builder ([#602](https://github.com/TimelyDataflow/differential-dataflow/pull/602))

## [0.15.3](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.15.2...differential-dataflow-v0.15.3) - 2025-06-24

### Other

- Update columnar to 0.6 ([#611](https://github.com/TimelyDataflow/differential-dataflow/pull/611))
- Remove use of borrow_as from trace wrappers ([#609](https://github.com/TimelyDataflow/differential-dataflow/pull/609))
- Modernize many where constraints ([#610](https://github.com/TimelyDataflow/differential-dataflow/pull/610))
- Added example commands to examples/multitemporal.rs ([#607](https://github.com/TimelyDataflow/differential-dataflow/pull/607))

## [0.15.2](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.15.1...differential-dataflow-v0.15.2) - 2025-05-19

### Other

- Make module ord_neu::val_batch public ([#603](https://github.com/TimelyDataflow/differential-dataflow/pull/603))

## [0.15.1](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.15.0...differential-dataflow-v0.15.1) - 2025-05-09

### Other

- Update columnar to 0.5 ([#600](https://github.com/TimelyDataflow/differential-dataflow/pull/600))

## [0.15.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.14.2...differential-dataflow-v0.15.0) - 2025-05-09

### Other

- Update timely dependence to `0.21`. ([#599](https://github.com/TimelyDataflow/differential-dataflow/pull/599))
- Document the collection invariant ([#384](https://github.com/TimelyDataflow/differential-dataflow/pull/384))
- Modernize `Cursor` API ([#596](https://github.com/TimelyDataflow/differential-dataflow/pull/596))

## [0.14.2](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.14.1...differential-dataflow-v0.14.2) - 2025-04-06

### Other

- Make builder::result members public ([#591](https://github.com/TimelyDataflow/differential-dataflow/pull/591))

## [0.14.1](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.14.0...differential-dataflow-v0.14.1) - 2025-03-28

### Other

- Update columnar to 0.4 ([#589](https://github.com/TimelyDataflow/differential-dataflow/pull/589))

## [0.14.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.7...differential-dataflow-v0.14.0) - 2025-03-28

### Other

- Test against TD with linear reachability ([#588](https://github.com/TimelyDataflow/differential-dataflow/pull/588))
- Remove deprecated functions and use statements ([#582](https://github.com/TimelyDataflow/differential-dataflow/pull/582))

## [0.13.7](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.6...differential-dataflow-v0.13.7) - 2025-02-28

### Other

- Move changelog to differential-dataflow crate ([#581](https://github.com/TimelyDataflow/differential-dataflow/pull/581))
- Move differential crate from . to directory ([#575](https://github.com/TimelyDataflow/differential-dataflow/pull/575))

## [0.13.6](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.5...differential-dataflow-v0.13.6) - 2025-02-12

### Other

- Update Timely, Columnar versions ([#569](https://github.com/TimelyDataflow/differential-dataflow/pull/569))
- Variable supports container streams ([#564](https://github.com/TimelyDataflow/differential-dataflow/pull/564))
- Fix Timely master check, pin dependency version in mdbook ([#568](https://github.com/TimelyDataflow/differential-dataflow/pull/568))
- Provide prescriptive versions in example

## [0.13.5](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.4...differential-dataflow-v0.13.5) - 2025-01-24

### Other

- Derive columnar for point stamp ([#562](https://github.com/TimelyDataflow/differential-dataflow/pull/562))

## [0.13.4](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.3...differential-dataflow-v0.13.4) - 2025-01-23

### Other

- Update to timely 0.17 ([#561](https://github.com/TimelyDataflow/differential-dataflow/pull/561))
- Define columnation chunker for all (D,T,R) ([#559](https://github.com/TimelyDataflow/differential-dataflow/pull/559))

## [0.13.3](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.2...differential-dataflow-v0.13.3) - 2025-01-09

### Other

- Incorporate breaking changes from Timely's logging update ([#558](https://github.com/TimelyDataflow/differential-dataflow/pull/558))
- Derive columnar for log events ([#557](https://github.com/TimelyDataflow/differential-dataflow/pull/557))
- Correct capacity logic
- Demonstrate container input batching ([#556](https://github.com/TimelyDataflow/differential-dataflow/pull/556))
- Work towards `Batcher` unification ([#553](https://github.com/TimelyDataflow/differential-dataflow/pull/553))

## [0.13.2](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.1...differential-dataflow-v0.13.2) - 2024-12-18

### Other

- Update to track timely changes ([#554](https://github.com/TimelyDataflow/differential-dataflow/pull/554))
- Consolidation consolidation ([#552](https://github.com/TimelyDataflow/differential-dataflow/pull/552))
- Pass description itself to builder ([#551](https://github.com/TimelyDataflow/differential-dataflow/pull/551))
- Simplify ContainerChunker::push_into ([#549](https://github.com/TimelyDataflow/differential-dataflow/pull/549))
- Remove time from MergeBatcher ([#550](https://github.com/TimelyDataflow/differential-dataflow/pull/550))
- Changes to track timely master ([#542](https://github.com/TimelyDataflow/differential-dataflow/pull/542))
- Remove (key, val) structure from merge batchers ([#548](https://github.com/TimelyDataflow/differential-dataflow/pull/548))
- Merge batcher for flat container without key and value ([#547](https://github.com/TimelyDataflow/differential-dataflow/pull/547))
- Move `Batcher::seal` to `Builder` ([#546](https://github.com/TimelyDataflow/differential-dataflow/pull/546))
- Extract `Builder` from `Trace` ([#545](https://github.com/TimelyDataflow/differential-dataflow/pull/545))
- Remove batcher from Trace ([#544](https://github.com/TimelyDataflow/differential-dataflow/pull/544))
- Build against timely master ([#539](https://github.com/TimelyDataflow/differential-dataflow/pull/539))

## [0.13.1](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.0...differential-dataflow-v0.13.1) - 2024-11-11

### Other

- Changes to track timely's [#597](https://github.com/TimelyDataflow/differential-dataflow/pull/597) ([#538](https://github.com/TimelyDataflow/differential-dataflow/pull/538))

## [0.13.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.12.0...differential-dataflow-v0.13.0) - 2024-10-29

Changelog bankruptcy declared.
