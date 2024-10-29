# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/TimelyDataflow/differential-dataflow/releases/tag/differential-dogs3-v0.1.0) - 2024-10-29

### Fixed

- fix example

### Other

- Depend on differential with version ([#536](https://github.com/TimelyDataflow/differential-dataflow/pull/536))
- release ([#534](https://github.com/TimelyDataflow/differential-dataflow/pull/534))
- Add support for release-plz ([#531](https://github.com/TimelyDataflow/differential-dataflow/pull/531))
- Fix typos ([#521](https://github.com/TimelyDataflow/differential-dataflow/pull/521))
- Update to latest Timely ([#519](https://github.com/TimelyDataflow/differential-dataflow/pull/519))
- Make Abelian::negate act on borrowed data ([#504](https://github.com/TimelyDataflow/differential-dataflow/pull/504))
- Introduce Time/Diff GATs ([#502](https://github.com/TimelyDataflow/differential-dataflow/pull/502))
- Introduce IsZero trait for is_zero() ([#503](https://github.com/TimelyDataflow/differential-dataflow/pull/503))
- Remove 'static requirement from difference traits ([#501](https://github.com/TimelyDataflow/differential-dataflow/pull/501))
- Remove `KeyOwned` ([#498](https://github.com/TimelyDataflow/differential-dataflow/pull/498))
- Introduce and integrate `IntoOwned` trait ([#495](https://github.com/TimelyDataflow/differential-dataflow/pull/495))
- Remove `ValOwned` ([#476](https://github.com/TimelyDataflow/differential-dataflow/pull/476))
- dogs^3 compaction improvement ([#457](https://github.com/TimelyDataflow/differential-dataflow/pull/457))
- Introduce trait constraints; simplify elsewhere ([#445](https://github.com/TimelyDataflow/differential-dataflow/pull/445))
- Bring differential to Rust 2021 ([#443](https://github.com/TimelyDataflow/differential-dataflow/pull/443))
- Arrangement GATs ([#438](https://github.com/TimelyDataflow/differential-dataflow/pull/438))
- Update halfjoin to new idioms ([#436](https://github.com/TimelyDataflow/differential-dataflow/pull/436))
- Cursor repivoting ([#435](https://github.com/TimelyDataflow/differential-dataflow/pull/435))
- Fix lookup_map compile error ([#433](https://github.com/TimelyDataflow/differential-dataflow/pull/433))
- Implement `OrdValBatch` without `retain_from` ([#419](https://github.com/TimelyDataflow/differential-dataflow/pull/419))
- Improve the precision of half_join ([#386](https://github.com/TimelyDataflow/differential-dataflow/pull/386))
- Further tidying up of submitted PRs ([#367](https://github.com/TimelyDataflow/differential-dataflow/pull/367))
- Add dogs^3 to workspace and silence warnings ([#349](https://github.com/TimelyDataflow/differential-dataflow/pull/349))
- Allow halfjoin to yield tastefully ([#342](https://github.com/TimelyDataflow/differential-dataflow/pull/342))
- join, half_join: add lower-level interfaces ([#327](https://github.com/TimelyDataflow/differential-dataflow/pull/327))
- Implement half-join operator ([#320](https://github.com/TimelyDataflow/differential-dataflow/pull/320))
- Use bespoke traits in place of std::ops traits ([#319](https://github.com/TimelyDataflow/differential-dataflow/pull/319))
- prepare 0.12 ([#316](https://github.com/TimelyDataflow/differential-dataflow/pull/316))
- Remove use of `timely_sort` crate ([#313](https://github.com/TimelyDataflow/differential-dataflow/pull/313))
- Clarify `Trace` capability nomenclature ([#308](https://github.com/TimelyDataflow/differential-dataflow/pull/308))
- Disable timely's default features in dogsdogsdogs, too ([#299](https://github.com/TimelyDataflow/differential-dataflow/pull/299))
- actually test from dogs3 crate
- change Fn constraint to FnMut
- update projects correctly
- Merge pull request [#255](https://github.com/TimelyDataflow/differential-dataflow/pull/255) from benesch/patch-1
- Move graph_map to a dev dependency in dogsdogsdogs
- more idiomatic code
- improve variable name
- add differentiation and integration
- Merge branch 'master' into dogs_distinguish_less
- Remove the `Default` requirement from keys.
- correct fundamental errors
- Require arrangement sharing to communicate frontier changes.
- Update abomonation_derive to 0.5
- Update abomonation_derive to 0.4
- corrections
- rename to semigroup
- frontier race, fixes [#183](https://github.com/TimelyDataflow/differential-dataflow/pull/183)
- better name
- factor lookup into file
- unify
- checkpoint
- remove Rc
- add Default constraint
- use published dependency
- rework example
- re-organization
- track dd master
- Multiply diffs before testing for zero
- Monoid multiplication in propose and validate
- Reuse arrangement
- Use distinct to count correctly
- Cleanup comments
- Remove maximum for altneu
- Move to AddAssign
- Count correctly
- Relax wco diff trait to Monoid
- update depedencies
- Derive Serialize and Deserialize for AltNeu
- tidy and rename
- readme
- readme
- wcoj example
- dogsdogsdogs update

## [0.1.0](https://github.com/TimelyDataflow/differential-dataflow/releases/tag/differential-dogs3-v0.1.0) - 2024-10-29

Changelog bankruptcy declared.
