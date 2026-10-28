//! Reviewed cases. Paths are relative to the interactive crate.
// Providers must precede their consumers. Each case gets only its named catalog.
use super::Case;

pub(super) const FORMAT: &str = "3 (whole DDP and stage diffs; desugared reductions)";
pub(super) const LANGUAGE: &str = "DDP (interactive 0.1.0); corgi lambdas (Cargo.lock)";
pub(super) const BACKEND: &str = "interactive 0.1.0 corgi backend; kernels pinned in Cargo.lock";
pub(super) const TRANSFORMS: &str = "6: desugar reductions and expand distinct; dead-code before structural; collapse arrangements, fuse linear, dedup; collapse parent arrangements; hoist invariant statements; canonical linear chains; projection; as-of drop";
pub(super) const ASSUMPTIONS: &str = "arranged trace imports; projection on; as-of on; G = (1, []); install epoch 2; no data or planner seed";

pub(super) const GENERATED: &[(&str, &str)] = &[
    ("graph", "generated/graph.ddp"),
    ("cascade", "generated/cascade.ddp"),
];

pub(super) const CASES: &[Case] = &[
    Case::new("ldbc_graph", "tests/plan_pairs/generated/graph.ddp", &[]),
    Case::new("ldbc_cascade", "tests/plan_pairs/generated/cascade.ddp", &[]),
    Case::new("ldbc_is01", "bench/ldbc/authored/plans/is01.ddp", &["ldbc_graph"]),
    Case::new("ldbc_is02", "bench/ldbc/authored/plans/is02.ddp", &["ldbc_graph"]),
    Case::new("ldbc_is03", "bench/ldbc/authored/plans/is03.ddp", &["ldbc_graph"]),
    Case::new("ldbc_is04", "bench/ldbc/authored/plans/is04.ddp", &["ldbc_graph"]),
    Case::new("ldbc_is05", "bench/ldbc/authored/plans/is05.ddp", &["ldbc_graph"]),
    Case::new("ldbc_is06", "bench/ldbc/authored/plans/is06.ddp", &["ldbc_graph"]),
    Case::new("ldbc_is07", "bench/ldbc/authored/plans/is07.ddp", &["ldbc_graph"]),
    Case::new("ldbc_ic01_oneoff", "bench/ldbc/interactive_v2/oneoff/ic01.oneoff.ddp", &["ldbc_graph"]).request("ic1"),
    Case::new("ldbc_ic02_standing", "bench/ldbc/interactive_v2/oneoff/ic02.standing.ddp", &["ldbc_graph"]),
    Case::new("ldbc_ic02_oneoff", "bench/ldbc/interactive_v2/oneoff/ic02.oneoff.ddp", &["ldbc_graph", "ldbc_ic02_standing"]).request("ic2"),
    Case::new("ldbc_ic03_standing", "bench/ldbc/interactive_v2/oneoff/ic03.standing.ddp", &["ldbc_graph"]),
    Case::new("ldbc_ic03_oneoff", "bench/ldbc/interactive_v2/oneoff/ic03.oneoff.ddp", &["ldbc_graph", "ldbc_ic03_standing"]).request("ic3"),
    Case::new("ldbc_ic04_standing", "bench/ldbc/interactive_v2/oneoff/ic04.standing.ddp", &["ldbc_graph"]),
    Case::new("ldbc_ic04_oneoff", "bench/ldbc/interactive_v2/oneoff/ic04.oneoff.ddp", &["ldbc_graph", "ldbc_ic04_standing"]).request("ic4"),
    Case::new("ldbc_ic05_standing", "bench/ldbc/interactive_v2/oneoff/ic05.standing.ddp", &["ldbc_graph"]),
    Case::new("ldbc_ic05_oneoff", "bench/ldbc/interactive_v2/oneoff/ic05.oneoff.ddp", &["ldbc_graph", "ldbc_ic05_standing"]).request("ic5"),
    Case::new("ldbc_ic06_oneoff", "bench/ldbc/interactive_v2/oneoff/ic06.oneoff.ddp", &["ldbc_graph"]).request("ic6"),
    Case::new("ldbc_ic07_oneoff", "bench/ldbc/interactive_v2/oneoff/ic07.oneoff.ddp", &["ldbc_graph"]).request("ic7"),
    Case::new("ldbc_ic08_oneoff", "bench/ldbc/interactive_v2/oneoff/ic08.oneoff.ddp", &["ldbc_graph"]).request("ic8"),
    Case::new("ldbc_ic09_standing", "bench/ldbc/interactive_v2/oneoff/ic09.standing.ddp", &["ldbc_graph"]),
    Case::new("ldbc_ic09_oneoff", "bench/ldbc/interactive_v2/oneoff/ic09.oneoff.ddp", &["ldbc_graph", "ldbc_ic09_standing"]).request("ic9"),
    Case::new("ldbc_ic10_standing", "bench/ldbc/interactive_v2/oneoff/ic10.standing.ddp", &["ldbc_graph"]),
    Case::new("ldbc_ic10_oneoff", "bench/ldbc/interactive_v2/oneoff/ic10.oneoff.ddp", &["ldbc_graph", "ldbc_ic10_standing"]).request("ic10"),
    Case::new("ldbc_ic11_oneoff", "bench/ldbc/interactive_v2/oneoff/ic11.oneoff.ddp", &["ldbc_graph"]).request("ic11"),
    Case::new("ldbc_ic12_standing", "bench/ldbc/interactive_v2/oneoff/ic12.standing.ddp", &["ldbc_graph"]),
    Case::new("ldbc_ic12_oneoff", "bench/ldbc/interactive_v2/oneoff/ic12.oneoff.ddp", &["ldbc_graph", "ldbc_ic12_standing"]).request("ic12"),
    Case::new("ldbc_ic13_oneoff", "bench/ldbc/interactive_v2/oneoff/ic13.oneoff.ddp", &["ldbc_graph"]).request("ic13"),
    Case::new("ldbc_ic14_standing", "bench/ldbc/interactive_v2/oneoff/ic14.standing.ddp", &["ldbc_graph"]),
    Case::new("ldbc_ic14_oneoff", "bench/ldbc/interactive_v2/oneoff/ic14.oneoff.ddp", &["ldbc_graph", "ldbc_ic14_standing"]).request("ic14"),
    Case::new("txn_store", "bench/txn/store.ddp", &[]),
    Case::new("txn_maintained", "bench/txn/maintained.ddp", &[]),
    Case::new("txn_resolve", "bench/txn/resolve.ddp", &["txn_store"]),
    Case::new("txn_resolve_by_key", "bench/txn/resolve_by_key.ddp", &["txn_store"]),
    Case::new("server_sources", "tests/plan_pairs/cases/server_sources.ddp", &[]),
    Case::new("server_producer", "examples/server/programs/producer.ddp", &[]),
    Case::new("server_access", "examples/server/programs/access.ddp", &[]),
    Case::new("server_arrange_idem", "examples/server/programs/arrange_idem.ddp", &[]),
    Case::new("server_clock_watch", "examples/server/programs/clock_watch.ddp", &["server_sources"]),
    Case::new("server_count_gen", "examples/server/programs/count_gen.ddp", &["server_sources"]),
    Case::new("server_echo", "examples/server/programs/echo.ddp", &[]),
    Case::new("server_rand_reach", "examples/server/programs/rand_reach.ddp", &["server_sources"]),
    Case::new("server_reach_gen", "examples/server/programs/reach_gen.ddp", &["server_sources"]),
    Case::new("server_reach_import", "examples/server/programs/reach_import.ddp", &["server_producer"]),
    Case::new("access_shared", "tests/plan_pairs/cases/access_shared.ddp", &[]),
    Case::new("arrange_collapse", "tests/plan_pairs/cases/arrange_collapse.ddp", &[]),
    Case::new("fusion_dedup", "tests/plan_pairs/cases/fusion_dedup.ddp", &[]),
    Case::new("concat_masks", "tests/plan_pairs/cases/concat_masks.ddp", &[]),
    Case::new("reduce_masks", "tests/plan_pairs/cases/reduce_masks.ddp", &[]),
    Case::new("shared_demand", "tests/plan_pairs/cases/shared_demand.ddp", &[]),
    Case::new("non_unit_distinct", "tests/plan_pairs/cases/non_unit_distinct.ddp", &[]),
    Case::new("nested", "tests/plan_pairs/cases/nested.ddp", &[]),
    Case::new("enums", "tests/plan_pairs/cases/enums.ddp", &[]),
    Case::new("rows", "tests/plan_pairs/cases/rows.ddp", &[]),
    Case::new("as_of_strict", "tests/plan_pairs/cases/as_of_strict.ddp", &["rows"]),
    Case::new("as_of_shared", "tests/plan_pairs/cases/as_of_shared.ddp", &["rows"]),
    Case::new("explain", "tests/plan_pairs/cases/explain.ddp", &[]).explain(),
    Case::new("rejected_export", "tests/plan_pairs/cases/rejected_export.ddp", &[]).problem("export `out`"),
    Case::new("registered", "tests/plan_pairs/cases/registered.ddp", &[]).functions("plan_pair_identity(int) -> int; pure identity"),
];
