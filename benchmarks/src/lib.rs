//! Shared machinery for longitudinal differential dataflow benchmarks.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Serialize;

/// One weighted edge in a canonical benchmark result.
pub type EdgeUpdate = ((u64, u64), i64);

/// The final, consolidated contents of an edge collection.
pub type EdgeResult = Vec<EdgeUpdate>;

/// Timings for phases whose boundaries are shared by all implementations.
#[derive(Clone, Copy, Debug, Default)]
pub struct Timings {
    pub prepare: Duration,
    pub build: Duration,
    pub ingest: Duration,
    pub stabilize: Duration,
}

impl Timings {
    pub fn measured(&self) -> Duration {
        self.prepare + self.build + self.ingest + self.stabilize
    }
}

/// The result of running one implementation once.
#[derive(Debug)]
pub struct Run {
    pub implementation: &'static str,
    pub timings: Timings,
    pub output: EdgeResult,
}

/// One machine-readable result row.
#[derive(Debug, Serialize)]
pub struct Record<'a> {
    pub schema: u32,
    pub benchmark: &'a str,
    pub implementation: &'a str,
    pub revision: &'a str,
    pub run: usize,
    pub nodes: u64,
    pub edges: u64,
    pub seed: u64,
    pub prepare_ns: u128,
    pub build_ns: u128,
    pub ingest_ns: u128,
    pub stabilize_ns: u128,
    pub measured_ns: u128,
    pub output_records: usize,
    pub output_weight: i64,
    pub output_digest: String,
    pub correct: bool,
}

/// Parameters shared by the four records in one benchmark run.
#[derive(Clone, Copy, Debug)]
pub struct Context<'a> {
    pub benchmark: &'a str,
    pub revision: &'a str,
    pub run: usize,
    pub nodes: u64,
    pub edges: u64,
    pub seed: u64,
}

impl<'a> Record<'a> {
    pub fn from_run(context: Context<'a>, run: &'a Run, correct: bool) -> Self {
        Record {
            schema: 1,
            benchmark: context.benchmark,
            implementation: run.implementation,
            revision: context.revision,
            run: context.run,
            nodes: context.nodes,
            edges: context.edges,
            seed: context.seed,
            prepare_ns: run.timings.prepare.as_nanos(),
            build_ns: run.timings.build.as_nanos(),
            ingest_ns: run.timings.ingest.as_nanos(),
            stabilize_ns: run.timings.stabilize.as_nanos(),
            measured_ns: run.timings.measured().as_nanos(),
            output_records: run.output.len(),
            output_weight: run.output.iter().map(|(_, diff)| *diff).sum(),
            output_digest: digest(&run.output),
            correct,
        }
    }
}

/// Generate a deterministic directed multigraph.
pub fn graph(nodes: u64, edges: u64, seed: u64) -> Vec<(u64, u64)> {
    assert!(nodes > 0, "nodes must be positive");
    let mut state = seed;
    (0..edges)
        .map(|_| {
            let src = xorshift(&mut state) % nodes;
            let dst = xorshift(&mut state) % nodes;
            (src, dst)
        })
        .collect()
}

/// Consolidate an iterator of weighted edges in a stable order.
pub fn consolidate<I>(updates: I) -> EdgeResult
where
    I: IntoIterator<Item = EdgeUpdate>,
{
    let mut result = BTreeMap::new();
    for (edge, diff) in updates {
        *result.entry(edge).or_insert(0) += diff;
    }
    result.into_iter().filter(|(_, diff)| *diff != 0).collect()
}

/// A stable digest for reporting, not a substitute for the full equality check.
pub fn digest(result: &EdgeResult) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for &((src, dst), diff) in result {
        for word in [src, dst, diff as u64] {
            for byte in word.to_le_bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x100000001b3);
            }
        }
    }
    format!("{hash:016x}")
}

/// Resolve the current revision without making it part of a timed region.
pub fn revision() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--verify", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|revision| revision.trim().to_owned())
        .filter(|revision| !revision.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_is_repeatable() {
        assert_eq!(graph(10, 20, 7), graph(10, 20, 7));
        assert_ne!(graph(10, 20, 7), graph(10, 20, 8));
    }

    #[test]
    fn consolidation_is_ordered_and_nets_differences() {
        assert_eq!(
            consolidate([((2, 1), 1), ((1, 2), 2), ((2, 1), -1)]),
            vec![((1, 2), 2)],
        );
    }
}
