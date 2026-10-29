//! Wall times for complete pass calls. No clocks or scope walks per rewrite.

use std::time::{Duration, Instant};
use crate::scope_ir::{Program, RowShape, Source};
use crate::shapes::{self, Problems, ScopeShapes};
use super::{Change, Observe};

/// Accumulated calls and rewrite observations, including work on rejected candidates.
#[derive(Clone, Copy, Debug, Default)]
pub struct Measurement {
    pub elapsed: Duration,
    pub runs: usize,
    pub changes: usize,
}

impl Measurement {
    pub fn record(&mut self, elapsed: Duration, changes: usize) {
        self.elapsed += elapsed;
        self.runs += 1;
        self.changes += changes;
    }

    pub(super) fn observed<T>(&mut self, observe: &mut Observe<'_>, run: impl FnOnce(&mut Observe<'_>) -> T) -> T {
        let start = Instant::now();
        let mut changes = 0;
        let result = run(&mut |path, scope, ordinals, action, from, to| {
            changes += 1;
            observe(path, scope, ordinals, action, from, to);
        });
        self.record(start.elapsed(), changes);
        result
    }

    pub(super) fn rewrites(&mut self, run: impl FnOnce() -> usize) -> bool {
        let start = Instant::now();
        let changes = run();
        self.record(start.elapsed(), changes);
        changes > 0
    }

    pub(super) fn infer_with_root(&mut self, program: &Program, source: &dyn Fn(&Source) -> Option<RowShape>, root: usize) -> (ScopeShapes, Problems) {
        let start = Instant::now();
        let result = shapes::infer_with_root(program, source, root);
        self.record(start.elapsed(), 0);
        result
    }
}

/// The order used by the timing table; these names do not change pass configuration.
#[derive(Clone, Copy, Debug)]
pub enum Pass { DeadCode, Structural, ComposeMaps, HoistInvariant, CollapseParentArranges, MergeImports, ShapeInference }

impl Pass {
    pub const ALL: [Self; 7] = [Self::DeadCode, Self::Structural, Self::ComposeMaps, Self::HoistInvariant, Self::CollapseParentArranges, Self::MergeImports, Self::ShapeInference];

    pub fn name(self) -> &'static str {
        match self {
            Self::Structural => "structural",
            Self::ComposeMaps => "compose-maps",
            Self::DeadCode => "dead-code",
            Self::HoistInvariant => "hoist-invariant",
            Self::CollapseParentArranges => "collapse-parent-arranges",
            Self::MergeImports => "merge_imports",
            Self::ShapeInference => "shape inference",
        }
    }
}

/// Stage times exclude observer callbacks. Pass times are nested within the stages;
/// they include optimizer calls made by projection, even on rejected candidates.
/// Shape inference covers the schedule and projection checks, excluding report snapshots.
#[derive(Clone, Debug, Default)]
pub struct Timings {
    /// `run` receives lowered IR. The caller supplies this measurement when available.
    pub lowering: Measurement,
    pub optimizer: Measurement,
    pub projection: Measurement,
    pub as_of_drop: Measurement,
    pub observers: Measurement,
    pub passes: [Measurement; 7],
    /// Complete optimizer rounds, including the final round with no changes.
    pub optimizer_rounds: usize,
    /// Hoisting rounds before structural fusion, including the final stable round.
    pub hoist_rounds: usize,
    /// Projection/optimizer alternations, including the final stable round.
    pub projection_rounds: usize,
}

impl Timings {
    pub fn pass(&self, pass: Pass) -> &Measurement { &self.passes[pass as usize] }
    pub(super) fn pass_mut(&mut self, pass: Pass) -> &mut Measurement { &mut self.passes[pass as usize] }

    pub(super) fn observe(&mut self, name: &'static str, program: &Program, changes: &[Change], observe: &mut dyn FnMut(&'static str, &Program, &[Change])) {
        let start = Instant::now();
        observe(name, program, changes);
        self.observers.record(start.elapsed(), 0);
    }
}
