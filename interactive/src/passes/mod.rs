//! Scope-tree rewrites. `run` is the preparation schedule; all host choices are inputs.

mod structural;
mod dead_code;
mod arrangements;
mod canonical;
pub mod projection;
mod as_of;
mod timing;
pub use timing::{Measurement, Pass, Timings};

use std::time::Instant;

use crate::ir::Time;
use crate::scope_ir::{Item, Program, RowShape, Scope, Source};

/// A statement label and its operator ordinal before compaction.
pub struct ItemName {
    pub label: String,
    pub ordinal: usize,
}

impl ItemName {
    fn new(scope: &Scope, ordinals: &[usize], index: usize) -> Self {
        if scope.labels.len() == scope.items.len() {
            let label = scope.labels[index].clone();
            let ordinal = ordinals[index];
            Self { label, ordinal }
        } else {
            Self { label: format!("unlabeled item {index}"), ordinal: 1 }
        }
    }
}

/// Count each label once per walk, before items are removed or renumbered.
fn item_ordinals(scope: &Scope) -> Vec<usize> {
    let mut counts = std::collections::HashMap::new();
    scope.items.iter().zip(&scope.labels).map(|(item, label)| {
        let count = counts.entry(label).or_insert(0);
        if matches!(item, Item::Op(_)) { *count += 1; }
        *count
    }).collect()
}

type Observe<'a> = dyn FnMut(&str, &Scope, &[usize], &'static str, usize, usize) + 'a;

/// Which passes `run` applies. Every pass is optional: the runtime runs any program the
/// language accepts, with the same answers, whichever of them ran. The default excludes projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// Remove unread computations, child exports, imports, and aliases; keep root inputs.
    pub dead_code: bool,
    /// Collapse arranges of arrangements, fuse linear chains, merge identical operators.
    pub structural: bool,
    /// Remove arranges of parent bindings that already carry arrangements.
    pub collapse_parent_arranges: bool,
    /// Move statements whose inputs are outside the scope, preserving time and effect barriers.
    pub hoist_invariant: bool,
    /// Compose adjacent maps, drop identity maps, cancel paired negations.
    pub compose_maps: bool,
    /// Replace fields nothing reads by `()`.
    pub projection: bool,
    /// Drop updates at the as-of time that only strict half joins would read.
    pub as_of_drop: bool,
}

impl Default for Config {
    fn default() -> Self { Self { projection: false, ..Self::all() } }
}

impl Config {
    /// The names `parse` takes, in reporting order.
    pub const NAMES: [&'static str; 7] = ["dead-code", "structural", "collapse-parent-arranges", "hoist-invariant", "compose-maps", "projection", "as-of-drop"];

    pub fn all() -> Self { Self { structural: true, collapse_parent_arranges: true, hoist_invariant: true, compose_maps: true, dead_code: true, projection: true, as_of_drop: true } }
    pub fn none() -> Self { Self { structural: false, collapse_parent_arranges: false, hoist_invariant: false, compose_maps: false, dead_code: false, projection: false, as_of_drop: false } }

    /// Start from the default passes and apply comma-separated words in order: `all`, `none`, a pass
    /// name to turn it on, or `-name` to turn it off. So `none`, `-projection` and
    /// `none,structural` all mean what they say. `hoist-arranges` is an alias for `hoist-invariant`.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut config = Self::default();
        for word in text.split(',').map(str::trim).filter(|w| !w.is_empty()) {
            match word {
                "all" => config = Self::all(),
                "none" => config = Self::none(),
                _ => {
                    let (name, on) = match word.strip_prefix('-') { Some(name) => (name, false), None => (word, true) };
                    *config.flag(name).ok_or_else(|| format!("unknown pass {name:?}; passes are all, none, {}", Self::NAMES.join(", ")))? = on;
                }
            }
        }
        Ok(config)
    }

    /// The passes switched off, by name, in schedule order.
    pub fn off(&self) -> Vec<&'static str> {
        let on = [self.dead_code, self.structural, self.collapse_parent_arranges, self.hoist_invariant, self.compose_maps, self.projection, self.as_of_drop];
        Self::NAMES.into_iter().zip(on).filter(|(_, on)| !on).map(|(name, _)| name).collect()
    }

    fn flag(&mut self, name: &str) -> Option<&mut bool> {
        match name {
            "structural" => Some(&mut self.structural),
            "collapse-parent-arranges" => Some(&mut self.collapse_parent_arranges),
            "hoist-invariant" | "hoist-arranges" => Some(&mut self.hoist_invariant),
            "compose-maps" => Some(&mut self.compose_maps),
            "dead-code" => Some(&mut self.dead_code),
            "projection" => Some(&mut self.projection),
            "as-of-drop" => Some(&mut self.as_of_drop),
            _ => None,
        }
    }
}

/// Changes made by a stage. Statement-level display diffs belong to the observer.
pub enum Change {
    Structural { path: String, action: &'static str, from: ItemName, to: ItemName },
    /// Aggregate rewrites without statement-level display diffs.
    Rewritten { pass: Pass, count: usize },
    Masked(usize),
    AsOf(usize),
    NoProjection,
    Rejected,
    Skipped,
    /// The named pass is switched off.
    Off(&'static str),
}

impl Change {
    /// Rewrite events, field masks, and inserted filters; status messages count zero.
    pub fn count(&self) -> usize {
        match self {
            Self::Structural { .. } => 1,
            Self::Rewritten { count, .. } | Self::Masked(count) | Self::AsOf(count) => *count,
            _ => 0,
        }
    }
}

/// Run the optimizer, projection, and the as-of drop, in that order, each as `config` says.
/// `source` supplies provider shapes, including generated sources. A declared trace
/// shape alone does not resolve its provider. Observations include the lowered tree
/// and every stage, even one that is switched off, has no changes, or is skipped.
/// Times have `root` root coordinates, followed by one per loop.
/// All row and arrangement times must be at or beyond `as_of`.
///
/// Identical import slots are always interned, as in lowering, even with all optional
/// passes off. This also normalizes programs supplied directly as IR.
/// Optimization runs before checking providers so unread imports can disappear.
/// Projection and as-of candidates must infer without problems and preserve the optimizer's
/// export shapes. A rejected candidate leaves the last accepted program in place.
pub fn run(
    program: &mut Program,
    source: &dyn Fn(&Source) -> Option<RowShape>,
    config: &Config,
    as_of: &Time,
    root: usize,
    observe: &mut dyn FnMut(&'static str, &Program, &[Change]),
) -> Timings {
    let mut timings = Timings::default();
    timings.observe("as lowered", program, &[], observe);
    let start = Instant::now();
    let mut changes = Vec::new();
    optimize_with(program, source, root, config, &mut timings, &mut |path, scope, ordinals, action, from, to| {
        changes.push(Change::Structural {
            path: path.into(), action,
            from: ItemName::new(scope, ordinals, from), to: ItemName::new(scope, ordinals, to),
        });
    });
    for pass in [Pass::DeadCode, Pass::ComposeMaps, Pass::MergeImports] {
        let count = timings.pass(pass).changes;
        if count > 0 { changes.push(Change::Rewritten { pass, count }); }
    }
    timings.optimizer.record(start.elapsed(), changes.iter().map(Change::count).sum());
    for (on, name) in [(config.dead_code, "dead-code"), (config.structural, "structural"), (config.collapse_parent_arranges, "collapse-parent-arranges"), (config.hoist_invariant, "hoist-invariant"), (config.compose_maps, "compose-maps")] {
        if !on { changes.push(Change::Off(name)); }
    }
    timings.observe("after optimizer", program, &changes, observe);
    let start = Instant::now();
    let (shapes, problems) = timings.pass_mut(Pass::ShapeInference).infer(program, source, root);
    let valid = problems.conflicts.is_empty() && problems.unknown.is_empty()
        && program.root.vars.is_empty()
        && program.root.imports.iter().all(|import| match &import.from {
            Source::Trace(_) => source(&import.from).is_some_and(|shape| import.shape.as_ref().is_none_or(|declared| *declared == shape)),
            Source::Input(_) => import.shape.is_some(),
            Source::Parent(_) => false,
        });
    let accept = |candidate: &Program, timings: &mut Timings| {
        let (checked, problems) = timings.pass_mut(Pass::ShapeInference).infer(candidate, source, root);
        problems.conflicts.is_empty() && problems.unknown.is_empty() && checked.exports == shapes.exports
    };
    changes.clear();
    if !config.projection { changes.push(Change::Off("projection")); }
    else if valid {
        let mut candidate = program.clone();
        let mut masked = 0;
        let mut converged = false;
        for _ in 0..16 {
            timings.projection_rounds += 1;
            let count = projection::pushdown_timed(&mut candidate, source, root, timings.pass_mut(Pass::ShapeInference)).unwrap_or(0);
            if count == 0 { converged = true; break; }
            // Masks expose optimizer rewrites. In turn, merging identical masks can
            // give a reduce one reader and make its remaining map fusible.
            optimize_with(&mut candidate, source, root, config, &mut timings, &mut |_, _, _, _, _, _| {});
            if !accept(&candidate, &mut timings) { break; }
            masked += count;
        }
        if converged {
            if masked > 0 {
                changes.push(Change::Masked(masked));
                *program = candidate;
            } else { changes.push(Change::NoProjection); }
        } else { changes.push(Change::Rejected); }
    } else { changes.push(Change::Skipped); }
    timings.projection.record(start.elapsed(), changes.iter().map(Change::count).sum());
    timings.observe("after projection", program, &changes, observe);
    let start = Instant::now();
    changes.clear();
    if !config.as_of_drop { changes.push(Change::Off("as-of-drop")); }
    else if valid {
        let mut candidate = program.clone();
        let inserted = as_of::drop_dead_as_of(&mut candidate, as_of, root);
        if inserted > 0 {
            if accept(&candidate, &mut timings) {
                changes.push(Change::AsOf(inserted));
                *program = candidate;
            } else { changes.push(Change::Rejected); }
        }
    } else { changes.push(Change::Skipped); }
    timings.as_of_drop.record(start.elapsed(), changes.iter().map(Change::count).sum());
    timings.observe("after as-of drop", program, &changes, observe);
    timings
}

/// Optimize every scope in place with every optimizer pass, without provider shapes.
pub fn optimize(program: &mut Program) {
    optimize_with(program, &|_| None, 1, &Config::all(), &mut Timings::default(), &mut |_, _, _, _, _, _| {});
}

fn optimize_with(program: &mut Program, source: &dyn Fn(&Source) -> Option<RowShape>, root: usize, config: &Config, timings: &mut Timings, observe: &mut Observe<'_>) {
    // Move pure statements before fusion can absorb them into a time-sensitive chain.
    // Finish dependent moves too: a hoisted arrange can expose a join, then a map.
    if config.hoist_invariant {
        loop {
            timings.hoist_rounds += 1;
            if !arrangements::run(&mut program.root, config.collapse_parent_arranges, true, timings, observe) { break; }
        }
    }
    loop {
        timings.optimizer_rounds += 1;
        let mut changed = false;
        // Remove dead readers before structural fusion counts uses. This shares
        // the optimizer's fixed point; liveness itself makes one backward walk.
        if config.dead_code {
            changed |= timings.pass_mut(Pass::DeadCode).rewrites(|| dead_code::run(&mut program.root));
        }
        if config.structural {
            let before = timings.pass(Pass::Structural).changes;
            timings.pass_mut(Pass::Structural).observed(observe, |observe| structural::optimize_scope(&mut program.root, "root", observe));
            // Redirects can leave redundant names for the next dead-code pass.
            changed |= timings.pass(Pass::Structural).changes != before;
        }
        if config.collapse_parent_arranges || config.hoist_invariant {
            changed |= arrangements::run(&mut program.root, config.collapse_parent_arranges, config.hoist_invariant, timings, observe);
        }
        if config.compose_maps {
            let (shapes, _) = timings.pass_mut(Pass::ShapeInference).infer(program, source, root);
            changed |= timings.pass_mut(Pass::ComposeMaps).rewrites(|| canonical::canonical_linear(&mut program.root, &shapes, crate::level::Level::root(root)));
        }
        // Import slots name edges, not distinct computations. Keep them interned even
        // with optional rewrites off, including programs supplied directly as IR.
        changed |= timings.pass_mut(Pass::MergeImports).rewrites(|| structural::merge_imports(&mut program.root));
        if !changed { break; }
    }
}

#[cfg(test)]
mod schedule_tests {
    use super::*;
    use corgi::Shape;

    #[test]
    fn options_and_observations_follow_the_same_schedule() {
        let text = include_str!("../../tests/plan_pairs/cases/as_of_shared.ddp");
        let source = |_: &Source| Some((Shape::Int, Shape::Int));
        let time = crate::server::host_time(7);
        for projection in [false, true] {
            for as_of_drop in [false, true] {
                let config = Config { projection, as_of_drop, ..Config::all() };
                let mut program = crate::lower::lower_tree(crate::parse::pipe::parse(text));
                let mut stages = Vec::new();
                run(&mut program, &source, &config, &time, 1, &mut |name, program, changes| {
                    for change in changes {
                        assert!(!matches!(change, Change::Rejected | Change::Skipped));
                    }
                    stages.push((name, format!("{program:?}")));
                });
                let names = ["as lowered", "after optimizer", "after projection", "after as-of drop"];
                assert_eq!(stages.iter().map(|s| s.0).collect::<Vec<_>>(), names);
                assert_eq!(stages.last().unwrap().1, format!("{program:?}"));
            }
        }
    }

    #[test]
    fn provider_checks_follow_dead_import_cleanup() {
        for (text, expected_skip) in [
            (r#"let dead = import "absent"; export "out" = input 0 : (int, ());"#, false),
            (r#"export "out" = import "absent" : (int, ());"#, true),
            (r#"export "out" : ((), ()) = input 0 : (int, ());"#, true),
        ] {
            let mut program = crate::lower::lower_tree(crate::parse::pipe::parse(text));
            let mut optimized = String::new();
            run(&mut program, &|_| None, &Config::all(), &crate::server::host_time(1), 1, &mut |name, program, changes| {
                if name == "after optimizer" { optimized = format!("{program:?}"); }
                if matches!(name, "after projection" | "after as-of drop") {
                    assert_eq!(changes.iter().any(|c| matches!(c, Change::Skipped)), expected_skip);
                    if expected_skip { assert_eq!(format!("{program:?}"), optimized); }
                }
            });
        }
    }

    #[test]
    fn config_words_apply_in_order_from_default() {
        assert_eq!(Config::default(), Config { projection: false, ..Config::all() });
        assert_eq!(Config::parse("").unwrap(), Config::default());
        assert_eq!(Config::parse("projection").unwrap(), Config::all());
        assert_eq!(Config::parse("all,-projection").unwrap(), Config::parse("-projection").unwrap());
        assert_eq!(Config::parse("none, structural").unwrap(), Config { structural: true, ..Config::none() });
        assert_eq!(Config::parse("none").unwrap().off(), Config::NAMES);
        assert!(Config::parse("fusion").is_err());
    }
}
