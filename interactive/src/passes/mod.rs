//! Scope-tree rewrites. `run` is the preparation schedule; all host choices are inputs.

mod structural;
mod arrangements;
mod canonical;
pub mod projection;
mod as_of;

use crate::ir::Time;
use crate::scope_ir::{Item, Program, RowShape, Scope, Source};
use crate::shapes;

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
    /// Prune unread computations, collapse arranges, fuse linear chains, merge identical operators.
    pub structural: bool,
    /// Remove arranges of parent bindings that already carry arrangements.
    pub collapse_parent_arranges: bool,
    /// Share explicit arrangements of unchanged imports across scopes.
    pub hoist_arranges: bool,
    /// Compose adjacent maps, drop identity maps, cancel paired negations.
    pub compose_maps: bool,
    /// Remove imports, child exports, and aliases nothing reads.
    pub dead_bindings: bool,
    /// Replace fields nothing reads by `()`.
    pub projection: bool,
    /// Drop updates at the as-of time that only strict half joins would read.
    pub as_of_drop: bool,
}

impl Default for Config {
    fn default() -> Self { Self { projection: false, ..Self::all() } }
}

impl Config {
    /// The names `parse` takes, in schedule order.
    pub const NAMES: [&'static str; 7] = ["structural", "collapse-parent-arranges", "hoist-arranges", "compose-maps", "dead-bindings", "projection", "as-of-drop"];

    pub fn all() -> Self { Self { structural: true, collapse_parent_arranges: true, hoist_arranges: true, compose_maps: true, dead_bindings: true, projection: true, as_of_drop: true } }
    pub fn none() -> Self { Self { structural: false, collapse_parent_arranges: false, hoist_arranges: false, compose_maps: false, dead_bindings: false, projection: false, as_of_drop: false } }

    /// Start from the default passes and apply comma-separated words in order: `all`, `none`, a pass
    /// name to turn it on, or `-name` to turn it off. So `none`, `-projection` and
    /// `none,structural` all mean what they say.
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
        let on = [self.structural, self.collapse_parent_arranges, self.hoist_arranges, self.compose_maps, self.dead_bindings, self.projection, self.as_of_drop];
        Self::NAMES.into_iter().zip(on).filter(|(_, on)| !on).map(|(name, _)| name).collect()
    }

    fn flag(&mut self, name: &str) -> Option<&mut bool> {
        match name {
            "structural" => Some(&mut self.structural),
            "collapse-parent-arranges" => Some(&mut self.collapse_parent_arranges),
            "hoist-arranges" => Some(&mut self.hoist_arranges),
            "compose-maps" => Some(&mut self.compose_maps),
            "dead-bindings" => Some(&mut self.dead_bindings),
            "projection" => Some(&mut self.projection),
            "as-of-drop" => Some(&mut self.as_of_drop),
            _ => None,
        }
    }
}

/// Changes made by a stage. Statement-level display diffs belong to the observer.
pub enum Change {
    Structural { path: String, action: &'static str, from: ItemName, to: ItemName },
    Masked(usize),
    AsOf(usize),
    NoProjection,
    Rejected,
    Skipped,
    /// The named pass is switched off.
    Off(&'static str),
}

/// Run the optimizer, projection, and the as-of drop, in that order, each as `config` says.
/// `source` supplies provider shapes, including generated sources. A declared trace
/// shape alone does not resolve its provider. Observations include the lowered tree
/// and every stage, even one that is switched off, has no changes, or is skipped.
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
    observe: &mut dyn FnMut(&'static str, &Program, &[Change]),
) {
    observe("as lowered", program, &[]);
    let mut changes = Vec::new();
    optimize_with(program, source, config, &mut |path, scope, ordinals, action, from, to| {
        changes.push(Change::Structural {
            path: path.into(), action,
            from: ItemName::new(scope, ordinals, from), to: ItemName::new(scope, ordinals, to),
        });
    });
    for (on, name) in [(config.structural, "structural"), (config.collapse_parent_arranges, "collapse-parent-arranges"), (config.hoist_arranges, "hoist-arranges"), (config.compose_maps, "compose-maps"), (config.dead_bindings, "dead-bindings")] {
        if !on { changes.push(Change::Off(name)); }
    }
    observe("after optimizer", program, &changes);
    let (shapes, problems) = shapes::infer(program, source);
    let valid = problems.conflicts.is_empty() && problems.unknown.is_empty()
        && program.root.vars.is_empty()
        && program.root.imports.iter().all(|import| match &import.from {
            Source::Trace(_) => source(&import.from).is_some_and(|shape| import.shape.as_ref().is_none_or(|declared| *declared == shape)),
            Source::Input(_) => import.shape.is_some(),
            Source::Parent(_) => false,
        });
    let accept = |candidate: &Program| {
        let (checked, problems) = shapes::infer(candidate, source);
        problems.conflicts.is_empty() && problems.unknown.is_empty() && checked.exports == shapes.exports
    };
    changes.clear();
    if !config.projection { changes.push(Change::Off("projection")); }
    else if valid {
        let mut candidate = program.clone();
        let mut masked = 0;
        let mut converged = false;
        for _ in 0..16 {
            let count = projection::pushdown(&mut candidate, source).unwrap_or(0);
            if count == 0 { converged = true; break; }
            // Masks expose optimizer rewrites. In turn, merging identical masks can
            // give a reduce one reader and make its remaining map fusible.
            optimize_with(&mut candidate, source, config, &mut |_, _, _, _, _, _| {});
            if !accept(&candidate) { break; }
            masked += count;
        }
        if converged {
            if masked > 0 {
                changes.push(Change::Masked(masked));
                *program = candidate;
            } else { changes.push(Change::NoProjection); }
        } else { changes.push(Change::Rejected); }
    } else { changes.push(Change::Skipped); }
    observe("after projection", program, &changes);
    changes.clear();
    if !config.as_of_drop { changes.push(Change::Off("as-of-drop")); }
    else if valid {
        let mut candidate = program.clone();
        let inserted = as_of::drop_dead_as_of(&mut candidate, as_of);
        if inserted > 0 {
            if accept(&candidate) {
                changes.push(Change::AsOf(inserted));
                *program = candidate;
            } else { changes.push(Change::Rejected); }
        }
    } else { changes.push(Change::Skipped); }
    observe("after as-of drop", program, &changes);
}

/// Optimize every scope in place with every optimizer pass, without provider shapes.
pub fn optimize(program: &mut Program) {
    optimize_with(program, &|_| None, &Config::all(), &mut |_, _, _, _, _, _| {});
}

fn optimize_with(program: &mut Program, source: &dyn Fn(&Source) -> Option<RowShape>, config: &Config, observe: &mut Observe<'_>) {
    loop {
        if config.structural { structural::optimize_scope(&mut program.root, "root", observe); }
        let mut changed = false;
        if config.collapse_parent_arranges || config.hoist_arranges {
            changed |= arrangements::run(&mut program.root, config.collapse_parent_arranges, config.hoist_arranges, observe);
        }
        if config.compose_maps {
            let (shapes, _) = shapes::infer(program, source);
            changed |= canonical::canonical_linear(&mut program.root, &shapes);
        }
        if config.dead_bindings { changed |= canonical::dead_bindings(&mut program.root); }
        // Import slots name edges, not distinct computations. Keep them interned even
        // with optional rewrites off, including programs supplied directly as IR.
        changed |= structural::merge_imports(&mut program.root);
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
        let time = Time::new(7, Default::default());
        for projection in [false, true] {
            for as_of_drop in [false, true] {
                let config = Config { projection, as_of_drop, ..Config::all() };
                let mut program = crate::lower::lower_tree(crate::parse::pipe::parse(text));
                let mut stages = Vec::new();
                run(&mut program, &source, &config, &time, &mut |name, program, changes| {
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
            run(&mut program, &|_| None, &Config::all(), &Time::new(1, Default::default()), &mut |name, program, changes| {
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
