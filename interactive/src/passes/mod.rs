//! Scope-tree rewrites. `run` is the preparation schedule; all host choices are inputs.

mod structural;
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

/// Changes made by a stage. Statement-level display diffs belong to the observer.
pub enum Change {
    Structural { path: String, action: &'static str, from: ItemName, to: ItemName },
    Masked(usize),
    AsOf(usize),
    NoProjection,
    Rejected,
    Skipped,
}

/// Run the optimizer, optional projection, and optional as-of drop, in that order.
/// `source` supplies provider shapes, including generated sources. A declared trace
/// shape alone does not resolve its provider. Observations include the lowered tree
/// and each enabled stage, even if that stage has no changes or is skipped.
/// When `as_of` is supplied, all row and arrangement times must be at or beyond it.
///
/// Optimization runs before checking providers so unread imports can disappear.
/// Optional candidates must infer without problems and preserve the optimizer's
/// export shapes. A rejected candidate leaves the last accepted program in place.
pub fn run(
    program: &mut Program,
    source: &dyn Fn(&Source) -> Option<RowShape>,
    project: bool,
    as_of: Option<&Time>,
    observe: &mut dyn FnMut(&'static str, &Program, &[Change]),
) {
    observe("as lowered", program, &[]);
    let mut changes = Vec::new();
    optimize_with(program, source, &mut |path, scope, ordinals, action, from, to| {
        changes.push(Change::Structural {
            path: path.into(), action,
            from: ItemName::new(scope, ordinals, from), to: ItemName::new(scope, ordinals, to),
        });
    });
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
    if project {
        changes.clear();
        if valid {
            let mut candidate = program.clone();
            let masked = projection::pushdown(&mut candidate, source).unwrap_or(0);
            if masked > 0 {
                if accept(&candidate) {
                    changes.push(Change::Masked(masked));
                    *program = candidate;
                } else { changes.push(Change::Rejected); }
            } else { changes.push(Change::NoProjection); }
        } else { changes.push(Change::Skipped); }
        observe("after projection", program, &changes);
    }
    if let Some(as_of) = as_of {
        changes.clear();
        if valid {
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
}

/// Optimize every scope in place, without provider shapes or optional passes.
pub fn optimize(program: &mut Program) {
    optimize_with(program, &|_| None, &mut |_, _, _, _, _, _| {});
}

fn optimize_with(program: &mut Program, source: &dyn Fn(&Source) -> Option<RowShape>, observe: &mut Observe<'_>) {
    loop {
        structural::optimize_scope(&mut program.root, "root", observe);
        let (shapes, _) = shapes::infer(program, source);
        let changed = canonical::canonical_linear(&mut program.root, &shapes)
            | canonical::dead_bindings(&mut program.root);
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
        for project in [false, true] {
            for as_of in [None, Some(&time)] {
                let mut program = crate::lower::lower_tree(crate::parse::pipe::parse(text));
                let mut stages = Vec::new();
                run(&mut program, &source, project, as_of, &mut |name, program, changes| {
                    for change in changes {
                        assert!(!matches!(change, Change::Rejected | Change::Skipped));
                    }
                    stages.push((name, format!("{program:?}")));
                });
                let mut names = vec!["as lowered", "after optimizer"];
                if project { names.push("after projection"); }
                if as_of.is_some() { names.push("after as-of drop"); }
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
            run(&mut program, &|_| None, true, Some(&Time::new(1, Default::default())), &mut |name, program, changes| {
                if name == "after optimizer" { optimized = format!("{program:?}"); }
                if matches!(name, "after projection" | "after as-of drop") {
                    assert_eq!(changes.iter().any(|c| matches!(c, Change::Skipped)), expected_skip);
                    if expected_skip { assert_eq!(format!("{program:?}"), optimized); }
                }
            });
        }
    }
}
