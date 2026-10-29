//! A rewrite that uses the time a program is installed as of.
//!
//! A program installed at epoch `e` reads every import as of `G = [e − 1]`
//! and its own inputs are fed from epoch `e` on, so every time in the
//! program, and every time an arrangement presents to a half join, is at or beyond `G`. An update
//! at exactly `G` can then meet only arranged rows at exactly `G`, and a strict half join, which
//! needs `t2 < t`, meets none. An update keeps its own time along a chain of half joins, so once a
//! lifted stream's every path reaches a strict half join, its updates at `G` produce nothing. In
//! a delta join that is every path but the one whose half joins are all non-strict: those paths
//! would carry the imports' whole history only to discard it.
//!
//! `drop_dead_as_of` drops those updates where the stream is lifted, with a `filter` on the time
//! `lift` wrote into the value. A lift that
//! other readers share, as both paths of a self-join do, keeps its rows, and the filter goes on
//! the edges to the readers it is dead for. It needs only
//! that every time is at or beyond `G`, which holds in any scope: a nested scope's times extend
//! `G` with coordinates that are zero at entry and only grow. So it applies at every depth, with
//! `G` read as `(e − 1, 0, …, 0)`.

use crate::ir::{Lambda, LinearOp, Time};
use crate::scope_ir::{Item, Node, Program, Ref, Scope};

/// Insert, at each lifted stream all of whose paths reach a strict half join, a filter that drops
/// its updates at `as_of`. Returns the number of filters inserted.
pub(super) fn drop_dead_as_of(prog: &mut Program, as_of: &Time) -> usize {
    drop_in_scope(&mut prog.root, 0, as_of)
}

/// Whether `op` leaves every row's time as it is. Only these may sit between a lift and the strict
/// half join that makes its updates at the as-of time dead.
fn keeps_times(op: &LinearOp) -> bool {
    match op {
        LinearOp::Project(_) | LinearOp::Filter(_) | LinearOp::Negate | LinearOp::LiftIter | LinearOp::FlatMap(_) | LinearOp::Lift => true,
        LinearOp::EnterAt(_) | LinearOp::JoinFunction(_) => false,
    }
}

/// How an item is read by another.
#[derive(Clone, Copy)]
enum Use {
    /// As the stream of the half join at this index.
    Stream { at: usize, strict: bool },
    /// As the input of the time-preserving linear chain at this index.
    Linear(usize),
    /// Any other way: as an arrangement, by a concat, join, reduce, inspect, child scope, bind or
    /// export.
    Other,
}

fn drop_in_scope(s: &mut Scope, depth: usize, as_of: &Time) -> usize {
    let mut inserted = 0;
    for item in s.items.iter_mut() {
        if let Item::Sub(child) = item { inserted += drop_in_scope(child, depth + 1, as_of); }
    }
    // The as-of time as `lift` writes it at this depth: the epoch, then one lane per scope. A time
    // with more coordinates than this scope has lanes is not one this scope's lift can write.
    if as_of.len() > depth + 1 { return inserted; }
    let lanes: Vec<u64> = (0..=depth).map(|j| as_of.get(j).copied().unwrap_or(0)).collect();

    let uses = uses(s);
    let mut covered = vec![false; s.items.len()];
    // Every reader follows its producer. Compute coverage backward without recursion.
    for i in (0..s.items.len()).rev() {
        covered[i] = !uses[i].is_empty() && uses[i].iter().all(|&u| dead_on(u, &covered));
    }
    // In-place filters (item, op position), and filters on some edges only (item, the consumers to
    // move onto the filter).
    let mut in_place: Vec<(usize, usize)> = Vec::new();
    let mut on_edges: Vec<(usize, Vec<Use>)> = Vec::new();
    for (i, item) in s.items.iter().enumerate() {
        let Item::Op(Node::Linear { ops, .. }) = item else { continue };
        let Some(at) = ops.iter().rposition(|op| matches!(op, LinearOp::Lift)) else { continue };
        if !ops[at + 1..].iter().all(keeps_times) || uses[i].is_empty() { continue; }
        let dead: Vec<Use> = uses[i].iter().copied().filter(|u| dead_on(*u, &covered)).collect();
        // The edge filter reads the time from the row value written by `lift`. Filter and Negate
        // leave that value intact. Project, FlatMap, LiftIter and other tails may move or drop it;
        // handling them needs tracking time through lambdas or splitting the node and duplicating
        // its tail on dead edges, adding work every tick and losing the "never worse" property.
        if dead.len() == uses[i].len() {
            in_place.push((i, at + 1));
        } else if !dead.is_empty() && ops[at + 1..].iter().all(|op| matches!(op, LinearOp::Filter(_) | LinearOp::Negate)) {
            on_edges.push((i, dead));
        }
    }
    for &(i, at) in &in_place {
        let Item::Op(Node::Linear { ops, .. }) = &mut s.items[i] else { unreachable!() };
        ops.insert(at, LinearOp::Filter(not_at(&lanes)));
    }
    inserted += in_place.len() + on_edges.len();
    insert_edge_filters(s, on_edges, &lanes);
    inserted
}

/// For each item, the ways later items, child scopes, binds and exports read it.
fn uses(s: &Scope) -> Vec<Vec<Use>> {
    let mut uses: Vec<Vec<Use>> = vec![Vec::new(); s.items.len()];
    let mut add = |r: &Ref, u: Use| if let Ref::Local(i) = r { uses[*i].push(u); };
    for (c, item) in s.items.iter().enumerate() {
        match item {
            Item::Op(Node::HalfJoin { stream, arrangement, strict, .. }) => {
                add(stream, Use::Stream { at: c, strict: *strict });
                add(arrangement, Use::Other);
            }
            Item::Op(Node::Linear { input, ops }) => add(input, if ops.iter().all(keeps_times) { Use::Linear(c) } else { Use::Other }),
            Item::Op(Node::Arrange(r) | Node::Reduce { input: r, .. } | Node::Inspect { input: r, .. }) => add(r, Use::Other),
            Item::Op(Node::Join { left, right, .. }) => { add(left, Use::Other); add(right, Use::Other); }
            Item::Op(Node::Concat(refs)) => for r in refs { add(r, Use::Other); },
            Item::Sub(child) => for imp in &child.imports {
                if let crate::scope_ir::Source::Parent(r) = &imp.from { add(r, Use::Other); }
            },
        }
    }
    for b in &s.binds { add(&b.value, Use::Other); }
    for e in &s.exports { add(&e.value, Use::Other); }
    uses
}

/// Whether an update at the as-of time read through `u` produces nothing.
fn dead_on(u: Use, covered: &[bool]) -> bool {
    match u {
        Use::Stream { strict: true, .. } => true,
        Use::Stream { at, strict: false } | Use::Linear(at) => covered[at],
        Use::Other => false,
    }
}

/// A filter on a lifted row, `(key, (val, time))`, keeping it unless its time is `lanes`.
fn not_at(lanes: &[u64]) -> Lambda {
    let names: Vec<String> = (0..lanes.len()).map(|j| format!("t{j}")).collect();
    let mut body = format!("({}, {}) ne", names[0], lanes[0]);
    for (name, lane) in names.iter().zip(lanes).skip(1) {
        body = format!("({body}, ({name}, {lane}) ne) or");
    }
    let mut lambda = Lambda::new(format!("let (_, (_, ({}))) = input in {body}", names.join(", ")), &corgi::Scope::default());
    lambda.added_by = Some("as-of drop");
    lambda
}

/// For each `(item, uses)`, a filter item right after `item`, read by those uses in its place.
/// The item ends in its lift or value-preserving ops, so the filter reads the row `lift` wrote.
fn insert_edge_filters(s: &mut Scope, edges: Vec<(usize, Vec<Use>)>, lanes: &[u64]) {
    if edges.is_empty() { return; }
    let mut filtered = vec![false; s.items.len()];
    for (i, _) in &edges { filtered[*i] = true; }
    // The new items, every reference still by old position, and each old position's new one.
    let labeled = s.labels.len() == s.items.len();
    let (mut items, mut labels, mut remap) = (Vec::new(), Vec::new(), Vec::new());
    for (i, item) in std::mem::take(&mut s.items).into_iter().enumerate() {
        remap.push(items.len());
        items.push(item);
        if labeled { labels.push(s.labels[i].clone()); }
        if filtered[i] {
            items.push(Item::Op(Node::Linear { input: Ref::Local(i), ops: vec![LinearOp::Filter(not_at(lanes))] }));
            if labeled { labels.push(s.labels[i].clone()); }
        }
    }
    s.items = items;
    if labeled { s.labels = labels; }
    s.for_each_ref(|r| match r {
        Ref::Local(i) | Ref::ChildExport(i, _) => *i = remap[*i],
        Ref::Import(_) | Ref::Var(_) => {},
    });
    // Move each dead use from the lifted item onto its filter.
    for (lifted, dead) in &edges {
        let (from, to) = (Ref::Local(remap[*lifted]), Ref::Local(remap[*lifted] + 1));
        for u in dead {
            let at = match u { Use::Stream { at, .. } | Use::Linear(at) => remap[*at], Use::Other => unreachable!("an `Other` use is never dead") };
            let slot = match &mut s.items[at] {
                Item::Op(Node::HalfJoin { stream, .. }) => stream,
                Item::Op(Node::Linear { input, .. }) => input,
                _ => unreachable!("a dead use is a half join's stream or a linear chain's input"),
            };
            if matches!((&*slot, &from), (Ref::Local(a), Ref::Local(b)) if a == b) { *slot = to.clone(); }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scope_ir::Source;

    const AS_OF: u64 = 7;

    /// The program, optimized as install sees it, and the filters the rewrite inserts.
    fn rewrite(src: &str) -> (Program, usize) {
        let mut prog = crate::lower::lower_tree(crate::parse::pipe::parse(src));
        crate::passes::optimize(&mut prog);
        let as_of = crate::server::host_time(AS_OF);
        let inserted = drop_dead_as_of(&mut prog, &as_of);
        let (_, problems) = crate::shapes::infer(&prog, &|s| match s {
            Source::Trace(_) => Some((corgi::Shape::Prod(vec![corgi::Shape::Int, corgi::Shape::Int]), corgi::Shape::Unit)),
            _ => None,
        });
        assert!(problems.conflicts.is_empty() && problems.unknown.is_empty(), "the rewritten program infers: {problems:?}");
        (prog, inserted)
    }

    /// The lambdas of the as-of filters in `s`, by the item they sit in.
    fn filters(s: &Scope) -> Vec<(usize, String)> {
        s.items.iter().enumerate().flat_map(|(i, item)| match item {
            Item::Op(Node::Linear { ops, .. }) => ops.iter().filter_map(|op| match op {
                LinearOp::Filter(l) if format!("{l:?}").contains(" ne") => Some((i, format!("{l:?}"))),
                _ => None,
            }).collect(),
            _ => Vec::new(),
        }).collect()
    }

    /// The half join reading item `i`'s output as its stream, and whether it is strict.
    fn stream_reader(s: &Scope, i: usize) -> Vec<bool> {
        s.items.iter().filter_map(|item| match item {
            Item::Op(Node::HalfJoin { stream: Ref::Local(j), strict, .. }) if *j == i => Some(*strict),
            _ => None,
        }).collect()
    }

    #[test]
    fn a_delta_join_drops_the_as_of_time_on_its_strict_path_only() {
        let (prog, inserted) = rewrite(r#"
            let a = import "a";
            let b = import "b";
            export "r" = ((a | lift | half_join(b, 0, (k, x, y) -> (k, (x, y))))
                        + (b | lift | half_join(a, 1, (k, y, x) -> (k, (x, y)))));
        "#);
        assert_eq!(inserted, 1);
        let found = filters(&prog.root);
        assert_eq!(found.len(), 1);
        let (at, lambda) = &found[0];
        assert_eq!(stream_reader(&prog.root, *at), vec![true], "the filter sits on the path of the strict half join");
        assert!(lambda.contains(&format!("(t0, {AS_OF}) ne")), "{lambda}");
    }

    #[test]
    fn a_three_way_path_through_a_non_strict_half_join_is_dead_at_its_strict_one() {
        let (_, inserted) = rewrite(r#"
            let a = import "a";
            let b = import "b";
            let c = import "c";
            let ab = a | lift | half_join(b, 0, (k, x, y) -> (k, x)) | half_join(c, 0, (k, x, z) -> (k, x));
            let ba = b | lift | half_join(a, 1, (k, y, x) -> (k, x)) | half_join(c, 0, (k, x, z) -> (k, x));
            let ca = c | lift | half_join(a, 1, (k, z, x) -> (k, x)) | half_join(b, 1, (k, x, y) -> (k, x));
            export "r" = ab + ba + ca;
        "#);
        assert_eq!(inserted, 2, "every path but `a`'s, which is non-strict throughout");
    }

    #[test]
    fn a_shared_lift_filters_only_the_edge_to_its_strict_reader() {
        let (prog, inserted) = rewrite(r#"
            let p = import "p";
            export "r" = ((p | lift | half_join(p, 0, (k, x, y) -> (k, (x, y))))
                        + (p | lift | half_join(p, 1, (k, x, y) -> (k, (x, y)))));
        "#);
        assert_eq!(inserted, 1);
        let found = filters(&prog.root);
        assert_eq!(found.len(), 1);
        let (at, _) = found[0];
        let Item::Op(Node::Linear { input: Ref::Local(lift), ops }) = &prog.root.items[at] else { panic!("an item of its own") };
        assert_eq!(ops.len(), 1, "the filter alone, after the shared lift");
        assert_eq!(stream_reader(&prog.root, at), vec![true]);
        assert_eq!(stream_reader(&prog.root, *lift), vec![false], "the non-strict path still reads the lift");
    }

    #[test]
    fn a_shared_lift_with_filter_and_negate_filters_its_strict_edge() {
        let (prog, inserted) = rewrite(r#"
            let p = import "p";
            let lifted = p | lift | filter((k, _) -> (k, k) eq) | negate;
            export "strict" = lifted | half_join(p, 1, (k, x, y) -> (k, (x, y)));
            export "live" = lifted;
        "#);
        assert_eq!(inserted, 1);
        let [(filtered, _)] = filters(&prog.root)[..] else { panic!("one edge filter") };
        let Item::Op(Node::Linear { input: Ref::Local(shared), ops }) = &prog.root.items[filtered] else { panic!("edge filter") };
        assert_eq!(ops.len(), 1);
        assert!(matches!(ops[0], LinearOp::Filter(_)));
        let Item::Op(Node::Linear { ops: shared_ops, .. }) = &prog.root.items[*shared] else { panic!("shared lift") };
        assert!(matches!(shared_ops.as_slice(), [.., LinearOp::Lift, LinearOp::Filter(_), LinearOp::Negate]));
        assert_eq!(stream_reader(&prog.root, filtered), vec![true]);
        assert!(matches!(prog.root.exports.iter().find(|e| e.name == "live").unwrap().value, Ref::Local(i) if i == *shared));
    }

    #[test]
    fn a_lifted_stream_read_any_other_way_keeps_its_rows_there() {
        let (prog, inserted) = rewrite(r#"
            let a = import "a";
            let b = import "b";
            let la = a | lift;
            export "r" = la | half_join(b, 1, (k, x, y) -> (k, (x, y)));
            export "lifted" = la;
        "#);
        assert_eq!(inserted, 1, "the edge to the strict half join is filtered");
        let lifted = prog.root.exports.iter().find(|e| e.name == "lifted").unwrap();
        let Ref::Local(i) = lifted.value else { panic!("a local") };
        let Item::Op(Node::Linear { ops, .. }) = &prog.root.items[i] else { panic!("the lift") };
        assert!(matches!(ops.last(), Some(LinearOp::Lift)), "the export reads the lift unfiltered");
        let (_, live) = rewrite(r#"
            let a = import "a";
            let b = import "b";
            export "r" = a | lift | half_join(b, 0, (k, x, y) -> (k, (x, y)));
        "#);
        assert_eq!(live, 0, "a non-strict path is live at the as-of time");
        let (_, moved) = rewrite(r#"
            let a = import "a";
            let b = import "b";
            export "r" = a | lift | delay((_, (_, t)) -> t) | half_join(b, 1, (k, x, y) -> (k, (x, y)));
        "#);
        assert_eq!(moved, 0, "a delay between the lift and the strict half join can move the time");
    }

    #[test]
    fn a_nested_scope_compares_every_lane() {
        let (prog, inserted) = rewrite(r#"
            let a = import "a";
            let b = import "b";
            s: {
                let r = a | lift | half_join(b, 1, (k, x, y) -> (k, x)) | map((k, (x, _)) -> (k, x));
                set x = (r + x) | distinct;
            }
            export "r" = s::x;
        "#);
        assert_eq!(inserted, 1);
        let Item::Sub(child) = prog.root.items.iter().find(|i| matches!(i, Item::Sub(_))).unwrap() else { unreachable!() };
        let found = filters(child);
        assert_eq!(found.len(), 1);
        assert!(found[0].1.contains(&format!("(t0, {AS_OF}) ne")) && found[0].1.contains("(t1, 0) ne"), "{}", found[0].1);
    }
}
