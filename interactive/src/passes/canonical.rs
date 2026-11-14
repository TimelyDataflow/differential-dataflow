//! Local, shape-preserving rewrites over the scope tree.

use corgi::{Graph, NodeRef, NumOp, Op, Shape};
use crate::ir::{Lambda, LinearOp};
use crate::level::Level;
use crate::lambda::{normalize::canonical, Projection};
use crate::scope_ir::{Item, Node, Ref, Scope};
use crate::shapes::{ItemShapes, ScopeShapes};

/// Compose maps, fold wiring into its sole producer, remove identities and paired negations.
pub(super) fn canonical_linear(scope: &mut Scope, shapes: &ScopeShapes, level: Level) -> usize {
    let mut changed = 0;
    for (item, shape) in scope.items.iter_mut().zip(&shapes.items) {
        match (item, shape) {
            (Item::Sub(child), ItemShapes::Sub(shapes)) => changed += canonical_linear(child, shapes, level.child()),
            (Item::Op(Node::Linear { input, ops }), _) => {
                let steps = shapes.of(input).and_then(|s| {
                    crate::shapes::linear_shapes(ops, s.key, s.val, level).ok()
                });
                let mut pending = Vec::new();
                for (i, op) in std::mem::take(ops).into_iter().enumerate() {
                    let input = steps.as_ref().map(|s| Shape::Prod(vec![s[i].0.clone(), s[i].1.clone()]));
                    if matches!(op, LinearOp::Negate) && matches!(pending.last(), Some((LinearOp::Negate, _))) {
                        pending.pop();
                        changed += 1;
                    } else if matches!((&op, &input), (LinearOp::Project(map), Some(input)) if identity(map, input)) {
                        changed += 1;
                    } else { pending.push((op, input)); }
                }
                let mut result: Vec<(LinearOp, Option<Shape>)> = Vec::new();
                let mut pending = pending.into_iter().peekable();
                while let Some((op, input)) = pending.next() {
                    if !matches!(op, LinearOp::Project(_)) || input.is_none() {
                        result.push((op, input));
                        continue;
                    }
                    let mut run = vec![(op, input)];
                    while matches!(pending.peek(), Some((LinearOp::Project(_), Some(_)))) {
                        run.push(pending.next().unwrap());
                    }
                    if run.len() > 16 {
                        let maps: Vec<_> = run.iter().map(|(op, _)| {
                            let LinearOp::Project(map) = op else { unreachable!() }; map
                        }).collect();
                        let start = run[0].1.as_ref().unwrap();
                        if let Some(composed) = compose_run(&maps, start) {
                            if !identity(&composed, start) {
                                result.push((LinearOp::Project(composed), Some(start.clone())));
                            }
                            changed += 1;
                            continue;
                        }
                        // Preserve the pairwise fallback at unsupported scalar kernels.
                        // Supported runs take the single composition above.
                    }
                    for (op, input) in run {
                        if let (LinearOp::Project(map), Some(input)) = (&op, &input) {
                            if let Some((LinearOp::FlatMap(list), _)) = result.last_mut() {
                                if let Some(projection) = map.graph().ok().and_then(|g| canonical(g, input).ok()).and_then(|g| wiring(&g)) {
                                    *list = Lambda::compose("arg", &[(list, "arg".into())], |out| {
                                        format!("{} map (row -> {})", out[0], projection.text("row"))
                                    });
                                    changed += 1;
                                    continue;
                                }
                            }
                            if let Some((LinearOp::Project(first), Some(start))) = result.last() {
                                if let Some(composed) = compose_run(&[first, map], start) {
                                    let start = start.clone();
                                    result.pop();
                                    if !identity(&composed, &start) {
                                        result.push((LinearOp::Project(composed), Some(start)));
                                    }
                                    changed += 1;
                                    continue;
                                }
                            }
                        }
                        result.push((op, input));
                    }
                }
                *ops = result.into_iter().map(|(op, _)| op).collect();
            }
            _ => {}
        }
    }
    changed += fold_wiring(scope, shapes, false);
    changed + remove_empty(scope)
}

/// An empty chain is an alias. Redirect its readers, including names and exports.
pub(super) fn remove_empty(scope: &mut Scope) -> usize {
    let mut changed = 0;
    let mut dead = vec![false; scope.items.len()];
    let mut remap: Vec<_> = (0..scope.items.len()).map(Ref::Local).collect();
    for i in 0..scope.items.len() {
        if let Item::Op(Node::Linear { input, ops }) = &scope.items[i] {
            if ops.is_empty() {
                remap[i] = if let Ref::Local(j) = input { remap[*j].clone() } else { input.clone() };
                dead[i] = true;
                changed += 1;
            }
        }
    }
    scope.for_each_ref(|r| if let Ref::Local(i) = r { *r = remap[*i].clone(); });
    super::structural::compact(scope, &dead);
    changed
}

/// Fold a leading wiring map across a single-reader edge. Projection also uses
/// the reduce case before computing demand; all other cases belong to compose-maps.
pub(super) fn fold_wiring(scope: &mut Scope, shapes: &ScopeShapes, reduce_only: bool) -> usize {
    let counts = super::structural::use_counts(scope, &vec![false; scope.items.len()]);
    let mut changed = 0;
    for i in 0..scope.items.len() {
        let Item::Op(Node::Linear { input: Ref::Local(j), ops }) = &scope.items[i] else { continue };
        let j = *j;
        if counts[j] != 1 { continue; }
        if !matches!(&scope.items[j], Item::Op(Node::Reduce { .. }))
            && (reduce_only || !matches!(&scope.items[j], Item::Op(Node::Join { .. } | Node::HalfJoin { .. }))) { continue; }
        let Some(LinearOp::Project(map)) = ops.first() else { continue };
        let Some(shape) = shapes.of(&Ref::Local(j)) else { continue };
        let input = Shape::Prod(vec![shape.key.clone(), shape.val.clone()]);
        let Some(projection) = map.graph().ok().and_then(|g| canonical(g, &input).ok()).and_then(|g| wiring(&g)) else { continue };
        let map = map.clone();
        match &mut scope.items[j] {
            Item::Op(Node::Join { projection, .. }) if !reduce_only => {
                *projection = Lambda::compose("arg", &[(projection, "arg".into()), (&map, "_call0".into())], |out| out[1].clone());
            }
            Item::Op(Node::HalfJoin { projection: base, .. }) if !reduce_only => {
                let input = Shape::Prod(vec![shape.key, shape.val]);
                let Some(projected) = half_join_wiring(&projection, &input) else { continue };
                *base = Lambda::compose("arg", &[(base, "arg".into())], |out| projected.text(&out[0]));
            }
            Item::Op(Node::Reduce { reducer, .. }) => {
                let Some(value) = projection.value(&shape.key, &shape.val) else { continue };
                *reducer = reducer.project(&value);
            }
            _ => continue,
        }
        let Item::Op(Node::Linear { ops, .. }) = &mut scope.items[i] else { unreachable!() };
        ops.remove(0);
        changed += 1;
    }
    changed
}

/// HalfJoin appends data time outside its lambda. Preserve that lane exactly,
/// and reject any other use of it; update time is never a lambda input.
fn half_join_wiring(projection: &Projection, input: &Shape) -> Option<Projection> {
    let projection = projection.expanded(input)?;
    if projection.field(1)?.field(1)? != Projection::Field(vec![1, 1]).expanded(input)? { return None; }
    fn strip(p: Projection) -> Option<Projection> {
        Some(match p {
            Projection::Field(path) if path.starts_with(&[0]) => Projection::Field(path),
            Projection::Field(path) if path.starts_with(&[1, 0]) => Projection::Field([vec![1], path[2..].to_vec()].concat()),
            Projection::Tuple(parts) => Projection::Tuple(parts.into_iter().map(strip).collect::<Option<_>>()?),
            Projection::Unit => Projection::Unit,
            _ => return None,
        })
    }
    strip(Projection::Tuple(vec![projection.field(0)?, projection.field(1)?.field(0)?]))
}

fn compose_run(maps: &[&Lambda], input: &Shape) -> Option<Lambda> {
    let mut builder = corgi::Builder::default();
    let mut output_id = builder.input();
    let mut output = input.clone();
    for map in maps {
        let graph = map.graph().ok()?;
        output = corgi::shape_of(graph, &output).ok()?;
        output_id = builder.graft(graph, output_id);
    }
    let graph = builder.finish(output_id);
    let mut composed = Lambda { src: String::new(), graph: Ok(std::sync::Arc::new(graph)), added_by: None };
    let graph = composed.graph().ok()?;
    let graph = canonical(graph, input).ok()?;
    if corgi::shape_of(&graph, input).ok()? != output { return None; }
    composed.graph = Ok(std::sync::Arc::new(graph));
    // The graph is authoritative. Keep a readable spelling only if it reparses to the
    // same checked graph. Unsupported kernels leave the original pair alone.
    let printed = crate::lambda::print::print(&composed, Some(input), false);
    if printed.fallback { return None; }
    let (pattern, body) = printed.text.split_once(" -> ")?;
    let src = format!("let {pattern} = input in {body}");
    let mut words = crate::lambda::base_scope();
    for f in crate::lambda::registered() { words.word(f.name(), crate::lambda::function_word(&f)).ok()?; }
    let parsed = corgi::parse_ml_in(&src, &words).ok()?;
    if corgi::shape_of(&parsed, input).ok()? != output || canonical(&parsed, input).ok()? != **composed.graph().ok()? { return None; }
    composed.src = src;
    // Retain the surface pattern checks after proving graph equality without them.
    // This keeps serialization and exact plan round trips faithful to the new graph.
    composed.graph = Ok(std::sync::Arc::new(corgi::optimize(&parsed)));
    #[cfg(test)]
    check_maps(maps, Some(&composed), input);
    Some(composed)
}

/// Prove identity by tuple wiring at the checked shape, including reconstructed products
/// and unit fields. Arithmetic identities and source-text guesses are not proofs here.
fn identity(lambda: &Lambda, input: &Shape) -> bool {
    let result = (|| {
        let graph = lambda.graph().ok()?;
        if corgi::shape_of(graph, input).ok()? != *input { return None; }
        let graph = canonical(graph, input).ok()?;
        let projection = wiring(&graph)?;
        Some(projection.expanded(input)? == Projection::Field(Vec::new()).expanded(input)?)
    })().unwrap_or(false);
    #[cfg(test)]
    if result { check_maps(&[lambda], None, input); }
    result
}

fn wiring(graph: &Graph<NumOp>) -> Option<Projection> {
    let mut values: Vec<Projection> = Vec::new();
    for node in graph.nodes() {
        values.push(match node {
            NodeRef::Input => Projection::Field(Vec::new()),
            NodeRef::Tuple(fields) => Projection::Tuple(fields.iter().map(|&i| values[i].clone()).collect()),
            NodeRef::Op(NumOp::Core(Op::Field(field)), i) => values[i].field(*field)?,
            NodeRef::Op(NumOp::Core(Op::Unit), _) => Projection::Unit,
            _ => return None,
        });
    }
    Some(values[graph.output()].clone())
}

/// Evaluate the original sequence, not an optimized composition of it. This runs for
/// every accepted rewrite in unit tests.
#[cfg(test)]
fn check_maps(before: &[&Lambda], after: Option<&Lambda>, shape: &Shape) {
    let mut seed = 0x5eed_d1ff_c0de_2026;
    for rows in [0, 1, 17, 64] {
        let input = crate::lambda::print::generated(shape, rows, &mut seed);
        let mut a = input.clone();
        for lambda in before { a = corgi::eval_graph(lambda.graph().unwrap(), a); }
        let b = after.map(|lambda| corgi::eval_graph(lambda.graph().unwrap(), input.clone())).unwrap_or(input);
        assert_eq!(a.len(), b.len());
        for row in 0..a.len() {
            assert_eq!(corgi::arrange::compare_at(&a, row, &b, row), std::cmp::Ordering::Equal,
                "map rewrite differs at row {row} in a {rows}-row batch: {before:?} -> {after:?}");
        }
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
