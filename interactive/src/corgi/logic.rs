//! corgi as DDIR's columnar scalar logic: compile a `Term` to a corgi `Graph<NumOp>`, and
//! transcode DDIR rows (`ir::Value`) to/from corgi columnar `Value` at the I/O boundaries.
//!
//! Shapes are STATIC. A collection's shape is fixed when it is first seen — pinned from its first
//! row at ingest ([`shape_of_row`]), or computed from its operator's term — and never re-derived
//! from data. Sums are the case where a row cannot tell: a `Variant` carries a tag, not its type,
//! so every sum a program builds is one it declared (`Term::Inject` carries the whole sum's lane
//! shapes), and a sum arriving on input needs a declared schema.
//!
//! The typer is corgi's. [`shape_of_term`] compiles a term into a scratch graph and asks
//! `corgi::shape_of` (its evaluator on zero rows) for the result shape, so every shape rule —
//! which lanes a `case` sees, what an `if` may blend, whether two operands compare — is the one
//! the kernels enforce, and a program that lowers is a program that runs. The compiler covers
//! Var/Bound/Int/Tuple(+Spread)/Proj/Binary/If/Fold, list and sum intro (`List`/`Inject`), sum
//! elimination (`Case`), and the Neg/Not/Len/IsTag unaries; `Err` is a type error, reported with
//! corgi's message. Ordered compares are signed-correct (`ToSigned`); `hash` is corgi's structural
//! `Op::Hash`, the same function `ir::eval` folds row-wise.

use crate::ir::Value as DValue;
use crate::parse::{BinOp, SumTy, Term, UnOp};

use corgi::{ArithOp, BinOp as CBinOp, Builder, CmpOp, Graph, Kind, NumOp, Op, Pred, Shape, Value as CValue};

type Res<T> = Result<T, String>;

/// The shape of one input row — what a collection is pinned to when its first row arrives. An
/// `Int`, a `Tuple`, or a non-empty `List` determines its shape alone; a `Variant` (whose type a
/// bare tag cannot name) or an empty `List` (whose element shape nothing supplies) needs the
/// input's schema declared.
pub fn shape_of_row(row: &DValue) -> Res<Shape> {
    match row {
        DValue::Int(_) => Ok(Shape::Prim(64)),
        DValue::Tuple(xs) if xs.is_empty() => Ok(Shape::Unit),
        DValue::Tuple(xs) => Ok(Shape::Prod(xs.iter().map(shape_of_row).collect::<Res<_>>()?)),
        DValue::List(xs) => match xs.first() {
            Some(x) => Ok(Shape::List(Box::new(shape_of_row(x)?))),
            None => Err("an empty list on input has no element shape; declare the input's schema".into()),
        },
        DValue::Variant(..) => Err("a variant on input has no type; declare the input's schema".into()),
    }
}

/// AoS rows -> SoA corgi columns, directed by `shape`. A row that does not fit the shape is a
/// panic: the shape was pinned from a row of this collection, so a misfit is an ingest error.
pub fn transcode(rows: &[DValue], shape: &Shape) -> CValue {
    match shape {
        Shape::Prim(_) => CValue::u64(rows.iter().map(|r| r.as_int() as u64).collect()),
        Shape::Unit => CValue::Unit(rows.len()),
        Shape::Prod(fs) => CValue::Prod(
            fs.iter()
                .enumerate()
                .map(|(i, fsi)| {
                    let sub: Vec<DValue> = rows
                        .iter()
                        .map(|r| match r {
                            DValue::Tuple(xs) => xs[i].clone(),
                            other => panic!("transcode: expected Tuple, got {other:?}"),
                        })
                        .collect();
                    transcode(&sub, fsi)
                })
                .collect(),
        ),
        Shape::List(elem) => {
            // List column = per-row END offsets + a flattened element column.
            let mut ends = Vec::with_capacity(rows.len());
            let mut flat: Vec<DValue> = Vec::new();
            let mut acc = 0usize;
            for r in rows {
                match r {
                    DValue::List(xs) => {
                        acc += xs.len();
                        ends.push(acc);
                        flat.extend(xs.iter().cloned());
                    }
                    other => panic!("transcode: expected List, got {other:?}"),
                }
            }
            CValue::List(ends.into(), Box::new(transcode(&flat, elem)))
        }
        Shape::Sum(lanes) => {
            // Per-row tag, plus one packed lane per variant (its arm's rows in row order; a
            // variant no row uses is an empty column of its declared shape).
            let tags: Vec<usize> = rows
                .iter()
                .map(|r| match r {
                    DValue::Variant(t, _) => *t as usize,
                    other => panic!("transcode: expected Variant, got {other:?}"),
                })
                .collect();
            if let Some(t) = tags.iter().find(|&&t| t >= lanes.len()) {
                panic!("transcode: tag {t} is outside the declared {}-variant sum", lanes.len());
            }
            let lane_vals: Vec<CValue> = lanes
                .iter()
                .enumerate()
                .map(|(tag, lshape)| {
                    let payloads: Vec<DValue> = rows
                        .iter()
                        .filter_map(|r| match r {
                            DValue::Variant(t, p) if *t as usize == tag => Some((**p).clone()),
                            _ => None,
                        })
                        .collect();
                    transcode(&payloads, lshape)
                })
                .collect();
            CValue::sum(tags, lane_vals)
        }
    }
}

/// SoA corgi columns -> AoS rows, directed by `shape`. Inverse of [`transcode`].
pub fn untranscode(col: CValue, shape: &Shape) -> Vec<DValue> {
    match shape {
        Shape::Prim(_) => col.into_u64("untranscode").unwrap().into_iter().map(|x| DValue::Int(x as i64)).collect(),
        Shape::Unit => vec![DValue::unit(); col.len()],
        Shape::Prod(fs) => {
            let cols = col.into_prod("untranscode").unwrap();
            let n = if cols.is_empty() { 0 } else { cols[0].len() };
            let per_field: Vec<Vec<DValue>> =
                cols.into_iter().zip(fs.iter()).map(|(c, fsi)| untranscode(c, fsi)).collect();
            (0..n).map(|i| DValue::Tuple(per_field.iter().map(|f| f[i].clone()).collect())).collect()
        }
        Shape::List(elem) => {
            // Inverse of transcode's List: per-row END offsets + a flattened element column → one
            // `List` per row, slicing the untranscoded flat column by each row's span.
            let (bounds, vals) = match col {
                CValue::List(b, vals) => (b, *vals),
                other => panic!("untranscode: expected List, got {other:?}"),
            };
            let flat = untranscode(vals, elem);
            let ends: Vec<usize> = match &bounds {
                corgi::Bounds::Offsets(v) => v.clone(),
                corgi::Bounds::Stride(k, rows) => (1..=*rows).map(|i| i * k).collect(),
            };
            let mut out = Vec::with_capacity(ends.len());
            let mut start = 0usize;
            for end in ends {
                out.push(DValue::List(flat[start..end].to_vec()));
                start = end;
            }
            out
        }
        Shape::Sum(lanes) => {
            // Inverse of transcode's Sum: untranscode each lane, then for each row pull its payload
            // from its lane at the recorded within-lane OFFSET (robust to row reordering from a
            // prior gather/merge — not a sequential cursor).
            let (tags, offsets, variant_vals) = col.into_sum("untranscode").unwrap();
            let lane_rows: Vec<Vec<DValue>> =
                variant_vals.into_iter().zip(lanes.iter()).map(|(v, ls)| untranscode(v, ls)).collect();
            tags.iter()
                .zip(&offsets)
                .map(|(&tag, &off)| DValue::Variant(tag as u32, Box::new(lane_rows[tag][off].clone())))
                .collect()
        }
    }
}

/// The shape of a term in an environment — corgi's typer, asked through a scratch lowering.
/// `expected` is the shape an enclosing `if` or `case` arm already fixed; it fills the lane a
/// built-in `None`/`Ok`/`Err` cannot fix from its payload.
pub fn shape_of_term(t: &Term, env_shapes: &[Shape], expected: Option<&Shape>) -> Res<Shape> {
    let mut b = Builder::<NumOp>::default();
    let inp = b.input();
    let env: Vec<usize> = (0..env_shapes.len()).map(|i| b.add(Op::Field(i), vec![inp])).collect();
    let out = compile(t, &mut b, &env, env_shapes, inp, expected)?;
    corgi::shape_of(&b.finish(out), &Shape::Prod(env_shapes.to_vec()))
}

/// Does `t` read anything outside its own `depth` innermost binders — an input row (`Var`) or an
/// enclosing binder? A fold step that does needs its environment captured into the fold.
fn mentions_env(t: &Term, depth: usize) -> bool {
    match t {
        Term::Var(_) => true,
        Term::Bound(k) => *k >= depth,
        Term::Int(_) => false,
        Term::Tuple(fs) | Term::List(fs) | Term::Hash(fs) => fs.iter().any(|f| mentions_env(f, depth)),
        Term::Spread(inner) | Term::Proj(inner, _) | Term::Unary(_, inner) => mentions_env(inner, depth),
        Term::Inject { tag, payload, .. } => mentions_env(tag, depth) || mentions_env(payload, depth),
        Term::Case { scrutinee, arms, default } => {
            mentions_env(scrutinee, depth)
                || arms.iter().any(|a| mentions_env(a, depth + 1))
                || default.as_ref().is_some_and(|d| mentions_env(d, depth))
        }
        Term::Fold { list, init, step } => {
            mentions_env(list, depth) || mentions_env(init, depth) || mentions_env(step, depth + 2)
        }
        Term::If { cond, then, els } => mentions_env(cond, depth) || mentions_env(then, depth) || mentions_env(els, depth),
        Term::Binary(_, l, r) => mentions_env(l, depth) || mentions_env(r, depth),
    }
}

/// Whether [`compile`] can lower this term WITHOUT knowing its operands' shapes — the gate for
/// join-INLINE projections, which are compiled before any container is in hand. It is therefore
/// deliberately narrower than `compile`: every shape-dependent form (`List`, `Case`, a built-in
/// sum whose lane the payload fixes, a data-driven tag) answers false here and is compiled by the
/// linear stage the join defers it to, which does have shapes.
pub fn compilable(t: &Term) -> bool {
    match t {
        Term::Var(_) | Term::Bound(_) | Term::Int(_) => true,
        Term::Proj(inner, _) | Term::Spread(inner) => compilable(inner),
        Term::Tuple(fs) => fs.iter().all(compilable),
        Term::Binary(_, l, r) => compilable(l) && compilable(r),
        Term::If { cond, then, els } => compilable(cond) && compilable(then) && compilable(els),
        Term::Fold { list, init, step } => compilable(list) && compilable(init) && compilable(step),
        Term::Unary(op, inner) => matches!(op, UnOp::Neg | UnOp::Not | UnOp::Len | UnOp::IsTag(_)) && compilable(inner),
        // A literal tag into a declared type knows its whole sum; the built-ins and a data-driven
        // tag need the payload's shape.
        Term::Inject { tag, payload, sum } => {
            matches!(&**tag, Term::Int(_)) && matches!(sum, SumTy::Declared(_)) && compilable(payload)
        }
        // `Op::Hash` is shape-generic (it folds whatever structure it is handed), so `hash`
        // needs no shapes to lower and can answer true here.
        Term::Hash(args) => args.iter().all(compilable),
        _ => false, // List intro, Case — see `compile`.
    }
}

/// The lane shapes of the sum an `Inject` builds: the declaration's, or a built-in's with the
/// payload in its lane and the other lane from `expected`.
fn lanes_of(sum: &SumTy, tag: usize, payload: &Shape, expected: Option<&Shape>) -> Res<Vec<Shape>> {
    let other = |k: usize, what: &str| -> Res<Shape> {
        match expected {
            Some(Shape::Sum(ls)) if ls.len() == 2 => Ok(ls[k].clone()),
            _ => Err(format!(
                "cannot infer the {what} lane of this built-in sum here; use a declared type, or an `if` whose other branch fixes it"
            )),
        }
    };
    match sum {
        SumTy::Declared(lanes) => {
            if tag >= lanes.len() {
                return Err(format!("constructor tag {tag} is outside the declared {}-variant sum", lanes.len()));
            }
            Ok(lanes.clone())
        }
        SumTy::Option => match tag {
            0 => Ok(vec![Shape::Unit, other(1, "Some")?]),
            1 => Ok(vec![Shape::Unit, payload.clone()]),
            _ => Err("Option has two variants".into()),
        },
        SumTy::Result => match tag {
            0 => Ok(vec![payload.clone(), other(1, "Err")?]),
            1 => Ok(vec![other(0, "Ok")?, payload.clone()]),
            _ => Err("Result has two variants".into()),
        },
        SumTy::Dynamic => Err("an untyped `inject(tag, payload)` names no sum; declare a `type` and use its constructor".into()),
    }
}

/// The shapes of an `if`'s two branches: each is typed on its own, and a branch that cannot be
/// (a bare `None`/`Ok`/`Err`) borrows the other's shape.
fn branch_shapes(then: &Term, els: &Term, env_shapes: &[Shape], expected: Option<&Shape>) -> Res<(Shape, Shape)> {
    match (shape_of_term(then, env_shapes, expected), shape_of_term(els, env_shapes, expected)) {
        (Ok(t), Ok(e)) => Ok((t, e)),
        (Ok(t), Err(_)) => {
            let e = shape_of_term(els, env_shapes, Some(&t))?;
            Ok((t, e))
        }
        (Err(_), Ok(e)) => {
            let t = shape_of_term(then, env_shapes, Some(&e))?;
            Ok((t, e))
        }
        (Err(e), Err(_)) => Err(e),
    }
}

/// Compile a `Term` to a corgi node. `env[i]` = node for `Var(i)`; `env_shapes[i]` = its shape.
/// Binders push on top (read by `Bound(k)`). `anchor` sizes `Lit` broadcasts. `expected` is the
/// shape the context already fixed for this term, if any (see [`shape_of_term`]). `Err` is a
/// type error: the term has no meaning at these shapes.
pub fn compile(
    term: &Term,
    b: &mut Builder<NumOp>,
    env: &[usize],
    env_shapes: &[Shape],
    anchor: usize,
    expected: Option<&Shape>,
) -> Res<usize> {
    match term {
        Term::Var(i) => env.get(*i).copied().ok_or_else(|| format!("`${i}` is not in scope here")),
        Term::Bound(k) => {
            env.len().checked_sub(1 + *k).map(|i| env[i]).ok_or_else(|| format!("binder `^{k}` is not in scope here"))
        }
        Term::Int(n) => Ok(b.add(Op::Lit(CValue::u64(vec![*n as u64])), vec![anchor])),
        Term::Tuple(fields) => {
            // A `Spread(t)` child splices `t`'s `Prod` fields in place (the flat-row model).
            let mut ids: Vec<usize> = Vec::new();
            for f in fields {
                match f {
                    Term::Spread(inner) => {
                        let node = compile(inner, b, env, env_shapes, anchor, None)?;
                        match shape_of_term(inner, env_shapes, None)? {
                            Shape::Prod(fs) => {
                                for i in 0..fs.len() {
                                    ids.push(b.add(Op::Field(i), vec![node]));
                                }
                            }
                            Shape::Unit => {} // unit splices nothing
                            _ => ids.push(node), // scalar: splice the value itself
                        }
                    }
                    _ => ids.push(compile(f, b, env, env_shapes, anchor, None)?),
                }
            }
            // An empty field list is DDIR unit: emit a length-carrying `Unit` column over the anchor,
            // NOT `Prod([])` (an empty product has no rows to count, so the row count would be lost).
            if ids.is_empty() {
                Ok(b.add(Op::Unit, vec![anchor]))
            } else {
                Ok(b.tuple(ids))
            }
        }
        Term::Spread(_) => Err("`$n...` spread is only meaningful inside a tuple".into()),
        // Projection: a tuple field, or a list element (`Get`, faulting out of range as `eval` does).
        Term::Proj(t, i) => {
            let id = compile(t, b, env, env_shapes, anchor, None)?;
            match shape_of_term(t, env_shapes, None)? {
                Shape::List(_) => {
                    let idx = b.add(Op::Lit(CValue::u64(vec![*i as u64])), vec![anchor]);
                    let pair = b.tuple(vec![idx, id]);
                    Ok(b.add(Op::Get, vec![pair]))
                }
                _ => Ok(b.add(Op::Field(*i), vec![id])),
            }
        }
        Term::Binary(op, l, r) => {
            let lid = compile(l, b, env, env_shapes, anchor, None)?;
            let rid = compile(r, b, env, env_shapes, anchor, None)?;
            let pair = |b: &mut Builder<NumOp>, x, y| b.tuple(vec![x, y]);
            Ok(match op {
                BinOp::Add => { let p = pair(b, lid, rid); b.add(ArithOp::Bin(CBinOp::Add, Kind::U, 64), vec![p]) }
                BinOp::Sub => { let p = pair(b, lid, rid); b.add(ArithOp::Bin(CBinOp::Sub, Kind::U, 64), vec![p]) }
                BinOp::Mul => { let p = pair(b, lid, rid); b.add(ArithOp::Bin(CBinOp::Mul, Kind::U, 64), vec![p]) }
                BinOp::Eq | BinOp::Ne => {
                    // Cross-shape structural compare folds to a constant (Eq→0, Ne→1) over `anchor`;
                    // same-shape emits a real corgi `Rel`.
                    if shape_of_term(l, env_shapes, None)? != shape_of_term(r, env_shapes, None)? {
                        let v = if matches!(op, BinOp::Ne) { 1u64 } else { 0u64 };
                        b.add(Op::Lit(CValue::u64(vec![v])), vec![anchor])
                    } else {
                        let pred = if matches!(op, BinOp::Eq) { Pred::Eq } else { Pred::Ne };
                        let p = pair(b, lid, rid);
                        b.add(CmpOp::Rel(pred), vec![p])
                    }
                }
                // Ordered compares go through `ToSigned` (XOR the sign bit: the order-preserving
                // signed encoding), so they agree with `ir::eval`'s signed semantics for negative
                // ints too. `Eq`/`Ne` are bit-equality — sign-safe as raw bits.
                BinOp::Lt => { let (ls, rs) = (b.add(ArithOp::ToSigned, vec![lid]), b.add(ArithOp::ToSigned, vec![rid])); let p = pair(b, ls, rs); b.add(CmpOp::Rel(Pred::Lt), vec![p]) }
                BinOp::Le => { let (ls, rs) = (b.add(ArithOp::ToSigned, vec![lid]), b.add(ArithOp::ToSigned, vec![rid])); let p = pair(b, ls, rs); b.add(CmpOp::Rel(Pred::Le), vec![p]) }
                BinOp::Gt => { let (ls, rs) = (b.add(ArithOp::ToSigned, vec![lid]), b.add(ArithOp::ToSigned, vec![rid])); let p = pair(b, rs, ls); b.add(CmpOp::Rel(Pred::Lt), vec![p]) }
                BinOp::Ge => { let (ls, rs) = (b.add(ArithOp::ToSigned, vec![lid]), b.add(ArithOp::ToSigned, vec![rid])); let p = pair(b, rs, ls); b.add(CmpOp::Rel(Pred::Le), vec![p]) }
                BinOp::And => { let p = pair(b, lid, rid); b.add(CmpOp::Min, vec![p]) }
                BinOp::Or => { let p = pair(b, lid, rid); b.add(CmpOp::Max, vec![p]) }
            })
        }
        Term::If { cond, then, els } => {
            // `Select` blends per row and is shape-generic; the branches must share one shape,
            // and a branch that cannot fix its own (a bare `None`) takes the other's.
            let (ts, es) = branch_shapes(then, els, env_shapes, expected)?;
            if ts != es {
                return Err(format!("if: the branches differ in shape, {ts} vs {es}"));
            }
            let c = compile(cond, b, env, env_shapes, anchor, None)?;
            let t = compile(then, b, env, env_shapes, anchor, Some(&ts))?;
            let e = compile(els, b, env, env_shapes, anchor, Some(&es))?;
            let sel = b.tuple(vec![c, t, e]);
            Ok(b.add(Op::Select, vec![sel]))
        }
        // Fold over a List. corgi `Op::Fold` consumes `Prod([seed, List<A>])` and folds each row's
        // list; its body is a closed sub-graph over `Prod([acc, elem])`. DDIR's step sees
        // elem=Bound(0), acc=Bound(1). A step that reads outside its two binders gets the
        // environment captured into the list first (`CapList`: every element paired with the
        // context), so the closed body can see it — the same closure conversion `Case` does.
        Term::Fold { list, init, step } => {
            let init_id = compile(init, b, env, env_shapes, anchor, expected)?;
            let list_id = compile(list, b, env, env_shapes, anchor, None)?;
            let elem = match shape_of_term(list, env_shapes, None)? {
                Shape::List(e) => *e,
                other => return Err(format!("fold over a non-list: {other}")),
            };
            let init_shape = shape_of_term(init, env_shapes, expected)?;
            if mentions_env(step, 2) {
                let ctx = b.tuple(env.to_vec());
                let cap_in = b.tuple(vec![ctx, list_id]);
                let cap = b.add(Op::CapList, vec![cap_in]);
                let pair = b.tuple(vec![init_id, cap]);
                let body = compile_fold_body(step, Some(env_shapes), &init_shape, &elem)?;
                Ok(b.add(Op::Fold(Box::new(body)), vec![pair]))
            } else {
                let pair = b.tuple(vec![init_id, list_id]);
                let body = compile_fold_body(step, None, &init_shape, &elem)?;
                Ok(b.add(Op::Fold(Box::new(body)), vec![pair]))
            }
        }
        // Sum intro. A literal tag is corgi's `Inject` into the whole declared sum (the lanes the
        // payload does not fill are built empty). A data-driven tag is a demux (`Branch`), which
        // needs every lane to share the payload's shape.
        Term::Inject { tag, payload, sum } => {
            let pid = compile(payload, b, env, env_shapes, anchor, None)?;
            let pshape = shape_of_term(payload, env_shapes, None)?;
            match &**tag {
                Term::Int(t) => {
                    let t = usize::try_from(*t).map_err(|_| format!("constructor tag {t} is negative"))?;
                    let lanes = lanes_of(sum, t, &pshape, expected)?;
                    Ok(b.add(Op::Inject(t, lanes), vec![pid]))
                }
                _ => {
                    let SumTy::Declared(lanes) = sum else {
                        return Err("a data-driven variant tag needs a declared type: `variant(Type, tag, payload)`".into());
                    };
                    if let Some(l) = lanes.iter().find(|l| **l != pshape) {
                        return Err(format!("variant: every lane must have the payload's shape {pshape}, but one is {l}"));
                    }
                    let tid = compile(tag, b, env, env_shapes, anchor, None)?;
                    let pair = b.tuple(vec![pid, tid]);
                    Ok(b.add(Op::Branch(lanes.len()), vec![pair]))
                }
            }
        }
        // Sum elimination: distribute the environment into each lane (`CapSum`), run each arm as a
        // closed body over `Prod([ctx, payload])` (`MapSum`), and collapse the homogeneous result
        // (`Unwrap`, which is where arms that disagree are reported). Arms see the outer env plus
        // the payload as the top binder; a `default` runs WITHOUT the payload binder (matching
        // `eval`). An arm that cannot fix its own shape (a bare `None`) takes the first that can.
        Term::Case { scrutinee, arms, default } => {
            let lanes = match shape_of_term(scrutinee, env_shapes, None)? {
                Shape::Sum(lanes) => lanes,
                other => return Err(format!("case on a non-sum: {other}")),
            };
            let sid = compile(scrutinee, b, env, env_shapes, anchor, None)?;
            let ctx = b.tuple(env.to_vec());
            let cap_in = b.tuple(vec![ctx, sid]);
            let cap = b.add(Op::CapSum, vec![cap_in]);
            let arm_graph = |i: usize, exp: Option<&Shape>| -> Res<(Graph<NumOp>, Shape)> {
                let mut bb = Builder::<NumOp>::default();
                let inp = bb.input();
                let cnode = bb.add(Op::Field(0), vec![inp]);
                let mut env2: Vec<usize> = (0..env.len()).map(|j| bb.add(Op::Field(j), vec![cnode])).collect();
                let mut shapes2: Vec<Shape> = env_shapes.to_vec();
                let term = if i < arms.len() {
                    let pnode = bb.add(Op::Field(1), vec![inp]);
                    env2.push(pnode);
                    shapes2.push(lanes[i].clone());
                    &arms[i]
                } else {
                    default.as_deref().ok_or_else(|| format!("case: no arm for tag {i} and no `_` default"))?
                };
                let out = compile(term, &mut bb, &env2, &shapes2, inp, exp)?;
                let g = bb.finish(out);
                let in_shape = Shape::Prod(vec![Shape::Prod(env_shapes.to_vec()), lanes[i].clone()]);
                let s = corgi::shape_of(&g, &in_shape)?;
                Ok((g, s))
            };
            let mut exp: Option<Shape> = expected.cloned();
            let mut bodies: Vec<(usize, Graph<NumOp>)> = Vec::with_capacity(lanes.len());
            let mut deferred: Vec<(usize, String)> = Vec::new();
            for i in 0..lanes.len() {
                match arm_graph(i, exp.as_ref()) {
                    Ok((g, s)) => {
                        bodies.push((i, g));
                        exp.get_or_insert(s);
                    }
                    Err(e) => deferred.push((i, e)),
                }
            }
            for (i, e) in deferred {
                let Some(s) = exp.as_ref() else { return Err(e) };
                let (g, _) = arm_graph(i, Some(s))?;
                bodies.push((i, g));
            }
            bodies.sort_by_key(|(i, _)| *i);
            let mapped = b.add(Op::MapSum(bodies), vec![cap]);
            Ok(b.add(Op::Unwrap, vec![mapped]))
        }
        Term::Unary(op, inner) => {
            let id = compile(inner, b, env, env_shapes, anchor, None)?;
            let shape = shape_of_term(inner, env_shapes, None)?;
            Ok(match op {
                // Wrapping negate on the raw two's-complement bits — exactly `-as_int()`.
                UnOp::Neg => b.add(ArithOp::Neg(Kind::U, 64), vec![id]),
                // `truthy` is "nonzero Int": scalars compare against zero; non-`Int` values
                // are never truthy, so their `not` folds to the constant 1 (the cross-shape
                // `Eq` fold's precedent).
                UnOp::Not => match shape {
                    Shape::Prim(_) => {
                        let zero = b.add(Op::Lit(CValue::u64(vec![0])), vec![anchor]);
                        let p = b.tuple(vec![id, zero]);
                        b.add(CmpOp::Rel(Pred::Eq), vec![p])
                    }
                    _ => b.add(Op::Lit(CValue::u64(vec![1])), vec![anchor]),
                },
                // Tuple arity is static (a shape fact); list length folds `acc + 1` along
                // each row's list; anything else is the program error `eval` reports.
                UnOp::Len => match shape {
                    Shape::Prod(fs) => b.add(Op::Lit(CValue::u64(vec![fs.len() as u64])), vec![anchor]),
                    Shape::Unit => b.add(Op::Lit(CValue::u64(vec![0])), vec![anchor]),
                    Shape::List(_) => {
                        let zero = b.add(Op::Lit(CValue::u64(vec![0])), vec![anchor]);
                        let seed = b.tuple(vec![zero, id]);
                        let body = {
                            let mut bb = Builder::<NumOp>::default();
                            let inp = bb.input();
                            let acc = bb.add(Op::Field(0), vec![inp]);
                            let out = bb.add(ArithOp::AddU64(1), vec![acc]);
                            bb.finish(out)
                        };
                        b.add(Op::Fold(Box::new(body)), vec![seed])
                    }
                    other => return Err(format!("len of a {other}")),
                },
                // On a sum, every lane maps to its constant answer and the result unwraps
                // (lanes are homogeneous `U64`); on any other shape, `istag` is constantly 0
                // (matching `eval`'s "non-Variant is never the tag").
                UnOp::IsTag(t) => match shape {
                    Shape::Sum(lanes) => {
                        let arms: Vec<(usize, Graph<NumOp>)> = (0..lanes.len())
                            .map(|i| {
                                let mut bb = Builder::<NumOp>::default();
                                let inp = bb.input();
                                let v = (i as u32 == *t) as u64;
                                let out = bb.add(Op::Lit(CValue::u64(vec![v])), vec![inp]);
                                (i, bb.finish(out))
                            })
                            .collect();
                        let mapped = b.add(Op::MapSum(arms), vec![id]);
                        b.add(Op::Unwrap, vec![mapped])
                    }
                    _ => b.add(Op::Lit(CValue::u64(vec![0])), vec![anchor]),
                },
            })
        }
        // Homogeneous list literal: `k` element columns become a length-`k` list per row through
        // the existing kernel matrix, with no per-row work and no new corgi op — `Enlist` each
        // element (a length-1 lane per row), `Iota` a per-row `[0..k)` tag list, `Weave`
        // interleaves the lanes in field order into `List<Sum{X x k}>`, and `MapList(Unwrap)`
        // strips the now-homogeneous sum (and reports a heterogeneous literal). A fused
        // list-intro kernel is corgi's call if this composition ever profiles hot.
        Term::List(fields) => {
            if fields.is_empty() {
                return Err("an empty list literal has no element shape".into());
            }
            let mut lanes = Vec::with_capacity(fields.len());
            for f in fields {
                let e = compile(f, b, env, env_shapes, anchor, None)?;
                lanes.push(b.add(Op::Enlist, vec![e]));
            }
            let count = b.add(Op::Lit(CValue::u64(vec![fields.len() as u64])), vec![anchor]);
            let mut weave_in = vec![b.add(Op::Iota, vec![count])];
            weave_in.extend(lanes);
            let woven_in = b.tuple(weave_in);
            let woven = b.add(Op::Weave, vec![woven_in]);
            let unwrap_body = {
                let mut bb = Builder::<NumOp>::default();
                let inp = bb.input();
                let out = bb.add(Op::Unwrap, vec![inp]);
                bb.finish(out)
            };
            Ok(b.add(Op::MapList(Box::new(unwrap_body)), vec![woven]))
        }
        // DDIR's `hash` IS corgi's `Op::Hash` (`ir::structural_hash` is the row-wise twin): hash
        // the arguments as one tuple, shift out the sign bit, reduce by the bound.
        //
        // The bound guard is pure arithmetic — no `Select`. `Rem`'s total `x % 0 = x` gives
        // `bound == 0` the identity, and a NEGATIVE bound reads as a `u64` at or above 2^63,
        // which is larger than the shifted hash, so it reduces to the identity too. Both are
        // exactly what `ir::eval`'s `if bound > 0` produces.
        Term::Hash(args) => {
            let (bound, rest) = args.split_first().ok_or("hash needs a bound")?;
            let bid = compile(bound, b, env, env_shapes, anchor, None)?;
            let payload = if rest.is_empty() {
                b.add(Op::Unit, vec![anchor])
            } else {
                let mut ids = Vec::with_capacity(rest.len());
                for a in rest {
                    ids.push(compile(a, b, env, env_shapes, anchor, None)?);
                }
                b.tuple(ids)
            };
            let h = b.add(Op::Hash, vec![payload]);
            let shifted = b.add(ArithOp::Shr(1), vec![h]);
            let pair = b.tuple(vec![shifted, bid]);
            Ok(b.add(ArithOp::Bin(CBinOp::Rem, Kind::U, 64), vec![pair]))
        }
    }
}

/// Compile a `Fold` step into a closed corgi sub-graph. Without capture the body's input is
/// `Prod([acc, elem])`; with it, `Prod([acc, (ctx, elem)])` where `ctx` is the captured
/// environment (its fields come first, so `Var(i)` and outer `Bound`s resolve as they do in
/// `ir::eval`'s stack). Either way `Bound(0)` = elem, `Bound(1)` = acc.
fn compile_fold_body(step: &Term, ctx: Option<&[Shape]>, init_shape: &Shape, elem_shape: &Shape) -> Res<Graph<NumOp>> {
    let mut bb = Builder::<NumOp>::default();
    let inp = bb.input();
    let acc = bb.add(Op::Field(0), vec![inp]);
    let (mut env, mut shapes) = (Vec::new(), Vec::new());
    let elem = match ctx {
        Some(cs) => {
            let ce = bb.add(Op::Field(1), vec![inp]);
            let c = bb.add(Op::Field(0), vec![ce]);
            for (j, s) in cs.iter().enumerate() {
                env.push(bb.add(Op::Field(j), vec![c]));
                shapes.push(s.clone());
            }
            bb.add(Op::Field(1), vec![ce])
        }
        None => bb.add(Op::Field(1), vec![inp]),
    };
    env.push(acc);
    env.push(elem);
    shapes.push(init_shape.clone());
    shapes.push(elem_shape.clone());
    let out = compile(step, &mut bb, &env, &shapes, inp, Some(init_shape))?;
    Ok(bb.finish(out))
}

/// Compile a term in the row environment `Var(0)=key` (shape `kshape`), `Var(1)=val` (`vshape`) —
/// the environment every `LinearOp` reads. The graph's input is `Prod([key, val])`, and it is
/// typechecked once here, so an `Ok` graph runs on every batch of these shapes.
fn compile_over_kv(term: &Term, kshape: &Shape, vshape: &Shape) -> Res<Graph<NumOp>> {
    let mut b = Builder::<NumOp>::default();
    let input = b.input();
    let var_k = b.add(Op::Field(0), vec![input]);
    let var_v = b.add(Op::Field(1), vec![input]);
    let out = compile(term, &mut b, &[var_k, var_v], &[kshape.clone(), vshape.clone()], input, None)?;
    let g = b.finish(out);
    corgi::shape_of(&g, &Shape::Prod(vec![kshape.clone(), vshape.clone()]))?;
    Ok(g)
}

/// Compile a `FlatMap`'s list term → a corgi `List` column, one list per input row. A term that is
/// not list-shaped is the type error: the backend explodes the column structurally.
pub fn compile_flatmap(list_term: &Term, kshape: &Shape, vshape: &Shape) -> Res<Graph<NumOp>> {
    match shape_of_term(list_term, &[kshape.clone(), vshape.clone()], None)? {
        Shape::List(_) => compile_over_kv(list_term, kshape, vshape),
        other => Err(format!("flatmap over a non-list: {other}")),
    }
}

/// Compile a scalar term (`EnterAt`'s delay field) → a `U64` column; a non-integer term is the
/// type error (the delay is read as one integer per row).
pub fn compile_scalar(term: &Term, kshape: &Shape, vshape: &Shape) -> Res<Graph<NumOp>> {
    match shape_of_term(term, &[kshape.clone(), vshape.clone()], None)? {
        Shape::Prim(_) => compile_over_kv(term, kshape, vshape),
        other => Err(format!("enter_at delay is not an integer: {other}")),
    }
}

/// Compile a `Filter` predicate → a mask column (nonzero keeps the row).
pub fn compile_predicate(cond: &Term, kshape: &Shape, vshape: &Shape) -> Res<Graph<NumOp>> {
    compile_over_kv(cond, kshape, vshape)
}

/// Compile a join projection: key/val Terms over `Var(0)=key`, `Var(1)=val0`, `Var(2)=val1` (with
/// their shapes). Input `Prod([key, val0, val1])`; output `Prod([newkey, newval])`.
pub fn compile_join_projection(key: &Term, val: &Term, kshape: &Shape, v0shape: &Shape, v1shape: &Shape) -> Res<Graph<NumOp>> {
    let mut b = Builder::<NumOp>::default();
    let input = b.input();
    let var_k = b.add(Op::Field(0), vec![input]);
    let var_0 = b.add(Op::Field(1), vec![input]);
    let var_1 = b.add(Op::Field(2), vec![input]);
    let env = [var_k, var_0, var_1];
    let shapes = [kshape.clone(), v0shape.clone(), v1shape.clone()];
    let nk = compile(key, &mut b, &env, &shapes, input, None)?;
    let nv = compile(val, &mut b, &env, &shapes, input, None)?;
    let out = b.tuple(vec![nk, nv]);
    let g = b.finish(out);
    corgi::shape_of(&g, &Shape::Prod(shapes.to_vec()))?;
    Ok(g)
}

/// Compile a DDIR `Projection` over `Var(0)=key` (`kshape`), `Var(1)=val` (`vshape`).
/// Input `Prod([key, val])`; output `Prod([newkey, newval])`.
pub fn compile_projection(key: &Term, val: &Term, kshape: &Shape, vshape: &Shape) -> Res<Graph<NumOp>> {
    let mut b = Builder::<NumOp>::default();
    let input = b.input();
    let var_k = b.add(Op::Field(0), vec![input]);
    let var_v = b.add(Op::Field(1), vec![input]);
    let env = [var_k, var_v];
    let shapes = [kshape.clone(), vshape.clone()];
    let nk = compile(key, &mut b, &env, &shapes, input, None)?;
    let nv = compile(val, &mut b, &env, &shapes, input, None)?;
    let out = b.tuple(vec![nk, nv]);
    let g = b.finish(out);
    corgi::shape_of(&g, &Shape::Prod(shapes.to_vec()))?;
    Ok(g)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::Value as V;

    fn u64s() -> Shape { Shape::Prim(64) }
    fn sum(lanes: Vec<Shape>) -> Shape { Shape::Sum(lanes) }

    /// The pin on DDIR's `hash`: `ir::structural_hash` is a row-at-a-time transcription of
    /// `corgi::hash`, and the two backends compute the SAME program value, so they must agree
    /// bit for bit on every shape the transcode layer covers. If corgi's salts or fold change,
    /// this is what fails.
    fn hash_agrees(rows: Vec<V>, shape: Shape) {
        let col = transcode(&rows, &shape);
        let columnar = corgi::hash(&col).into_u64("hash").unwrap();
        let row_wise: Vec<u64> = rows.iter().map(crate::ir::structural_hash).collect();
        assert_eq!(columnar, row_wise, "hash disagrees (shape {shape:?})");
    }

    #[test]
    fn hash_matches_corgi_on_scalars() {
        hash_agrees(vec![V::Int(0), V::Int(1), V::Int(-1), V::Int(i64::MIN), V::Int(i64::MAX)], u64s());
    }

    #[test]
    fn hash_matches_corgi_on_tuples_and_units() {
        let rows = vec![V::Tuple(vec![V::Int(1), V::Int(2)]), V::Tuple(vec![V::Int(2), V::Int(1)])];
        let shape = shape_of_row(&rows[0]).unwrap();
        hash_agrees(rows, shape);
        hash_agrees(vec![V::unit(), V::unit()], Shape::Unit);
        // A 1-tuple must not collapse onto its scalar, nor a unit onto an empty anything.
        assert_ne!(
            crate::ir::structural_hash(&V::Tuple(vec![V::Int(7)])),
            crate::ir::structural_hash(&V::Int(7))
        );
    }

    #[test]
    fn hash_matches_corgi_on_lists() {
        hash_agrees(
            vec![V::List(vec![V::Int(1), V::Int(2), V::Int(3)]), V::List(vec![]), V::List(vec![V::Int(3), V::Int(2), V::Int(1)])],
            Shape::List(Box::new(u64s())),
        );
    }

    #[test]
    fn hash_matches_corgi_on_variants() {
        hash_agrees(
            vec![V::Variant(0, Box::new(V::Int(5))), V::Variant(1, Box::new(V::Int(5))), V::Variant(0, Box::new(V::Int(6)))],
            sum(vec![u64s(), u64s()]),
        );
    }

    #[test]
    fn hash_matches_corgi_on_nesting() {
        let pair = Shape::Prod(vec![u64s(), u64s()]);
        hash_agrees(
            vec![
                V::Tuple(vec![V::List(vec![V::Int(1)]), V::Variant(0, Box::new(V::Tuple(vec![V::Int(2), V::Int(3)])))]),
                V::Tuple(vec![V::List(vec![V::Int(1), V::Int(1)]), V::Variant(0, Box::new(V::Tuple(vec![V::Int(2), V::Int(4)])))]),
            ],
            Shape::Prod(vec![Shape::List(Box::new(u64s())), sum(vec![pair, Shape::Unit])]),
        );
    }

    /// Round-trip a column of rows through transcode → untranscode at a given shape.
    fn roundtrip(rows: Vec<V>, shape: Shape) {
        let col = transcode(&rows, &shape);
        assert_eq!(corgi::shape_of_value(&col), shape, "transcode builds the declared shape");
        let back = untranscode(col, &shape);
        assert_eq!(back, rows, "roundtrip mismatch (shape {shape:?})");
    }

    #[test]
    fn roundtrip_variant_single_arm() {
        // binders-style: a single constructor wrapping a list.
        roundtrip(
            vec![
                V::Variant(0, Box::new(V::List(vec![V::Int(1), V::Int(2)]))),
                V::Variant(0, Box::new(V::List(vec![V::Int(3)]))),
                V::Variant(0, Box::new(V::List(vec![]))),
            ],
            sum(vec![Shape::List(Box::new(u64s()))]),
        );
    }

    #[test]
    fn roundtrip_variant_multi_arm() {
        // adt-style: two arms, interleaved; payloads of different shape per arm.
        roundtrip(
            vec![
                V::Variant(0, Box::new(V::Int(10))),
                V::Variant(1, Box::new(V::Tuple(vec![V::Int(1), V::Int(2)]))),
                V::Variant(0, Box::new(V::Int(20))),
                V::Variant(1, Box::new(V::Tuple(vec![V::Int(3), V::Int(4)]))),
                V::Variant(0, Box::new(V::Int(30))),
            ],
            sum(vec![u64s(), Shape::Prod(vec![u64s(), u64s()])]),
        );
    }

    #[test]
    fn roundtrip_variant_absent_arm_is_an_empty_lane() {
        // tags {0, 2} present, arm 1 absent: its lane is an empty column of the declared shape.
        roundtrip(
            vec![V::Variant(0, Box::new(V::Int(1))), V::Variant(2, Box::new(V::Int(2))), V::Variant(0, Box::new(V::Int(3)))],
            sum(vec![u64s(), Shape::List(Box::new(u64s())), u64s()]),
        );
    }

    #[test]
    fn roundtrip_nested_variant_in_tuple() {
        roundtrip(
            vec![
                V::Tuple(vec![V::Int(1), V::Variant(0, Box::new(V::Int(7)))]),
                V::Tuple(vec![V::Int(2), V::Variant(1, Box::new(V::unit()))]),
            ],
            Shape::Prod(vec![u64s(), sum(vec![u64s(), Shape::Unit])]),
        );
    }

    #[test]
    fn shape_of_row_pins_what_a_row_can_say() {
        assert_eq!(shape_of_row(&V::Tuple(vec![V::Int(1), V::List(vec![V::Int(2)])])).unwrap(), Shape::Prod(vec![u64s(), Shape::List(Box::new(u64s()))]));
        assert!(shape_of_row(&V::List(vec![])).is_err());
        assert!(shape_of_row(&V::Variant(0, Box::new(V::Int(1)))).is_err());
    }
}
