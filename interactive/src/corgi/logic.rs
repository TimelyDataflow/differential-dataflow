//! corgi as DDIR's columnar scalar logic: compile a `Term` to a corgi `Graph<NumOp>`,
//! transcode DDIR rows (`ir::Value`) to/from corgi columnar `Value`, directed by a `Shape`
//! inferred from the data (the dynamic-typing primitive). See SPIKE.md.
//!
//! The compiler (`compile`) covers Var/Bound/Int/Tuple(+Spread)/Proj/Binary/If/Fold over non-negative
//! ints; terms it can't lower (List, Case/Inject, Unary, Hash — see `compilable`) fall back to row-wise
//! `ir::eval` in the backend. The transcode layer is total over `Shape` (Prim/Unit/Prod/List/Sum), so a
//! `Variant` column round-trips via corgi `Sum` (see `infer_shape_cols` for the all-rows arm scan).

use crate::ir::Value as DValue;
use crate::parse::{BinOp, Term};

use corgi::{ArithOp, BinOp as CBinOp, Builder, CmpOp, Graph, Kind, NumOp, Op, Pred, Shape, Value as CValue};

/// Dynamic typing: form a corgi `Shape` by observing a sample row.
pub fn infer_shape(sample: &DValue) -> Shape {
    match sample {
        DValue::Int(_) => Shape::Prim(64),
        DValue::Tuple(xs) if xs.is_empty() => Shape::Unit, // DDIR unit ↔ corgi length-carrying Unit
        DValue::Tuple(xs) => Shape::Prod(xs.iter().map(infer_shape).collect()),
        DValue::List(xs) => Shape::List(Box::new(infer_shape(xs.first().expect("nonempty list")))),
        DValue::Variant(..) => panic!("variant shape inference is a later rung"),
    }
}

/// Dynamic typing over a whole COLUMN: infer a `Shape` by scanning every row, not just a sample.
/// Required for sum types — a `Variant` column's shape is the union of all arms that appear, which a
/// single sample can't reveal (it shows only one tag). Non-sum shapes match [`infer_shape`] but recurse
/// column-wise so nested variants are covered too. (`from_updates` uses this; `infer_shape` stays for
/// the single-sample callers that never carry variants.)
pub fn infer_shape_cols(rows: &[DValue]) -> Shape {
    let Some(first) = rows.first() else { return Shape::Unit };
    match first {
        DValue::Int(_) => Shape::Prim(64),
        DValue::Tuple(xs) if xs.is_empty() => Shape::Unit,
        DValue::Tuple(xs) => {
            let n = xs.len();
            Shape::Prod(
                (0..n)
                    .map(|i| {
                        let col: Vec<DValue> = rows
                            .iter()
                            .map(|r| match r {
                                DValue::Tuple(f) => f[i].clone(),
                                other => panic!("infer_shape_cols: expected Tuple, got {other:?}"),
                            })
                            .collect();
                        infer_shape_cols(&col)
                    })
                    .collect(),
            )
        }
        DValue::List(_) => {
            let flat: Vec<DValue> = rows
                .iter()
                .flat_map(|r| match r {
                    DValue::List(xs) => xs.clone(),
                    other => panic!("infer_shape_cols: expected List, got {other:?}"),
                })
                .collect();
            Shape::List(Box::new(infer_shape_cols(&flat)))
        }
        DValue::Variant(..) => {
            // One lane per variant 0..=max_tag; a lane present in the data gets its arm's shape (from
            // that arm's rows), an absent tag stays `None` (⊥, uncommitted — adopts a sibling on join).
            let max_tag = rows
                .iter()
                .map(|r| match r {
                    DValue::Variant(t, _) => *t as usize,
                    other => panic!("infer_shape_cols: expected Variant, got {other:?}"),
                })
                .max()
                .unwrap();
            let lanes = (0..=max_tag)
                .map(|tag| {
                    let payloads: Vec<DValue> = rows
                        .iter()
                        .filter_map(|r| match r {
                            DValue::Variant(t, p) if *t as usize == tag => Some((**p).clone()),
                            _ => None,
                        })
                        .collect();
                    (!payloads.is_empty()).then(|| infer_shape_cols(&payloads))
                })
                .collect();
            Shape::Sum(lanes)
        }
    }
}

/// AoS rows -> SoA corgi columns, directed by `shape`.
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
            // Per-row tag, plus one packed lane per committed variant (its arm's rows in row order;
            // `Value::sum_opt` derives each row's within-lane offset). Absent arms stay `⊥` (None).
            let tags: Vec<usize> = rows
                .iter()
                .map(|r| match r {
                    DValue::Variant(t, _) => *t as usize,
                    other => panic!("transcode: expected Variant, got {other:?}"),
                })
                .collect();
            let lane_vals: Vec<Option<CValue>> = lanes
                .iter()
                .enumerate()
                .map(|(tag, ls)| {
                    ls.as_ref().map(|lshape| {
                        let payloads: Vec<DValue> = rows
                            .iter()
                            .filter_map(|r| match r {
                                DValue::Variant(t, p) if *t as usize == tag => Some((**p).clone()),
                                _ => None,
                            })
                            .collect();
                        transcode(&payloads, lshape)
                    })
                })
                .collect();
            CValue::sum_opt(tags, lane_vals)
        }
    }
}

/// SoA corgi columns -> AoS rows, directed by `shape`. Inverse of [`transcode`].
pub fn untranscode(col: CValue, shape: &Shape) -> Vec<DValue> {
    match shape {
        Shape::Prim(_) => col.into_u64("untranscode").into_iter().map(|x| DValue::Int(x as i64)).collect(),
        Shape::Unit => vec![DValue::unit(); col.len()],
        Shape::Prod(fs) => {
            let cols = col.into_prod("untranscode");
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
            // Inverse of transcode's Sum: untranscode each committed lane, then for each row pull its
            // payload from its lane at the recorded within-lane OFFSET (robust to row reordering from
            // a prior gather/merge — not a sequential cursor).
            let (tags, offsets, variant_vals) = col.into_sum("untranscode");
            let lane_rows: Vec<Option<Vec<DValue>>> = variant_vals
                .into_iter()
                .zip(lanes.iter())
                .map(|(v, ls)| match (v, ls) {
                    (Some(cv), Some(lshape)) => Some(untranscode(cv, lshape)),
                    _ => None,
                })
                .collect();
            tags.iter()
                .zip(&offsets)
                .map(|(&tag, &off)| {
                    let payload = lane_rows[tag].as_ref().expect("untranscode: committed lane")[off].clone();
                    DValue::Variant(tag as u32, Box::new(payload))
                })
                .collect()
        }
    }
}

/// Structural shape of a "place" Term (Var/Proj chain) given the env vars' shapes — used to resolve
/// `Spread`'s arity (how many fields to splice) and (later) Proj-on-list vs Proj-on-tuple.
pub fn shape_of_place(t: &Term, env_shapes: &[Shape]) -> Shape {
    match t {
        Term::Var(i) => env_shapes[*i].clone(),
        Term::Proj(inner, i) => match shape_of_place(inner, env_shapes) {
            Shape::Prod(fs) => fs[*i].clone(),
            Shape::List(e) => *e,
            other => panic!("shape_of_place: Proj on non-aggregate {other:?}"),
        },
        other => panic!("shape_of_place: unsupported place {other:?}"),
    }
}

/// Best-effort structural `Shape` of a (non-place) compiled `Term`, used to decide cross-shape
/// `Eq`/`Ne`: DDIR compares `Value`s structurally, so e.g. `Tuple != Int` is *always* true (the
/// variants differ) — but corgi's `CmpOp::Rel` requires matching shapes. When the operand shapes
/// differ we fold the comparison to a constant instead of emitting a (panicking) `Rel`.
fn infer_term_shape(t: &Term, env_shapes: &[Shape]) -> Shape {
    match t {
        Term::Var(i) => env_shapes[*i].clone(),
        Term::Int(_) => Shape::Prim(64),
        Term::Proj(inner, i) => match infer_term_shape(inner, env_shapes) {
            Shape::Prod(fs) => fs[*i].clone(),
            Shape::List(e) => *e,
            other => panic!("infer_term_shape: Proj on non-aggregate {other:?}"),
        },
        Term::Tuple(fields) => {
            let mut fs = Vec::new();
            for f in fields {
                match f {
                    Term::Spread(inner) => match infer_term_shape(inner, env_shapes) {
                        Shape::Prod(inner_fs) => fs.extend(inner_fs),
                        Shape::Unit => {}
                        other => fs.push(other),
                    },
                    _ => fs.push(infer_term_shape(f, env_shapes)),
                }
            }
            if fs.is_empty() { Shape::Unit } else { Shape::Prod(fs) }
        }
        Term::If { then, .. } => infer_term_shape(then, env_shapes),
        // Arithmetic, comparisons, and anything else scalar-ish reduce to a primitive column.
        _ => Shape::Prim(64),
    }
}

/// Whether [`compile`] can lower this term to a corgi graph. Terms that use features the corgi
/// compiler doesn't model yet — `List`, `Inject`/`Case` (sum types), `Unary`, `Hash` — return false,
/// and the backend falls back to row-wise `ir::eval` (parity with `backend::vec`).
pub fn compilable(t: &Term) -> bool {
    match t {
        Term::Var(_) | Term::Bound(_) | Term::Int(_) => true,
        Term::Proj(inner, _) | Term::Spread(inner) => compilable(inner),
        Term::Tuple(fs) => fs.iter().all(compilable),
        Term::Binary(_, l, r) => compilable(l) && compilable(r),
        Term::If { cond, then, els } => compilable(cond) && compilable(then) && compilable(els),
        Term::Fold { list, init, step } => compilable(list) && compilable(init) && compilable(step),
        _ => false, // List, Inject, Case, Unary, Hash — row-wise fallback.
    }
}

/// Compile a `Term` to a corgi node. `env[i]` = node for `Var(i)`; `env_shapes[i]` = its shape
/// (for `Spread`). Binders push on top (read by `Bound(k)`). `anchor` sizes `Lit` broadcasts.
pub fn compile(term: &Term, b: &mut Builder<NumOp>, env: &[usize], env_shapes: &[Shape], anchor: usize) -> usize {
    match term {
        Term::Var(i) => env[*i],
        Term::Bound(k) => env[env.len() - 1 - *k],
        Term::Int(n) => b.add(Op::Lit(CValue::u64(vec![*n as u64])), vec![anchor]),
        Term::Tuple(fields) => {
            // A `Spread(place)` child splices the place's `Prod` fields in place (the flat-row model).
            let mut ids: Vec<usize> = Vec::new();
            for f in fields {
                match f {
                    Term::Spread(inner) => {
                        let node = compile(inner, b, env, env_shapes, anchor);
                        match shape_of_place(inner, env_shapes) {
                            Shape::Prod(fs) => {
                                for i in 0..fs.len() {
                                    ids.push(b.add(Op::Field(i), vec![node]));
                                }
                            }
                            Shape::Unit => {} // unit splices nothing
                            _ => ids.push(node), // scalar: splice the value itself
                        }
                    }
                    _ => ids.push(compile(f, b, env, env_shapes, anchor)),
                }
            }
            // An empty field list is DDIR unit: emit a length-carrying `Unit` column over the anchor,
            // NOT `Prod([])` (an empty product has no rows to count, so the row count would be lost).
            if ids.is_empty() {
                b.add(Op::Unit, vec![anchor])
            } else {
                b.tuple(ids)
            }
        }
        Term::Proj(t, i) => {
            let id = compile(t, b, env, env_shapes, anchor);
            b.add(Op::Field(*i), vec![id])
        }
        Term::Binary(op, l, r) => {
            let lid = compile(l, b, env, env_shapes, anchor);
            let rid = compile(r, b, env, env_shapes, anchor);
            let pair = |b: &mut Builder<NumOp>, x, y| b.tuple(vec![x, y]);
            match op {
                BinOp::Add => { let p = pair(b, lid, rid); b.add(ArithOp::Bin(CBinOp::Add, Kind::U, 64), vec![p]) }
                BinOp::Sub => { let p = pair(b, lid, rid); b.add(ArithOp::Bin(CBinOp::Sub, Kind::U, 64), vec![p]) }
                BinOp::Mul => { let p = pair(b, lid, rid); b.add(ArithOp::Bin(CBinOp::Mul, Kind::U, 64), vec![p]) }
                BinOp::Eq | BinOp::Ne => {
                    // Cross-shape structural compare folds to a constant (Eq→0, Ne→1) over `anchor`;
                    // same-shape emits a real corgi `Rel`.
                    if infer_term_shape(l, env_shapes) != infer_term_shape(r, env_shapes) {
                        let v = if matches!(op, BinOp::Ne) { 1u64 } else { 0u64 };
                        b.add(Op::Lit(CValue::u64(vec![v])), vec![anchor])
                    } else {
                        let pred = if matches!(op, BinOp::Eq) { Pred::Eq } else { Pred::Ne };
                        let p = pair(b, lid, rid);
                        b.add(CmpOp::Rel(pred), vec![p])
                    }
                }
                BinOp::Lt => { let p = pair(b, lid, rid); b.add(CmpOp::Rel(Pred::Lt), vec![p]) }
                BinOp::Le => { let p = pair(b, lid, rid); b.add(CmpOp::Rel(Pred::Le), vec![p]) }
                BinOp::Gt => { let p = pair(b, rid, lid); b.add(CmpOp::Rel(Pred::Lt), vec![p]) }
                BinOp::Ge => { let p = pair(b, rid, lid); b.add(CmpOp::Rel(Pred::Le), vec![p]) }
                BinOp::And => { let p = pair(b, lid, rid); b.add(CmpOp::Min, vec![p]) }
                BinOp::Or => { let p = pair(b, lid, rid); b.add(CmpOp::Max, vec![p]) }
            }
        }
        Term::If { cond, then, els } => {
            let c = compile(cond, b, env, env_shapes, anchor);
            let t = compile(then, b, env, env_shapes, anchor);
            let e = compile(els, b, env, env_shapes, anchor);
            let sel = b.tuple(vec![c, t, e]);
            b.add(Op::Select, vec![sel])
        }
        // Fold over a List. corgi `Op::Fold` consumes `Prod([seed, List<A>])` and folds each row's
        // list; its body is a closed sub-graph over `Prod([acc, elem])`. DDIR's step sees
        // elem=Bound(0), acc=Bound(1), so the body env is [acc, elem] (Bound counts from the top).
        // Restriction (this rung): the step references only its binders (monoid-style), not outer
        // Vars — corgi closes the body; an outer reference would need CapList capture.
        Term::Fold { list, init, step } => {
            let init_id = compile(init, b, env, env_shapes, anchor);
            let list_id = compile(list, b, env, env_shapes, anchor);
            let pair = b.tuple(vec![init_id, list_id]);
            let body = compile_fold_body(step);
            b.add(Op::Fold(Box::new(body)), vec![pair])
        }
        other => panic!("compile: unsupported Term in this rung: {other:?}"),
    }
}

/// Compile a `Fold` step into a closed corgi sub-graph over `Prod([acc, elem])`.
/// Env `[acc, elem]` so `Bound(0)`=elem (top), `Bound(1)`=acc — matching `ir::eval`'s Fold.
fn compile_fold_body(step: &Term) -> Graph<NumOp> {
    let mut bb = Builder::<NumOp>::default();
    let inp = bb.input();
    let acc = bb.add(Op::Field(0), vec![inp]);
    let elem = bb.add(Op::Field(1), vec![inp]);
    // Monoid fold bodies use only binders (no Spread/Proj-on-list), so no env shapes are needed.
    let out = compile(step, &mut bb, &[acc, elem], &[], inp);
    bb.finish(out)
}

/// Compile a single `Term` whose `Var(0)` is the whole input row/column. (Spread-free terms only —
/// the bench/chain/fold examples — so no env shape is needed.)
pub fn compile_term_single(term: &Term) -> Graph<NumOp> {
    let mut b = Builder::<NumOp>::default();
    let input = b.input();
    let out = compile(term, &mut b, &[input], &[], input);
    b.finish(out)
}

/// Compile a `Filter` predicate over `Var(0)=key` (shape `kshape`), `Var(1)=val` (`vshape`) → mask.
pub fn compile_predicate(cond: &Term, kshape: &Shape, vshape: &Shape) -> Graph<NumOp> {
    let mut b = Builder::<NumOp>::default();
    let input = b.input();
    let var_k = b.add(Op::Field(0), vec![input]);
    let var_v = b.add(Op::Field(1), vec![input]);
    let out = compile(cond, &mut b, &[var_k, var_v], &[kshape.clone(), vshape.clone()], input);
    b.finish(out)
}

/// Compile a join projection: key/val Terms over `Var(0)=key`, `Var(1)=val0`, `Var(2)=val1` (with
/// their shapes for `Spread`). Input `Prod([key, val0, val1])`; output `Prod([newkey, newval])`.
pub fn compile_join_projection(key: &Term, val: &Term, kshape: &Shape, v0shape: &Shape, v1shape: &Shape) -> Graph<NumOp> {
    let mut b = Builder::<NumOp>::default();
    let input = b.input();
    let var_k = b.add(Op::Field(0), vec![input]);
    let var_0 = b.add(Op::Field(1), vec![input]);
    let var_1 = b.add(Op::Field(2), vec![input]);
    let env = [var_k, var_0, var_1];
    let shapes = [kshape.clone(), v0shape.clone(), v1shape.clone()];
    let nk = compile(key, &mut b, &env, &shapes, input);
    let nv = compile(val, &mut b, &env, &shapes, input);
    let out = b.tuple(vec![nk, nv]);
    b.finish(out)
}

/// Compile a DDIR `Projection` over `Var(0)=key` (`kshape`), `Var(1)=val` (`vshape`).
/// Input `Prod([key, val])`; output `Prod([newkey, newval])`.
pub fn compile_projection(key: &Term, val: &Term, kshape: &Shape, vshape: &Shape) -> Graph<NumOp> {
    let mut b = Builder::<NumOp>::default();
    let input = b.input();
    let var_k = b.add(Op::Field(0), vec![input]);
    let var_v = b.add(Op::Field(1), vec![input]);
    let env = [var_k, var_v];
    let shapes = [kshape.clone(), vshape.clone()];
    let nk = compile(key, &mut b, &env, &shapes, input);
    let nv = compile(val, &mut b, &env, &shapes, input);
    let out = b.tuple(vec![nk, nv]);
    b.finish(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::Value as V;

    /// Round-trip a column of rows through infer_shape_cols → transcode → untranscode.
    fn roundtrip(rows: Vec<V>) {
        let shape = infer_shape_cols(&rows);
        let col = transcode(&rows, &shape);
        let back = untranscode(col, &shape);
        assert_eq!(back, rows, "roundtrip mismatch (shape {shape:?})");
    }

    #[test]
    fn roundtrip_variant_single_arm() {
        // binders-style: a single constructor wrapping a list.
        roundtrip(vec![
            V::Variant(0, Box::new(V::List(vec![V::Int(1), V::Int(2)]))),
            V::Variant(0, Box::new(V::List(vec![V::Int(3)]))),
            V::Variant(0, Box::new(V::List(vec![]))),
        ]);
    }

    #[test]
    fn roundtrip_variant_multi_arm() {
        // adt-style: two arms, interleaved; payloads of different shape per arm.
        roundtrip(vec![
            V::Variant(0, Box::new(V::Int(10))),
            V::Variant(1, Box::new(V::Tuple(vec![V::Int(1), V::Int(2)]))),
            V::Variant(0, Box::new(V::Int(20))),
            V::Variant(1, Box::new(V::Tuple(vec![V::Int(3), V::Int(4)]))),
            V::Variant(0, Box::new(V::Int(30))),
        ]);
    }

    #[test]
    fn roundtrip_variant_absent_arm_is_bottom() {
        // tags {0, 2} present, arm 1 absent → a ⊥ lane; round-trip must still reconstruct.
        roundtrip(vec![
            V::Variant(0, Box::new(V::Int(1))),
            V::Variant(2, Box::new(V::Int(2))),
            V::Variant(0, Box::new(V::Int(3))),
        ]);
    }

    #[test]
    fn roundtrip_nested_variant_in_tuple() {
        roundtrip(vec![
            V::Tuple(vec![V::Int(1), V::Variant(0, Box::new(V::Int(7)))]),
            V::Tuple(vec![V::Int(2), V::Variant(1, Box::new(V::unit()))]),
        ]);
    }
}
