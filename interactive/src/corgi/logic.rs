//! corgi as DDIR's columnar scalar logic: compile a `Term` to a corgi `Graph<NumOp>`,
//! transcode DDIR rows (`ir::Value`) to/from corgi columnar `Value`, directed by a `Shape`
//! inferred from the data (the dynamic-typing primitive).
//!
//! The compiler (`compile`) covers Var/Bound/Int/Tuple(+Spread)/Proj/Binary/If/Fold, list and
//! sum intro (`List`/`Inject`), sum elimination (`Case`), and the Neg/Not/Len/IsTag unaries.
//! Ordered compares are signed-correct (`ToSigned`); the residual non-negative-int assumption is
//! confined to order-SENSITIVE contexts (the `Min` reducer and structural sort order compare raw
//! `u64` bits). `hash` is corgi's structural `Op::Hash`, the same function `ir::eval` folds row-wise.
//! Only shape-dependent cases decline (heterogeneous lists, conflicting `Case` arms, data-driven
//! tags); those fall back to row-wise `ir::eval` in the backend. The transcode layer is total over `Shape` (Prim/Unit/Prod/List/Sum), so a
//! `Variant` column round-trips via corgi `Sum` (see `infer_shape_cols` for the all-rows arm scan).

use crate::ir::Value as DValue;
use crate::parse::{BinOp, Term, UnOp};

use corgi::{ArithOp, BinOp as CBinOp, Builder, CmpOp, Graph, Kind, NumOp, Op, Pred, Shape, Value as CValue};

/// Dynamic typing over a whole COLUMN: infer a `Shape` by scanning every row, not just a sample.
/// Required for sum types — a `Variant` column's shape is the union of all arms that appear, which a
/// single sample can't reveal (it shows only one tag), so the scan is over every row, recursing
/// column-wise to cover nested variants too.
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

/// TOTAL best-effort shape of a `Proj` operand: resolve the `Var`/`Bound`/`Proj` spine
/// through KNOWN shapes only; `None` means "can't tell" (never a panic), and the caller
/// keeps the old lowering. The non-panicking cousin of [`shape_of_place`].
fn place_shape_opt(t: &Term, env_shapes: &[Shape]) -> Option<Shape> {
    match t {
        Term::Var(i) => env_shapes.get(*i).cloned(),
        Term::Bound(k) => env_shapes.get(env_shapes.len().checked_sub(1 + *k)?).cloned(),
        Term::Proj(inner, i) => match place_shape_opt(inner, env_shapes)? {
            Shape::Prod(fs) => fs.get(*i).cloned(),
            Shape::List(e) => Some(*e),
            _ => None,
        },
        _ => None,
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
        // A list literal is a `List` of its elements' joined shape. `Fold` reads this to find its
        // element shape, so a literal folded in place (`fold(list(..), ..)`) depends on it; a
        // heterogeneous literal keeps the first field's shape here and declines in `compile`.
        Term::List(fields) => {
            let elem = fields
                .iter()
                .map(|f| infer_term_shape(f, env_shapes))
                .reduce(|acc, s| shape_join(&acc, &s).unwrap_or(acc))
                .unwrap_or(Shape::Unit);
            Shape::List(Box::new(elem))
        }
        Term::Bound(k) => env_shapes.get(env_shapes.len().wrapping_sub(1 + *k)).cloned().unwrap_or(Shape::Prim(64)),
        Term::If { then, els, .. } => {
            // Join the branch shapes (⊥ sum lanes unify), so a downstream `Case` sees every
            // lane either branch can commit; under-approximating lanes would leave runtime
            // rows unmapped.
            let t = infer_term_shape(then, env_shapes);
            shape_join(&t, &infer_term_shape(els, env_shapes)).unwrap_or(t)
        }
        Term::Case { scrutinee, arms, default } => {
            // The joined shape of the reachable arms (the committed scrutinee lanes).
            let lanes = match infer_term_shape(scrutinee, env_shapes) { Shape::Sum(l) => l, _ => Vec::new() };
            let mut shape: Option<Shape> = None;
            for (i, lane) in lanes.iter().enumerate() {
                let Some(lane_shape) = lane else { continue };
                let s = if i < arms.len() {
                    let mut es = env_shapes.to_vec();
                    es.push(lane_shape.clone());
                    infer_term_shape(&arms[i], &es)
                } else if let Some(d) = default {
                    infer_term_shape(d, env_shapes)
                } else {
                    continue;
                };
                shape = Some(match shape { None => s, Some(prev) => shape_join(&prev, &s).unwrap_or(prev) });
            }
            shape.unwrap_or(Shape::Prim(64))
        }
        Term::Inject(tag, payload) => {
            let t = match &**tag { Term::Int(t) => *t as usize, _ => 0 };
            let mut lanes: Vec<Option<Shape>> = vec![None; t + 1];
            lanes[t] = Some(infer_term_shape(payload, env_shapes));
            Shape::Sum(lanes)
        }
        // Arithmetic, comparisons, and anything else scalar-ish reduce to a primitive column.
        _ => Shape::Prim(64),
    }
}

/// The ⊥-tolerant join of two shapes: `Sum` lanes unify lane-wise with an uncommitted (`None`)
/// lane adopting its sibling; `None` (the function's) means the shapes genuinely conflict.
/// Local until corgi exports its `shape::join`.
fn shape_join(a: &Shape, b: &Shape) -> Option<Shape> {
    match (a, b) {
        (Shape::Prim(x), Shape::Prim(y)) if x == y => Some(Shape::Prim(*x)),
        (Shape::Unit, Shape::Unit) => Some(Shape::Unit),
        (Shape::Prod(xs), Shape::Prod(ys)) if xs.len() == ys.len() => {
            let fs: Option<Vec<Shape>> = xs.iter().zip(ys).map(|(x, y)| shape_join(x, y)).collect();
            Some(Shape::Prod(fs?))
        }
        (Shape::List(x), Shape::List(y)) => Some(Shape::List(Box::new(shape_join(x, y)?))),
        (Shape::Sum(xs), Shape::Sum(ys)) => {
            let n = xs.len().max(ys.len());
            let mut lanes = Vec::with_capacity(n);
            for i in 0..n {
                lanes.push(match (xs.get(i).cloned().flatten(), ys.get(i).cloned().flatten()) {
                    (Some(x), Some(y)) => Some(shape_join(&x, &y)?),
                    (x, y) => x.or(y),
                });
            }
            Some(Shape::Sum(lanes))
        }
        _ => None,
    }
}

/// Whether [`compile`] can lower this term WITHOUT knowing its operands' shapes — the gate for
/// join-INLINE projections, which are compiled before any container is in hand. It is therefore
/// deliberately narrower than `compile`: every shape-dependent form (`List`, `Case`, data-driven
/// `Inject`) answers false here and is compiled by the linear stage the join defers it to, which
/// does have shapes. Capability never depends on this; only where the work happens.
///
/// The only term with no lowering at all is a data-driven `Inject` tag, which has no static lane
/// count — and no surface syntax either, so nothing a program can write reaches the row-wise
/// fallback on shape-free grounds.
pub fn compilable(t: &Term) -> bool {
    match t {
        Term::Var(_) | Term::Bound(_) | Term::Int(_) => true,
        Term::Proj(inner, _) | Term::Spread(inner) => compilable(inner),
        Term::Tuple(fs) => fs.iter().all(compilable),
        Term::Binary(_, l, r) => compilable(l) && compilable(r),
        Term::If { cond, then, els } => compilable(cond) && compilable(then) && compilable(els),
        Term::Fold { list, init, step } => compilable(list) && compilable(init) && compilable(step),
        Term::Unary(op, inner) => matches!(op, UnOp::Neg | UnOp::Not | UnOp::Len | UnOp::IsTag(_)) && compilable(inner),
        // Literal-tag sum intro lowers (`Op::Inject`); a data-driven tag has no static lane
        // count, so it stays row-wise.
        Term::Inject(tag, payload) => matches!(&**tag, Term::Int(_)) && compilable(payload),
        // `Op::Hash` is shape-generic (it folds whatever structure it is handed), so `hash`
        // needs no shapes to lower and can answer true here.
        Term::Hash(args) => args.iter().all(compilable),
        // `List` and `Case` deliberately stay false HERE even though `compile` lowers both:
        // each needs shapes to decide homogeneity (a list's elements, a case's arms), and this
        // check runs without them. The join defers such projections to a linear stage, whose
        // shape-aware `compile` lowers them there.
        _ => false, // List intro, Case (here), data-driven Inject — see `compile`.
    }
}

/// Compile a `Term` to a corgi node. `env[i]` = node for `Var(i)`; `env_shapes[i]` = its shape
/// (for `Spread`). Binders push on top (read by `Bound(k)`). `anchor` sizes `Lit` broadcasts.
pub fn compile(term: &Term, b: &mut Builder<NumOp>, env: &[usize], env_shapes: &[Shape], anchor: usize) -> Option<usize> {
    match term {
        // Out-of-range env references decline rather than panic: closed bodies (fold steps,
        // case arms) truncate the environment by design, and a term reaching past it is the
        // documented restriction speaking — rows handle it.
        Term::Var(i) => env.get(*i).copied(),
        Term::Bound(k) => env.len().checked_sub(1 + *k).map(|i| env[i]),
        Term::Int(n) => Some(b.add(Op::Lit(CValue::u64(vec![*n as u64])), vec![anchor])),
        Term::Tuple(fields) => {
            // A `Spread(place)` child splices the place's `Prod` fields in place (the flat-row model).
            let mut ids: Vec<usize> = Vec::new();
            for f in fields {
                match f {
                    Term::Spread(inner) => {
                        let node = compile(inner, b, env, env_shapes, anchor)?;
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
                    _ => ids.push(compile(f, b, env, env_shapes, anchor)?),
                }
            }
            // An empty field list is DDIR unit: emit a length-carrying `Unit` column over the anchor,
            // NOT `Prod([])` (an empty product has no rows to count, so the row count would be lost).
            if ids.is_empty() {
                Some(b.add(Op::Unit, vec![anchor]))
            } else {
                Some(b.tuple(ids))
            }
        }
        Term::Proj(t, i) => {
            // Shape-directed: `Op::Field` is PRODUCT elimination only. On a `List` operand,
            // DDIR's Proj means indexing (`ir::eval` ir.rs:112), which has no columnar
            // lowering here yet (corgi's Get tier) — decline, and rows handle it. The check
            // is the TOTAL place resolver (never panics); an unresolved operand compiles as
            // before (`Field`, with corgi's runtime shape check behind it).
            if matches!(place_shape_opt(t, env_shapes), Some(Shape::List(_))) {
                return None;
            }
            let id = compile(t, b, env, env_shapes, anchor)?;
            Some(b.add(Op::Field(*i), vec![id]))
        }
        Term::Binary(op, l, r) => {
            let lid = compile(l, b, env, env_shapes, anchor)?;
            let rid = compile(r, b, env, env_shapes, anchor)?;
            let pair = |b: &mut Builder<NumOp>, x, y| b.tuple(vec![x, y]);
            Some(match op {
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
            // `Select` blends per row and is shape-generic, but the branches must agree up to
            // ⊥ lanes; genuinely conflicting branch shapes (dynamic typing) defer to rows.
            // Sum-shaped results included: `Select` gathers lanes, and the branches commit
            // different ones (`if(c, Fwd(x), Bwd(y))` is the shape of every conditional
            // constructor), which is what the pin bumped for in #817.
            shape_join(&infer_term_shape(then, env_shapes), &infer_term_shape(els, env_shapes))?;
            let c = compile(cond, b, env, env_shapes, anchor)?;
            let t = compile(then, b, env, env_shapes, anchor)?;
            let e = compile(els, b, env, env_shapes, anchor)?;
            let sel = b.tuple(vec![c, t, e]);
            Some(b.add(Op::Select, vec![sel]))
        }
        // Fold over a List. corgi `Op::Fold` consumes `Prod([seed, List<A>])` and folds each row's
        // list; its body is a closed sub-graph over `Prod([acc, elem])`. DDIR's step sees
        // elem=Bound(0), acc=Bound(1), so the body env is [acc, elem] (Bound counts from the top).
        // Restriction: the step references only its binders (monoid-style), not outer
        // Vars — corgi closes the body; an outer reference would need CapList capture.
        Term::Fold { list, init, step } => {
            let init_id = compile(init, b, env, env_shapes, anchor)?;
            let list_id = compile(list, b, env, env_shapes, anchor)?;
            let elem = match infer_term_shape(list, env_shapes) { Shape::List(e) => *e, _ => return None };
            let init_shape = infer_term_shape(init, env_shapes);
            let pair = b.tuple(vec![init_id, list_id]);
            let body = compile_fold_body(step, &init_shape, &elem)?;
            Some(b.add(Op::Fold(Box::new(body)), vec![pair]))
        }
        // Literal-tag sum intro is `Op::Inject` (lane t of a t+1-lane sum); a data-driven tag
        // has no static lane count, so it defers to rows.
        Term::Inject(tag, payload) => {
            let Term::Int(t) = &**tag else { return None };
            let pid = compile(payload, b, env, env_shapes, anchor)?;
            Some(b.add(Op::Inject(*t as usize, *t as usize + 1), vec![pid]))
        }
        // Sum elimination: distribute the environment into each committed lane (`CapSum`), run
        // each arm as a closed body over `Prod([ctx, payload])` (`MapSum`), and collapse the
        // homogeneous result (`Unwrap`). Arms see the outer env plus the payload as the top
        // binder; a `default` runs WITHOUT the payload binder (matching `eval`). Arms whose
        // result shapes genuinely conflict (dynamic typing) defer to rows, as does a lane with
        // neither arm nor default (where `eval` panics).
        Term::Case { scrutinee, arms, default } => {
            let Shape::Sum(lanes) = infer_term_shape(scrutinee, env_shapes) else { return None };
            let sid = compile(scrutinee, b, env, env_shapes, anchor)?;
            let ctx = b.tuple(env.to_vec());
            let cap_in = b.tuple(vec![ctx, sid]);
            let cap = b.add(Op::CapSum, vec![cap_in]);
            let mut bodies: Vec<(usize, Graph<NumOp>)> = Vec::new();
            let mut result: Option<Shape> = None;
            for (i, lane) in lanes.iter().enumerate() {
                let Some(lane_shape) = lane else { continue };
                let mut bb = Builder::<NumOp>::default();
                let inp = bb.input();
                let cnode = bb.add(Op::Field(0), vec![inp]);
                let mut env2: Vec<usize> = (0..env.len()).map(|j| bb.add(Op::Field(j), vec![cnode])).collect();
                let mut shapes2: Vec<Shape> = env_shapes.to_vec();
                let (out, out_shape) = if i < arms.len() {
                    let pnode = bb.add(Op::Field(1), vec![inp]);
                    env2.push(pnode);
                    shapes2.push(lane_shape.clone());
                    (compile(&arms[i], &mut bb, &env2, &shapes2, inp)?, infer_term_shape(&arms[i], &shapes2))
                } else if let Some(d) = default {
                    (compile(d, &mut bb, &env2, &shapes2, inp)?, infer_term_shape(d, &shapes2))
                } else {
                    return None;
                };
                result = Some(match result { None => out_shape, Some(prev) => shape_join(&prev, &out_shape)? });
                bodies.push((i, bb.finish(out)));
            }
            if bodies.is_empty() {
                return None; // an all-⊥ scrutinee shape: nothing to map
            }
            let mapped = b.add(Op::MapSum(bodies), vec![cap]);
            Some(b.add(Op::Unwrap, vec![mapped]))
        }
        Term::Unary(op, inner) => {
            let id = compile(inner, b, env, env_shapes, anchor)?;
            Some(match op {
                // Wrapping negate on the raw two's-complement bits — exactly `-as_int()`.
                // (Order-sensitive use of negatives inherits the crate-wide non-negative-int
                // comparison contract; `Neg` adds no new exposure over `Sub` below zero.)
                UnOp::Neg => b.add(ArithOp::Neg(Kind::U, 64), vec![id]),
                // `truthy` is "nonzero Int": scalars compare against zero; non-`Int` values
                // are never truthy, so their `not` folds to the constant 1 (the cross-shape
                // `Eq` fold's precedent).
                UnOp::Not => match infer_term_shape(inner, env_shapes) {
                    Shape::Prim(_) => {
                        let zero = b.add(Op::Lit(CValue::u64(vec![0])), vec![anchor]);
                        let p = b.tuple(vec![id, zero]);
                        b.add(CmpOp::Rel(Pred::Eq), vec![p])
                    }
                    _ => b.add(Op::Lit(CValue::u64(vec![1])), vec![anchor]),
                },
                // Tuple arity is static (a shape fact); list length folds `acc + 1` along
                // each row's list; anything else is the program error `eval` reports.
                UnOp::Len => match infer_term_shape(inner, env_shapes) {
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
                    _ => return None,
                },
                // On a sum, every committed lane maps to its constant answer and the result
                // unwraps (lanes are homogeneous `U64`); on any other shape, `istag` is
                // constantly 0 (matching `eval`'s "non-Variant is never the tag").
                UnOp::IsTag(t) => match infer_term_shape(inner, env_shapes) {
                    Shape::Sum(lanes) => {
                        let arms: Vec<(usize, Graph<NumOp>)> = lanes
                            .iter()
                            .enumerate()
                            .filter_map(|(i, lane)| {
                                lane.as_ref().map(|_| {
                                    let mut bb = Builder::<NumOp>::default();
                                    let inp = bb.input();
                                    let v = (i as u32 == *t) as u64;
                                    let out = bb.add(Op::Lit(CValue::u64(vec![v])), vec![inp]);
                                    (i, bb.finish(out))
                                })
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
        // strips the now-homogeneous sum. A fused list-intro kernel is corgi's call if this
        // composition ever profiles hot.
        //
        // Empty and heterogeneous literals decline: `Weave` needs at least one lane, and
        // `Unwrap` needs the committed lanes to join. Rows handle both.
        Term::List(fields) => {
            let (first, rest) = fields.split_first()?;
            rest.iter().try_fold(infer_term_shape(first, env_shapes), |acc, f| {
                shape_join(&acc, &infer_term_shape(f, env_shapes))
            })?;
            let mut lanes = Vec::with_capacity(fields.len());
            for f in fields {
                let e = compile(f, b, env, env_shapes, anchor)?;
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
            Some(b.add(Op::MapList(Box::new(unwrap_body)), vec![woven]))
        }
        // DDIR's `hash` IS corgi's `Op::Hash` (`ir::structural_hash` is the row-wise twin): hash
        // the arguments as one tuple, shift out the sign bit, reduce by the bound.
        //
        // The bound guard is pure arithmetic — no `Select`. `Rem`'s total `x % 0 = x` gives
        // `bound == 0` the identity, and a NEGATIVE bound reads as a `u64` at or above 2^63,
        // which is larger than the shifted hash, so it reduces to the identity too. Both are
        // exactly what `ir::eval`'s `if bound > 0` produces.
        Term::Hash(args) => {
            let (bound, rest) = args.split_first()?;
            let bid = compile(bound, b, env, env_shapes, anchor)?;
            let payload = if rest.is_empty() {
                b.add(Op::Unit, vec![anchor])
            } else {
                let mut ids = Vec::with_capacity(rest.len());
                for a in rest {
                    ids.push(compile(a, b, env, env_shapes, anchor)?);
                }
                b.tuple(ids)
            };
            let h = b.add(Op::Hash, vec![payload]);
            let shifted = b.add(ArithOp::Shr(1), vec![h]);
            let pair = b.tuple(vec![shifted, bid]);
            Some(b.add(ArithOp::Bin(CBinOp::Rem, Kind::U, 64), vec![pair]))
        }
        _ => None,
    }
}

/// Compile a `Fold` step into a closed corgi sub-graph over `Prod([acc, elem])`.
/// Env `[acc, elem]` so `Bound(0)`=elem (top), `Bound(1)`=acc — matching `ir::eval`'s Fold.
fn compile_fold_body(step: &Term, init_shape: &Shape, elem_shape: &Shape) -> Option<Graph<NumOp>> {
    let mut bb = Builder::<NumOp>::default();
    let inp = bb.input();
    let acc = bb.add(Op::Field(0), vec![inp]);
    let elem = bb.add(Op::Field(1), vec![inp]);
    let out = compile(step, &mut bb, &[acc, elem], &[init_shape.clone(), elem_shape.clone()], inp)?;
    Some(bb.finish(out))
}

/// Compile a term in the row environment `Var(0)=key` (shape `kshape`), `Var(1)=val` (`vshape`) —
/// the environment every `LinearOp` reads. The graph's input is `Prod([key, val])`. `None` when
/// the term has no lowering with these shapes; every caller falls back to rows there.
fn compile_over_kv(term: &Term, kshape: &Shape, vshape: &Shape) -> Option<Graph<NumOp>> {
    let mut b = Builder::<NumOp>::default();
    let input = b.input();
    let var_k = b.add(Op::Field(0), vec![input]);
    let var_v = b.add(Op::Field(1), vec![input]);
    let out = compile(term, &mut b, &[var_k, var_v], &[kshape.clone(), vshape.clone()], input)?;
    Some(b.finish(out))
}

/// Compile a `FlatMap`'s list term → a corgi `List` column, one list per input row. Declines when
/// the term is not list-shaped: the backend explodes the column structurally and needs real list
/// bounds to do it, where `ir::eval` would take any `List` value it happened to produce.
pub fn compile_flatmap(list_term: &Term, kshape: &Shape, vshape: &Shape) -> Option<Graph<NumOp>> {
    matches!(infer_term_shape(list_term, &[kshape.clone(), vshape.clone()]), Shape::List(_))
        .then(|| compile_over_kv(list_term, kshape, vshape))
        .flatten()
}

/// Compile a scalar term (`EnterAt`'s delay field) → a `U64` column. Declines when the term is not
/// `Prim`-shaped: the delay is read as one integer per row, which a `Prod`/`List`/`Sum` column has
/// no reading of.
pub fn compile_scalar(term: &Term, kshape: &Shape, vshape: &Shape) -> Option<Graph<NumOp>> {
    matches!(infer_term_shape(term, &[kshape.clone(), vshape.clone()]), Shape::Prim(_))
        .then(|| compile_over_kv(term, kshape, vshape))
        .flatten()
}

/// Compile a `Filter` predicate → a mask column (nonzero keeps the row).
pub fn compile_predicate(cond: &Term, kshape: &Shape, vshape: &Shape) -> Option<Graph<NumOp>> {
    compile_over_kv(cond, kshape, vshape)
}

/// Compile a join projection: key/val Terms over `Var(0)=key`, `Var(1)=val0`, `Var(2)=val1` (with
/// their shapes for `Spread`). Input `Prod([key, val0, val1])`; output `Prod([newkey, newval])`.
/// Join-inline projections are gated by [`compilable`], so the lowering must succeed.
pub fn compile_join_projection(key: &Term, val: &Term, kshape: &Shape, v0shape: &Shape, v1shape: &Shape) -> Graph<NumOp> {
    let mut b = Builder::<NumOp>::default();
    let input = b.input();
    let var_k = b.add(Op::Field(0), vec![input]);
    let var_0 = b.add(Op::Field(1), vec![input]);
    let var_1 = b.add(Op::Field(2), vec![input]);
    let env = [var_k, var_0, var_1];
    let shapes = [kshape.clone(), v0shape.clone(), v1shape.clone()];
    let nk = compile(key, &mut b, &env, &shapes, input).expect("join-inline projections are gated by `compilable`");
    let nv = compile(val, &mut b, &env, &shapes, input).expect("join-inline projections are gated by `compilable`");
    let out = b.tuple(vec![nk, nv]);
    b.finish(out)
}

/// Compile a DDIR `Projection` over `Var(0)=key` (`kshape`), `Var(1)=val` (`vshape`).
/// Input `Prod([key, val])`; output `Prod([newkey, newval])`. `None` when either term (with
/// these shapes) has no lowering; the caller falls back to rows.
pub fn compile_projection(key: &Term, val: &Term, kshape: &Shape, vshape: &Shape) -> Option<Graph<NumOp>> {
    let mut b = Builder::<NumOp>::default();
    let input = b.input();
    let var_k = b.add(Op::Field(0), vec![input]);
    let var_v = b.add(Op::Field(1), vec![input]);
    let env = [var_k, var_v];
    let shapes = [kshape.clone(), vshape.clone()];
    let nk = compile(key, &mut b, &env, &shapes, input)?;
    let nv = compile(val, &mut b, &env, &shapes, input)?;
    let out = b.tuple(vec![nk, nv]);
    Some(b.finish(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::Value as V;

    /// The pin on DDIR's `hash`: `ir::structural_hash` is a row-at-a-time transcription of
    /// `corgi::hash`, and the two backends compute the SAME program value, so they must agree
    /// bit for bit on every shape the transcode layer covers. If corgi's salts or fold change,
    /// this is what fails.
    fn hash_agrees(rows: Vec<V>) {
        let shape = infer_shape_cols(&rows);
        let col = transcode(&rows, &shape);
        let columnar = corgi::hash(&col).into_u64("hash");
        let row_wise: Vec<u64> = rows.iter().map(crate::ir::structural_hash).collect();
        assert_eq!(columnar, row_wise, "hash disagrees (shape {shape:?})");
    }

    #[test]
    fn hash_matches_corgi_on_scalars() {
        hash_agrees(vec![V::Int(0), V::Int(1), V::Int(-1), V::Int(i64::MIN), V::Int(i64::MAX)]);
    }

    #[test]
    fn hash_matches_corgi_on_tuples_and_units() {
        hash_agrees(vec![V::Tuple(vec![V::Int(1), V::Int(2)]), V::Tuple(vec![V::Int(2), V::Int(1)])]);
        hash_agrees(vec![V::unit(), V::unit()]);
        // A 1-tuple must not collapse onto its scalar, nor a unit onto an empty anything.
        assert_ne!(
            crate::ir::structural_hash(&V::Tuple(vec![V::Int(7)])),
            crate::ir::structural_hash(&V::Int(7))
        );
    }

    #[test]
    fn hash_matches_corgi_on_lists() {
        hash_agrees(vec![
            V::List(vec![V::Int(1), V::Int(2), V::Int(3)]),
            V::List(vec![]),
            V::List(vec![V::Int(3), V::Int(2), V::Int(1)]),
        ]);
    }

    #[test]
    fn hash_matches_corgi_on_variants() {
        hash_agrees(vec![
            V::Variant(0, Box::new(V::Int(5))),
            V::Variant(1, Box::new(V::Int(5))),
            V::Variant(0, Box::new(V::Int(6))),
        ]);
    }

    #[test]
    fn hash_matches_corgi_on_nesting() {
        hash_agrees(vec![
            V::Tuple(vec![V::List(vec![V::Int(1)]), V::Variant(0, Box::new(V::Tuple(vec![V::Int(2), V::Int(3)])))]),
            V::Tuple(vec![V::List(vec![V::Int(1), V::Int(1)]), V::Variant(0, Box::new(V::Tuple(vec![V::Int(2), V::Int(4)])))]),
        ]);
    }

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
