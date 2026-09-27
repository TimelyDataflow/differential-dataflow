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
//! the kernels enforce, and a program that lowers is a program that runs. `Err` is a type error,
//! reported with corgi's message. Ordered compares are signed-correct (`ToSigned`); `hash` is corgi's structural
//! `Op::Hash`, the same function `ir::eval` folds row-wise.

use crate::ir::{BinOp, F64Fn, SumTy, Term, UnOp, Value as DValue};

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
/// Borrowing form: one clone of the rows, then [`transcode_owned`].
pub fn transcode(rows: &[DValue], shape: &Shape) -> CValue {
    transcode_owned(rows.to_vec(), shape)
}

/// As [`transcode`], consuming the rows: each field moves into its column, nothing is cloned.
pub fn transcode_owned(rows: Vec<DValue>, shape: &Shape) -> CValue {
    match shape {
        Shape::Prim(_) => CValue::u64(rows.iter().map(|r| r.as_int() as u64).collect()),
        Shape::Unit => CValue::Unit(rows.len()),
        Shape::Prod(fs) => {
            // Transpose by moving: one Vec per field, filled by draining each tuple.
            let mut cols: Vec<Vec<DValue>> = fs.iter().map(|_| Vec::with_capacity(rows.len())).collect();
            for r in rows {
                match r {
                    DValue::Tuple(xs) => {
                        assert!(xs.len() >= fs.len(), "transcode: a {}-tuple for a {}-field shape", xs.len(), fs.len());
                        for (col, x) in cols.iter_mut().zip(xs) { col.push(x) }
                    }
                    other => panic!("transcode: expected Tuple, got {other:?}"),
                }
            }
            CValue::Prod(cols.into_iter().zip(fs).map(|(c, fsi)| transcode_owned(c, fsi)).collect())
        }
        Shape::List(elem) => {
            // List column = per-row END offsets + a flattened element column.
            let mut ends = Vec::with_capacity(rows.len());
            let mut flat: Vec<DValue> = Vec::new();
            for r in rows {
                match r {
                    DValue::List(xs) => {
                        flat.extend(xs);
                        ends.push(flat.len());
                    }
                    other => panic!("transcode: expected List, got {other:?}"),
                }
            }
            CValue::List(ends.into(), Box::new(transcode_owned(flat, elem)))
        }
        Shape::Sum(lanes) => {
            // Per-row tag, plus one packed lane per variant (its arm's rows in row order; a
            // variant no row uses is an empty column of its declared shape).
            let mut tags: Vec<usize> = Vec::with_capacity(rows.len());
            let mut payloads: Vec<Vec<DValue>> = lanes.iter().map(|_| Vec::new()).collect();
            for r in rows {
                match r {
                    DValue::Variant(t, p) => {
                        let t = t as usize;
                        if t >= lanes.len() { panic!("transcode: tag {t} is outside the declared {}-variant sum", lanes.len()) }
                        tags.push(t);
                        payloads[t].push(*p);
                    }
                    other => panic!("transcode: expected Variant, got {other:?}"),
                }
            }
            let lane_vals = payloads.into_iter().zip(lanes).map(|(p, lshape)| transcode_owned(p, lshape)).collect();
            CValue::sum(tags, lane_vals)
        }
    }
}

/// SoA corgi columns -> AoS rows, directed by `shape`. Inverse of [`transcode`]. Each value
/// moves into its row; nothing is cloned.
pub fn untranscode(col: CValue, shape: &Shape) -> Vec<DValue> {
    match shape {
        Shape::Prim(_) => col.into_u64("untranscode").unwrap().into_iter().map(|x| DValue::Int(x as i64)).collect(),
        Shape::Unit => vec![DValue::unit(); col.len()],
        Shape::Prod(fs) => {
            let cols = col.into_prod("untranscode").unwrap();
            let n = if cols.is_empty() { 0 } else { cols[0].len() };
            let mut per_field: Vec<std::vec::IntoIter<DValue>> =
                cols.into_iter().zip(fs.iter()).map(|(c, fsi)| untranscode(c, fsi).into_iter()).collect();
            (0..n).map(|_| DValue::Tuple(per_field.iter_mut().map(|f| f.next().expect("field columns agree in length")).collect())).collect()
        }
        Shape::List(elem) => {
            // Inverse of transcode's List: per-row END offsets + a flattened element column → one
            // `List` per row, taking each row's span off the front of the untranscoded column.
            let (bounds, vals) = match col {
                CValue::List(b, vals) => (b, *vals),
                other => panic!("untranscode: expected List, got {other:?}"),
            };
            let mut flat = untranscode(vals, elem).into_iter();
            let ends: Vec<usize> = bounds.to_vec();
            let mut out = Vec::with_capacity(ends.len());
            let mut start = 0usize;
            for end in ends {
                out.push(DValue::List(flat.by_ref().take(end - start).collect()));
                start = end;
            }
            out
        }
        Shape::Sum(lanes) => {
            // Inverse of transcode's Sum: untranscode each lane, then for each row take its payload
            // from its lane at the recorded within-lane OFFSET (robust to row reordering from a
            // prior gather/merge — not a sequential cursor). Rows may share a payload, so each is
            // cloned except at its last use, where it moves.
            let (tags, variant_vals) = col.into_sum("untranscode").unwrap();
            let mut lane_rows: Vec<Vec<DValue>> =
                variant_vals.into_iter().zip(lanes.iter()).map(|(v, ls)| untranscode(v, ls)).collect();
            let mut uses: Vec<Vec<u32>> = lane_rows.iter().map(|l| vec![0; l.len()]).collect();
            for r in 0..tags.len() { uses[tags.tag_at(r)][tags.offset_at(r)] += 1 }
            (0..tags.len())
                .map(|r| {
                    let (tag, off) = (tags.tag_at(r), tags.offset_at(r));
                    uses[tag][off] -= 1;
                    let payload = if uses[tag][off] == 0 {
                        std::mem::replace(&mut lane_rows[tag][off], DValue::Int(0))
                    } else {
                        lane_rows[tag][off].clone()
                    };
                    DValue::Variant(tag as u32, Box::new(payload))
                })
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
        Term::Tuple(fs) | Term::List(fs) | Term::Hash(fs) | Term::Call(_, fs) => fs.iter().any(|f| mentions_env(f, depth)),
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
        Term::Call(name, args) => {
            // One host-kernel node over the tuple of its arguments; the kernel checks their shapes.
            let kernel = crate::ir::kernel_of(name).ok_or_else(|| format!("`{name}` is not a registered function"))?;
            // A call with no arguments passes a `Unit` over the anchor (`ir::call_input`).
            let input = if args.is_empty() {
                b.add(Op::Unit, vec![anchor])
            } else {
                let fields = args.iter().map(|a| compile(a, b, env, env_shapes, anchor, None)).collect::<Res<Vec<_>>>()?;
                b.tuple(fields)
            };
            Ok(b.add(NumOp::Host(corgi::HostOp(kernel)), vec![input]))
        }
        Term::Var(i) => env.get(*i).copied().ok_or_else(|| format!("`${i}` is not in scope here")),
        Term::Bound(k) => {
            env.len().checked_sub(1 + *k).map(|i| env[i]).ok_or_else(|| format!("binder `^{k}` is not in scope here"))
        }
        Term::Int(n) => Ok(b.add(Op::Lit(CValue::u64(vec![*n as u64])), vec![anchor])),
        Term::Tuple(fields) => {
            // A `Spread(t)` child splices `t`'s `Prod` fields in place; any other value is one field.
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
                BinOp::Div => {
                    let l = b.add(ArithOp::ToSigned, vec![lid]);
                    let r = b.add(ArithOp::ToSigned, vec![rid]);
                    let p = pair(b, l, r);
                    let q = b.add(ArithOp::Bin(CBinOp::Div, Kind::I, 64), vec![p]);
                    b.add(ArithOp::ToSigned, vec![q])
                }
                BinOp::Append => { let p = pair(b, lid, rid); b.add(Op::Append, vec![p]) }
                BinOp::F64Add | BinOp::F64Sub | BinOp::F64Mul | BinOp::F64Div => {
                    let f64_shape = Shape::Sum(vec![Shape::Prim(64)]);
                    if shape_of_term(l, env_shapes, None)? != f64_shape || shape_of_term(r, env_shapes, None)? != f64_shape {
                        return Err("floating arithmetic expects two F64 newtypes; use float(int)".into());
                    }
                    let l = b.add(Op::Unwrap, vec![lid]);
                    let r = b.add(Op::Unwrap, vec![rid]);
                    let l = b.add(ArithOp::ToSigned, vec![l]);
                    let r = b.add(ArithOp::ToSigned, vec![r]);
                    let p = pair(b, l, r);
                    let op = match op { BinOp::F64Add => CBinOp::Add, BinOp::F64Sub => CBinOp::Sub, BinOp::F64Mul => CBinOp::Mul, _ => CBinOp::Div };
                    let f = b.add(ArithOp::Bin(op, Kind::F, 64), vec![p]);
                    let payload = b.add(ArithOp::ToSigned, vec![f]);
                    b.add(Op::Inject(0, vec![Shape::Prim(64)]), vec![payload])
                }
                BinOp::F64Min | BinOp::F64Max | BinOp::F64Eq | BinOp::F64Ne | BinOp::F64Lt | BinOp::F64Le | BinOp::F64Gt | BinOp::F64Ge => {
                    let f64_shape = Shape::Sum(vec![Shape::Prim(64)]);
                    if shape_of_term(l, env_shapes, None)? != f64_shape || shape_of_term(r, env_shapes, None)? != f64_shape {
                        return Err(format!("{op:?} expects two F64 newtypes; use float(int)"));
                    }
                    let (x, y) = (float_leaf(b, lid), float_leaf(b, rid));
                    match op {
                        BinOp::F64Min | BinOp::F64Max => {
                            // The total-order pick, then a NaN operand yields the other operand
                            // (and two NaNs the second), as `ir::eval` does.
                            let pick = if matches!(op, BinOp::F64Min) { CmpOp::Min } else { CmpOp::Max };
                            let p = pair(b, x, y);
                            let total = b.add(pick, vec![p]);
                            let y_nan = is_nan(b, y);
                            let choices = b.tuple(vec![y_nan, x, total]);
                            let unless_y = b.add(Op::Select, vec![choices]);
                            let x_nan = is_nan(b, x);
                            let choices_x = b.tuple(vec![x_nan, y, unless_y]);
                            let f = b.add(Op::Select, vec![choices_x]);
                            let payload = b.add(ArithOp::ToSigned, vec![f]);
                            b.add(Op::Inject(0, vec![Shape::Prim(64)]), vec![payload])
                        }
                        _ => {
                            // IEEE: the total order once `-0.0` is folded onto `0.0`, and false
                            // whenever an operand is NaN; `fne` is the negation of `feq`.
                            let (cx, cy) = (fold_negative_zero(b, x, anchor), fold_negative_zero(b, y, anchor));
                            let (p, pred) = match op {
                                BinOp::F64Eq | BinOp::F64Ne => (pair(b, cx, cy), Pred::Eq),
                                BinOp::F64Lt => (pair(b, cx, cy), Pred::Lt),
                                BinOp::F64Le => (pair(b, cx, cy), Pred::Le),
                                BinOp::F64Gt => (pair(b, cy, cx), Pred::Lt),
                                _ => (pair(b, cy, cx), Pred::Le),
                            };
                            let rel = b.add(CmpOp::Rel(pred), vec![p]);
                            let (x_nan, y_nan) = (is_nan(b, x), is_nan(b, y));
                            let nans = pair(b, x_nan, y_nan);
                            let any_nan = b.add(CmpOp::Max, vec![nans]);
                            let zero = b.add(Op::Lit(CValue::u64(vec![0])), vec![anchor]);
                            let no_nan = pair(b, any_nan, zero);
                            let ordered = b.add(CmpOp::Rel(Pred::Eq), vec![no_nan]);
                            let both = pair(b, rel, ordered);
                            let holds = b.add(CmpOp::Min, vec![both]);
                            if matches!(op, BinOp::F64Ne) {
                                let negate = pair(b, holds, zero);
                                b.add(CmpOp::Rel(Pred::Eq), vec![negate])
                            } else {
                                holds
                            }
                        }
                    }
                }
                BinOp::F64Pow | BinOp::F64PowI => return Err(format!("{op:?} has no columnar kernel; it runs a row at a time (see `row_only`)")),
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
                BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
                    let float_shape = Shape::Sum(vec![Shape::Prim(64)]);
                    let (lid, rid) = if shape_of_term(l, env_shapes, None)? == float_shape && shape_of_term(r, env_shapes, None)? == float_shape {
                        (b.add(Op::Unwrap, vec![lid]), b.add(Op::Unwrap, vec![rid]))
                    } else { (lid, rid) };
                    let ls = b.add(ArithOp::ToSigned, vec![lid]);
                    let rs = b.add(ArithOp::ToSigned, vec![rid]);
                    let p = if matches!(op, BinOp::Gt | BinOp::Ge) { pair(b, rs, ls) } else { pair(b, ls, rs) };
                    b.add(CmpOp::Rel(if matches!(op, BinOp::Le | BinOp::Ge) { Pred::Le } else { Pred::Lt }), vec![p])
                }
                BinOp::And => { let p = pair(b, lid, rid); b.add(CmpOp::Min, vec![p]) }
                BinOp::Or => { let p = pair(b, lid, rid); b.add(CmpOp::Max, vec![p]) }
            })
        }
        Term::If { cond, then, els } => {
            // A literal condition takes one branch, so only that branch needs a shape. This is
            // the `if(1, $1, 0)` idiom: it carries a whole value as ONE field where a bare `$1`
            // would splice, and its dead branch need not agree in shape.
            if let Term::Int(c) = **cond {
                return compile(if c != 0 { then } else { els }, b, env, env_shapes, anchor, expected);
            }
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
            // A declared constructor is also a type annotation for an empty
            // payload (notably List<T>, whose T cannot come from runtime rows).
            let payload_expected = match (sum, &**tag) {
                (SumTy::Declared(lanes), Term::Int(t)) => usize::try_from(*t).ok().and_then(|t| lanes.get(t)),
                _ => None,
            };
            let pid = compile(payload, b, env, env_shapes, anchor, payload_expected)?;
            let pshape = shape_of_term(payload, env_shapes, payload_expected)?;
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
                let arm = if i < arms.len() {
                    let pnode = bb.add(Op::Field(1), vec![inp]);
                    env2.push(pnode);
                    shapes2.push(lanes[i].clone());
                    &arms[i]
                } else {
                    default.as_deref().ok_or_else(|| format!("case: no arm for tag {i} and no `_` default"))?
                };
                let out = compile(arm, &mut bb, &env2, &shapes2, inp, exp)?;
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
                UnOp::ToF64 => {
                    if shape != Shape::Prim(64) { return Err("float expects an Int".into()); }
                    let sign = b.add(ArithOp::Shr(63), vec![id]);
                    let negative = b.add(ArithOp::Neg(Kind::U, 64), vec![id]);
                    let choices = b.tuple(vec![sign, negative, id]);
                    let magnitude = b.add(Op::Select, vec![choices]);
                    let positive = b.add(ArithOp::ToFloat(64), vec![magnitude]);
                    let negative = b.add(ArithOp::Neg(Kind::F, 64), vec![positive]);
                    let choices = b.tuple(vec![sign, negative, positive]);
                    let f = b.add(Op::Select, vec![choices]);
                    let payload = b.add(ArithOp::ToSigned, vec![f]);
                    b.add(Op::Inject(0, vec![Shape::Prim(64)]), vec![payload])
                }
                UnOp::F64Neg => {
                    if shape != Shape::Sum(vec![Shape::Prim(64)]) { return Err("fneg expects an F64 newtype".into()); }
                    let payload = b.add(Op::Unwrap, vec![id]);
                    let f = b.add(ArithOp::ToSigned, vec![payload]);
                    let negative = b.add(ArithOp::Neg(Kind::F, 64), vec![f]);
                    let payload = b.add(ArithOp::ToSigned, vec![negative]);
                    b.add(Op::Inject(0, vec![Shape::Prim(64)]), vec![payload])
                }
                // |x| is the larger of x and -x in the total order: that clears the sign bit,
                // NaN included, exactly as `f64::abs`.
                UnOp::F64Fn(F64Fn::Abs) => {
                    if shape != Shape::Sum(vec![Shape::Prim(64)]) { return Err("fabs expects an F64 newtype".into()); }
                    let f = float_leaf(b, id);
                    let abs = float_abs(b, f);
                    let payload = b.add(ArithOp::ToSigned, vec![abs]);
                    b.add(Op::Inject(0, vec![Shape::Prim(64)]), vec![payload])
                }
                UnOp::F64Fn(_) | UnOp::F64ToInt => return Err(format!("{op:?} has no columnar kernel; it runs a row at a time (see `row_only`)")),
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
        // strips the now-homogeneous sum (and reports a heterogeneous literal).
        Term::List(fields) => {
            if fields.is_empty() {
                let Some(Shape::List(element)) = expected else {
                    return Err("an empty list literal has no element shape".into());
                };
                let value = CValue::List(corgi::Bounds::Stride(0, 1), Box::new(CValue::empty(element)));
                return Ok(b.add(Op::Lit(value), vec![anchor]));
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

/// Corgi's float encoding of an F64 constant: the total-order key, as a `U64` leaf holds it.
fn float_key(f: f64) -> u64 {
    let bits = f.to_bits();
    if bits >> 63 == 1 { !bits } else { bits ^ (1 << 63) }
}

/// An F64 newtype column -> its corgi float leaf (total-order key). The DDIR payload is the
/// signed form of that key, and `ToSigned` is the involution between the two.
fn float_leaf(b: &mut Builder<NumOp>, newtype: usize) -> usize {
    let payload = b.add(Op::Unwrap, vec![newtype]);
    b.add(ArithOp::ToSigned, vec![payload])
}

/// `|x|` on a float leaf: the larger of `x` and `-x` in the total order.
fn float_abs(b: &mut Builder<NumOp>, x: usize) -> usize {
    let negated = b.add(ArithOp::Neg(Kind::F, 64), vec![x]);
    let p = b.tuple(vec![x, negated]);
    b.add(CmpOp::Max, vec![p])
}

/// A 0/1 mask: is the float leaf NaN? The NaNs are exactly the keys whose magnitude exceeds +inf.
fn is_nan(b: &mut Builder<NumOp>, x: usize) -> usize {
    let abs = float_abs(b, x);
    b.add(CmpOp::Gt(float_key(f64::INFINITY)), vec![abs])
}

/// Replace `-0.0` by `0.0` in a float leaf. Their keys are adjacent, so this adds the mask.
fn fold_negative_zero(b: &mut Builder<NumOp>, x: usize, anchor: usize) -> usize {
    let negative_zero = b.add(Op::Lit(CValue::u64(vec![float_key(-0.0)])), vec![anchor]);
    let probe = b.tuple(vec![x, negative_zero]);
    let is_negative_zero = b.add(CmpOp::Rel(Pred::Eq), vec![probe]);
    let sum = b.tuple(vec![x, is_negative_zero]);
    b.add(ArithOp::Bin(CBinOp::Add, Kind::U, 64), vec![sum])
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

/// A compiled term, ready to run on a batch of columns.
pub enum Kernel {
    /// Every op in the terms has a corgi kernel: one columnar graph.
    Columnar(Graph<NumOp>),
    /// Some op has none (see [`row_only`]). The terms run through `ir::eval` a row at a time:
    /// the input columns are untranscoded from `inputs`, and the results transcoded to `output`.
    /// The output shape is still corgi's typing, of the terms with each row-only op replaced by a
    /// columnar op of the same shape ([`columnar_stand_in`]), so both paths type a term alike.
    Rows { terms: Vec<Term>, inputs: Vec<Shape>, output: Shape },
}

impl Kernel {
    /// Run on `Prod(inputs)`. One term yields its column; several yield `Prod` of their columns.
    pub fn eval(&self, input: CValue) -> CValue {
        match self {
            Kernel::Columnar(g) => corgi::eval_graph(g, input),
            Kernel::Rows { terms, inputs, output } => {
                let rows = untranscode(input, &Shape::Prod(inputs.clone()));
                let results: Vec<DValue> = rows
                    .into_iter()
                    .map(|row| {
                        let DValue::Tuple(mut env) = row else { unreachable!("untranscode of a Prod is a Tuple") };
                        match &terms[..] {
                            [term] => crate::ir::eval(term, &mut env),
                            _ => DValue::Tuple(terms.iter().map(|t| crate::ir::eval(t, &mut env)).collect()),
                        }
                    })
                    .collect();
                transcode_owned(results, output)
            }
        }
    }
}

/// Does `t` use an op with no corgi kernel? These are the F64 functions other than `fabs`, `fint`,
/// `fpow` and `fpowi`: libm calls and rounding, which corgi's arithmetic does not offer.
pub fn row_only(t: &Term) -> bool {
    match t {
        Term::Var(_) | Term::Bound(_) | Term::Int(_) => false,
        Term::Unary(UnOp::F64Fn(f), _) if *f != F64Fn::Abs => true,
        Term::Unary(UnOp::F64ToInt, _) | Term::Binary(BinOp::F64Pow | BinOp::F64PowI, _, _) => true,
        Term::Tuple(fs) | Term::List(fs) | Term::Hash(fs) | Term::Call(_, fs) => fs.iter().any(row_only),
        Term::Spread(inner) | Term::Proj(inner, _) | Term::Unary(_, inner) => row_only(inner),
        Term::Inject { tag, payload, .. } => row_only(tag) || row_only(payload),
        Term::Case { scrutinee, arms, default } => {
            row_only(scrutinee) || arms.iter().any(row_only) || default.as_deref().is_some_and(row_only)
        }
        Term::Fold { list, init, step } => row_only(list) || row_only(init) || row_only(step),
        Term::If { cond, then, els } => row_only(cond) || row_only(then) || row_only(els),
        Term::Binary(_, l, r) => row_only(l) || row_only(r),
    }
}

/// `t` with each row-only op replaced by a columnar op that demands the same operand shapes and
/// yields the same result shape: F64 -> F64 by `fneg`, `fint` by a compare of two `fneg`s,
/// `fpow` by `fadd`, and `fpowi(x, n)` by `fadd(x, float(n))`. Only its typing is used.
fn columnar_stand_in(t: &Term) -> Term {
    let bx = |term: &Term| Box::new(columnar_stand_in(term));
    match t {
        Term::Var(_) | Term::Bound(_) | Term::Int(_) => t.clone(),
        Term::Unary(UnOp::F64Fn(f), x) if *f != F64Fn::Abs => Term::Unary(UnOp::F64Neg, bx(x)),
        Term::Unary(UnOp::F64ToInt, x) => {
            let x = Term::Unary(UnOp::F64Neg, bx(x));
            Term::Binary(BinOp::Lt, Box::new(x.clone()), Box::new(x))
        }
        Term::Binary(BinOp::F64Pow, l, r) => Term::Binary(BinOp::F64Add, bx(l), bx(r)),
        // A registered function: its arguments still compile (so they typecheck), and the
        // result is a literal of the declared result shape.
        Term::Call(name, args) => match crate::ir::lookup(name) {
            Some(f) => {
                let mut fields: Vec<Term> = args.iter().map(columnar_stand_in).collect();
                fields.push(literal_of_shape(&f.result));
                Term::Proj(Box::new(Term::Tuple(fields)), args.len())
            }
            None => t.clone(),
        },
        Term::Binary(BinOp::F64PowI, l, r) => Term::Binary(BinOp::F64Add, bx(l), Box::new(Term::Unary(UnOp::ToF64, bx(r)))),
        Term::Tuple(fs) => Term::Tuple(fs.iter().map(columnar_stand_in).collect()),
        Term::List(fs) => Term::List(fs.iter().map(columnar_stand_in).collect()),
        Term::Hash(fs) => Term::Hash(fs.iter().map(columnar_stand_in).collect()),
        Term::Spread(inner) => Term::Spread(bx(inner)),
        Term::Proj(inner, i) => Term::Proj(bx(inner), *i),
        Term::Unary(op, inner) => Term::Unary(*op, bx(inner)),
        Term::Inject { tag, payload, sum } => Term::Inject { tag: bx(tag), payload: bx(payload), sum: sum.clone() },
        Term::Case { scrutinee, arms, default } => Term::Case {
            scrutinee: bx(scrutinee),
            arms: arms.iter().map(columnar_stand_in).collect(),
            default: default.as_deref().map(bx),
        },
        Term::Fold { list, init, step } => Term::Fold { list: bx(list), init: bx(init), step: bx(step) },
        Term::If { cond, then, els } => Term::If { cond: bx(cond), then: bx(then), els: bx(els) },
        Term::Binary(op, l, r) => Term::Binary(*op, bx(l), bx(r)),
    }
}

/// A term whose corgi shape is `shape`: zeros, first lanes, one-element lists.
fn literal_of_shape(shape: &Shape) -> Term {
    match shape {
        Shape::Prim(_) => Term::Int(0),
        Shape::Unit => Term::Tuple(Vec::new()),
        Shape::Prod(fs) => Term::Tuple(fs.iter().map(literal_of_shape).collect()),
        Shape::List(e) => Term::List(vec![literal_of_shape(e)]),
        Shape::Sum(lanes) => Term::Inject {
            tag: Box::new(Term::Int(0)),
            payload: Box::new(literal_of_shape(&lanes[0])),
            sum: SumTy::Declared(lanes.clone()),
        },
    }
}

/// Compile `terms` over the environment `Var(i)` = field `i` of the input, whose shapes are
/// `shapes`. The graph's input is `Prod(shapes)`; its output is the one term's column, or `Prod`
/// of the terms' columns. Typechecked once here, so an `Ok` kernel runs on every batch of these
/// shapes. Returns the kernel and its output shape.
fn lower(terms: &[&Term], shapes: &[Shape]) -> Res<(Kernel, Shape)> {
    let rows = terms.iter().any(|t| row_only(t));
    let stand_ins: Vec<Term> = if rows { terms.iter().map(|t| columnar_stand_in(t)).collect() } else { Vec::new() };
    let typed: Vec<&Term> = if rows { stand_ins.iter().collect() } else { terms.to_vec() };
    let mut b = Builder::<NumOp>::default();
    let input = b.input();
    let env: Vec<usize> = (0..shapes.len()).map(|i| b.add(Op::Field(i), vec![input])).collect();
    let outs = typed.iter().map(|t| compile(t, &mut b, &env, shapes, input, None)).collect::<Res<Vec<_>>>()?;
    let out = if let [one] = outs[..] { one } else { b.tuple(outs) };
    let g = b.finish(out);
    let output = corgi::shape_of(&g, &Shape::Prod(shapes.to_vec()))?;
    let kernel = if rows {
        Kernel::Rows { terms: terms.iter().map(|t| (*t).clone()).collect(), inputs: shapes.to_vec(), output: output.clone() }
    } else {
        Kernel::Columnar(g)
    };
    Ok((kernel, output))
}

/// Compile a `FlatMap`'s list term → a corgi `List` column, one list per input row. A term that is
/// not list-shaped is the type error: the backend explodes the column structurally.
pub fn compile_flatmap(list_term: &Term, kshape: &Shape, vshape: &Shape) -> Res<Kernel> {
    match lower(&[list_term], &[kshape.clone(), vshape.clone()])? {
        (k, Shape::List(_)) => Ok(k),
        (_, other) => Err(format!("flatmap over a non-list: {other}")),
    }
}

/// Compile a scalar term (`EnterAt`'s delay field) → a `U64` column; a non-integer term is the
/// type error (the delay is read as one integer per row).
pub fn compile_scalar(term: &Term, kshape: &Shape, vshape: &Shape) -> Res<Kernel> {
    match lower(&[term], &[kshape.clone(), vshape.clone()])? {
        (k, Shape::Prim(_)) => Ok(k),
        (_, other) => Err(format!("enter_at delay is not an integer: {other}")),
    }
}

/// Compile a `Filter` predicate → a mask column (nonzero keeps the row). A predicate must be an
/// `Int`; any other shape is a type error, as it is in the row backend.
pub fn compile_predicate(cond: &Term, kshape: &Shape, vshape: &Shape) -> Res<Kernel> {
    match lower(&[cond], &[kshape.clone(), vshape.clone()])? {
        (k, Shape::Prim(_)) => Ok(k),
        (_, other) => Err(format!("a filter predicate must be an Int, got {other}")),
    }
}

/// Compile a join projection: key/val Terms over `Var(0)=key`, `Var(1)=val0`, `Var(2)=val1` (with
/// their shapes). Input `Prod([key, val0, val1])`; output `Prod([newkey, newval])`.
pub fn compile_join_projection(key: &Term, val: &Term, kshape: &Shape, v0shape: &Shape, v1shape: &Shape) -> Res<Kernel> {
    Ok(lower(&[key, val], &[kshape.clone(), v0shape.clone(), v1shape.clone()])?.0)
}

/// Compile a DDIR `Projection` over `Var(0)=key` (`kshape`), `Var(1)=val` (`vshape`).
/// Input `Prod([key, val])`; output `Prod([newkey, newval])`.
pub fn compile_projection(key: &Term, val: &Term, kshape: &Shape, vshape: &Shape) -> Res<Kernel> {
    Ok(lower(&[key, val], &[kshape.clone(), vshape.clone()])?.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::Value as V;

    fn u64s() -> Shape { Shape::Prim(64) }
    fn sum(lanes: Vec<Shape>) -> Shape { Shape::Sum(lanes) }

    fn agrees_with_rows(term: &Term, shapes: &[Shape], rows: &[Vec<V>]) {
        let mut b = Builder::<NumOp>::default();
        let inp = b.input();
        let env: Vec<_> = (0..shapes.len()).map(|i| b.add(Op::Field(i), vec![inp])).collect();
        let out = compile(term, &mut b, &env, shapes, inp, None).unwrap();
        let g = b.finish(out);
        let os = corgi::shape_of(&g, &Shape::Prod(shapes.to_vec())).unwrap();
        let cols = shapes.iter().enumerate().map(|(i, s)| {
            transcode(&rows.iter().map(|r| r[i].clone()).collect::<Vec<_>>(), s)
        }).collect();
        let actual = untranscode(corgi::eval_graph(&g, CValue::Prod(cols)), &os);
        let expected: Vec<_> = rows.iter().map(|r| crate::ir::eval(term, &mut r.clone())).collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn explicit_numeric_and_list_operations_agree() {
        let rows: Vec<_> = [i64::MIN, -100, -1, 0, 1, 100, i64::MAX].into_iter()
            .flat_map(|a| [-7, -1, 0, 1, 7].into_iter().map(move |b| vec![V::Int(a), V::Int(b)]))
            .collect();
        for source in ["idiv($0, $1)", "float($0)", "fneg(float($0))",
            "fadd(float($0), float($1))", "fsub(float($0), float($1))",
            "fmul(float($0), float($1))", "fdiv(float($0), float($1))",
            "float($0) < float($1)", "float($0) >= float($1)",
            "append(list($0), list($1, $0))"] {
            let term = crate::parse::pipe::parse_term(source);
            agrees_with_rows(&term, &[u64s(), u64s()], &rows);
        }
    }

    /// Every pair of these F64 values: signed zeros, infinities, and both signs of NaN.
    fn float_pairs() -> Vec<Vec<V>> {
        let specials = [f64::NEG_INFINITY, -2.5, -1.0, -0.0, 0.0, 0.5, 1.0, 2.0, 3.7, f64::INFINITY, f64::NAN, -f64::NAN];
        specials.iter().flat_map(|&a| specials.iter().map(move |&b| vec![V::f64_value(a), V::f64_value(b)])).collect()
    }

    /// The F64 ops with a columnar kernel agree with `ir::eval` on every special pair.
    #[test]
    fn columnar_float_math_agrees() {
        let f = sum(vec![u64s()]);
        for source in ["fabs($0)", "fmin($0, $1)", "fmax($0, $1)", "feq($0, $1)", "fne($0, $1)",
            "flt($0, $1)", "fle($0, $1)", "fgt($0, $1)", "fge($0, $1)"] {
            let term = crate::parse::pipe::parse_term(source);
            assert!(!row_only(&term), "{source} should have a columnar kernel");
            agrees_with_rows(&term, &[f.clone(), f.clone()], &float_pairs());
        }
    }

    /// The F64 ops without one run a row at a time, typed as their columnar stand-ins; the
    /// kernel's output round-trips through the transcode layer at that shape.
    #[test]
    fn row_wise_float_math_agrees() {
        let f = sum(vec![u64s()]);
        for source in ["fsqrt($0)", "fexp($0)", "fln($0)", "ffloor($0)", "fceil($0)", "fround($0)",
            "fsin($0)", "fcos($0)", "ftan($0)", "fint($0)", "fpow($0, $1)", "fpowi($0, fint($1))",
            "tuple(fadd(fexp($0), $1), flt(fln($0), $1))"] {
            let term = crate::parse::pipe::parse_term(source);
            assert!(row_only(&term), "{source} should run row-wise");
            let shapes = [f.clone(), f.clone()];
            let (kernel, output) = lower(&[&term], &shapes).unwrap();
            assert!(matches!(kernel, Kernel::Rows { .. }));
            let rows = float_pairs();
            let cols = (0..2).map(|i| transcode(&rows.iter().map(|r| r[i].clone()).collect::<Vec<_>>(), &shapes[i])).collect();
            let actual = untranscode(kernel.eval(CValue::Prod(cols)), &output);
            let expected: Vec<_> = rows.iter().map(|r| crate::ir::eval(&term, &mut r.clone())).collect();
            assert_eq!(actual, expected, "{source}");
        }
        // A row-only op is still typed: its operands must be F64 newtypes.
        assert!(lower(&[&crate::parse::pipe::parse_term("fsqrt($0)")], &[u64s()]).is_err());
        assert!(lower(&[&crate::parse::pipe::parse_term("fpow($0, $0)")], &[u64s()]).is_err());
    }

    /// Spot checks of the chosen semantics, independent of either backend.
    #[test]
    fn float_math_semantics() {
        let f = |x: f64| V::f64_value(x);
        let ev = |src: &str, a: f64, b: f64| crate::ir::eval(&crate::parse::pipe::parse_term(src), &mut vec![f(a), f(b)]);
        assert_eq!(ev("fint($0)", -2.7, 0.0), V::Int(-2));
        assert_eq!(ev("fint($0)", f64::NAN, 0.0), V::Int(0));
        assert_eq!(ev("fint($0)", 1e300, 0.0), V::Int(i64::MAX));
        assert_eq!(ev("flt($0, $1)", -2.0, -1.0), V::Int(1));
        assert_eq!(ev("$0 < $1", -2.0, -1.0), V::Int(1));
        assert_eq!(ev("feq($0, $1)", -0.0, 0.0), V::Int(1));
        assert_eq!(ev("$0 == $1", -0.0, 0.0), V::Int(0));
        assert_eq!(ev("fne($0, $1)", f64::NAN, f64::NAN), V::Int(1));
        assert_eq!(ev("fmax($0, $1)", f64::NAN, 1.0), f(1.0));
        assert_eq!(ev("fmin($0, $1)", 0.0, -0.0).as_f64().to_bits(), (-0.0f64).to_bits());
        assert!(ev("fln($0)", -1.0, 0.0).as_f64().is_nan());
        assert_eq!(ev("fln($0)", 0.0, 0.0), f(f64::NEG_INFINITY));
        assert_eq!(ev("fpow($0, $1)", 4.0, 0.5), f(2.0));
        assert!(ev("fpow($0, $1)", -8.0, 0.5).as_f64().is_nan());
    }

    #[test]
    fn declared_constructor_types_an_empty_list() {
        let term = Term::Inject {
            tag: Box::new(Term::Int(0)), payload: Box::new(Term::List(vec![])),
            sum: SumTy::Declared(vec![Shape::List(Box::new(Shape::List(Box::new(u64s()))))]),
        };
        agrees_with_rows(&term, &[u64s()], &[vec![V::Int(0)], vec![V::Int(1)]]);
    }

    /// The pin on DDIR's `hash`: `ir::structural_hash` is a row-at-a-time transcription of
    /// `corgi::hash`, and the two backends compute the SAME program value, so they must agree
    /// bit for bit on every shape the transcode layer covers. If corgi's salts or fold change,
    /// this is what fails.
    fn hash_agrees(rows: Vec<V>, shape: Shape) {
        let col = transcode(&rows, &shape);
        let columnar = corgi::hash(&col);
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
