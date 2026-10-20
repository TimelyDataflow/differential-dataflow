//! corgi as DDIR's columnar scalar logic: type and compile each operator's slot, a corgi lambda,
//! at the shapes of the rows it reads; the Float functions as words; and transcode DDIR rows
//! (`ir::Value`) to and from corgi columns, for registered functions with row bodies.
//!
//! Shapes are STATIC. Every input declares its shape, every other collection's follows from its
//! operator's slot (`crate::shapes`), and all are fixed at install, never re-derived from data.
//!
//! The typer is corgi's: [`slot_shape`] asks `corgi::shape_of` (its evaluator on zero rows) for
//! the shape a slot gives, so every shape rule is the one the kernels enforce, and a program that
//! types is a program that runs. `Err` is a type error, reported with corgi's message.

use crate::ir::{Lambda, Value as DValue};

use corgi::{ArithOp, Builder, CmpOp, Graph, NumOp, Op, Pred, Scalar, Shape, Value as CValue};

type Res<T> = Result<T, String>;

/// The shape of one row, as far as the row alone says: an `Int`, a `Tuple`, or a non-empty `List`
/// determines its shape; a `Variant` (whose type a bare tag cannot name) or an empty `List` (whose
/// element shape nothing supplies) does not.
pub fn shape_of_row(row: &DValue) -> Res<Shape> {
    match row {
        DValue::Int(_) => Ok(Shape::Int),
        DValue::Float(_) => Ok(Shape::Float),
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
/// panic: an input checks its rows against its declared shape before converting them.
/// Borrowing form: one clone of the rows, then [`transcode_owned`].
pub fn transcode(rows: &[DValue], shape: &Shape) -> CValue {
    transcode_owned(rows.to_vec(), shape)
}

/// As [`transcode`], consuming the rows: each field moves into its column, nothing is cloned.
pub fn transcode_owned(rows: Vec<DValue>, shape: &Shape) -> CValue {
    match shape {
        Shape::Int => CValue::int(rows.iter().map(DValue::as_int)),
        Shape::Float => CValue::f64(rows.iter().map(DValue::as_f64).collect()),
        Shape::Unit => CValue::Unit(rows.len()),
        Shape::Prod(fs) => {
            // Transpose by moving: one Vec per field, filled by draining each tuple.
            let mut cols: Vec<Vec<DValue>> = fs.iter().map(|_| Vec::with_capacity(rows.len())).collect();
            for r in rows {
                match r {
                    DValue::Tuple(xs) => {
                        assert!(xs.len() == fs.len(), "transcode: a {}-tuple for a {}-field shape", xs.len(), fs.len());
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
        // A reference names rows of a shared list: build the list, and refer to each of its rows.
        Shape::Ref(list) => {
            let named: Vec<usize> = (0..rows.len()).collect();
            CValue::Ref(std::sync::Arc::new(transcode_owned(rows, list)), std::sync::Arc::new(named))
        }
    }
}

/// SoA corgi columns -> AoS rows, directed by `shape`. Inverse of [`transcode`]. Each value
/// moves into its row; nothing is cloned.
pub fn untranscode(col: CValue, shape: &Shape) -> Vec<DValue> {
    match shape {
        Shape::Int => col.into_i64("untranscode").unwrap().into_iter().map(DValue::Int).collect(),
        Shape::Float => col.as_f64("untranscode").unwrap().into_iter().map(DValue::f64_value).collect(),
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
        // A row of references reads as the rows it names.
        Shape::Ref(list) => match col {
            CValue::Ref(arena, rows) => untranscode(corgi::arrange::gather(&arena, &rows), list),
            other => untranscode(other, list),
        },
    }
}

/// The total-order key of an `f64`, as corgi's `Scalar::Float` holds a float constant.
fn float_key(f: f64) -> u64 { crate::ir::F64(f).key() }

/// DDIR's Float functions past corgi's arithmetic, as words over their input: the argument, or the
/// tuple of the two arguments. `fabs`, `fmin`, `fmax` and the comparisons are compositions of corgi
/// ops; the rest are host kernels ([`float_kernels`]).
#[derive(Clone, Copy)]
enum FloatWord {
    /// `|x|`: the larger of `x` and `-x` in the total order, which clears the sign bit, NaN
    /// included, exactly as `f64::abs`.
    Abs,
    Kernel(float_kernels::FloatOp),
    /// The total-order pick, except that a NaN operand yields the other operand (and two NaNs the
    /// second).
    Pick { max: bool },
    /// IEEE: `pred` in the total order once `-0.0` is folded onto `0.0`, with the operands swapped
    /// for `>` and `>=`, and false whenever an operand is NaN; `fne` is the negation of `feq`.
    Compare { pred: Pred, swap: bool, negate: bool },
}

impl FloatWord {
    /// Every word, by name.
    fn all() -> impl Iterator<Item = (&'static str, FloatWord)> {
        use float_kernels::FloatOp;
        use FloatWord::{Compare, Kernel, Pick};
        let functions = F64Fn::ALL.into_iter().map(|(name, f)| (name, if f == F64Fn::Abs { FloatWord::Abs } else { Kernel(FloatOp::Fn(f)) }));
        functions.chain([
            ("fint", Kernel(FloatOp::ToInt)), ("fpow", Kernel(FloatOp::Pow)), ("fpowi", Kernel(FloatOp::PowI)),
            ("fmin", Pick { max: false }), ("fmax", Pick { max: true }),
            ("feq", Compare { pred: Pred::Eq, swap: false, negate: false }), ("fne", Compare { pred: Pred::Eq, swap: false, negate: true }),
            ("flt", Compare { pred: Pred::Lt, swap: false, negate: false }), ("fle", Compare { pred: Pred::Le, swap: false, negate: false }),
            ("fgt", Compare { pred: Pred::Lt, swap: true, negate: false }), ("fge", Compare { pred: Pred::Le, swap: true, negate: false }),
        ])
    }

    /// The word over `input`, built in `b`.
    fn build(self, b: &mut Builder<NumOp>, input: usize) -> usize {
        match self {
            FloatWord::Abs => float_abs(b, input),
            FloatWord::Kernel(op) => b.add(NumOp::Host(float_kernels::op(op)), vec![input]),
            FloatWord::Pick { max } => {
                let (x, y) = (b.add(Op::Field(0), vec![input]), b.add(Op::Field(1), vec![input]));
                let total = b.add(if max { CmpOp::Max } else { CmpOp::Min }, vec![input]);
                let y_nan = is_nan(b, y);
                let choices = b.tuple(vec![y_nan, x, total]);
                let unless_y = b.add(Op::Select, vec![choices]);
                let x_nan = is_nan(b, x);
                let choices_x = b.tuple(vec![x_nan, y, unless_y]);
                b.add(Op::Select, vec![choices_x])
            }
            FloatWord::Compare { pred, swap, negate } => {
                let (x, y) = (b.add(Op::Field(0), vec![input]), b.add(Op::Field(1), vec![input]));
                let zero = b.add(Op::Lit(CValue::f64(vec![0.0])), vec![input]);
                let (cx, cy) = (fold_negative_zero(b, x, zero), fold_negative_zero(b, y, zero));
                let p = b.tuple(if swap { vec![cy, cx] } else { vec![cx, cy] });
                let rel = b.add(CmpOp::Rel(pred), vec![p]);
                let (x_nan, y_nan) = (is_nan(b, x), is_nan(b, y));
                let nans = b.tuple(vec![x_nan, y_nan]);
                let any_nan = b.add(CmpOp::Max, vec![nans]);
                let ordered = b.add(CmpOp::RelImm(Pred::Eq, Scalar::Int(0)), vec![any_nan]);
                let both = b.tuple(vec![rel, ordered]);
                let holds = b.add(CmpOp::Min, vec![both]);
                if negate { b.add(CmpOp::RelImm(Pred::Eq, Scalar::Int(0)), vec![holds]) } else { holds }
            }
        }
    }
}

/// `|x|` on a float leaf: the larger of `x` and `-x` in the total order.
fn float_abs(b: &mut Builder<NumOp>, x: usize) -> usize {
    let negated = b.add(ArithOp::Neg, vec![x]);
    let p = b.tuple(vec![x, negated]);
    b.add(CmpOp::Max, vec![p])
}

/// A 0/1 mask: is the float leaf NaN? The NaNs are exactly the values whose magnitude exceeds
/// +inf in the total order.
fn is_nan(b: &mut Builder<NumOp>, x: usize) -> usize {
    let abs = float_abs(b, x);
    b.add(CmpOp::RelImm(Pred::Gt, Scalar::Float(float_key(f64::INFINITY))), vec![abs])
}

/// Replace `-0.0` by `zero`, a column of `0.0`, in a float leaf.
fn fold_negative_zero(b: &mut Builder<NumOp>, x: usize, zero: usize) -> usize {
    let is_negative_zero = b.add(CmpOp::RelImm(Pred::Eq, Scalar::Float(float_key(-0.0))), vec![x]);
    let choices = b.tuple(vec![is_negative_zero, zero, x]);
    b.add(Op::Select, vec![choices])
}

/// The shape a slot gives over rows of `shapes`: corgi's typer, on its graph's input `Prod(shapes)`.
pub fn slot_shape(lambda: &Lambda, shapes: &[Shape]) -> Res<Shape> {
    corgi::shape_of(lambda.graph()?, &Shape::Prod(shapes.to_vec()))
}

/// A slot's graph over rows of `shapes`, and the shape it gives. Typechecked once here, so an `Ok`
/// kernel runs on every batch of these shapes.
fn slot_graph(lambda: &Lambda, shapes: &[Shape]) -> Res<(Graph<NumOp>, Shape)> {
    let output = slot_shape(lambda, shapes)?;
    // Optimized only now that it types: among corgi's passes, a field of a tuple the lambda built
    // becomes the field itself, and common subexpressions merge.
    Ok((corgi::optimize(lambda.graph()?), output))
}

/// Compile a `FlatMap`'s lambda → a corgi `List` column, one list per input row. A lambda that is
/// not list-shaped is the type error: the backend explodes the column structurally.
pub fn compile_flatmap(list: &Lambda, kshape: &Shape, vshape: &Shape) -> Res<Graph<NumOp>> {
    match slot_graph(list, &[kshape.clone(), vshape.clone()])? {
        (k, Shape::List(_)) => Ok(k),
        (_, other) => Err(format!("flatmap over a non-list: {other}")),
    }
}

/// Compile a `reduce` lambda over `(key, vals)`, `vals` a list of `(val, diff)` (`vshape` the
/// values') → a corgi `List` column of `(val, diff)`, one list per key.
pub fn compile_reduce(lambda: &Lambda, kshape: &Shape, vshape: &Shape) -> Res<Graph<NumOp>> {
    let list = Shape::List(Box::new(Shape::Prod(vec![vshape.clone(), Shape::Int])));
    match slot_graph(lambda, &[kshape.clone(), list])? {
        (g, Shape::List(e)) if matches!(&*e, Shape::Prod(fs) if fs.len() == 2 && fs[1] == Shape::Int) => Ok(g),
        (_, other) => Err(format!("reduce output is not a list of (val, diff): {other}")),
    }
}

/// Compile a `JoinFunction`'s lambda → a `List` column of `((key, val), time, diff)`, one list per
/// input row, with `time` a tuple of `width` `Int`s. Any other shape is the type error: the backend
/// explodes the column structurally and joins the time lanes into the rows' times.
pub fn compile_join_function(lambda: &Lambda, kshape: &Shape, vshape: &Shape, width: usize) -> Res<Graph<NumOp>> {
    let (g, out) = slot_graph(lambda, &[kshape.clone(), vshape.clone()])?;
    if let Shape::List(elem) = &out {
        if let Shape::Prod(fs) = &**elem {
            let time = Shape::Prod(vec![Shape::Int; width]);
            if fs.len() == 3 && matches!(&fs[0], Shape::Prod(kv) if kv.len() == 2) && fs[1] == time && fs[2] == Shape::Int {
                return Ok(g);
            }
        }
    }
    Err(format!("join_function output is not a list of ((key, val), time, diff) with {width} time coordinates: {out}"))
}

/// Compile an `EnterAt`'s lambda → an `Int` column; a non-integer is the type error (the round is
/// read as one integer per row).
pub fn compile_scalar(lambda: &Lambda, kshape: &Shape, vshape: &Shape) -> Res<Graph<NumOp>> {
    match slot_graph(lambda, &[kshape.clone(), vshape.clone()])? {
        (k, Shape::Int) => Ok(k),
        (_, other) => Err(format!("enter_at delay is not an integer: {other}")),
    }
}

/// Compile a `Filter` predicate → a mask column (nonzero keeps the row). A predicate must be an
/// `Int`; any other shape is a type error.
pub fn compile_predicate(cond: &Lambda, kshape: &Shape, vshape: &Shape) -> Res<Graph<NumOp>> {
    match slot_graph(cond, &[kshape.clone(), vshape.clone()])? {
        (k, Shape::Int) => Ok(k),
        (_, other) => Err(format!("a filter predicate must be an Int, got {other}")),
    }
}

/// Compile a projection over `rows`, the shapes of `(key, val)` or `(key, val0, val1)`: input
/// `Prod(rows)`, output `Prod([newkey, newval])`.
pub fn compile_projection(lambda: &Lambda, rows: &[Shape]) -> Res<Graph<NumOp>> {
    match slot_graph(lambda, rows)? {
        (g, Shape::Prod(fs)) if fs.len() == 2 => Ok(g),
        (_, other) => Err(format!("a projection gives (key, val), not {other}")),
    }
}

/// Evaluate a slot's lambda on one row, for models that run a row at a time. The row's shape is
/// its own, so a row whose shape it cannot tell alone (an empty list, a variant) panics.
pub fn eval_corgi_row(lambda: &crate::ir::Lambda, input: DValue) -> DValue {
    let shape = shape_of_row(&input).unwrap_or_else(|e| panic!("corgi program `{}`: {e}", lambda.src));
    let graph = lambda.graph().unwrap_or_else(|e| panic!("{e}"));
    let out = corgi::eval_graph(graph, transcode_owned(vec![input], &shape));
    let out_shape = corgi::shape_of_value(&out);
    untranscode(out, &out_shape).pop().expect("one row in, one row out")
}

/// The scope every DDIR program's lambdas start from: the Float functions as words, and the
/// built-in sums `Option` and `Result`. A word's input is its argument, or the tuple of its
/// arguments when it takes several, so `x call fsqrt` and `(x, y) call fpow`. The sums leave their
/// payload lanes open, as they have no shapes of their own: `inject` fills an open lane with its
/// payload's shape, and `branch`, `map_variant` and `match` read only their tags.
pub fn base_scope() -> corgi::Scope {
    static BASE: std::sync::OnceLock<corgi::Scope> = std::sync::OnceLock::new();
    BASE.get_or_init(|| {
        let mut scope = corgi::Scope::default();
        for (name, word) in FloatWord::all() {
            let mut b = Builder::<NumOp>::default();
            let input = b.input();
            let out = word.build(&mut b, input);
            scope.word(name, b.finish(out)).unwrap();
        }
        scope.declare("enum Option = None () | Some").unwrap();
        scope.declare("enum Result = Ok | Err").unwrap();
        scope
    }).clone()
}

/// Whether `name` is one of the words every lambda's scope starts with (a registered function may
/// not take its name).
pub fn is_base_word(name: &str) -> bool { FloatWord::all().any(|(word, _)| word == name) }

/// The registered function `f` as a word: one host-kernel node over the call's input, which is its
/// argument when it takes one, and the tuple of its arguments (or `()`) otherwise.
pub fn function_word(f: &crate::ir::Function) -> Graph<NumOp> {
    let kernel = crate::ir::kernel_of(&f.name).expect("a registered function has a kernel");
    let mut b = Builder::<NumOp>::default();
    let x = b.input();
    let input = if f.args.len() == 1 { b.tuple(vec![x]) } else { x };
    let out = b.add(NumOp::Host(corgi::HostOp(kernel)), vec![input]);
    b.finish(out)
}

/// The one-argument F64 -> F64 functions. Each is the Rust `f64` method of the same name, so a
/// program ported from Rust gets bit-identical results on the same platform. `Abs` is exact
/// everywhere; the transcendental ones (`Exp`, `Ln`, `Sin`, `Cos`, `Tan`) call the platform libm
/// and are only as reproducible across platforms as it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum F64Fn {
    Abs, Sqrt, Exp, Ln, Floor, Ceil, Round, Sin, Cos, Tan,
}

impl F64Fn {
    /// The words' names, `f` + the Rust method name (`ln`, not `log`).
    const ALL: [(&'static str, F64Fn); 10] = [
        ("fabs", F64Fn::Abs), ("fsqrt", F64Fn::Sqrt), ("fexp", F64Fn::Exp), ("fln", F64Fn::Ln),
        ("ffloor", F64Fn::Floor), ("fceil", F64Fn::Ceil), ("fround", F64Fn::Round),
        ("fsin", F64Fn::Sin), ("fcos", F64Fn::Cos), ("ftan", F64Fn::Tan),
    ];
    fn apply(self, x: f64) -> f64 {
        match self {
            F64Fn::Abs => x.abs(),
            F64Fn::Sqrt => x.sqrt(),
            F64Fn::Exp => x.exp(),
            F64Fn::Ln => x.ln(),
            F64Fn::Floor => x.floor(),
            F64Fn::Ceil => x.ceil(),
            F64Fn::Round => x.round(),
            F64Fn::Sin => x.sin(),
            F64Fn::Cos => x.cos(),
            F64Fn::Tan => x.tan(),
        }
    }
}

/// The Float functions without a composition of corgi ops, as host kernels over the float
/// column, each the Rust `f64` function. One kernel per function, shared by every call site, so
/// CSE merges equal calls.
mod float_kernels {
    use std::sync::{Arc, OnceLock};
    use corgi::{HostKernel, HostOp, Shape, Value};
    use super::F64Fn;

    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
    pub enum FloatOp { Fn(F64Fn), ToInt, Pow, PowI }

    struct Kernel { name: String, op: FloatOp, input: Shape, output: Shape }
    impl HostKernel for Kernel {
        fn name(&self) -> &str { &self.name }
        fn input(&self) -> &Shape { &self.input }
        fn output(&self) -> &Shape { &self.output }
        fn eval(&self, input: Value) -> Result<Value, String> {
            Ok(match self.op {
                FloatOp::Fn(f) => Value::f64(input.as_f64(&self.name)?.into_iter().map(|x| f.apply(x)).collect()),
                FloatOp::ToInt => Value::i64(input.as_f64(&self.name)?.into_iter().map(|x| x as i64).collect()),
                FloatOp::Pow | FloatOp::PowI => {
                    let args = input.into_prod(&self.name)?;
                    let x = args[0].as_f64(&self.name)?;
                    Value::f64(if self.op == FloatOp::Pow {
                        x.into_iter().zip(args[1].as_f64(&self.name)?).map(|(a, b)| a.powf(b)).collect()
                    } else {
                        x.into_iter().zip(args[1].as_i64(&self.name)?.iter()).map(|(a, &b)| {
                            a.powi(b.clamp(i32::MIN as i64, i32::MAX as i64) as i32)
                        }).collect()
                    })
                }
            })
        }
    }

    /// The host op for `op`: one `Arc` per function for the life of the process.
    pub fn op(op: FloatOp) -> HostOp {
        static KERNELS: OnceLock<std::sync::Mutex<std::collections::HashMap<FloatOp, Arc<dyn HostKernel>>>> = OnceLock::new();
        let mut map = KERNELS.get_or_init(Default::default).lock().unwrap();
        let k = map.entry(op).or_insert_with(|| {
            let (f, i) = (Shape::Float, Shape::Int);
            let (name, input, output) = match op {
                FloatOp::Fn(g) => (format!("{g:?}").to_lowercase(), f.clone(), f),
                FloatOp::ToInt => ("fint".into(), f, i),
                FloatOp::Pow => ("fpow".into(), Shape::Prod(vec![f.clone(), f.clone()]), f),
                FloatOp::PowI => ("fpowi".into(), Shape::Prod(vec![f.clone(), i]), f),
            };
            Arc::new(Kernel { name: format!("f64:{name}"), op, input, output })
        });
        HostOp(Arc::clone(k))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::Value as V;

    fn ints() -> Shape { Shape::Int }
    fn sum(lanes: Vec<Shape>) -> Shape { Shape::Sum(lanes) }

    /// `body`, a lambda body over the floats `a` and `b` in the base scope, on each pair: the rows
    /// it gives.
    fn floats(body: &str, pairs: &[(f64, f64)]) -> Vec<V> {
        let lambda = Lambda::new(format!("let (a, b) = input in {body}"), &base_scope());
        let (a, b) = pairs.iter().copied().unzip();
        let out = corgi::eval_graph(lambda.graph().unwrap(), CValue::Prod(vec![CValue::f64(a), CValue::f64(b)]));
        let shape = corgi::shape_of_value(&out);
        untranscode(out, &shape)
    }

    /// Every pair of these floats: signed zeros, infinities, and both signs of NaN.
    fn float_pairs() -> Vec<(f64, f64)> {
        let specials = [f64::NEG_INFINITY, -2.5, -1.0, -0.0, 0.0, 0.5, 1.0, 2.0, 3.7, f64::INFINITY, f64::NAN, -f64::NAN];
        specials.iter().flat_map(|&a| specials.iter().map(move |&b| (a, b))).collect()
    }

    /// Every Float word, called as a program calls it, gives Rust's own `f64` answer on every
    /// special pair: the words composed of corgi ops and the host kernels (`float_kernels`), alone
    /// and inside a larger body.
    #[test]
    fn float_words_agree_with_rust() {
        let f = V::f64_value;
        let mask = |x: bool| V::Int(x as i64);
        // `fmin`/`fmax`: skip a NaN operand, else the total order; two NaNs give the second.
        let pick = |x: f64, y: f64, min: bool| {
            let pick_x = if x.is_nan() { false } else if y.is_nan() { true }
                else if min { x.total_cmp(&y).is_le() } else { x.total_cmp(&y).is_ge() };
            f(if pick_x { x } else { y })
        };
        let powi = |x: f64, y: f64| f(x.powi((y as i64).clamp(i32::MIN as i64, i32::MAX as i64) as i32));
        let mut cases: Vec<(String, Box<dyn Fn(f64, f64) -> V>)> = vec![
            ("(a, b) call fmin".into(), Box::new(move |x, y| pick(x, y, true))),
            ("(a, b) call fmax".into(), Box::new(move |x, y| pick(x, y, false))),
            ("(a, b) call feq".into(), Box::new(move |x, y| mask(x == y))),
            ("(a, b) call fne".into(), Box::new(move |x, y| mask(x != y))),
            ("(a, b) call flt".into(), Box::new(move |x, y| mask(x < y))),
            ("(a, b) call fle".into(), Box::new(move |x, y| mask(x <= y))),
            ("(a, b) call fgt".into(), Box::new(move |x, y| mask(x > y))),
            ("(a, b) call fge".into(), Box::new(move |x, y| mask(x >= y))),
            ("a call fint".into(), Box::new(|x, _| V::Int(x as i64))),
            ("(a, b) call fpow".into(), Box::new(move |x, y| f(x.powf(y)))),
            ("(a, b call fint) call fpowi".into(), Box::new(powi)),
        ];
        for (name, g) in F64Fn::ALL {
            cases.push((format!("a call {name}"), Box::new(move |x, _| f(g.apply(x)))));
        }
        let pairs = float_pairs();
        for (body, rust) in &cases {
            let expected: Vec<V> = pairs.iter().map(|&(x, y)| rust(x, y)).collect();
            assert_eq!(floats(body, &pairs), expected, "{body}");
        }
        // Inside a larger body. Over non-NaN pairs only: corgi's float `add` returns the positive
        // quiet NaN where Rust may keep a NaN operand's sign; the words themselves agree on NaNs.
        let finite: Vec<_> = pairs.into_iter().filter(|(x, y)| !x.is_nan() && !y.is_nan()).collect();
        let body = "((a call fexp, b) add, (a call fln, b) call flt, (a call fsqrt, b call fint) call fpowi)";
        let expected: Vec<V> = finite.iter().map(|&(x, y)| V::Tuple(vec![f(x.exp() + y), mask(x.ln() < y), powi(x.sqrt(), y)])).collect();
        assert_eq!(floats(body, &finite), expected);
        // The kernels are typed: their operands must be Floats, and `fpowi`'s exponent an Int.
        let int_pair = [Shape::Int, Shape::Int];
        for bad in ["a call fsqrt", "a call fint", "(a, a) call fpow"] {
            let lambda = Lambda::new(format!("let (a, b) = input in {bad}"), &base_scope());
            assert!(slot_shape(&lambda, &int_pair).is_err(), "{bad}");
        }
        let lambda = Lambda::new("let (a, b) = input in (a, b) call fpowi".into(), &base_scope());
        assert!(slot_shape(&lambda, &[Shape::Float, Shape::Float]).is_err());
    }

    /// Spot checks of the chosen semantics, as constants.
    #[test]
    fn float_math_semantics() {
        let f = V::f64_value;
        let one = |body: &str, a: f64, b: f64| floats(body, &[(a, b)]).pop().unwrap();
        assert_eq!(one("a call fint", -2.7, 0.0), V::Int(-2));
        assert_eq!(one("a call fint", f64::NAN, 0.0), V::Int(0));
        assert_eq!(one("a call fint", 1e300, 0.0), V::Int(i64::MAX));
        assert_eq!(one("(a, b) call flt", -2.0, -1.0), V::Int(1));
        assert_eq!(one("(a, b) lt", -2.0, -1.0), V::Int(1));
        assert_eq!(one("(a, b) call feq", -0.0, 0.0), V::Int(1));
        assert_eq!(one("(a, b) eq", -0.0, 0.0), V::Int(0));
        assert_eq!(one("(a, b) call fne", f64::NAN, f64::NAN), V::Int(1));
        assert_eq!(one("(a, b) call fmax", f64::NAN, 1.0), f(1.0));
        assert_eq!(one("(a, b) call fmin", 0.0, -0.0).as_f64().to_bits(), (-0.0f64).to_bits());
        assert!(one("a call fln", -1.0, 0.0).as_f64().is_nan());
        assert_eq!(one("a call fln", 0.0, 0.0), f(f64::NEG_INFINITY));
        assert_eq!(one("(a, b) call fpow", 4.0, 0.5), f(2.0));
        assert!(one("(a, b) call fpow", -8.0, 0.5).as_f64().is_nan());
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
            sum(vec![Shape::List(Box::new(ints()))]),
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
            sum(vec![ints(), Shape::Prod(vec![ints(), ints()])]),
        );
    }

    #[test]
    fn roundtrip_variant_absent_arm_is_an_empty_lane() {
        // tags {0, 2} present, arm 1 absent: its lane is an empty column of the declared shape.
        roundtrip(
            vec![V::Variant(0, Box::new(V::Int(1))), V::Variant(2, Box::new(V::Int(2))), V::Variant(0, Box::new(V::Int(3)))],
            sum(vec![ints(), Shape::List(Box::new(ints())), ints()]),
        );
    }

    #[test]
    fn roundtrip_nested_variant_in_tuple() {
        roundtrip(
            vec![
                V::Tuple(vec![V::Int(1), V::Variant(0, Box::new(V::Int(7)))]),
                V::Tuple(vec![V::Int(2), V::Variant(1, Box::new(V::unit()))]),
            ],
            Shape::Prod(vec![ints(), sum(vec![ints(), Shape::Unit])]),
        );
    }

    #[test]
    fn shape_of_row_pins_what_a_row_can_say() {
        assert_eq!(shape_of_row(&V::Tuple(vec![V::Int(1), V::List(vec![V::Int(2)])])).unwrap(), Shape::Prod(vec![ints(), Shape::List(Box::new(ints()))]));
        assert!(shape_of_row(&V::List(vec![])).is_err());
        assert!(shape_of_row(&V::Variant(0, Box::new(V::Int(1)))).is_err());
    }
}
