//! The expression language of DDIR's slots: every operator's per-row logic (a map's projection, a
//! filter's predicate, a reduce's body, …) is a lambda, and this module is everything the rest of
//! DDIR needs from the language those lambdas are written in. Today that language is corgi's; this
//! is the module another expression language would replace.
//!
//! What the rest of DDIR relies on, and nothing more:
//! - **A slot is a [`Lambda`]:** text as written, and what it parses to in its program's scope
//!   (`Lambda::new`). The parser (`crate::parse`) builds one per slot; the IR (`crate::ir`) holds
//!   them; a parse error is kept and reported where the program is compiled.
//! - **The scope a program's lambdas are parsed in** (`base_scope`, plus `function_word` for
//!   each [registered function](register) and the program's declared types).
//! - **Typing a slot** at the shapes of the rows it reads (`slot_shape`), which is how
//!   `crate::shapes` infers every collection's shape. Shapes are static: every input declares its
//!   shape, every other collection's follows from its operator's slot, and all are fixed at
//!   install, never re-derived from data. The typer is corgi's: `slot_shape` asks
//!   `corgi::shape_of` (its evaluator on zero rows) for the shape a slot gives, so every shape rule
//!   is the one the kernels enforce, and a program that types is a program that runs. `Err` is a
//!   type error, reported with corgi's message.
//! - **Compiling a slot** for the runtime to evaluate, one function per operator kind
//!   (`compile_projection`, `compile_predicate`, `compile_flatmap`, `compile_reduce`,
//!   `compile_join_function`, `compile_scalar`).
//! - **Composing lambdas** (`Lambda::compose`), which explain and the projection pass use to
//!   write new slots from existing ones.
//! - **Registered functions** ([`register`]): how an embedding program extends the language.
//!
//! Outside the crate, only [`Lambda`], [`Function`], [`register`] and [`lookup`] are public.
//!
//! Corgi's types (`corgi::Shape` for shapes, `corgi::Graph` for compiled slots, `corgi::Value` for
//! columns) still appear outside this module, in the shapes pass and the runtime; this module is
//! where the language is chosen, not yet where it is abstracted.

use corgi::{ArithOp, Builder, CmpOp, Graph, NumOp, Op, Pred, Scalar, Shape, Value as CValue};

pub(crate) mod print;

/// A slot of the IR: a corgi program as written, `let pat = input in body`, and its graph, whose
/// input is the tuple of the slot's rows (`(key, val)` for a linear op, `(key, left val, right
/// val)` for a join). The parser binds it where it is written, in the scope of its DDIR program:
/// the registered functions and the Float functions as words `call` reaches, and the program's
/// `type` declarations as enums. A parse error is kept, and reported where the program is compiled
/// or run. A composed lambda's graph is authoritative: its readable `src` is
/// reconstructed from the component sources and may reparse to a different
/// graph, especially after masking. Serialization stores only `src` and
/// deserialization reparses it in the empty scope.
#[derive(Clone)]
pub struct Lambda {
    pub(crate) src: String,
    pub(crate) graph: Result<std::sync::Arc<corgi::Graph<corgi::NumOp>>, String>,
}

impl Lambda {
    /// `src` parsed in `scope`. The graph is as written: it is optimized only once it types at the
    /// shapes of the rows it reads ([`slot_graph`]), as optimizing first could drop or trip on
    /// a type error.
    pub(crate) fn new(src: String, scope: &corgi::Scope) -> Self {
        let graph = corgi::parse_ml_in(&src, scope).map(std::sync::Arc::new);
        Lambda { src, graph }
    }
    /// The lambda `pattern -> body`, where `body` reads the results of `calls`: each runs a lambda
    /// on an argument written over `pattern`'s names, and is bound to the name `body` is given for
    /// it. Each lambda's compiled graph is bound as a private word, which `call` grafts, so its text
    /// is never parsed again and its program's scope is not needed; the readable source writes the
    /// lambda's own text in its place. Only `pattern`, the arguments and `body` are parsed, in the
    /// empty scope.
    pub(crate) fn compose(pattern: &str, calls: &[(&Lambda, String)], body: impl FnOnce(&[String]) -> String) -> Self {
        let mut scope = corgi::Scope::default();
        let (mut code, mut src, mut names, mut error) = (String::new(), String::new(), Vec::new(), None);
        for (i, (lambda, arg)) in calls.iter().enumerate() {
            let name = format!("_call{i}");
            match lambda.graph() {
                Ok(g) => scope.word(&name, g.as_ref().clone()).expect("a fresh word"),
                Err(e) => error = Some(e),
            }
            code += &format!("let {name} = {arg} call {name} in ");
            src += &format!("let {name} = let input = {arg} in {} in ", lambda.src);
            names.push(name);
        }
        let body = body(&names);
        let mut lambda = Lambda::new(format!("let {pattern} = input in {code}{body}"), &scope);
        lambda.src = format!("let {pattern} = input in {src}{body}");
        if let Some(e) = error { lambda.graph = Err(e); }
        lambda
    }
    /// The graph, or the parse error naming the program.
    pub(crate) fn graph(&self) -> Result<&std::sync::Arc<corgi::Graph<corgi::NumOp>>, String> {
        self.graph.as_ref().map_err(|e| format!("corgi program `{}`: {e}", self.src))
    }
}

impl std::fmt::Debug for Lambda {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.src.fmt(f) }
}

// Identity is the stored graph, without optimizing it or reparsing its display text.
impl PartialEq for Lambda {
    fn eq(&self, other: &Self) -> bool { self.graph == other.graph }
}
impl Eq for Lambda {}
impl std::hash::Hash for Lambda {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) { self.graph.hash(state); }
}

impl serde::Serialize for Lambda {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> { self.src.serialize(s) }
}

impl<'de> serde::Deserialize<'de> for Lambda {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Lambda::new(String::deserialize(d)?, &corgi::Scope::default()))
    }
}


// ---------------------------------------------------------------------------
// Registered functions: how an embedding program extends its programs' lambdas.

/// A function an embedding program supplies to DDIR programs, called by name: a corgi host
/// kernel, which runs on whole columns. Its name is the kernel's, and its input shape is `()` for a
/// function of no arguments, or else the tuple of its arguments' shapes.
///
/// The contract is purity: the same arguments must always give the same result, because
/// differential dataflow re-evaluates a call when it retracts what the call produced, and a
/// retraction must cancel exactly.
pub type Function = std::sync::Arc<dyn corgi::HostKernel>;

fn functions() -> &'static std::sync::RwLock<std::collections::HashMap<String, Function>> {
    static REGISTRY: std::sync::OnceLock<std::sync::RwLock<std::collections::HashMap<String, Function>>> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

/// Register `f` under its name, for programs parsed afterwards (the parser resolves calls
/// against the registry). Registering a name again replaces it; a name may not shadow a builtin.
pub fn register(f: Function) {
    assert!(!is_base_word(f.name()) && !crate::ir::KEYWORDS.contains(&f.name()), "`{}` is a builtin or keyword and cannot be registered", f.name());
    assert!(matches!(f.input(), corgi::Shape::Unit | corgi::Shape::Prod(_)), "`{}`: a function's input is `()` or the tuple of its arguments", f.name());
    functions().write().unwrap().insert(f.name().to_string(), f);
}

/// Every registered function.
pub(crate) fn registered() -> Vec<Function> {
    functions().read().unwrap().values().cloned().collect()
}

/// The registered function called `name`, if any.
pub fn lookup(name: &str) -> Option<Function> {
    functions().read().unwrap().get(name).cloned()
}

// ---------------------------------------------------------------------------
// Typing and compiling slots, and the words every program's lambdas start with.

type Res<T> = Result<T, String>;

/// The total-order key of an `f64`, as corgi's `Scalar::Float` holds a float constant.
fn float_key(f: f64) -> u64 {
    let bits = f.to_bits();
    if bits >> 63 == 1 { !bits } else { bits ^ (1 << 63) }
}

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
pub(crate) fn slot_shape(lambda: &Lambda, shapes: &[Shape]) -> Res<Shape> {
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
pub(crate) fn compile_flatmap(list: &Lambda, kshape: &Shape, vshape: &Shape) -> Res<Graph<NumOp>> {
    match slot_graph(list, &[kshape.clone(), vshape.clone()])? {
        (k, Shape::List(_)) => Ok(k),
        (_, other) => Err(format!("flatmap over a non-list: {other}")),
    }
}

/// Compile a `reduce` lambda over `(key, vals)`, `vals` a list of `(val, diff)` (`vshape` the
/// values') → a corgi `List` column of `(val, diff)`, one list per key.
pub(crate) fn compile_reduce(lambda: &Lambda, kshape: &Shape, vshape: &Shape) -> Res<Graph<NumOp>> {
    let list = Shape::List(Box::new(Shape::Prod(vec![vshape.clone(), Shape::Int])));
    match slot_graph(lambda, &[kshape.clone(), list])? {
        (g, Shape::List(e)) if matches!(&*e, Shape::Prod(fs) if fs.len() == 2 && fs[1] == Shape::Int) => Ok(g),
        (_, other) => Err(format!("reduce output is not a list of (val, diff): {other}")),
    }
}

/// Compile a `JoinFunction`'s lambda → a `List` column of `((key, val), time, diff)`, one list per
/// input row, with `time` a tuple of `width` `Int`s. Any other shape is the type error: the backend
/// explodes the column structurally and joins the time lanes into the rows' times.
pub(crate) fn compile_join_function(lambda: &Lambda, kshape: &Shape, vshape: &Shape, width: usize) -> Res<Graph<NumOp>> {
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
pub(crate) fn compile_scalar(lambda: &Lambda, kshape: &Shape, vshape: &Shape) -> Res<Graph<NumOp>> {
    match slot_graph(lambda, &[kshape.clone(), vshape.clone()])? {
        (k, Shape::Int) => Ok(k),
        (_, other) => Err(format!("enter_at delay is not an integer: {other}")),
    }
}

/// Compile a `Filter` predicate → a mask column (nonzero keeps the row). A predicate must be an
/// `Int`; any other shape is a type error.
pub(crate) fn compile_predicate(cond: &Lambda, kshape: &Shape, vshape: &Shape) -> Res<Graph<NumOp>> {
    match slot_graph(cond, &[kshape.clone(), vshape.clone()])? {
        (k, Shape::Int) => Ok(k),
        (_, other) => Err(format!("a filter predicate must be an Int, got {other}")),
    }
}

/// Compile a projection over `rows`, the shapes of `(key, val)` or `(key, val0, val1)`: input
/// `Prod(rows)`, output `Prod([newkey, newval])`.
pub(crate) fn compile_projection(lambda: &Lambda, rows: &[Shape]) -> Res<Graph<NumOp>> {
    match slot_graph(lambda, rows)? {
        (g, Shape::Prod(fs)) if fs.len() == 2 => Ok(g),
        (_, other) => Err(format!("a projection gives (key, val), not {other}")),
    }
}

/// The scope every DDIR program's lambdas start from: the Float functions as words, and the
/// built-in sums `Option` and `Result`. A word's input is its argument, or the tuple of its
/// arguments when it takes several, so `x call fsqrt` and `(x, y) call fpow`. The sums leave their
/// payload lanes open, as they have no shapes of their own: `inject` fills an open lane with its
/// payload's shape, and `branch`, `map_variant` and `match` read only their tags.
pub(crate) fn base_scope() -> corgi::Scope {
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
fn is_base_word(name: &str) -> bool { FloatWord::all().any(|(word, _)| word == name) }

/// The registered function `f` as a word: one host-kernel node over the call's input, which is its
/// argument when it takes one, and the tuple of its arguments (or `()`) otherwise.
pub(crate) fn function_word(f: &Function) -> Graph<NumOp> {
    let mut b = Builder::<NumOp>::default();
    let x = b.input();
    let input = if matches!(f.input(), Shape::Prod(args) if args.len() == 1) { b.tuple(vec![x]) } else { x };
    let out = b.add(NumOp::Host(corgi::HostOp(std::sync::Arc::clone(f))), vec![input]);
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
    pub(super) enum FloatOp { Fn(F64Fn), ToInt, Pow, PowI }

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
    pub(super) fn op(op: FloatOp) -> HostOp {
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

    /// `body`, a lambda body over the floats `a` and `b` in the base scope, on each pair.
    fn floats(body: &str, pairs: &[(f64, f64)]) -> CValue {
        let lambda = Lambda::new(format!("let (a, b) = input in {body}"), &base_scope());
        let (a, b) = pairs.iter().copied().unzip();
        corgi::eval_graph(lambda.graph().unwrap(), CValue::Prod(vec![CValue::f64(a), CValue::f64(b)]))
    }

    /// Every pair of these floats: signed zeros, infinities, and both signs of NaN.
    fn float_pairs() -> Vec<(f64, f64)> {
        let specials = [f64::NEG_INFINITY, -2.5, -1.0, -0.0, 0.0, 0.5, 1.0, 2.0, 3.7, f64::INFINITY, f64::NAN, -f64::NAN];
        specials.iter().flat_map(|&a| specials.iter().map(move |&b| (a, b))).collect()
    }

    /// What a Float word gives in Rust, a float or an integer.
    enum Rust { Float(Box<dyn Fn(f64, f64) -> f64>), Int(Box<dyn Fn(f64, f64) -> i64>) }

    /// Every Float word, called as a program calls it, gives Rust's own `f64` answer on every
    /// special pair: the words composed of corgi ops and the host kernels (`float_kernels`), alone
    /// and inside a larger body. Floats compare by their total order, so signed zeros and NaNs
    /// must agree too.
    #[test]
    fn float_words_agree_with_rust() {
        // `fmin`/`fmax`: skip a NaN operand, else the total order; two NaNs give the second.
        let pick = |x: f64, y: f64, min: bool| {
            let pick_x = if x.is_nan() { false } else if y.is_nan() { true }
                else if min { x.total_cmp(&y).is_le() } else { x.total_cmp(&y).is_ge() };
            if pick_x { x } else { y }
        };
        let powi = |x: f64, y: f64| x.powi((y as i64).clamp(i32::MIN as i64, i32::MAX as i64) as i32);
        let mask = |test: fn(&f64, &f64) -> bool| Rust::Int(Box::new(move |x, y| test(&x, &y) as i64));
        let mut cases: Vec<(String, Rust)> = vec![
            ("(a, b) call fmin".into(), Rust::Float(Box::new(move |x, y| pick(x, y, true)))),
            ("(a, b) call fmax".into(), Rust::Float(Box::new(move |x, y| pick(x, y, false)))),
            ("(a, b) call feq".into(), mask(f64::eq)),
            ("(a, b) call fne".into(), mask(f64::ne)),
            ("(a, b) call flt".into(), mask(f64::lt)),
            ("(a, b) call fle".into(), mask(f64::le)),
            ("(a, b) call fgt".into(), mask(f64::gt)),
            ("(a, b) call fge".into(), mask(f64::ge)),
            ("a call fint".into(), Rust::Int(Box::new(|x, _| x as i64))),
            ("(a, b) call fpow".into(), Rust::Float(Box::new(f64::powf))),
            ("(a, b call fint) call fpowi".into(), Rust::Float(Box::new(powi))),
        ];
        for (name, g) in F64Fn::ALL {
            cases.push((format!("a call {name}"), Rust::Float(Box::new(move |x, _| g.apply(x)))));
        }
        let pairs = float_pairs();
        for (body, rust) in &cases {
            let expected = match rust {
                Rust::Float(g) => CValue::f64(pairs.iter().map(|&(x, y)| g(x, y)).collect()),
                Rust::Int(g) => CValue::int(pairs.iter().map(|&(x, y)| g(x, y))),
            };
            assert_eq!(floats(body, &pairs), expected, "{body}");
        }
        // Inside a larger body. Over non-NaN pairs only: corgi's float `add` returns the positive
        // quiet NaN where Rust may keep a NaN operand's sign; the words themselves agree on NaNs.
        let finite: Vec<_> = pairs.into_iter().filter(|(x, y)| !x.is_nan() && !y.is_nan()).collect();
        let body = "((a call fexp, b) add, (a call fln, b) call flt, (a call fsqrt, b call fint) call fpowi)";
        let expected = CValue::Prod(vec![
            CValue::f64(finite.iter().map(|&(x, y)| x.exp() + y).collect()),
            CValue::int(finite.iter().map(|&(x, y)| (x.ln() < y) as i64)),
            CValue::f64(finite.iter().map(|&(x, y)| powi(x.sqrt(), y)).collect()),
        ]);
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
        let one = |body: &str, a: f64, b: f64| floats(body, &[(a, b)]);
        let (int, float) = (|n: i64| CValue::int([n]), |x: f64| CValue::f64(vec![x]));
        assert_eq!(one("a call fint", -2.7, 0.0), int(-2));
        assert_eq!(one("a call fint", f64::NAN, 0.0), int(0));
        assert_eq!(one("a call fint", 1e300, 0.0), int(i64::MAX));
        assert_eq!(one("(a, b) call flt", -2.0, -1.0), int(1));
        assert_eq!(one("(a, b) lt", -2.0, -1.0), int(1));
        assert_eq!(one("(a, b) call feq", -0.0, 0.0), int(1));
        assert_eq!(one("(a, b) eq", -0.0, 0.0), int(0));
        assert_eq!(one("(a, b) call fne", f64::NAN, f64::NAN), int(1));
        assert_eq!(one("(a, b) call fmax", f64::NAN, 1.0), float(1.0));
        assert_eq!(one("(a, b) call fmin", 0.0, -0.0), float(-0.0));
        assert!(one("a call fln", -1.0, 0.0).as_f64("fln").unwrap()[0].is_nan());
        assert_eq!(one("a call fln", 0.0, 0.0), float(f64::NEG_INFINITY));
        assert_eq!(one("(a, b) call fpow", 4.0, 0.5), float(2.0));
        assert!(one("(a, b) call fpow", -8.0, 0.5).as_f64("fpow").unwrap()[0].is_nan());
    }
}
