//! Row vocabulary shared by the IR and the renderers: the `Value` data model,
//! the scalar language (`Term` and its operators) with its interpreter
//! (`eval`), and the `LinearOp` operator steps. The concrete syntax that
//! produces a `Term` lives in `parse`; the program structure in `scope_ir`.

pub type Diff = i64;
pub type Id = usize;
pub type Time = timely::order::Product<u64, differential_dataflow::dynamic::pointstamp::PointStamp<u64>>;

/// A runtime value: the data model of the interpreter.
///
/// An algebraic data type over a single scalar (`Int`). `Tuple`/`Variant`/
/// `List` are the product/sum/sequence constructors; together they cover
/// JSON-shaped data and program ASTs. A collection element is a `(key, val)`
/// pair of `Value`s (typically `Tuple`s). The row backend uses the derived
/// `Ord` as its physical arrangement order. In particular, `min` observes the
/// signed ordering of `Int`; columnar backends reproduce that ordering without
/// materializing rows.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, serde::Serialize, serde::Deserialize)]
pub enum Value {
    Int(i64),
    Tuple(Vec<Value>),
    Variant(u32, Box<Value>),
    List(Vec<Value>),
}

impl Value {
    /// Whether a boundary value satisfies an explicitly ascribed column shape.
    pub fn has_shape(&self, shape: &corgi::Shape) -> bool {
        use corgi::Shape;
        match (self, shape) {
            (Self::Int(_), Shape::Prim(64)) => true,
            (Self::Tuple(xs), Shape::Unit) => xs.is_empty(),
            (Self::Tuple(xs), Shape::Prod(fs)) => xs.len() == fs.len() && xs.iter().zip(fs).all(|(x, f)| x.has_shape(f)),
            (Self::List(xs), Shape::List(f)) => xs.iter().all(|x| x.has_shape(f)),
            (Self::Variant(tag, value), Shape::Sum(fs)) => fs.get(*tag as usize).is_some_and(|f| value.has_shape(f)),
            _ => false,
        }
    }
    /// F64 is an explicit one-variant newtype, not an implicit second meaning
    /// for Int arithmetic. Its payload is the signed-order form of Corgi's
    /// total-order float encoding, so existing structural hash/Ord and the
    /// columnar SUM representation apply without row/column type erasure.
    /// Like other DDIR sum types, the nominal type name is erased at runtime.
    pub fn f64_value(value: f64) -> Self {
        let bits = value.to_bits();
        let ordered = if bits >> 63 == 1 { !bits } else { bits ^ (1 << 63) };
        Self::Variant(0, Box::new(Self::Int((ordered ^ (1 << 63)) as i64)))
    }
    pub fn as_f64(&self) -> f64 {
        let Self::Variant(0, payload) = self else { panic!("expected F64 newtype, got {self:?}") };
        let ordered = payload.as_int() as u64 ^ (1 << 63);
        f64::from_bits(if ordered >> 63 == 1 { ordered ^ (1 << 63) } else { !ordered })
    }
    /// The empty tuple — the conventional "unit"/empty value.
    pub fn unit() -> Value { Value::Tuple(Vec::new()) }
    /// Truthiness: a nonzero `Int` is true; everything else is false.
    pub fn truthy(&self) -> bool { matches!(self, Value::Int(n) if *n != 0) }
    /// Unwrap an `Int`, panicking otherwise (interpreter is dynamically typed).
    pub fn as_int(&self) -> i64 { match self { Value::Int(n) => *n, other => panic!("expected Int, got {:?}", other) } }
}

/// Scalar expression over [`Value`]. For the concrete surface
/// syntax (how each of these is written in a `.ddp` program), see the
/// module-level reference on [`crate::parse::pipe`].
///
/// A `Term` is evaluated against an *environment* — a stack of `Value`s.
/// The bottom of the stack is the operator's input rows:
///
/// - linear ops (`map`/`filter`/`enter_at`): `Var(0)` = key, `Var(1)` = val;
/// - joins: `Var(0)` = key, `Var(1)` = left val, `Var(2)` = right val.
///
/// `Case` and `Fold` push their binders on top of the stack; those are read
/// back with `Bound(k)` (de Bruijn, `0` = innermost) so a sub-term is
/// independent of how deep it sits.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Term {
    /// Input row at absolute environment index.
    Var(usize),
    /// `case`/`fold` binder, counting from the innermost (`0`).
    Bound(usize),
    /// Integer literal.
    Int(i64),
    /// Product intro. A `Spread` child splices a tuple's fields in place; any
    /// other value is one field.
    Tuple(Vec<Term>),
    /// List intro.
    List(Vec<Term>),
    /// Splice marker; only meaningful as a direct child of `Tuple`.
    /// Lets a whole input row (`$n`) contribute all its fields.
    Spread(Box<Term>),
    /// Product/list elimination: index into a `Tuple` or `List`.
    Proj(Box<Term>, usize),
    /// Sum intro: tag a payload into a KNOWN sum type. `sum` names every lane's
    /// shape (from a `type` declaration, or a built-in `Option`/`Result`), so a
    /// column of these is one concrete columnar sum whichever lanes its rows
    /// happen to use. The tag is a `Term`: a literal for a constructor call,
    /// or data-driven (`variant(Type, tag, payload)`) when every lane of the
    /// type shares the payload's shape.
    Inject { tag: Box<Term>, payload: Box<Term>, sum: SumTy },
    /// Sum elimination. The scrutinee's payload is pushed as `Bound(0)` for
    /// the chosen arm. `arms[t]` handles tag `t`; `default` handles the rest.
    Case { scrutinee: Box<Term>, arms: Vec<Term>, default: Option<Box<Term>> },
    /// List elimination (left fold). For each element, `step` is evaluated
    /// with the element as `Bound(0)` and the accumulator as `Bound(1)`.
    Fold { list: Box<Term>, init: Box<Term>, step: Box<Term> },
    /// Conditional; `cond` is truthy when it is a nonzero `Int`.
    If { cond: Box<Term>, then: Box<Term>, els: Box<Term> },
    Unary(UnOp, Box<Term>),
    Binary(BinOp, Box<Term>, Box<Term>),
    /// `hash(bound, keys…)`: a deterministic pseudo-random `Int` in `[0, bound)`
    /// (the raw non-negative hash if `bound <= 0`), mixed from the keys.
    /// The building block for generators derived from `iota`/`clock`.
    Hash(Vec<Term>),
    /// `name(args…)` for a function the embedding program registered (see
    /// [`register`]): a pure Rust function from argument values to a value.
    /// DDIR only moves its arguments and result; what it computes is the
    /// embedder's. The corgi backend runs it as a host kernel: the function's
    /// columnar kernel if one is registered (`register_kernel`), otherwise its
    /// row body over just the call's arguments.
    Call(String, Vec<Term>),
}

/// The sum type an `Inject` builds into. `Declared` carries the full lane shapes of a `type`
/// declaration. The built-ins are shape constructors whose parameter is filled in at compile
/// time from the payload (`Some(x)`, `Ok(x)`, `Err(e)`) or from the other branch of an enclosing
/// `if` (`None`, and the lane the payload does not fill).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SumTy {
    Declared(Vec<corgi::Shape>),
    /// `Option(T)` = `Sum{ () | T }`: `None` is lane 0, `Some` lane 1.
    Option,
    /// `Result(T, E)` = `Sum{ T | E }`: `Ok` is lane 0, `Err` lane 1.
    Result,
    /// An untyped literal, `inject(tag, payload)`: a `Value::Variant` written as a constant, for
    /// closed terms fed to the row interpreter (the server's `feed`). It names no sum, so the
    /// columnar lowering rejects it — a program builds sums from declared types.
    Dynamic,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum UnOp {
    /// Integer negation.
    Neg,
    /// Logical negation (truthy -> 0, else 1).
    Not,
    /// `1` if the operand is a `Variant` with the given tag, else `0`.
    IsTag(u32),
    /// Number of elements in a `Tuple` or `List`, as an `Int`.
    Len,
    /// Explicit signed Int -> F64 newtype conversion (see Value::f64_value).
    ToF64,
    /// Floating-point negation; does not reinterpret integer arithmetic.
    F64Neg,
    /// A one-argument F64 -> F64 function (`fsqrt`, `fexp`, ...), with Rust's `f64` semantics.
    F64Fn(F64Fn),
    /// F64 -> Int, truncating toward zero and saturating, exactly Rust's `x as i64`:
    /// NaN is 0, and values beyond the `i64` range clamp to `i64::MIN`/`i64::MAX`.
    F64ToInt,
}

/// The one-argument F64 -> F64 functions. Each is the Rust `f64` method of the same name, so a
/// program ported from Rust gets bit-identical results on the same platform. `Abs` is exact
/// everywhere; the transcendental ones (`Exp`, `Ln`, `Sin`, `Cos`, `Tan`) call the platform libm
/// and are only as reproducible across platforms as it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum F64Fn {
    Abs, Sqrt, Exp, Ln, Floor, Ceil, Round, Sin, Cos, Tan,
}

impl F64Fn {
    /// The surface names, `f` + the Rust method name (`ln`, not `log`).
    pub const ALL: [(&'static str, F64Fn); 10] = [
        ("fabs", F64Fn::Abs), ("fsqrt", F64Fn::Sqrt), ("fexp", F64Fn::Exp), ("fln", F64Fn::Ln),
        ("ffloor", F64Fn::Floor), ("fceil", F64Fn::Ceil), ("fround", F64Fn::Round),
        ("fsin", F64Fn::Sin), ("fcos", F64Fn::Cos), ("ftan", F64Fn::Tan),
    ];
    pub fn apply(self, x: f64) -> f64 {
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

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum BinOp {
    Add, Sub, Mul,
    /// Truncating signed division; zero divisor returns zero, MIN / -1 wraps.
    Div,
    /// Concatenation of two lists with the same element type.
    Append,
    F64Add, F64Sub, F64Mul, F64Div,
    /// `x.powf(y)`.
    F64Pow,
    /// `x.powi(n)` with an Int exponent (saturated to `i32`). Rust's `powi` is repeated
    /// multiplication, which need not round like `powf`; this is here so ported code can match.
    F64PowI,
    /// Minimum and maximum that skip a NaN operand (as Rust's `f64::min`/`max` do), and otherwise
    /// follow the total order, so `fmin(-0.0, 0.0)` is `-0.0` and `fmax` of them is `0.0` — a
    /// deterministic choice where Rust leaves signed zeros unspecified. Two NaNs give the second.
    F64Min, F64Max,
    /// IEEE comparisons, returning `Int` 0/1 like the generic ones: every comparison with a NaN is
    /// false except `F64Ne`, which is true, and `-0.0 == 0.0`. The generic `== < ...` instead
    /// order F64 values by the total order (`f64::total_cmp`), which is right for negative numbers
    /// but distinguishes the two zeros and places NaNs at the ends.
    F64Eq, F64Ne, F64Lt, F64Le, F64Gt, F64Ge,
    Eq, Ne, Lt, Le, Gt, Ge,
    And, Or,
}

/// A `(key, val)` reshaping: each component evaluates to one `Value`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Projection { pub key: Term, pub val: Term }

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Reducer {
    Min,
    Distinct,
    Count,
    /// NEST: collect a key's values into a `Value::List`, in the values' own
    /// (`Value: Ord`) order — so deterministic, and position-ordered when the
    /// values are `tuple(pos, …)` as `flatmap` emits. The inverse of `FlatMap`.
    Collect,
}

/// An individual step within a Linear node.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum LinearOp {
    /// Rekey/reval: project to new (key, val).
    Project(Projection),
    /// Keep the record when the `Term` evaluates to a truthy `Value`.
    Filter(Term),
    /// Negate the diff.
    Negate,
    /// Shift the timestamp based on an `Int`-valued `Term`.
    EnterAt(Term),
    /// Append the current user-iter coord (at the row's scope depth) to
    /// the value. Time itself is unchanged. See `Expr::LiftIter` for the
    /// discipline restriction.
    LiftIter,
    /// UNNEST: explode a `List`-valued `Term` into one row per element, value
    /// `tuple(pos, element)`. See `parse::Expr::FlatMap`.
    FlatMap(Term),
}

// DDIR's `hash` IS corgi's structural hash, evaluated a row at a time here and a column at a
// time in the corgi backend. The two must agree bit for bit — they are the same program value,
// and the backends are checked against each other — so this is a transcription of
// `corgi::hash`'s fold, not an independent design. The `hash_matches_corgi_*` tests in
// `corgi::logic` pin it.
//
// The values are DDIR's; the shapes they transcode to are corgi's, and the fold follows those:
// `Int` is a `Prim` leaf, the empty `Tuple` is `Unit` (NOT a fieldless `Prod`), a `Tuple` is a
// `Prod`, a `List` folds its length then its elements, and a `Variant` folds its tag then its
// payload. The salts and multipliers are corgi's constants; changing one re-ids everything.
//
// No cross-run or cross-implementation stability is promised, exactly as Rust's own `Hash` makes
// no such promise: nothing may test a literal hash value or an order derived from one.
const HASH_PROD: u64 = 0x243f_6a88_85a3_08d3;
const HASH_SUM: u64 = 0x1319_8a2e_0370_7344;
const HASH_LIST: u64 = 0xa409_3822_299f_31d0;
const HASH_UNIT: u64 = 0x082e_fa98_ec4e_6c89;

/// splitmix64's finalizer — the one bit-mixing primitive, as in `corgi::hash::mix64`.
fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Fold one child hash into an accumulator — order-sensitive, so field order, element order and
/// tag position all matter.
fn hash_combine(acc: u64, x: u64) -> u64 {
    (acc ^ mix64(x)).wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

/// The stable structural hash of one DDIR value. Row-wise twin of `corgi::hash`.
pub fn structural_hash(v: &Value) -> u64 {
    match v {
        Value::Int(x) => mix64(*x as u64),
        Value::Tuple(fs) if fs.is_empty() => HASH_UNIT,
        Value::Tuple(fs) => fs.iter().fold(HASH_PROD, |a, f| hash_combine(a, structural_hash(f))),
        Value::List(xs) => xs
            .iter()
            .fold(hash_combine(HASH_LIST, xs.len() as u64), |a, x| hash_combine(a, structural_hash(x))),
        Value::Variant(t, p) => hash_combine(hash_combine(HASH_SUM, *t as u64), structural_hash(p)),
    }
}

/// Evaluate a scalar `Term` against an environment of `Value`s.
///
/// `env` holds the operator's input rows at the bottom (`Var(i)` indexes it
/// absolutely); `Case`/`Fold` push their binders on top, read back with
/// `Bound(k)` counting from the innermost. Binders are pushed and popped
/// around sub-evaluation, so `env` is restored on return.
pub fn eval(term: &Term, env: &mut Vec<Value>) -> Value {
    match term {
        Term::Call(name, args) => {
            let f = lookup(name).unwrap_or_else(|| panic!("call to unregistered function `{name}`"));
            let vals: Vec<Value> = args.iter().map(|a| eval(a, env)).collect();
            // The corgi backend rejects mis-shaped arguments when it types the program; the row
            // backend has no typing pass, so it checks each call rather than run the body on them.
            for (i, (v, s)) in vals.iter().zip(&f.args).enumerate() {
                assert!(v.has_shape(s), "`{name}`: argument {i} is {v:?}, not of the declared shape {s}");
            }
            (f.body)(&vals)
        }
        Term::Var(i) => env[*i].clone(),
        Term::Bound(k) => env[env.len() - 1 - *k].clone(),
        Term::Int(n) => Value::Int(*n),
        Term::Tuple(fields) => Value::Tuple(tuple_fields(fields, env)),
        Term::List(fields) => Value::List(fields.iter().map(|f| eval(f, env)).collect()),
        Term::Spread(_) => panic!("Spread is only valid as a direct child of Tuple"),
        Term::Proj(t, i) => {
            // If the operand is a "place" (a Var/Bound/Proj chain), index into
            // it by reference and clone only the selected field — avoids deep-
            // cloning the whole environment slot just to discard most of it.
            if let Some(place) = eval_ref(t, env) {
                match place {
                    Value::Tuple(xs) | Value::List(xs) => {
                        assert!(*i < xs.len(), "Proj index {} out of bounds (len {})", i, xs.len());
                        xs[*i].clone()
                    }
                    other => panic!("Proj on non-aggregate value: {:?}", other),
                }
            } else {
                match eval(t, env) {
                    Value::Tuple(mut xs) | Value::List(mut xs) => {
                        assert!(*i < xs.len(), "Proj index {} out of bounds (len {})", i, xs.len());
                        xs.swap_remove(*i)
                    }
                    other => panic!("Proj on non-aggregate value: {:?}", other),
                }
            }
        }
        Term::Inject { tag, payload, .. } => Value::Variant(eval(tag, env).as_int() as u32, Box::new(eval(payload, env))),
        Term::Case { scrutinee, arms, default } => {
            let Value::Variant(tag, payload) = eval(scrutinee, env) else {
                panic!("Case scrutinee is not a Variant")
            };
            match arms.get(tag as usize) {
                Some(arm) => {
                    env.push(*payload);
                    let r = eval(arm, env);
                    env.pop();
                    r
                }
                None => match default {
                    Some(d) => eval(d, env),
                    None => panic!("Case: no arm for tag {} and no default", tag),
                },
            }
        }
        Term::Fold { list, init, step } => {
            let Value::List(items) = eval(list, env) else { panic!("Fold on non-list value") };
            let mut acc = eval(init, env);
            for item in items {
                // step sees elem = Bound(0), acc = Bound(1).
                env.push(acc);
                env.push(item);
                acc = eval(step, env);
                env.pop();
                env.pop();
            }
            acc
        }
        Term::If { cond, then, els } => {
            if eval(cond, env).truthy() { eval(then, env) } else { eval(els, env) }
        }
        Term::Hash(args) => {
            // args[0] is the (exclusive) bound; the rest are hashed as one tuple.
            let bound = eval(&args[0], env).as_int();
            let payload = Value::Tuple(args[1..].iter().map(|a| eval(a, env)).collect());
            let h = (structural_hash(&payload) >> 1) as i64; // non-negative
            Value::Int(if bound > 0 { h % bound } else { h })
        }
        Term::Unary(op, t) => eval_unary(*op, eval(t, env)),
        Term::Binary(op, l, r) => {
            // Short-circuit the logical operators.
            match op {
                BinOp::And => return Value::Int((eval(l, env).truthy() && eval(r, env).truthy()) as i64),
                BinOp::Or => return Value::Int((eval(l, env).truthy() || eval(r, env).truthy()) as i64),
                _ => {}
            }
            eval_binary(*op, eval(l, env), eval(r, env))
        }
    }
}

/// Resolve a "place" term (a `Var`/`Bound`/`Proj` chain) to a borrowed
/// reference into `env`, without cloning. Returns `None` for terms that build
/// a fresh value (constructors, primitives) — those must be evaluated owned.
fn eval_ref<'a>(term: &Term, env: &'a [Value]) -> Option<&'a Value> {
    match term {
        Term::Var(i) => env.get(*i),
        Term::Bound(k) => env.get(env.len().checked_sub(1 + *k)?),
        Term::Proj(t, i) => match eval_ref(t, env)? {
            Value::Tuple(xs) | Value::List(xs) => xs.get(*i),
            _ => None,
        },
        _ => None,
    }
}

/// Build a tuple's fields. A `Spread` child splices a tuple's fields in place (a unit splices
/// nothing); any other value is one field, so a tuple's arity never depends on the data.
fn tuple_fields(fields: &[Term], env: &mut Vec<Value>) -> Vec<Value> {
    let mut out = Vec::with_capacity(fields.len());
    for f in fields {
        match f {
            Term::Spread(inner) => match eval(inner, env) {
                Value::Tuple(xs) => out.extend(xs),
                other => out.push(other),
            },
            _ => out.push(eval(f, env)),
        }
    }
    out
}

fn eval_unary(op: UnOp, v: Value) -> Value {
    match op {
        UnOp::Neg => Value::Int(-v.as_int()),
        UnOp::ToF64 => Value::f64_value(v.as_int() as f64),
        UnOp::F64Neg => Value::f64_value(-v.as_f64()),
        UnOp::F64Fn(f) => Value::f64_value(f.apply(v.as_f64())),
        UnOp::F64ToInt => Value::Int(v.as_f64() as i64),
        UnOp::Not => Value::Int((!v.truthy()) as i64),
        UnOp::IsTag(t) => Value::Int(matches!(&v, Value::Variant(tag, _) if *tag == t) as i64),
        UnOp::Len => match v {
            Value::Tuple(xs) | Value::List(xs) => Value::Int(xs.len() as i64),
            other => panic!("Len on non-aggregate value: {:?}", other),
        },
    }
}

fn eval_binary(op: BinOp, l: Value, r: Value) -> Value {
    let b = |x: bool| Value::Int(x as i64);
    match op {
        BinOp::Add => Value::Int(l.as_int() + r.as_int()),
        BinOp::Sub => Value::Int(l.as_int() - r.as_int()),
        BinOp::Mul => Value::Int(l.as_int() * r.as_int()),
        BinOp::Div => Value::Int(if r.as_int() == 0 { 0 } else { l.as_int().wrapping_div(r.as_int()) }),
        BinOp::Append => match (l, r) {
            (Value::List(mut a), Value::List(b)) => { a.extend(b); Value::List(a) }
            other => panic!("append expects two lists, got {other:?}"),
        },
        BinOp::F64Add => Value::f64_value(l.as_f64() + r.as_f64()),
        BinOp::F64Sub => Value::f64_value(l.as_f64() - r.as_f64()),
        BinOp::F64Mul => Value::f64_value(l.as_f64() * r.as_f64()),
        BinOp::F64Div => Value::f64_value(l.as_f64() / r.as_f64()),
        BinOp::F64Pow => Value::f64_value(l.as_f64().powf(r.as_f64())),
        BinOp::F64PowI => Value::f64_value(l.as_f64().powi(r.as_int().clamp(i32::MIN as i64, i32::MAX as i64) as i32)),
        BinOp::F64Min | BinOp::F64Max => {
            let (x, y) = (l.as_f64(), r.as_f64());
            let pick_x = if x.is_nan() { false } else if y.is_nan() { true }
                else if matches!(op, BinOp::F64Min) { x.total_cmp(&y).is_le() } else { x.total_cmp(&y).is_ge() };
            if pick_x { l } else { r }
        }
        BinOp::F64Eq => b(l.as_f64() == r.as_f64()),
        BinOp::F64Ne => b(l.as_f64() != r.as_f64()),
        BinOp::F64Lt => b(l.as_f64() < r.as_f64()),
        BinOp::F64Le => b(l.as_f64() <= r.as_f64()),
        BinOp::F64Gt => b(l.as_f64() > r.as_f64()),
        BinOp::F64Ge => b(l.as_f64() >= r.as_f64()),
        // Comparisons are structural, using the derived `Ord`/`Eq` on `Value`.
        BinOp::Eq => b(l == r),
        BinOp::Ne => b(l != r),
        BinOp::Lt => b(l < r),
        BinOp::Le => b(l <= r),
        BinOp::Gt => b(l > r),
        BinOp::Ge => b(l >= r),
        BinOp::And | BinOp::Or => unreachable!("logical ops short-circuit in eval"),
    }
}

// ---------------------------------------------------------------------------
// Registered functions: how an embedding program extends the scalar language.

/// A function an embedding program supplies to DDIR programs, called by name.
///
/// The contract is purity: the same arguments must always give the same
/// result, because differential dataflow re-evaluates terms when it retracts
/// what they produced, and a retraction must cancel exactly. The declared
/// shapes are what the columnar backend types the call as; values that do not
/// match them are an error of the embedder's.
pub struct Function {
    pub name: String,
    pub args: Vec<corgi::Shape>,
    pub result: corgi::Shape,
    pub body: Box<dyn Fn(&[Value]) -> Value + Send + Sync>,
}

fn functions() -> &'static std::sync::RwLock<std::collections::HashMap<String, std::sync::Arc<Function>>> {
    static REGISTRY: std::sync::OnceLock<std::sync::RwLock<std::collections::HashMap<String, std::sync::Arc<Function>>>> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

/// Register `f` under its name, for programs parsed afterwards (the parser
/// resolves calls against the registry). Registering a name again replaces it;
/// a name may not shadow a builtin.
pub fn register(f: Function) {
    assert!(!crate::parse::is_builtin(&f.name), "`{}` is a builtin or keyword and cannot be registered", f.name);
    // A kernel for the old registration (its columnar body, or the row adapter holding the old
    // body) no longer describes this function; `register_kernel` may give it a new one.
    kernels().write().unwrap().remove(&f.name);
    functions().write().unwrap().insert(f.name.clone(), std::sync::Arc::new(f));
}

fn kernels() -> &'static std::sync::RwLock<std::collections::HashMap<String, std::sync::Arc<dyn corgi::HostKernel>>> {
    static KERNELS: std::sync::OnceLock<std::sync::RwLock<std::collections::HashMap<String, std::sync::Arc<dyn corgi::HostKernel>>>> = std::sync::OnceLock::new();
    KERNELS.get_or_init(Default::default)
}

/// Give the registered function `name` a columnar body: the corgi backend calls `kernel` on whole
/// columns instead of `body` a row at a time. Its declared shapes must be the function's.
pub fn register_kernel(name: &str, kernel: std::sync::Arc<dyn corgi::HostKernel>) {
    let f = lookup(name).unwrap_or_else(|| panic!("register_kernel: `{name}` is not a registered function"));
    assert_eq!(kernel.input(), &call_input(&f.args), "register_kernel: `{name}` input shape");
    assert_eq!(kernel.output(), &f.result, "register_kernel: `{name}` output shape");
    kernels().write().unwrap().insert(name.to_string(), kernel);
}

/// The columnar body of `name`: its registered kernel, or its row body behind an adapter that
/// converts only the call's arguments and result.
pub fn kernel_of(name: &str) -> Option<std::sync::Arc<dyn corgi::HostKernel>> {
    if let Some(k) = kernels().read().unwrap().get(name) { return Some(std::sync::Arc::clone(k)) }
    let f = lookup(name)?;
    let k: std::sync::Arc<dyn corgi::HostKernel> = std::sync::Arc::new(RowKernel { input: call_input(&f.args), f });
    // Keep it, so every call site shares one kernel (and CSE can merge equal calls).
    Some(std::sync::Arc::clone(kernels().write().unwrap().entry(name.to_string()).or_insert(k)))
}

/// The column a call passes its kernel: the tuple of its arguments, or `Unit` for a call with none
/// (an empty product carries no row count, and corgi rejects it as a kernel input).
pub fn call_input(args: &[corgi::Shape]) -> corgi::Shape {
    if args.is_empty() { corgi::Shape::Unit } else { corgi::Shape::Prod(args.to_vec()) }
}

/// A row-at-a-time function as a host kernel.
struct RowKernel { f: std::sync::Arc<Function>, input: corgi::Shape }
impl corgi::HostKernel for RowKernel {
    fn name(&self) -> &str { &self.f.name }
    fn input(&self) -> &corgi::Shape { &self.input }
    fn output(&self) -> &corgi::Shape { &self.f.result }
    fn eval(&self, input: corgi::Value) -> Result<corgi::Value, String> {
        let rows = crate::corgi::logic::untranscode(input, &self.input);
        // The input is `Prod(args)` or `Unit` (`call_input`); both untranscode to tuples.
        let out: Vec<Value> = rows.into_iter().map(|r| {
            let Value::Tuple(args) = r else { unreachable!("a call's arguments untranscode to a tuple") };
            (self.f.body)(&args)
        }).collect();
        Ok(crate::corgi::logic::transcode_owned(out, &self.f.result))
    }
}

/// The registered function called `name`, if any.
pub fn lookup(name: &str) -> Option<std::sync::Arc<Function>> {
    functions().read().unwrap().get(name).cloned()
}
