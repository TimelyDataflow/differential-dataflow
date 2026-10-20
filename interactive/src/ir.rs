//! Row vocabulary shared by the IR and the renderers: the `Value` data model, the
//! operators' slots (corgi [`Lambda`]s), and the `LinearOp` operator steps. The program
//! structure lives in `scope_ir`.

pub type Diff = i64;
pub type Id = usize;
pub type Time = timely::order::Product<u64, differential_dataflow::dynamic::pointstamp::PointStamp<u64>>;

/// A value, a row at a time: the data model of registered functions' row bodies.
///
/// An algebraic data type over two scalars, `Int` and `Float`. `Tuple`/`Variant`/
/// `List` are the product/sum/sequence constructors; together they cover
/// JSON-shaped data and program ASTs. A collection element is a `(key, val)`
/// pair of `Value`s (typically `Tuple`s). The derived `Ord` is the structural
/// order the columnar backend keeps without materializing rows; in particular,
/// `min` observes the signed ordering of `Int`.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, serde::Serialize, serde::Deserialize)]
pub enum Value {
    Int(i64),
    Float(F64),
    Tuple(Vec<Value>),
    Variant(u32, Box<Value>),
    List(Vec<Value>),
}

impl Value {
    /// Whether a boundary value satisfies an explicitly ascribed column shape.
    pub fn has_shape(&self, shape: &corgi::Shape) -> bool {
        use corgi::Shape;
        match (self, shape) {
            (Self::Int(_), Shape::Int) => true,
            (Self::Float(_), Shape::Float) => true,
            (Self::Tuple(xs), Shape::Unit) => xs.is_empty(),
            (Self::Tuple(xs), Shape::Prod(fs)) => xs.len() == fs.len() && xs.iter().zip(fs).all(|(x, f)| x.has_shape(f)),
            (Self::List(xs), Shape::List(f)) => xs.iter().all(|x| x.has_shape(f)),
            (Self::Variant(tag, value), Shape::Sum(fs)) => fs.get(*tag as usize).is_some_and(|f| value.has_shape(f)),
            _ => false,
        }
    }
    /// A `Float`.
    pub fn f64_value(value: f64) -> Self { Self::Float(F64(value)) }
    /// Unwrap a `Float`, panicking otherwise.
    pub fn as_f64(&self) -> f64 { match self { Value::Float(x) => x.0, other => panic!("expected Float, got {other:?}") } }
    /// The empty tuple — the conventional "unit"/empty value.
    pub fn unit() -> Value { Value::Tuple(Vec::new()) }
    /// Truthiness: a nonzero `Int` is true; everything else is false.
    pub fn truthy(&self) -> bool { matches!(self, Value::Int(n) if *n != 0) }
    /// Unwrap an `Int`, panicking otherwise.
    pub fn as_int(&self) -> i64 { match self { Value::Int(n) => *n, other => panic!("expected Int, got {:?}", other) } }
}

/// An `f64` that compares, orders, and hashes by its total order (`f64::total_cmp`), as a corgi
/// `Float` does: `-0.0` and `0.0` differ, and a NaN equals itself.
#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct F64(pub f64);

impl PartialEq for F64 { fn eq(&self, other: &Self) -> bool { self.0.total_cmp(&other.0).is_eq() } }
impl Eq for F64 {}
impl PartialOrd for F64 { fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) } }
impl Ord for F64 { fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.0.total_cmp(&other.0) } }
impl std::hash::Hash for F64 { fn hash<H: std::hash::Hasher>(&self, state: &mut H) { self.0.to_bits().hash(state) } }
impl std::fmt::Debug for F64 { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.fmt(f) } }

impl F64 {
    /// The order-preserving key of the total order: negatives flip every bit, the rest flip the
    /// sign bit. Corgi hashes a `Float` by this key.
    pub fn key(self) -> u64 {
        let bits = self.0.to_bits();
        if bits >> 63 == 1 { !bits } else { bits ^ (1 << 63) }
    }
}

/// A slot of the IR: a corgi program as written, `let pat = input in body`, and its graph, whose
/// input is the tuple of the slot's rows (`(key, val)` for a linear op, `(key, left val, right
/// val)` for a join). The parser binds it where it is written, in the scope of its DDIR program:
/// the registered functions and the Float functions as words `call` reaches, and the program's
/// `type` declarations as enums. A parse error is kept, and reported where the program is compiled
/// or run. It serializes as its source, which deserializes in the empty scope.
#[derive(Clone)]
pub struct Lambda {
    pub src: String,
    pub graph: Result<std::sync::Arc<corgi::Graph<corgi::NumOp>>, String>,
}

impl Lambda {
    /// `src` parsed in `scope`. The graph is as written: it is optimized only once it types at the
    /// shapes of the rows it reads (`logic::slot_graph`), as optimizing first could drop or trip on
    /// a type error.
    pub fn new(src: String, scope: &corgi::Scope) -> Self {
        let graph = corgi::parse_ml_in(&src, scope).map(std::sync::Arc::new);
        Lambda { src, graph }
    }
    /// The lambda `pattern -> body`, where `body` reads the results of `calls`: each runs a lambda
    /// on an argument written over `pattern`'s names, and is bound to the name `body` is given for
    /// it. Each lambda's compiled graph is bound as a private word, which `call` grafts, so its text
    /// is never parsed again and its program's scope is not needed; the readable source writes the
    /// lambda's own text in its place. Only `pattern`, the arguments and `body` are parsed, in the
    /// empty scope.
    pub fn compose(pattern: &str, calls: &[(&Lambda, String)], body: impl FnOnce(&[String]) -> String) -> Self {
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
    pub fn graph(&self) -> Result<&std::sync::Arc<corgi::Graph<corgi::NumOp>>, String> {
        self.graph.as_ref().map_err(|e| format!("corgi program `{}`: {e}", self.src))
    }
}

impl std::fmt::Debug for Lambda {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.src.fmt(f) }
}

impl serde::Serialize for Lambda {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> { self.src.serialize(s) }
}

impl<'de> serde::Deserialize<'de> for Lambda {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Lambda::new(String::deserialize(d)?, &corgi::Scope::default()))
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Reducer {
    Min,
    Distinct,
    /// The sum of a key's diffs, as an `Int`, when it is not zero. It may be negative, as DD's
    /// `count` is: a negative count is how a program finds a negative accumulation.
    Count,
    /// NEST: collect a key's values into a `Value::List`, in the values' own
    /// (`Value: Ord`) order — so deterministic, and position-ordered when the
    /// values are `tuple(pos, …)` as `flatmap` emits. The inverse of `FlatMap`.
    Collect,
    /// A key's output by a lambda over `(key, vals)`, `vals` the key's values with non-zero
    /// accumulated diffs, as a `List` of `(val, diff)` in the values' order. The lambda gives a
    /// `List` of `(val, diff)`: the key's output values and their diffs. A key with no such values
    /// has no output.
    Expr(Lambda),
}

/// An individual step within a Linear node.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum LinearOp {
    /// Rekey/reval: the lambda gives the new `(key, val)`.
    Project(Lambda),
    /// Keep the record when the lambda gives a nonzero `Int`.
    Filter(Lambda),
    /// Negate the diff.
    Negate,
    /// Shift the timestamp to the round an `Int`-valued lambda gives.
    EnterAt(Lambda),
    /// Append the current user-iter coord (at the row's scope depth) to
    /// the value. Time itself is unchanged. See `Expr::LiftIter` for the
    /// discipline restriction.
    LiftIter,
    /// UNNEST: explode the `List` a lambda gives into one row per element, value
    /// `(pos, element)`. See `parse::Expr::FlatMap`.
    FlatMap(Lambda),
    /// Pair each value with its whole time: `(key ; val)` becomes `(key ; (val, time))`.
    /// The time is a tuple of `Int`s, the epoch and then every scope coordinate, so its
    /// width is one more than the op's scope depth. Time itself is unchanged.
    Lift,
    /// Each row becomes the rows of a lambda giving a list of `((key, val), time, diff)`.
    /// The time is a tuple of `Int`s as `Lift` gives it; the output row's time is the
    /// join of the input's time with it, coordinate-wise max, and its diff is the
    /// product of the two diffs. A negative coordinate is below every time, so it
    /// leaves that coordinate unchanged. `delay(t)` is `[((key, val), t, 1)]`.
    JoinFunction(Lambda),
}

// ---------------------------------------------------------------------------
// Registered functions: how an embedding program extends its programs' lambdas.

/// A function an embedding program supplies to DDIR programs, called by name.
///
/// The contract is purity: the same arguments must always give the same
/// result, because differential dataflow re-evaluates a call when it retracts
/// what the call produced, and a retraction must cancel exactly. The declared
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

/// Every registered function.
pub fn registered() -> Vec<std::sync::Arc<Function>> {
    functions().read().unwrap().values().cloned().collect()
}

/// The registered function called `name`, if any.
pub fn lookup(name: &str) -> Option<std::sync::Arc<Function>> {
    functions().read().unwrap().get(name).cloned()
}
