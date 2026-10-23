//! Vocabulary shared by the IR and the renderers: the operators' slots (corgi [`Lambda`]s), the
//! `LinearOp` operator steps, and the registered functions. The program structure lives in
//! `scope_ir`.

pub type Diff = i64;
pub type Id = usize;
pub type Time = timely::order::Product<u64, differential_dataflow::dynamic::pointstamp::PointStamp<u64>>;

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
    /// Each `(key, val)` whose accumulated diff is not zero, once: a collection read as a set of
    /// records. A key's values are kept. When they aren't `()`, rendering keys the reduction by the
    /// whole record (see `backend::render_scope`), so a change at a key with many values touches
    /// only its own records rather than all of the key's values.
    Distinct,
    /// The sum of a key's diffs, as an `Int`, when it is not zero. It may be negative, as DD's
    /// `count` is: a negative count is how a program finds a negative accumulation.
    Count,
    /// NEST: collect a key's values into a `List`, in corgi's order of values — so
    /// deterministic, and position-ordered when the values are `(pos, …)` as `flatmap`
    /// emits. The inverse of `FlatMap`.
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
    /// Pair each value with its whole time: `(key, val)` becomes `(key, (val, time))`.
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
    assert!(!crate::parse::is_builtin(f.name()), "`{}` is a builtin or keyword and cannot be registered", f.name());
    assert!(matches!(f.input(), corgi::Shape::Unit | corgi::Shape::Prod(_)), "`{}`: a function's input is `()` or the tuple of its arguments", f.name());
    functions().write().unwrap().insert(f.name().to_string(), f);
}

/// Every registered function.
pub fn registered() -> Vec<Function> {
    functions().read().unwrap().values().cloned().collect()
}

/// The registered function called `name`, if any.
pub fn lookup(name: &str) -> Option<Function> {
    functions().read().unwrap().get(name).cloned()
}
