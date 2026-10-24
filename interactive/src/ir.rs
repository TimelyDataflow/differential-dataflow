//! Vocabulary shared by the IR and the renderers: the operators' slots ([`Lambda`]s, from
//! `crate::lambda`), the `LinearOp` operator steps, the reducers, and the program syntax's
//! keywords. The program structure lives in `scope_ir`.

pub type Diff = i64;
pub(crate) type Time = timely::order::Product<u64, differential_dataflow::dynamic::pointstamp::PointStamp<u64>>;

/// A slot of the IR: a lambda of the expression language (see `crate::lambda`).
pub use crate::lambda::Lambda;

/// The program syntax's keywords. The lexer claims them before a lambda's text is read, so a
/// registered function may not take one as its name (`crate::lambda::register`). The lexer's own
/// table (`parse::pipe`) must agree, which its tests check.
pub(crate) const KEYWORDS: &[&str] = &[
    "let", "var", "export", "type", "input", "import", "map", "join", "min", "distinct", "count",
    "collect", "flatmap", "arrange", "negate", "filter", "enter_at", "inspect", "lift_iter",
];

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
