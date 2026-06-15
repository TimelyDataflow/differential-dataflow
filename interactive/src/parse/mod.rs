//! Concrete syntax for DD IR programs.
//!
//! Two front-ends that produce the same AST:
//! - `applicative::parse()` — S-expression-like syntax (MAP, JOIN, etc.) for .ddir files
//! - `pipe::parse()` — pipe-oriented syntax (`expr | op | op`) for .ddp files
//!
//! The *collection* language (`Expr`/`Stmt`) describes the dataflow graph;
//! the *scalar* language (`Term`) describes per-row value computation over
//! [`crate::ir::Value`]. Only the scalar language knows about ADTs.

pub mod applicative;
pub mod pipe;

/// Scalar expression over [`crate::ir::Value`].
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
#[derive(Debug, Clone)]
pub enum Term {
    /// Input row at absolute environment index.
    Var(usize),
    /// `case`/`fold` binder, counting from the innermost (`0`).
    Bound(usize),
    /// Integer literal.
    Int(i64),
    /// Product intro. A `Spread` child splices its tuple's fields in place.
    Tuple(Vec<Term>),
    /// List intro. A `Spread` child splices in place.
    List(Vec<Term>),
    /// Splice marker; only meaningful as a direct child of `Tuple`/`List`.
    /// Lets a whole input row (`$n`) contribute all its fields, preserving
    /// the flat-row concatenation the original `[i64]` model relied on.
    Spread(Box<Term>),
    /// Product/list elimination: index into a `Tuple` or `List`.
    Proj(Box<Term>, usize),
    /// Sum intro: tag a payload. The tag is a `Term` (evaluated to an `Int`),
    /// so the constructor can be data-driven — essential for ASTs/JSON whose
    /// node kind comes from the data, not a literal.
    Inject(Box<Term>, Box<Term>),
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
}

#[derive(Debug, Clone, Copy)]
pub enum UnOp {
    /// Integer negation.
    Neg,
    /// Logical negation (truthy -> 0, else 1).
    Not,
    /// `1` if the operand is a `Variant` with the given tag, else `0`.
    IsTag(u32),
    /// Number of elements in a `Tuple` or `List`, as an `Int`.
    Len,
}

#[derive(Debug, Clone, Copy)]
pub enum BinOp {
    Add, Sub, Mul,
    Eq, Ne, Lt, Le, Gt, Ge,
    And, Or,
}

/// A `(key, val)` reshaping: each component evaluates to one `Value`.
#[derive(Debug, Clone)]
pub struct Projection { pub key: Term, pub val: Term }

#[derive(Debug, Clone)]
pub enum Reducer {
    Min,
    Distinct,
    Count,
    /// NEST: collect a key's values into a `Value::List`, in the values' own
    /// (`Value: Ord`) order — so deterministic, and position-ordered when the
    /// values are `tuple(pos, …)` as `flatmap` emits. The inverse of `FlatMap`.
    Collect,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Input(usize),
    /// Named external trace resolved at install time. Carries only the name;
    /// shape comes from the registry the program is installed against.
    Import(String),
    Name(String),
    Qualified(String, String),
    Map(Box<Expr>, Projection),
    Join(Box<Expr>, Box<Expr>, Projection),
    Reduce(Box<Expr>, Reducer),
    Filter(Box<Expr>, Term),
    Negate(Box<Expr>),
    EnterAt(Box<Expr>, Term),
    /// Append the current user-iter coord (at the operator's scope depth)
    /// to each row's value. Time itself is unchanged.
    ///
    /// Discipline (post-lowering check, see `lower::validate_lift_iter`):
    /// the result of `LiftIter` must not be referenced inside the same
    /// scope it appears in — only from an enclosing scope, after the
    /// implicit leave. This preserves the "loop body is a time-invariant
    /// function" property; in-scope use risks defeating the fixpoint.
    LiftIter(Box<Expr>),
    /// UNNEST: explode a `List`-valued `Term` into one row per element, keyed
    /// as the input, with value `tuple(pos, element)` (position innermost-first
    /// so `Collect` can restore order). The cross-join of a row with its list.
    FlatMap(Box<Expr>, Term),
    Inspect(Box<Expr>, String),
    Concat(Vec<Expr>),
    Arrange(Box<Expr>),
}

#[derive(Debug)]
pub enum Stmt {
    Let(String, Expr),
    Var(String, Expr),
    Scope(String, Vec<Stmt>),
    /// `export "name" = expr;` — registers a named output in the program.
    /// Only valid at the root scope.
    Export(String, Expr),
}

/// Build a scalar builtin call from its (already-parsed) argument terms.
/// Shared by both front-ends; validates arity and pulls out int-literal
/// tags/indices. `args` is consumed in place.
pub(crate) fn build_builtin(name: &str, args: &mut Vec<Term>) -> Term {
    let int_arg = |t: &Term| -> i64 {
        match t { Term::Int(n) => *n, o => panic!("builtin `{}` expects an int literal here, got {:?}", name, o) }
    };
    match name {
        "tuple" => Term::Tuple(std::mem::take(args)),
        "list" => Term::List(std::mem::take(args)),
        "inject" | "variant" => { assert_eq!(args.len(), 2, "{}(tag, payload)", name); let payload = Box::new(args.remove(1)); Term::Inject(Box::new(args.remove(0)), payload) }
        "case" => { assert!(args.len() >= 2, "case(scrutinee, arm0, ...)"); let scrutinee = Box::new(args.remove(0)); Term::Case { scrutinee, arms: std::mem::take(args), default: None } }
        "fold" => { assert_eq!(args.len(), 3, "fold(list, init, step)"); let step = Box::new(args.remove(2)); let init = Box::new(args.remove(1)); let list = Box::new(args.remove(0)); Term::Fold { list, init, step } }
        "proj" => { assert_eq!(args.len(), 2, "proj(value, index)"); let i = int_arg(&args[1]) as usize; Term::Proj(Box::new(args.remove(0)), i) }
        "len" => { assert_eq!(args.len(), 1, "len(value)"); Term::Unary(UnOp::Len, Box::new(args.remove(0))) }
        "istag" => { assert_eq!(args.len(), 2, "istag(tag, value)"); let tag = int_arg(&args[0]) as u32; Term::Unary(UnOp::IsTag(tag), Box::new(args.remove(1))) }
        "not" => { assert_eq!(args.len(), 1, "not(value)"); Term::Unary(UnOp::Not, Box::new(args.remove(0))) }
        "or" => { assert_eq!(args.len(), 2, "or(a, b)"); let b = Box::new(args.remove(1)); let a = Box::new(args.remove(0)); Term::Binary(BinOp::Or, a, b) }
        "if" => { assert_eq!(args.len(), 3, "if(cond, then, els)"); let els = Box::new(args.remove(2)); let then = Box::new(args.remove(1)); let cond = Box::new(args.remove(0)); Term::If { cond, then, els } }
        other => panic!("Unknown scalar builtin: {}", other),
    }
}
