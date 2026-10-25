//! Concrete syntax for DD IR programs.
//!
//! One front-end: `pipe::parse()` — pipe-oriented syntax (`expr | op | op`)
//! for `.ddp` files.
//!
//! This module is syntax only. The *collection* language (`Expr`/`Stmt`)
//! defined here describes the dataflow graph; its slots are corgi lambdas
//! ([`Lambda`]).

pub mod pipe;

use crate::ir::{Lambda, Reducer};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Expr {
    Input(usize),
    /// Shape ascription on an external source: `input N : (key_shape, val_shape)`.
    /// The same syntax follows `import "name"` for independently installed consumers.
    TypedSource(Box<Expr>, corgi::Shape, corgi::Shape),
    /// Named external trace resolved at install time. Carries only the name;
    /// shape comes from the registry the program is installed against.
    Import(String),
    Name(String),
    /// A scope path. The bool marks a leading `::`.
    Path(Vec<String>, bool),
    /// A path checked against the scope tree before lowering.
    ResolvedPath { anchor: Vec<String>, target: Vec<String>, field: String },
    Map(Box<Expr>, Lambda),
    Join(Box<Expr>, Box<Expr>, Lambda),
    /// `stream | half_join(arrangement, strict, projection)`: see `scope_ir::Node::HalfJoin`.
    HalfJoin(Box<Expr>, Box<Expr>, bool, Lambda),
    Reduce(Box<Expr>, Reducer),
    Filter(Box<Expr>, Lambda),
    Negate(Box<Expr>),
    EnterAt(Box<Expr>, Lambda),
    /// Append the current user-iter coord (at the operator's scope depth)
    /// to each row's value. Time itself is unchanged.
    ///
    /// Discipline, which nothing checks: the result of `LiftIter` should not be referenced inside the same
    /// scope it appears in — only from an enclosing scope, after the
    /// implicit leave. This preserves the "loop body is a time-invariant
    /// function" property; in-scope use risks defeating the fixpoint.
    LiftIter(Box<Expr>),
    /// UNNEST: explode the `List` a lambda gives into one row per element, keyed
    /// as the input, with value `(pos, element)` (position innermost-first
    /// so `Collect` can restore order). The cross-join of a row with its list.
    FlatMap(Box<Expr>, Lambda),
    /// Pair each row's value with its whole time. See `LinearOp::Lift`.
    Lift(Box<Expr>),
    /// Replace each row by a list of `((key, val), time, diff)`. See `LinearOp::JoinFunction`.
    JoinFunction(Box<Expr>, Lambda),
    Inspect(Box<Expr>, String),
    Concat(Vec<Expr>),
    Arrange(Box<Expr>),
}

#[derive(Debug)]
pub enum Stmt {
    Let(String, Expr),
    /// Optional feedback declaration: `var name;` or `var name : (k, v);`.
    Var(String, Option<crate::scope_ir::RowShape>),
    /// Feedback binding: `set name = expr;`.
    Set(String, Expr),
    Scope(String, Vec<Stmt>),
    /// `export "name" = expr;` — registers a named output in the program, with
    /// an optional declared shape as for `var`. Only valid at the root scope.
    Export(String, Option<crate::scope_ir::RowShape>, Expr),
}
