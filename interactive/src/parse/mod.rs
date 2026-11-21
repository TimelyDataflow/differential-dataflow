//! Concrete syntax for DD IR programs.
//!
//! One front-end: `pipe::parse()` — pipe-oriented syntax (`expr | op | op`)
//! for `.ddp` files.
//!
//! This module is syntax only. The *collection* language (`Expr`/`Stmt`)
//! defined here describes the dataflow graph; its slots are corgi lambdas
//! ([`Lambda`]).

pub mod pipe;

use crate::ir::Lambda;

/// Surface reduction names. Lowering expands these into reduction expressions.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Reducer {
    Min,
    /// Records with nonzero accumulated diffs, regardless of sign.
    Distinct,
    /// The sum of diffs, emitted only when nonzero.
    Count,
    /// Ordered values, each repeated by its positive diff.
    Collect,
    /// A lambda over each key and its values, handed only the first `limit` of them when given.
    Expr(Lambda, Option<usize>),
}

/// An input, by its label: a number or a name.
/// The label is the input's identity, by which a client feeds or binds it; the order inputs are
/// written in never matters.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum InputRef {
    /// `input N`, at position `N`.
    Numbered(usize),
    /// `input "name"`, at a position after every numbered input's, in the order of names.
    Named(usize, String),
}

impl InputRef {
    pub fn position(&self) -> usize { match self { InputRef::Numbered(n) | InputRef::Named(n, _) => *n } }
    /// The label, as a client writes it: the number's digits, or the name.
    pub fn label(&self) -> String { match self { InputRef::Numbered(n) => n.to_string(), InputRef::Named(_, name) => name.clone() } }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Expr {
    Input(InputRef),
    /// Shape ascription on an input: `input N : (key_shape, val_shape)`.
    TypedSource(Box<Expr>, corgi::Shape, corgi::Shape),
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
    /// Replace each row with the lambda's list of complete `(key, val)` rows.
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
    /// `output "name" = expr;` — registers a named output in the program, with
    /// an optional declared shape as for `var`. Only valid at the root scope.
    Output(String, Option<crate::scope_ir::RowShape>, Expr),
}
