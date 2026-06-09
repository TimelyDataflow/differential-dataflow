//! Concrete syntax for DD IR programs.
//!
//! Two front-ends that produce the same AST:
//! - `applicative::parse()` — S-expression-like syntax (MAP, JOIN, etc.) for .ddir files
//! - `pipe::parse()` — pipe-oriented syntax (`expr | op | op`) for .ddp files

pub mod applicative;
pub mod pipe;

#[derive(Debug, Clone)]
pub enum FieldExpr {
    Pos(usize),
    Index(usize, usize),
    Const(i64),
    Neg(Box<FieldExpr>),
    /// Element-wise subtraction. IR-level only (no surface syntax).
    /// Used by the explanation rewrite for the user-iter decrement.
    Sub(Box<FieldExpr>, Box<FieldExpr>),
}

#[derive(Debug, Clone)]
pub enum Condition {
    Eq(FieldExpr, FieldExpr), Ne(FieldExpr, FieldExpr),
    Lt(FieldExpr, FieldExpr), Le(FieldExpr, FieldExpr),
    Gt(FieldExpr, FieldExpr), Ge(FieldExpr, FieldExpr),
    /// Conjunction. Used to express partial-order time comparisons across
    /// multiple coords (`t0 <= s0 && t1 <= s1 && ...`).
    And(Box<Condition>, Box<Condition>),
}

#[derive(Debug, Clone)]
pub struct Projection { pub key: Vec<FieldExpr>, pub val: Vec<FieldExpr> }

#[derive(Debug, Clone)]
pub enum Reducer { Min, Distinct, Count }

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
    Filter(Box<Expr>, Condition),
    Negate(Box<Expr>),
    EnterAt(Box<Expr>, FieldExpr),
    /// Append the current user-iter coord (at the operator's scope depth)
    /// to each row's value as one additional i64 field. Time itself is
    /// unchanged.
    ///
    /// Discipline (post-lowering check, see `lower::validate_lift_iter`):
    /// the result of `LiftIter` must not be referenced inside the same
    /// scope it appears in — only from an enclosing scope, after the
    /// implicit leave. This preserves the "loop body is a time-invariant
    /// function" property; in-scope use risks defeating the fixpoint.
    LiftIter(Box<Expr>),
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
