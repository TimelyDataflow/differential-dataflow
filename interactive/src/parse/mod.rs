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
}

#[derive(Debug, Clone)]
pub enum Condition { Eq(FieldExpr, FieldExpr), Ne(FieldExpr, FieldExpr), Lt(FieldExpr, FieldExpr), Le(FieldExpr, FieldExpr), Gt(FieldExpr, FieldExpr), Ge(FieldExpr, FieldExpr) }

#[derive(Debug, Clone)]
pub struct Projection { pub key: Vec<FieldExpr>, pub val: Vec<FieldExpr> }

#[derive(Debug, Clone)]
pub enum Reducer { Min, Distinct, Count }

#[derive(Debug, Clone)]
pub enum Expr {
    Input(usize),
    Name(String),
    Qualified(String, String),
    Map(Box<Expr>, Projection),
    Join(Box<Expr>, Box<Expr>, Projection),
    Reduce(Box<Expr>, Reducer),
    Filter(Box<Expr>, Condition),
    Negate(Box<Expr>),
    EnterAt(Box<Expr>, FieldExpr),
    Inspect(Box<Expr>, String),
    Concat(Vec<Expr>),
    Arrange(Box<Expr>),
}

#[derive(Debug)]
pub enum Stmt {
    Let(String, Expr),
    Var(String, Expr),
    Scope(String, Vec<Stmt>),
    Result(Expr),
}
