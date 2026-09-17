//! Concrete syntax for DD IR programs.
//!
//! One front-end: `pipe::parse()` — pipe-oriented syntax (`expr | op | op`)
//! for `.ddp` files.
//!
//! This module is syntax only. The *collection* language (`Expr`/`Stmt`)
//! defined here describes the dataflow graph; the *scalar* language it
//! embeds — [`Term`] and its operators — is the IR's own vocabulary, defined
//! in [`crate::ir`].

pub mod pipe;

use crate::ir::{BinOp, Projection, Reducer, SumTy, Term, UnOp};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Expr {
    Input(usize),
    /// Shape ascription on an external source: `input N : (key_shape ; val_shape)`.
    /// The same syntax follows `import "name"` for independently installed consumers.
    TypedSource(Box<Expr>, corgi::Shape, corgi::Shape),
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
/// Validates arity and pulls out int-literal tags/indices. `args` is
/// consumed in place.
pub(crate) fn build_builtin(name: &str, args: &mut Vec<Term>) -> Term {
    let int_arg = |t: &Term| -> i64 {
        match t { Term::Int(n) => *n, o => panic!("builtin `{}` expects an int literal here, got {:?}", name, o) }
    };
    match name {
        "tuple" => Term::Tuple(std::mem::take(args)),
        "list" => Term::List(std::mem::take(args)),
        "inject" | "variant" => { assert_eq!(args.len(), 2, "{}(tag, payload)", name); let payload = Box::new(args.remove(1)); Term::Inject { tag: Box::new(args.remove(0)), payload, sum: SumTy::Dynamic } }
        "case" => { assert!(args.len() >= 2, "case(scrutinee, arm0, ...)"); let scrutinee = Box::new(args.remove(0)); Term::Case { scrutinee, arms: std::mem::take(args), default: None } }
        "fold" => { assert_eq!(args.len(), 3, "fold(list, init, step)"); let step = Box::new(args.remove(2)); let init = Box::new(args.remove(1)); let list = Box::new(args.remove(0)); Term::Fold { list, init, step } }
        "proj" => { assert_eq!(args.len(), 2, "proj(value, index)"); let i = int_arg(&args[1]) as usize; Term::Proj(Box::new(args.remove(0)), i) }
        "len" => { assert_eq!(args.len(), 1, "len(value)"); Term::Unary(UnOp::Len, Box::new(args.remove(0))) }
        "istag" => { assert_eq!(args.len(), 2, "istag(tag, value)"); let tag = int_arg(&args[0]) as u32; Term::Unary(UnOp::IsTag(tag), Box::new(args.remove(1))) }
        "not" => { assert_eq!(args.len(), 1, "not(value)"); Term::Unary(UnOp::Not, Box::new(args.remove(0))) }
        "float" | "fneg" => {
            assert_eq!(args.len(), 1, "{name}(value)");
            Term::Unary(if name == "float" { UnOp::ToF64 } else { UnOp::F64Neg }, Box::new(args.remove(0)))
        }
        "fadd" | "fsub" | "fmul" | "fdiv" => {
            assert_eq!(args.len(), 2, "{name}(a, b)");
            let b = Box::new(args.remove(1)); let a = Box::new(args.remove(0));
            Term::Binary(match name { "fadd" => BinOp::F64Add, "fsub" => BinOp::F64Sub, "fmul" => BinOp::F64Mul, _ => BinOp::F64Div }, a, b)
        }
        "or" => { assert_eq!(args.len(), 2, "or(a, b)"); let b = Box::new(args.remove(1)); let a = Box::new(args.remove(0)); Term::Binary(BinOp::Or, a, b) }
        "idiv" | "append" => {
            assert_eq!(args.len(), 2, "{name}(a, b)");
            let b = Box::new(args.remove(1)); let a = Box::new(args.remove(0));
            Term::Binary(if name == "idiv" { BinOp::Div } else { BinOp::Append }, a, b)
        }
        "if" => { assert_eq!(args.len(), 3, "if(cond, then, els)"); let els = Box::new(args.remove(2)); let then = Box::new(args.remove(1)); let cond = Box::new(args.remove(0)); Term::If { cond, then, els } }
        "hash" => { assert!(args.len() >= 2, "hash(bound, key, ...)"); Term::Hash(std::mem::take(args)) }
        other => panic!("Unknown scalar builtin: {}", other),
    }
}
