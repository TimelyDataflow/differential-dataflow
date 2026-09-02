//! What the optimizer needs to know about scalar operators, and nothing more.
//!
//! The e-graph never looks inside a term: a projection, a predicate, a reducer is an opaque
//! payload. What it needs from the scalar language is a handful of *facts about composition*,
//! answered here for DDIR's `Term`/`Projection`/`Reducer` vocabulary:
//!
//!   * **widths** — how many fields a row has after an operator, given how many it had before,
//!     so a cost can price a row by its width and a narrowing can be recognized;
//!   * **demand** — which fields of a row an operator reads (`count` and `distinct` read no
//!     values; a projection reads the fields its terms index), so what it does not read can be
//!     projected away before it;
//!   * **narrowing** — the operator that keeps only what a consumer demands, and the consumer
//!     rewritten to read the narrowed row;
//!   * **renaming** — an operator with two inputs, with its inputs swapped.
//!
//! `None` for a width means the language cannot tell statically (a `Spread` of something whose
//! arity is not fixed); the optimizer then neither prices nor narrows that row. The intended
//! growth of this interface is the rest of the list — fuse two operators, extract common
//! subexpressions between two terms, report the equalities an operator implies — each as a
//! question about how scalar parts compose, not about any one language's syntax.

use std::collections::{BTreeMap, BTreeSet};

use crate::ir::LinearOp;
use crate::parse::{Projection, Reducer, Term};

/// A row's width: (key fields, value fields). DDIR unit is width 0.
pub type Width = (usize, usize);

/// The number of fields a projection term produces over rows of the given widths (`env[n]` is
/// the width of `$n`'s row), or `None` when a `Spread` reaches something of unknown arity.
pub fn term_width(t: &Term, env: &[usize]) -> Option<usize> {
    match t {
        Term::Tuple(fields) => {
            let mut w = 0;
            for f in fields {
                w += match f {
                    Term::Spread(inner) => match &**inner {
                        Term::Var(n) => *env.get(*n)?,
                        _ => return None,
                    },
                    _ => 1,
                };
            }
            Some(w)
        }
        Term::Var(n) => env.get(*n).copied(),
        _ => Some(1),
    }
}

/// Which fields of a row a term reads: a set of field indices when every read is a `$n[i]`,
/// `All` when the row is used whole (`$n`, `..$n`) or through anything but a direct index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Demand {
    All,
    Fields(BTreeSet<usize>),
}

impl Demand {
    fn union(self, other: Demand) -> Demand {
        match (self, other) {
            (Demand::Fields(mut a), Demand::Fields(b)) => {
                a.extend(b);
                Demand::Fields(a)
            }
            _ => Demand::All,
        }
    }
}

/// The fields of row `$var` that `t` reads.
pub fn demand(t: &Term, var: usize) -> Demand {
    let mut fields = BTreeSet::new();
    let mut all = false;
    walk(t, var, &mut fields, &mut all);
    if all { Demand::All } else { Demand::Fields(fields) }
}

/// The fields of row `$var` a projection reads.
pub fn projection_demand(p: &Projection, var: usize) -> Demand {
    demand(&p.key, var).union(demand(&p.val, var))
}

fn walk(t: &Term, var: usize, fields: &mut BTreeSet<usize>, all: &mut bool) {
    match t {
        Term::Var(n) => {
            if *n == var {
                *all = true;
            }
        }
        Term::Proj(inner, i) => match &**inner {
            Term::Var(n) if *n == var => {
                fields.insert(*i);
            }
            other => walk(other, var, fields, all),
        },
        Term::Bound(_) | Term::Int(_) => {}
        Term::Spread(inner) | Term::Unary(_, inner) => walk(inner, var, fields, all),
        Term::Tuple(xs) | Term::List(xs) | Term::Hash(xs) => xs.iter().for_each(|x| walk(x, var, fields, all)),
        Term::Inject { tag, payload, .. } => {
            walk(tag, var, fields, all);
            walk(payload, var, fields, all);
        }
        Term::Case { scrutinee, arms, default } => {
            walk(scrutinee, var, fields, all);
            arms.iter().for_each(|x| walk(x, var, fields, all));
            if let Some(d) = default {
                walk(d, var, fields, all);
            }
        }
        Term::Fold { list, init, step } => {
            walk(list, var, fields, all);
            walk(init, var, fields, all);
            walk(step, var, fields, all);
        }
        Term::If { cond, then, els } => {
            walk(cond, var, fields, all);
            walk(then, var, fields, all);
            walk(els, var, fields, all);
        }
        Term::Binary(_, a, b) => {
            walk(a, var, fields, all);
            walk(b, var, fields, all);
        }
    }
}

/// `t` with every `$var[i]` read through `map` (`$var[i]` becomes `$var[map[i]]`) and every
/// row `$n` renamed through `rows` (`$n` becomes `$rows[n]`, identity when absent). The two
/// halves of narrowing and of renaming an operator's inputs.
pub fn rewrite(t: &Term, var: usize, map: &BTreeMap<usize, usize>, rows: &BTreeMap<usize, usize>) -> Term {
    let r = |x: &Term| rewrite(x, var, map, rows);
    let rename = |n: usize| rows.get(&n).copied().unwrap_or(n);
    match t {
        Term::Var(n) => Term::Var(rename(*n)),
        Term::Proj(inner, i) => match &**inner {
            Term::Var(n) if *n == var => Term::Proj(Box::new(Term::Var(rename(*n))), *map.get(i).unwrap_or(i)),
            other => Term::Proj(Box::new(r(other)), *i),
        },
        Term::Bound(_) | Term::Int(_) => t.clone(),
        Term::Spread(inner) => Term::Spread(Box::new(r(inner))),
        Term::Unary(op, inner) => Term::Unary(*op, Box::new(r(inner))),
        Term::Tuple(xs) => Term::Tuple(xs.iter().map(r).collect()),
        Term::List(xs) => Term::List(xs.iter().map(r).collect()),
        Term::Hash(xs) => Term::Hash(xs.iter().map(r).collect()),
        Term::Inject { tag, payload, sum } => Term::Inject { tag: Box::new(r(tag)), payload: Box::new(r(payload)), sum: sum.clone() },
        Term::Case { scrutinee, arms, default } => Term::Case {
            scrutinee: Box::new(r(scrutinee)),
            arms: arms.iter().map(r).collect(),
            default: default.as_ref().map(|d| Box::new(r(d))),
        },
        Term::Fold { list, init, step } => Term::Fold { list: Box::new(r(list)), init: Box::new(r(init)), step: Box::new(r(step)) },
        Term::If { cond, then, els } => Term::If { cond: Box::new(r(cond)), then: Box::new(r(then)), els: Box::new(r(els)) },
        Term::Binary(op, a, b) => Term::Binary(*op, Box::new(r(a)), Box::new(r(b))),
    }
}

/// The step that keeps a row's key and only the value fields in `fields` (in order), and the map
/// from old to new value field indices a consumer must read through.
pub fn keep_fields(fields: &BTreeSet<usize>) -> (LinearOp, BTreeMap<usize, usize>) {
    let map: BTreeMap<usize, usize> = fields.iter().enumerate().map(|(new, &old)| (old, new)).collect();
    let val = Term::Tuple(fields.iter().map(|&i| Term::Proj(Box::new(Term::Var(1)), i)).collect());
    (LinearOp::Project(Projection { key: Term::Tuple(vec![Term::Spread(Box::new(Term::Var(0)))]), val }), map)
}

/// A join projection with its value rows narrowed: `$var` read through `map`.
pub fn narrow_join(p: &Projection, var: usize, map: &BTreeMap<usize, usize>) -> Projection {
    let none = BTreeMap::new();
    Projection { key: rewrite(&p.key, var, map, &none), val: rewrite(&p.val, var, map, &none) }
}

/// A join projection for the same join with its inputs swapped: `$1` and `$2` exchanged.
pub fn swap_join(p: &Projection) -> Projection {
    let rows: BTreeMap<usize, usize> = [(1, 2), (2, 1)].into_iter().collect();
    let none = BTreeMap::new();
    Projection { key: rewrite(&p.key, usize::MAX, &none, &rows), val: rewrite(&p.val, usize::MAX, &none, &rows) }
}

/// The row width after a linear step over rows of width `w`.
pub fn width_after(op: &LinearOp, w: Width) -> Option<Width> {
    match op {
        LinearOp::Project(p) => {
            let env = [w.0, w.1];
            Some((term_width(&p.key, &env)?, term_width(&p.val, &env)?))
        }
        LinearOp::Filter(_) | LinearOp::Negate | LinearOp::EnterAt(_) => Some(w),
        LinearOp::LiftIter => Some((w.0, w.1 + 1)),
        LinearOp::FlatMap(_) => Some((w.0, 2)),
    }
}

/// The row width after a reducer over rows of width `w`.
pub fn reducer_width(r: &Reducer, w: Width) -> Width {
    match r {
        Reducer::Count => (w.0, 1),
        Reducer::Distinct => (w.0, 0),
        Reducer::Min => w,
        Reducer::Collect => (w.0, 1),
    }
}

/// The row width after a join projection over `(key, left value, right value)` rows.
pub fn join_width(p: &Projection, wk: usize, w0: usize, w1: usize) -> Option<Width> {
    let env = [wk, w0, w1];
    Some((term_width(&p.key, &env)?, term_width(&p.val, &env)?))
}

/// Does the reducer read its input's values? `count` and `distinct` see only keys.
pub fn reducer_reads_values(r: &Reducer) -> bool {
    !matches!(r, Reducer::Count | Reducer::Distinct)
}

/// The step that keeps a row's key and drops its value: `map($0 ;)`.
pub fn keep_key_only() -> LinearOp {
    LinearOp::Project(Projection { key: Term::Tuple(vec![Term::Spread(Box::new(Term::Var(0)))]), val: Term::Tuple(vec![]) })
}
