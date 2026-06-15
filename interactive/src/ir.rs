//! Row vocabulary shared by the IR and the renderers: the `Value` data model,
//! the `LinearOp` operator steps, and the scalar `Term` interpreter (`eval`).
//! The program structure itself lives in `scope_ir`.

use crate::parse::{Projection, Term, UnOp, BinOp};

pub type Diff = i64;
pub type Id = usize;
pub type Time = timely::order::Product<u64, differential_dataflow::dynamic::pointstamp::PointStamp<u64>>;

/// A runtime value: the data model of the interpreter.
///
/// An algebraic data type over a single scalar (`Int`). `Tuple`/`Variant`/
/// `List` are the product/sum/sequence constructors; together they cover
/// JSON-shaped data and program ASTs. A collection element is a `(key, val)`
/// pair of `Value`s (typically `Tuple`s). The derived `Ord` gives `min`,
/// join-key matching, and `distinct` for free.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, serde::Serialize, serde::Deserialize)]
pub enum Value {
    Int(i64),
    Tuple(Vec<Value>),
    Variant(u32, Box<Value>),
    List(Vec<Value>),
}

impl Value {
    /// The empty tuple — the conventional "unit"/empty value.
    pub fn unit() -> Value { Value::Tuple(Vec::new()) }
    /// Truthiness: a nonzero `Int` is true; everything else is false.
    pub fn truthy(&self) -> bool { matches!(self, Value::Int(n) if *n != 0) }
    /// Unwrap an `Int`, panicking otherwise (interpreter is dynamically typed).
    pub fn as_int(&self) -> i64 { match self { Value::Int(n) => *n, other => panic!("expected Int, got {:?}", other) } }
}

/// An individual step within a Linear node.
#[derive(Debug, Clone)]
pub enum LinearOp {
    /// Rekey/reval: project to new (key, val).
    Project(Projection),
    /// Keep the record when the `Term` evaluates to a truthy `Value`.
    Filter(Term),
    /// Negate the diff.
    Negate,
    /// Shift the timestamp based on an `Int`-valued `Term`.
    EnterAt(Term),
    /// Append the current user-iter coord (at the row's scope depth) to
    /// the value. Time itself is unchanged. See `Expr::LiftIter` for the
    /// discipline restriction.
    LiftIter,
    /// UNNEST: explode a `List`-valued `Term` into one row per element, value
    /// `tuple(pos, element)`. See `parse::Expr::FlatMap`.
    FlatMap(Term),
}

/// Evaluate a scalar `Term` against an environment of `Value`s.
///
/// `env` holds the operator's input rows at the bottom (`Var(i)` indexes it
/// absolutely); `Case`/`Fold` push their binders on top, read back with
/// `Bound(k)` counting from the innermost. Binders are pushed and popped
/// around sub-evaluation, so `env` is restored on return.
pub fn eval(term: &Term, env: &mut Vec<Value>) -> Value {
    match term {
        Term::Var(i) => env[*i].clone(),
        Term::Bound(k) => env[env.len() - 1 - *k].clone(),
        Term::Int(n) => Value::Int(*n),
        Term::Tuple(fields) => Value::Tuple(build_seq(fields, env)),
        Term::List(fields) => Value::List(build_seq(fields, env)),
        Term::Spread(_) => panic!("Spread is only valid as a direct child of Tuple/List"),
        Term::Proj(t, i) => {
            // If the operand is a "place" (a Var/Bound/Proj chain), index into
            // it by reference and clone only the selected field — avoids deep-
            // cloning the whole environment slot just to discard most of it.
            if let Some(place) = eval_ref(t, env) {
                match place {
                    Value::Tuple(xs) | Value::List(xs) => {
                        assert!(*i < xs.len(), "Proj index {} out of bounds (len {})", i, xs.len());
                        xs[*i].clone()
                    }
                    other => panic!("Proj on non-aggregate value: {:?}", other),
                }
            } else {
                match eval(t, env) {
                    Value::Tuple(mut xs) | Value::List(mut xs) => {
                        assert!(*i < xs.len(), "Proj index {} out of bounds (len {})", i, xs.len());
                        xs.swap_remove(*i)
                    }
                    other => panic!("Proj on non-aggregate value: {:?}", other),
                }
            }
        }
        Term::Inject(tag, t) => Value::Variant(eval(tag, env).as_int() as u32, Box::new(eval(t, env))),
        Term::Case { scrutinee, arms, default } => {
            let Value::Variant(tag, payload) = eval(scrutinee, env) else {
                panic!("Case scrutinee is not a Variant")
            };
            match arms.get(tag as usize) {
                Some(arm) => {
                    env.push(*payload);
                    let r = eval(arm, env);
                    env.pop();
                    r
                }
                None => match default {
                    Some(d) => eval(d, env),
                    None => panic!("Case: no arm for tag {} and no default", tag),
                },
            }
        }
        Term::Fold { list, init, step } => {
            let Value::List(items) = eval(list, env) else { panic!("Fold on non-list value") };
            let mut acc = eval(init, env);
            for item in items {
                // step sees elem = Bound(0), acc = Bound(1).
                env.push(acc);
                env.push(item);
                acc = eval(step, env);
                env.pop();
                env.pop();
            }
            acc
        }
        Term::If { cond, then, els } => {
            if eval(cond, env).truthy() { eval(then, env) } else { eval(els, env) }
        }
        Term::Unary(op, t) => eval_unary(*op, eval(t, env)),
        Term::Binary(op, l, r) => {
            // Short-circuit the logical operators.
            match op {
                BinOp::And => return Value::Int((eval(l, env).truthy() && eval(r, env).truthy()) as i64),
                BinOp::Or => return Value::Int((eval(l, env).truthy() || eval(r, env).truthy()) as i64),
                _ => {}
            }
            eval_binary(*op, eval(l, env), eval(r, env))
        }
    }
}

/// Resolve a "place" term (a `Var`/`Bound`/`Proj` chain) to a borrowed
/// reference into `env`, without cloning. Returns `None` for terms that build
/// a fresh value (constructors, primitives) — those must be evaluated owned.
fn eval_ref<'a>(term: &Term, env: &'a [Value]) -> Option<&'a Value> {
    match term {
        Term::Var(i) => env.get(*i),
        Term::Bound(k) => env.get(env.len().checked_sub(1 + *k)?),
        Term::Proj(t, i) => match eval_ref(t, env)? {
            Value::Tuple(xs) | Value::List(xs) => xs.get(*i),
            _ => None,
        },
        _ => None,
    }
}

/// Build a tuple/list element sequence, splicing any `Spread` children.
fn build_seq(fields: &[Term], env: &mut Vec<Value>) -> Vec<Value> {
    let mut out = Vec::with_capacity(fields.len());
    for f in fields {
        if let Term::Spread(inner) = f {
            match eval(inner, env) {
                Value::Tuple(xs) | Value::List(xs) => out.extend(xs),
                other => panic!("Spread of non-aggregate value: {:?}", other),
            }
        } else {
            out.push(eval(f, env));
        }
    }
    out
}

fn eval_unary(op: UnOp, v: Value) -> Value {
    match op {
        UnOp::Neg => Value::Int(-v.as_int()),
        UnOp::Not => Value::Int((!v.truthy()) as i64),
        UnOp::IsTag(t) => Value::Int(matches!(&v, Value::Variant(tag, _) if *tag == t) as i64),
        UnOp::Len => match v {
            Value::Tuple(xs) | Value::List(xs) => Value::Int(xs.len() as i64),
            other => panic!("Len on non-aggregate value: {:?}", other),
        },
    }
}

fn eval_binary(op: BinOp, l: Value, r: Value) -> Value {
    let b = |x: bool| Value::Int(x as i64);
    match op {
        BinOp::Add => Value::Int(l.as_int() + r.as_int()),
        BinOp::Sub => Value::Int(l.as_int() - r.as_int()),
        BinOp::Mul => Value::Int(l.as_int() * r.as_int()),
        // Comparisons are structural, using the derived `Ord`/`Eq` on `Value`.
        BinOp::Eq => b(l == r),
        BinOp::Ne => b(l != r),
        BinOp::Lt => b(l < r),
        BinOp::Le => b(l <= r),
        BinOp::Gt => b(l > r),
        BinOp::Ge => b(l >= r),
        BinOp::And | BinOp::Or => unreachable!("logical ops short-circuit in eval"),
    }
}
