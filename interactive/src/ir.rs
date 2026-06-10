//! Row and operator vocabulary shared by the IR and the renderers:
//! `LinearOp`, `RowLike`, field/condition evaluation, and the arity
//! transfer functions.
//! The program structure itself lives in `scope_ir`.


use crate::parse::{Projection, Condition, FieldExpr};

pub type Diff = i64;
pub type Id = usize;
pub type Time = timely::order::Product<u64, differential_dataflow::dynamic::pointstamp::PointStamp<u64>>;

/// Minimal interface for a row type used by renderers.
pub trait RowLike: Clone + Ord + std::fmt::Debug + Send + Sync + 'static {
    fn new() -> Self;
    fn push(&mut self, v: i64);
    fn as_slice(&self) -> &[i64];
    fn extend_from_slice(&mut self, other: &[i64]);
}

impl RowLike for Vec<i64> {
    fn new() -> Self { Vec::new() }
    fn push(&mut self, v: i64) { Vec::push(self, v); }
    fn as_slice(&self) -> &[i64] { self }
    fn extend_from_slice(&mut self, other: &[i64]) { Vec::extend_from_slice(self, other); }
}

impl<A> RowLike for smallvec::SmallVec<A>
where
    A: smallvec::Array<Item = i64> + Send + Sync + 'static,
{
    fn new() -> Self { smallvec::SmallVec::new() }
    fn push(&mut self, v: i64) { smallvec::SmallVec::push(self, v); }
    fn as_slice(&self) -> &[i64] { self }
    fn extend_from_slice(&mut self, other: &[i64]) { smallvec::SmallVec::extend_from_slice(self, other); }
}

/// An individual step within a Linear node.
#[derive(Debug, Clone)]
pub enum LinearOp {
    /// Rekey/reval: project to new (key, val).
    Project(Projection),
    /// Keep or discard the record.
    Filter(Condition),
    /// Negate the diff.
    Negate,
    /// Shift the timestamp based on a field value.
    EnterAt(FieldExpr),
    /// Append the current user-iter coord (at the row's scope depth) to
    /// the value as one i64 field. Time itself is unchanged. See
    /// `Expr::LiftIter` for the discipline restriction.
    LiftIter,
}
/// Evaluate fields into a row.
pub fn eval_fields<R: RowLike>(fields: &[FieldExpr], inputs: &[&[i64]]) -> R {
    let mut r = R::new();
    for f in fields { eval_field_into(f, inputs, &mut r); }
    r
}

pub fn eval_field_into<R: RowLike>(field: &FieldExpr, inputs: &[&[i64]], result: &mut R) {
    match field {
        FieldExpr::Pos(i) => result.extend_from_slice(inputs[*i]),
        FieldExpr::Index(row, idx) => result.push(inputs[*row][*idx]),
        FieldExpr::Const(v) => result.push(*v),
        FieldExpr::Neg(inner) => { let mut tmp = R::new(); eval_field_into(inner, inputs, &mut tmp); for v in tmp.as_slice().iter() { result.push(-v); } },
        FieldExpr::Sub(a, b) => {
            let mut la = R::new(); eval_field_into(a, inputs, &mut la);
            let mut lb = R::new(); eval_field_into(b, inputs, &mut lb);
            // Element-wise subtract; rows must have matching length.
            assert_eq!(la.as_slice().len(), lb.as_slice().len(),
                "FieldExpr::Sub: operands must produce same-length rows");
            for (x, y) in la.as_slice().iter().zip(lb.as_slice().iter()) { result.push(x - y); }
        }
    }
}

pub fn eval_condition(cond: &Condition, inputs: &[&[i64]]) -> bool {
    let cmp = |l, r, op: fn(&Vec<i64>, &Vec<i64>) -> bool| {
        let mut a = Vec::<i64>::new(); let mut b = Vec::<i64>::new();
        eval_field_raw(l, inputs, &mut a); eval_field_raw(r, inputs, &mut b);
        op(&a, &b)
    };
    match cond {
        Condition::And(l, r) => eval_condition(l, inputs) && eval_condition(r, inputs),
        Condition::Eq(l, r) => cmp(l, r, |a, b| a == b),
        Condition::Ne(l, r) => cmp(l, r, |a, b| a != b),
        Condition::Lt(l, r) => cmp(l, r, |a, b| a < b),
        Condition::Le(l, r) => cmp(l, r, |a, b| a <= b),
        Condition::Gt(l, r) => cmp(l, r, |a, b| a > b),
        Condition::Ge(l, r) => cmp(l, r, |a, b| a >= b),
    }
}

fn eval_field_raw(field: &FieldExpr, inputs: &[&[i64]], result: &mut Vec<i64>) {
    match field {
        FieldExpr::Pos(i) => result.extend_from_slice(inputs[*i]),
        FieldExpr::Index(row, idx) => result.push(inputs[*row][*idx]),
        FieldExpr::Const(v) => result.push(*v),
        FieldExpr::Neg(inner) => { let mut tmp = Vec::new(); eval_field_raw(inner, inputs, &mut tmp); for v in tmp.iter() { result.push(-v); } },
        FieldExpr::Sub(a, b) => {
            let mut la = Vec::new(); eval_field_raw(a, inputs, &mut la);
            let mut lb = Vec::new(); eval_field_raw(b, inputs, &mut lb);
            assert_eq!(la.len(), lb.len(), "FieldExpr::Sub: operands must produce same-length rows");
            for (x, y) in la.iter().zip(lb.iter()) { result.push(x - y); }
        }
    }
}

// Arity transfer functions: how ops and projections change a row's (k, v)
// shape. A projection field's width is the sum of its parts — `Pos(r)` is a
// whole-row reference of width = row r's arity, not one column (the miscount
// that once made SCC's explanation unsound).
pub(crate) fn apply_ops_arity((mut k, mut v): (usize, usize), ops: &[LinearOp]) -> (usize, usize) {
    for op in ops {
        match op {
            // Project's input rows are [key, val]; expand `Pos` refs to
            // their row arities rather than counting field-exprs.
            LinearOp::Project(p) => {
                let rows = [k, v];
                k = proj_arity(&p.key, &rows);
                v = proj_arity(&p.val, &rows);
            }
            LinearOp::Filter(_) | LinearOp::Negate | LinearOp::EnterAt(_) => {}
            LinearOp::LiftIter => { v += 1; }
        }
    }
    (k, v)
}

/// Width (output columns) a single `FieldExpr` expands to, given the
/// arities of the input rows it may reference. `Pos(r)` is a whole-row
/// reference of width `rows[r]`; index/const are single columns.
fn field_width(f: &FieldExpr, rows: &[usize]) -> usize {
    match f {
        FieldExpr::Pos(r) => rows.get(*r).copied().unwrap_or(0),
        FieldExpr::Index(_, _) | FieldExpr::Const(_) => 1,
        FieldExpr::Neg(inner) => field_width(inner, rows),
        FieldExpr::Sub(a, _) => field_width(a, rows),
    }
}

/// Total arity of one projection side (`key`/`val`): the sum of its
/// fields' widths.
pub(crate) fn proj_arity(fields: &[FieldExpr], rows: &[usize]) -> usize {
    fields.iter().map(|f| field_width(f, rows)).sum()
}
