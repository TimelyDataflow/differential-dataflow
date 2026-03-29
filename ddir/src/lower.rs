//! Lowering from AST to IR, and scalar evaluation helpers.

use std::collections::HashMap;
use std::sync::Arc;
use smallvec::smallvec as svec;

use crate::parse::*;
use crate::ir::*;

struct Lowering<R: RowLike> {
    nodes: Vec<Node<R>>,
    scopes: Vec<HashMap<String, Id>>,
    named_scopes: HashMap<String, (usize, HashMap<String, Id>)>,
    level: usize,
}

impl<R: RowLike> Lowering<R> {
    fn new() -> Self { Lowering { nodes: Vec::new(), scopes: vec![HashMap::new()], named_scopes: HashMap::new(), level: 0 } }
    fn push(&mut self, node: Node<R>) -> Id { let id = self.nodes.len(); self.nodes.push(node); id }
    fn resolve_name(&self, name: &str) -> Id { for scope in self.scopes.iter().rev() { if let Some(&id) = scope.get(name) { return id; } } panic!("Unresolved name: {}", name); }
    fn bind_name(&mut self, name: String, id: Id) { self.scopes.last_mut().unwrap().insert(name, id); }

    fn lower_program(mut self, stmts: Vec<Stmt>) -> Program<R> {
        let mut result_id = None;
        self.lower_stmts(stmts, &mut result_id);
        Program { result: result_id.expect("No result statement"), nodes: self.nodes }
    }

    fn lower_stmts(&mut self, stmts: Vec<Stmt>, result_id: &mut Option<Id>) {
        // First pass: create Variables for all var bindings.
        for stmt in &stmts {
            if let Stmt::Var(name, _) = stmt {
                let var_id = self.push(Node::Variable);
                self.bind_name(name.clone(), var_id);
            }
        }
        // Second pass: lower let bindings and collect var bodies.
        let mut var_bindings = Vec::new();
        for stmt in stmts {
            match stmt {
                Stmt::Let(name, expr) => { let id = self.lower_expr(expr); self.bind_name(name, id); },
                Stmt::Var(name, expr) => { var_bindings.push((name, expr)); },
                Stmt::Scope(name, body) => {
                    self.push(Node::Scope);
                    self.level += 1;
                    self.scopes.push(HashMap::new());
                    let mut inner_result = None;
                    self.lower_stmts(body, &mut inner_result);
                    let inner_scope = self.scopes.pop().unwrap();
                    let scope_level = self.level;
                    self.named_scopes.insert(name, (scope_level, inner_scope));
                    self.level -= 1;
                    self.push(Node::EndScope);
                },
                Stmt::Result(expr) => { let id = self.lower_expr(expr); *result_id = Some(id); },
            }
        }
        // Third pass: bind vars.
        for (name, expr) in var_bindings {
            let var_id = self.resolve_name(&name);
            let value_id = self.lower_expr(expr);
            self.push(Node::Bind { variable: var_id, value: value_id });
        }
    }

    fn lower_expr(&mut self, expr: Expr) -> Id {
        match expr {
            Expr::Input(n) => self.push(Node::Input(n)),
            Expr::Name(name) => self.resolve_name(&name),
            Expr::Qualified(scope_name, name) => {
                let (scope_level, inner_id) = {
                    let (lvl, scope) = self.named_scopes.get(&scope_name).unwrap_or_else(|| panic!("Unknown scope: {}", scope_name));
                    (*lvl, *scope.get(&name).unwrap_or_else(|| panic!("Unknown name {}::{}", scope_name, name)))
                };
                self.push(Node::Leave(inner_id, scope_level))
            },
            Expr::Map(input, proj) => { let id = self.lower_expr(*input); let logic = compile_map(proj); self.push(Node::Map { input: id, logic }) },
            Expr::Join(left, right, proj) => { let l = self.lower_expr(*left); let r = self.lower_expr(*right); let logic = compile_join(proj); self.push(Node::Join { left: l, right: r, logic }) },
            Expr::Reduce(input, reducer) => { let id = self.lower_expr(*input); let logic = compile_reducer(reducer); self.push(Node::Reduce { input: id, logic }) },
            Expr::Filter(input, cond) => { let id = self.lower_expr(*input); let logic = compile_filter(cond); self.push(Node::Map { input: id, logic }) },
            Expr::Negate(input) => {
                let id = self.lower_expr(*input);
                use timely::progress::Timestamp;
                let logic: Arc<dyn Fn((R, R)) -> smallvec::SmallVec<[((R, R), Time, Diff); 2]> + Send + Sync> =
                    Arc::new(move |(k, v)| svec![((k, v), Time::minimum(), -1)]);
                self.push(Node::Map { input: id, logic })
            },
            Expr::EnterAt(input, field) => {
                let id = self.lower_expr(*input);
                let level = self.level;
                let logic: Arc<dyn Fn((R, R)) -> smallvec::SmallVec<[((R, R), Time, Diff); 2]> + Send + Sync> = {
                    use timely::order::Product;
                    use differential_dataflow::dynamic::pointstamp::PointStamp;
                    Arc::new(move |(key, val)| {
                        let delay = {
                            let mut r = R::new();
                            eval_field_into(&field, &[key.as_slice(), val.as_slice()], &mut r);
                            256 * (64 - (r.as_slice().first().copied().unwrap_or(0) as u64).leading_zeros() as u64)
                        };
                        let mut coords = smallvec::SmallVec::<[u64; 2]>::new();
                        for _ in 0..level.saturating_sub(1) { coords.push(0); }
                        coords.push(delay);
                        svec![((key, val), Product::new(0u64, PointStamp::new(coords)), 1)]
                    })
                };
                self.push(Node::Map { input: id, logic })
            },
            Expr::Inspect(input, label) => { let id = self.lower_expr(*input); self.push(Node::Inspect { input: id, label }) },
            Expr::Concat(exprs) => { let ids: Vec<Id> = exprs.into_iter().map(|e| self.lower_expr(e)).collect(); self.push(Node::Concat(ids)) },
            Expr::Arrange(input) => { let id = self.lower_expr(*input); self.push(Node::Arrange(id)) },
        }
    }
}

fn compile_map<R: RowLike>(proj: Projection) -> Arc<dyn Fn((R, R)) -> smallvec::SmallVec<[((R, R), Time, Diff); 2]> + Send + Sync> {
    use timely::progress::Timestamp;
    Arc::new(move |(key, val)| { let i = [key.as_slice(), val.as_slice()]; svec![((eval_fields(&proj.key, &i), eval_fields(&proj.val, &i)), Time::minimum(), 1)] })
}

fn compile_join<R: RowLike>(proj: Projection) -> Arc<dyn Fn(&R, &R, &R) -> smallvec::SmallVec<[(R, R); 2]> + Send + Sync> {
    Arc::new(move |key, left, right| { let i = [key.as_slice(), left.as_slice(), right.as_slice()]; svec![(eval_fields(&proj.key, &i), eval_fields(&proj.val, &i))] })
}

fn compile_filter<R: RowLike>(cond: Condition) -> Arc<dyn Fn((R, R)) -> smallvec::SmallVec<[((R, R), Time, Diff); 2]> + Send + Sync> {
    use timely::progress::Timestamp;
    Arc::new(move |(key, val)| { let i = [key.as_slice(), val.as_slice()]; if eval_condition(&cond, &i) { svec![((key, val), Time::minimum(), 1)] } else { svec![] } })
}

fn compile_reducer<R: RowLike>(reducer: Reducer) -> Arc<dyn Fn(&R, &[(&R, Diff)], &mut Vec<(R, Diff)>) + Send + Sync> {
    match reducer {
        Reducer::Min => Arc::new(|_key, vals, output| { if let Some(min) = vals.iter().map(|(v, _)| *v).min() { output.push((min.clone(), 1)); } }),
        Reducer::Distinct => Arc::new(|_key, _vals, output| { output.push((R::new(), 1)); }),
        Reducer::Count => Arc::new(|_key, vals, output| { let count: Diff = vals.iter().map(|(_, d)| *d).sum(); if count > 0 { let mut r = R::new(); r.push(count); output.push((r, 1)); } }),
    }
}

fn eval_fields<R: RowLike>(fields: &[FieldExpr], inputs: &[&[i64]]) -> R {
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
    }
}

fn eval_condition(cond: &Condition, inputs: &[&[i64]]) -> bool {
    let (l, r) = match cond {
        Condition::Eq(l, r) | Condition::Ne(l, r) | Condition::Lt(l, r)
        | Condition::Le(l, r) | Condition::Gt(l, r) | Condition::Ge(l, r) => (l, r),
    };
    let mut a = Vec::<i64>::new(); let mut b = Vec::<i64>::new();
    eval_field_raw(l, inputs, &mut a); eval_field_raw(r, inputs, &mut b);
    match cond {
        Condition::Eq(..) => a == b, Condition::Ne(..) => a != b,
        Condition::Lt(..) => a < b,  Condition::Le(..) => a <= b,
        Condition::Gt(..) => a > b,  Condition::Ge(..) => a >= b,
    }
}

fn eval_field_raw(field: &FieldExpr, inputs: &[&[i64]], result: &mut Vec<i64>) {
    match field {
        FieldExpr::Pos(i) => result.extend_from_slice(inputs[*i]),
        FieldExpr::Index(row, idx) => result.push(inputs[*row][*idx]),
        FieldExpr::Const(v) => result.push(*v),
        FieldExpr::Neg(inner) => { let mut tmp = Vec::new(); eval_field_raw(inner, inputs, &mut tmp); for v in tmp.iter() { result.push(-v); } },
    }
}

pub fn lower<R: RowLike>(stmts: Vec<Stmt>) -> Program<R> { Lowering::new().lower_program(stmts) }
