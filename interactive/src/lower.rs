//! Lowering from AST to IR.

use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::parse::*;
use crate::ir::{Node, LinearOp, Id, Program};

struct Lowering {
    nodes: BTreeMap<Id, Node>,
    next_id: Id,
    scopes: Vec<HashMap<String, Id>>,
    named_scopes: HashMap<String, (usize, HashMap<String, Id>)>,
    level: usize,
}

impl Lowering {
    fn new() -> Self { Lowering { nodes: BTreeMap::new(), next_id: 0, scopes: vec![HashMap::new()], named_scopes: HashMap::new(), level: 0 } }
    fn push(&mut self, node: Node) -> Id { let id = self.next_id; self.next_id += 1; self.nodes.insert(id, node); id }
    fn resolve_name(&self, name: &str) -> Id { for scope in self.scopes.iter().rev() { if let Some(&id) = scope.get(name) { return id; } } panic!("Unresolved name: {}", name); }
    fn bind_name(&mut self, name: String, id: Id) { self.scopes.last_mut().unwrap().insert(name, id); }

    fn lower_program(mut self, stmts: Vec<Stmt>) -> Program {
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
            Expr::Map(input, proj) => { let id = self.lower_expr(*input); self.push(Node::Linear { input: id, ops: vec![LinearOp::Project(proj)] }) },
            Expr::Join(left, right, proj) => {
                let l = self.lower_expr(*left);
                let l = self.push(Node::Arrange(l));
                let r = self.lower_expr(*right);
                let r = self.push(Node::Arrange(r));
                self.push(Node::Join { left: l, right: r, projection: proj })
            },
            Expr::Reduce(input, reducer) => {
                let id = self.lower_expr(*input);
                let id = self.push(Node::Arrange(id));
                self.push(Node::Reduce { input: id, reducer })
            },
            Expr::Filter(input, cond) => { let id = self.lower_expr(*input); self.push(Node::Linear { input: id, ops: vec![LinearOp::Filter(cond)] }) },
            Expr::Negate(input) => { let id = self.lower_expr(*input); self.push(Node::Linear { input: id, ops: vec![LinearOp::Negate] }) },
            Expr::EnterAt(input, field) => { let id = self.lower_expr(*input); self.push(Node::Linear { input: id, ops: vec![LinearOp::EnterAt(field)] }) },
            Expr::Inspect(input, label) => { let id = self.lower_expr(*input); self.push(Node::Inspect { input: id, label }) },
            Expr::Concat(exprs) => { let ids: Vec<Id> = exprs.into_iter().map(|e| self.lower_expr(e)).collect(); self.push(Node::Concat(ids)) },
            Expr::Arrange(input) => { let id = self.lower_expr(*input); self.push(Node::Arrange(id)) },
        }
    }
}

pub fn lower(stmts: Vec<Stmt>) -> Program { Lowering::new().lower_program(stmts) }
