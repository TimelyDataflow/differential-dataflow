//! DD IR with concrete syntax: parse, lower, render, execute.
//!
//! Syntax:
//!   let name = expr;          -- non-iterative binding
//!   var name = expr;          -- iterative variable (concurrent update)
//!   name: { ... }             -- iterative scope
//!   result expr;              -- output
//!
//! Expressions:
//!   INPUT n                   -- external input
//!   MAP(e, projection)        -- rearrange fields
//!   JOIN(e, e, projection)    -- join on key (both must be arrangements)
//!   REDUCE(e, reducer)        -- reduce by key (input must be arrangement)
//!   CONCAT(e, e, ...)         -- union
//!   ARRANGE(e)                -- arrange by key
//!   FILTER(e, condition)      -- keep matching records
//!   NEGATE(e)                 -- flip diff sign
//!   ENTER_AT(e, field)        -- delay introduction by field value
//!   INSPECT(e, label)         -- eprintln each update with label
//!   name | scope::name        -- reference
//!
//! Projections: (key_fields ; val_fields)
//!   $0, $1, $2     -- whole input row
//!   $0[1]          -- element 1 of input 0
//!   42, -1         -- constants
//!
//! Reducers: MIN, DISTINCT
//! Conditions: $a[i] == $b[j]
//!
//! Semantics:
//! Each expression denotes a multiset collection of (key, val) pairs.
//! The conventional relational operators like MAP, JOIN, REDUCE apply
//! their transformation to each collection, with the expected output.
//! In an iterative scope variable (`var`) expressions are initially empty,
//! but they indefinitely update concurrently to equal their definitions.
//! These terms can be referenced outside the scope and denote their limits;
//! if the limits do not exist the computation diverges.
mod types {
    pub type Row = smallvec::SmallVec<[i64; 2]>;
    pub type Diff = i64;
    pub type Id = usize;
    pub type Time = timely::order::Product<u64, differential_dataflow::dynamic::pointstamp::PointStamp<u64>>;
}
use types::*;

mod parse {
    #[derive(Debug, Clone, PartialEq)]
    pub enum Token {
        Let, Var, Scope, Result,
        Input, Map, Join, Reduce, Concat, Arrange, Filter, Negate, EnterAt, Inspect,
        Min, Distinct,
        Ident(String), Int(i64),
        Dollar, LParen, RParen, LBrace, RBrace, LBracket, RBracket,
        Comma, Semi, Colon, ColonColon, Eq, EqEq, Plus, Minus, Star, Eof,
    }

    #[derive(Debug, Clone)]
    pub enum FieldExpr {
        Pos(usize),
        Index(usize, usize),
        Const(i64),
        Neg(Box<FieldExpr>),
    }

    #[derive(Debug, Clone)]
    pub enum Condition { Eq(FieldExpr, FieldExpr) }

    #[derive(Debug, Clone)]
    pub struct Projection { pub key: Vec<FieldExpr>, pub val: Vec<FieldExpr> }

    #[derive(Debug, Clone)]
    pub enum Reducer { Min, Distinct }

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

    pub fn tokenize(input: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut chars = input.chars().peekable();
        while let Some(&ch) = chars.peek() {
            match ch {
                ' ' | '\t' | '\n' | '\r' => { chars.next(); },
                '-' if chars.clone().nth(1).map_or(false, |c| c == '-') => {
                    while let Some(&c) = chars.peek() { chars.next(); if c == '\n' { break; } }
                },
                '(' => { chars.next(); tokens.push(Token::LParen); },
                ')' => { chars.next(); tokens.push(Token::RParen); },
                '{' => { chars.next(); tokens.push(Token::LBrace); },
                '}' => { chars.next(); tokens.push(Token::RBrace); },
                '[' => { chars.next(); tokens.push(Token::LBracket); },
                ']' => { chars.next(); tokens.push(Token::RBracket); },
                ',' => { chars.next(); tokens.push(Token::Comma); },
                ';' => { chars.next(); tokens.push(Token::Semi); },
                '=' => { chars.next(); if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::EqEq); } else { tokens.push(Token::Eq); } },
                '+' => { chars.next(); tokens.push(Token::Plus); },
                '*' => { chars.next(); tokens.push(Token::Star); },
                '$' => { chars.next(); tokens.push(Token::Dollar); },
                '-' => { chars.next(); tokens.push(Token::Minus); },
                ':' => { chars.next(); if chars.peek() == Some(&':') { chars.next(); tokens.push(Token::ColonColon); } else { tokens.push(Token::Colon); } },
                c if c.is_ascii_digit() => {
                    let mut num = String::new();
                    while let Some(&c) = chars.peek() { if c.is_ascii_digit() { num.push(c); chars.next(); } else { break; } }
                    tokens.push(Token::Int(num.parse().unwrap()));
                },
                c if c.is_ascii_alphabetic() || c == '_' => {
                    let mut ident = String::new();
                    while let Some(&c) = chars.peek() { if c.is_ascii_alphanumeric() || c == '_' { ident.push(c); chars.next(); } else { break; } }
                    tokens.push(match ident.as_str() {
                        "let" => Token::Let, "var" => Token::Var, "scope" => Token::Scope, "result" => Token::Result,
                        "INPUT" => Token::Input, "MAP" => Token::Map, "JOIN" => Token::Join,
                        "REDUCE" => Token::Reduce, "CONCAT" => Token::Concat, "ARRANGE" => Token::Arrange,
                        "FILTER" => Token::Filter, "NEGATE" => Token::Negate, "ENTER_AT" => Token::EnterAt, "INSPECT" => Token::Inspect,
                        "MIN" => Token::Min, "DISTINCT" => Token::Distinct,
                        _ => Token::Ident(ident),
                    });
                },
                other => panic!("Unexpected character: {:?}", other),
            }
        }
        tokens.push(Token::Eof);
        tokens
    }

    pub struct Parser { tokens: Vec<Token>, pos: usize }

    impl Parser {
        pub fn new(tokens: Vec<Token>) -> Self { Parser { tokens, pos: 0 } }
        fn peek(&self) -> &Token { &self.tokens[self.pos] }
        fn next(&mut self) -> Token { let t = self.tokens[self.pos].clone(); self.pos += 1; t }
        fn expect(&mut self, expected: &Token) { let t = self.next(); assert_eq!(&t, expected, "Expected {:?}, got {:?}", expected, t); }

        pub fn parse_program(&mut self) -> Vec<Stmt> {
            let mut stmts = Vec::new();
            while *self.peek() != Token::Eof && *self.peek() != Token::RBrace { stmts.push(self.parse_stmt()); }
            stmts
        }

        fn parse_stmt(&mut self) -> Stmt {
            match self.peek().clone() {
                Token::Let => { self.next(); let n = self.parse_ident(); self.expect(&Token::Eq); let e = self.parse_expr(); self.expect(&Token::Semi); Stmt::Let(n, e) },
                Token::Var => { self.next(); let n = self.parse_ident(); self.expect(&Token::Eq); let e = self.parse_expr(); self.expect(&Token::Semi); Stmt::Var(n, e) },
                Token::Result => { self.next(); let e = self.parse_expr(); self.expect(&Token::Semi); Stmt::Result(e) },
                Token::Ident(_) => {
                    let n = self.parse_ident(); self.expect(&Token::Colon);
                    // Allow optional `scope` keyword
                    if *self.peek() == Token::Scope { self.next(); }
                    self.expect(&Token::LBrace); let b = self.parse_program(); self.expect(&Token::RBrace); Stmt::Scope(n, b)
                },
                other => panic!("Unexpected token: {:?}", other),
            }
        }

        fn parse_ident(&mut self) -> String { match self.next() { Token::Ident(s) => s, other => panic!("Expected ident, got {:?}", other) } }

        fn parse_expr(&mut self) -> Expr {
            match self.peek().clone() {
                Token::Input => { self.next(); match self.next() { Token::Int(n) => Expr::Input(n as usize), o => panic!("Expected int, got {:?}", o) } },
                Token::Map => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::Comma); let p = self.parse_projection(); self.expect(&Token::RParen); Expr::Map(Box::new(i), p) },
                Token::Join => { self.next(); self.expect(&Token::LParen); let l = self.parse_expr(); self.expect(&Token::Comma); let r = self.parse_expr(); self.expect(&Token::Comma); let p = self.parse_projection(); self.expect(&Token::RParen); Expr::Join(Box::new(l), Box::new(r), p) },
                Token::Reduce => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::Comma); let r = self.parse_reducer(); self.expect(&Token::RParen); Expr::Reduce(Box::new(i), r) },
                Token::Filter => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::Comma); let c = self.parse_condition(); self.expect(&Token::RParen); Expr::Filter(Box::new(i), c) },
                Token::Negate => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::RParen); Expr::Negate(Box::new(i)) },
                Token::EnterAt => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::Comma); let f = self.parse_field(); self.expect(&Token::RParen); Expr::EnterAt(Box::new(i), f) },
                Token::Inspect => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::Comma); let label = self.parse_ident(); self.expect(&Token::RParen); Expr::Inspect(Box::new(i), label) },
                Token::Concat => { self.next(); self.expect(&Token::LParen); let mut v = vec![self.parse_expr()]; while *self.peek() == Token::Comma { self.next(); v.push(self.parse_expr()); } self.expect(&Token::RParen); Expr::Concat(v) },
                Token::Arrange => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::RParen); Expr::Arrange(Box::new(i)) },
                Token::Ident(_) => { let n = self.parse_ident(); if *self.peek() == Token::ColonColon { self.next(); let f = self.parse_ident(); Expr::Qualified(n, f) } else { Expr::Name(n) } },
                other => panic!("Unexpected token in expr: {:?}", other),
            }
        }

        fn parse_projection(&mut self) -> Projection {
            self.expect(&Token::LParen);
            // Handle empty projection: () or (;)
            if *self.peek() == Token::RParen { self.next(); return Projection { key: vec![], val: vec![] }; }
            // Handle (; ...) — empty key
            if *self.peek() == Token::Semi { self.next();
                if *self.peek() == Token::RParen { self.next(); return Projection { key: vec![], val: vec![] }; }
                let mut val = vec![self.parse_field()];
                while *self.peek() == Token::Comma { self.next(); val.push(self.parse_field()); }
                self.expect(&Token::RParen);
                return Projection { key: vec![], val };
            }
            let mut key = vec![self.parse_field()];
            while *self.peek() == Token::Comma { self.next(); key.push(self.parse_field()); }
            let val = if *self.peek() == Token::Semi { self.next();
                if *self.peek() == Token::RParen { vec![] }
                else { let mut v = vec![self.parse_field()]; while *self.peek() == Token::Comma { self.next(); v.push(self.parse_field()); } v }
            } else { vec![] };
            self.expect(&Token::RParen);
            Projection { key, val }
        }

        fn parse_field(&mut self) -> FieldExpr {
            match self.peek().clone() {
                Token::Dollar => { self.next(); let n = match self.next() { Token::Int(n) => n as usize, o => panic!("Expected int, got {:?}", o) }; if *self.peek() == Token::LBracket { self.next(); let i = match self.next() { Token::Int(i) => i as usize, o => panic!("Expected int, got {:?}", o) }; self.expect(&Token::RBracket); FieldExpr::Index(n, i) } else { FieldExpr::Pos(n) } },
                Token::Minus => { self.next(); FieldExpr::Neg(Box::new(self.parse_field())) },
                Token::Int(n) => { self.next(); FieldExpr::Const(n) },
                other => panic!("Unexpected token in field: {:?}", other),
            }
        }

        fn parse_condition(&mut self) -> Condition { let l = self.parse_field(); self.expect(&Token::EqEq); let r = self.parse_field(); Condition::Eq(l, r) }
        fn parse_reducer(&mut self) -> Reducer { match self.next() { Token::Min => Reducer::Min, Token::Distinct => Reducer::Distinct, o => panic!("Expected reducer, got {:?}", o) } }
    }

    pub fn parse(input: &str) -> Vec<Stmt> { let tokens = tokenize(input); let mut p = Parser::new(tokens); p.parse_program() }
}

mod ir {
    use std::sync::Arc;
    use super::types::*;

    pub enum Node {
        Input(usize),
        Map { input: Id, logic: Arc<dyn Fn((Row, Row)) -> smallvec::SmallVec<[((Row, Row), Time, Diff); 1]> + Send + Sync> },
        Concat(Vec<Id>),
        Arrange(Id),
        Join { left: Id, right: Id, logic: Arc<dyn Fn(&Row, &Row, &Row) -> smallvec::SmallVec<[(Row, Row); 1]> + Send + Sync> },
        Reduce { input: Id, logic: Arc<dyn Fn(&Row, &[(&Row, Diff)], &mut Vec<(Row, Diff)>) + Send + Sync> },
        Variable { init: Option<Id> },
        Inspect { input: Id, label: String },
        Leave(Id, usize), // (inner_id, scope_level)
        Scope,
        EndScope,
        Bind { variable: Id, value: Id },
    }

    pub struct Program {
        pub nodes: Vec<Node>,
        pub result: Id,
    }
}

mod lower {
    use std::collections::HashMap;
    use std::sync::Arc;
    use smallvec::smallvec as svec;
    use super::types::*;
    use super::parse::*;
    use super::ir::*;

    struct Lowering {
        nodes: Vec<Node>,
        scopes: Vec<HashMap<String, Id>>,
        named_scopes: HashMap<String, (usize, HashMap<String, Id>)>,
        level: usize,
    }

    impl Lowering {
        fn new() -> Self { Lowering { nodes: Vec::new(), scopes: vec![HashMap::new()], named_scopes: HashMap::new(), level: 0 } }
        fn push(&mut self, node: Node) -> Id { let id = self.nodes.len(); self.nodes.push(node); id }
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
                    let var_id = self.push(Node::Variable { init: None });
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
                    let logic: Arc<dyn Fn((Row, Row)) -> smallvec::SmallVec<[((Row, Row), Time, Diff); 1]> + Send + Sync> =
                        Arc::new(move |(k, v)| svec![((k, v), Time::minimum(), -1)]);
                    self.push(Node::Map { input: id, logic })
                },
                Expr::EnterAt(input, field) => {
                    let id = self.lower_expr(*input);
                    let level = self.level;
                    let logic: Arc<dyn Fn((Row, Row)) -> smallvec::SmallVec<[((Row, Row), Time, Diff); 1]> + Send + Sync> = {
                        use timely::order::Product;
                        use differential_dataflow::dynamic::pointstamp::PointStamp;
                        Arc::new(move |(key, val)| {
                            let inputs = [&key, &val];
                            let mut r = Row::new();
                            eval_field_into(&field, &inputs, &mut r);
                            let delay = 256 * (64 - (r.first().copied().unwrap_or(0) as u64).leading_zeros() as u64);
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

    fn compile_map(proj: Projection) -> Arc<dyn Fn((Row, Row)) -> smallvec::SmallVec<[((Row, Row), Time, Diff); 1]> + Send + Sync> {
        use timely::progress::Timestamp;
        Arc::new(move |(key, val)| { let i = [&key, &val]; svec![((eval_fields(&proj.key, &i), eval_fields(&proj.val, &i)), Time::minimum(), 1)] })
    }

    fn compile_join(proj: Projection) -> Arc<dyn Fn(&Row, &Row, &Row) -> smallvec::SmallVec<[(Row, Row); 1]> + Send + Sync> {
        Arc::new(move |key, left, right| { let i = [key, left, right]; svec![(eval_fields(&proj.key, &i), eval_fields(&proj.val, &i))] })
    }

    fn compile_filter(cond: Condition) -> Arc<dyn Fn((Row, Row)) -> smallvec::SmallVec<[((Row, Row), Time, Diff); 1]> + Send + Sync> {
        use timely::progress::Timestamp;
        Arc::new(move |(key, val)| { let i = [&key, &val]; if eval_condition(&cond, &i) { svec![((key, val), Time::minimum(), 1)] } else { svec![] } })
    }

    fn compile_reducer(reducer: Reducer) -> Arc<dyn Fn(&Row, &[(&Row, Diff)], &mut Vec<(Row, Diff)>) + Send + Sync> {
        match reducer {
            Reducer::Min => Arc::new(|_key, vals, output| { if let Some(min) = vals.iter().map(|(v, _)| *v).min() { output.push((min.clone(), 1)); } }),
            Reducer::Distinct => Arc::new(|_key, _vals, output| { output.push((Row::new(), 1)); }),
        }
    }

    fn eval_fields(fields: &[FieldExpr], inputs: &[&Row]) -> Row { let mut r = Row::new(); for f in fields { eval_field_into(f, inputs, &mut r); } r }

    pub fn eval_field_into(field: &FieldExpr, inputs: &[&Row], result: &mut Row) {
        match field {
            FieldExpr::Pos(i) => result.extend_from_slice(inputs[*i]),
            FieldExpr::Index(row, idx) => result.push(inputs[*row][*idx]),
            FieldExpr::Const(v) => result.push(*v),
            FieldExpr::Neg(inner) => { let mut tmp = Row::new(); eval_field_into(inner, inputs, &mut tmp); for v in tmp.iter() { result.push(-v); } },
        }
    }

    fn eval_condition(cond: &Condition, inputs: &[&Row]) -> bool {
        match cond { Condition::Eq(l, r) => { let mut a = Row::new(); let mut b = Row::new(); eval_field_into(l, inputs, &mut a); eval_field_into(r, inputs, &mut b); a == b } }
    }

    pub fn lower(stmts: Vec<Stmt>) -> Program { Lowering::new().lower_program(stmts) }
}

mod render {
    use std::collections::HashMap;
    use std::sync::Arc;
    use timely::order::Product;
    use timely::dataflow::Scope;
    use differential_dataflow::VecCollection;
    use differential_dataflow::operators::iterate::VecVariable;
    use differential_dataflow::dynamic::pointstamp::{PointStamp, PointStampSummary};
    use differential_dataflow::dynamic::feedback_summary;
    use differential_dataflow::trace::implementations::ValSpine;
    use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
    use super::types::*;
    use super::ir::*;

    pub type Col<G> = VecCollection<G, (Row, Row), Diff>;
    type Arr<G> = Arranged<G, TraceAgent<ValSpine<Row, Row, <G as timely::dataflow::scopes::ScopeParent>::Timestamp, Diff>>>;

    enum Rendered<G: Scope<Timestamp: differential_dataflow::lattice::Lattice>> {
        Collection(Col<G>),
        Arrangement(Arr<G>),
    }

    impl<G: Scope<Timestamp: differential_dataflow::lattice::Lattice>> Rendered<G> {
        fn collection(&self) -> Col<G> { match self { Rendered::Collection(c) => c.clone(), Rendered::Arrangement(a) => a.clone().as_collection(|k, v| (k.clone(), v.clone())) } }
        fn arrange(&self) -> Arr<G> { match self { Rendered::Arrangement(a) => a.clone(), Rendered::Collection(c) => c.clone().arrange_by_key() } }
    }

    pub fn render_program<G>(program: &Program, scope: &mut G, inputs: &[Col<G>]) -> HashMap<Id, Col<G>>
    where G: Scope<Timestamp = Product<u64, PointStamp<u64>>>
    {
        let mut nodes: HashMap<Id, Rendered<G>> = HashMap::new();
        let mut level: usize = 0;
        let mut variables: HashMap<Id, (VecVariable<G, (Row, Row), Diff>, usize)> = HashMap::new();
        let mut var_levels: HashMap<Id, usize> = HashMap::new();

        for (id, node) in program.nodes.iter().enumerate() {
            match node {
                Node::Input(i) => { nodes.insert(id, Rendered::Collection(inputs[*i].clone())); },
                Node::Map { input, logic } => { let c = nodes[input].collection(); let l = Arc::clone(logic); nodes.insert(id, Rendered::Collection(c.join_function(move |kv| l(kv)))); },
                Node::Concat(ids) => { let mut r = nodes[&ids[0]].collection(); for i in &ids[1..] { r = r.concat(nodes[i].collection()); } nodes.insert(id, Rendered::Collection(r)); },
                Node::Arrange(input) => { nodes.insert(id, Rendered::Arrangement(nodes[input].arrange())); },
                Node::Join { left, right, logic } => {
                    let l = nodes[left].arrange();
                    let r = nodes[right].arrange();
                    let f = Arc::clone(logic);
                    nodes.insert(id, Rendered::Collection(l.join_core(r, move |k, v1, v2| f(k, v1, v2))));
                },
                Node::Reduce { input, logic } => {
                    let a = nodes[input].arrange();
                    let f = Arc::clone(logic);
                    let reduced = a.reduce_abelian::<_, differential_dataflow::trace::implementations::ValBuilder<_,_,_,_>, ValSpine<_,_,_,_>>("Reduce", move |k, v, o| f(k, v, o));
                    nodes.insert(id, Rendered::Arrangement(reduced));
                },
                Node::Variable { init } => {
                    let step: Product<u64, PointStampSummary<u64>> = Product::new(0, feedback_summary::<u64>(level, 1));
                    if let Some(init_id) = init {
                        let c = nodes[init_id].collection();
                        let (var, col) = VecVariable::new_from(c, step);
                        nodes.insert(id, Rendered::Collection(col)); variables.insert(id, (var, level)); var_levels.insert(id, level);
                    } else {
                        let (var, col) = VecVariable::new(scope, step);
                        nodes.insert(id, Rendered::Collection(col)); variables.insert(id, (var, level)); var_levels.insert(id, level);
                    }
                },
                Node::Inspect { input, label } => {
                    let col = nodes[input].collection();
                    let label = label.clone();
                    nodes.insert(id, Rendered::Collection(col.inspect(move |x| eprintln!("  [{}] {:?}", label, x))));
                },
                Node::Leave(inner_id, scope_level) => { let c = nodes[inner_id].collection(); nodes.insert(id, Rendered::Collection(c.leave_dynamic(*scope_level))); },
                Node::Scope => { level += 1; },
                Node::EndScope => { level -= 1; },
                Node::Bind { variable, value } => { let c = nodes[value].collection(); let (var, _) = variables.remove(variable).expect("Bind: variable not found"); var.set(c); },
            }
        }

        // Return collections only
        nodes.into_iter().filter_map(|(id, r)| match r { Rendered::Collection(c) => Some((id, c)), _ => None }).collect()
    }
}

use timely::dataflow::Scope;
use differential_dataflow::input::Input;
use differential_dataflow::dynamic::pointstamp::PointStamp;

fn row(vals: &[i64]) -> Row { vals.into() }

fn run(name: &str, source: &str, n_inputs: usize, setup: impl Fn(usize, usize, &mut Vec<differential_dataflow::input::InputSession<u64, (Row, Row), Diff>>) + Send + Sync + 'static) {
    let stmts = parse::parse(source);
    let compiled = lower::lower(stmts);
    println!("{}: {} IR nodes, result = {}", name, compiled.nodes.len(), compiled.result);
    let name = name.to_string();
    let result_id = compiled.result;

    timely::execute_from_args(std::env::args().skip(3), move |worker| {
        let (mut inputs, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let mut handles = Vec::new();
            let mut collections = Vec::new();
            for _ in 0..n_inputs {
                let (h, c) = scope.new_collection::<(Row, Row), Diff>();
                handles.push(h); collections.push(c);
            }
            let mut probe = timely::dataflow::ProbeHandle::new();
            let output = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered: Vec<_> = collections.iter().map(|c| c.clone().enter(inner)).collect();
                let rendered = render::render_program(&compiled, inner, &entered);
                rendered[&result_id].clone().leave()
            });
            if std::env::var("DDIR_PRINT").is_ok() { output.inspect(|x| println!("{:?}", x)).probe_with(&mut probe); }
            else { output.probe_with(&mut probe); }
            (handles, probe)
        });
        setup(worker.index(), worker.peers(), &mut inputs);
        for i in inputs.iter_mut() { i.advance_to(1); i.flush(); }
        while probe.less_than(&1u64) { worker.step(); }
        println!("worker {}: {} complete", worker.index(), name);
    }).unwrap();
}

fn load_program(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("Cannot read {}: {}", path, e))
}

fn main() {
    let program = std::env::args().nth(1).unwrap_or("reach".into());
    let n: usize = std::env::args().nth(2).unwrap_or("10".into()).parse().unwrap();

    let source = load_program(&program);

    // Count INPUT nodes to determine how many inputs we need.
    let stmts = parse::parse(&source);
    let n_inputs = count_inputs(&stmts);

    // Generate data based on program name (heuristic for demos).
    let name = std::path::Path::new(&program).file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or(program.clone());
    let data_name = program.clone();
    run(&name, &source, n_inputs, move |index, peers, inputs| {
        match data_name.as_str() {
            p if p.contains("reach") => {
                let mut i = index;
                while i < n.saturating_sub(1) { inputs[0].update((row(&[i as i64]), row(&[(i + 1) as i64])), 1); i += peers; }
                if index == 0 && inputs.len() > 1 { inputs[1].update((row(&[0]), row(&[])), 1); }
            },
            p if p.contains("scc") => {
                let edges = 2 * n;
                let mut rng = n as u64;
                let lcg = |r: &mut u64| { *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); *r };
                for e in 0..edges { let s = (lcg(&mut rng) >> 32) % (n as u64); let d = (lcg(&mut rng) >> 32) % (n as u64); if e % peers == index { inputs[0].update((row(&[s as i64]), row(&[d as i64])), 1); } }
            },
            _ => {
                let mut i = index;
                while i < n.saturating_sub(1) { inputs[0].update((row(&[i as i64]), row(&[(i + 1) as i64])), 1); i += peers; }
            },
        }
    });
}

fn count_inputs(stmts: &[parse::Stmt]) -> usize {
    let mut max_input = 0usize;
    for stmt in stmts {
        match stmt {
            parse::Stmt::Let(_, expr) | parse::Stmt::Var(_, expr) | parse::Stmt::Result(expr) => {
                max_input = max_input.max(count_inputs_expr(expr));
            },
            parse::Stmt::Scope(_, body) => {
                max_input = max_input.max(count_inputs(body));
            },
        }
    }
    max_input
}

fn count_inputs_expr(expr: &parse::Expr) -> usize {
    match expr {
        parse::Expr::Input(n) => n + 1,
        parse::Expr::Map(e, _) | parse::Expr::Negate(e) | parse::Expr::Arrange(e)
            | parse::Expr::EnterAt(e, _) | parse::Expr::Filter(e, _) | parse::Expr::Reduce(e, _)
            | parse::Expr::Inspect(e, _) => count_inputs_expr(e),
        parse::Expr::Join(l, r, _) => count_inputs_expr(l).max(count_inputs_expr(r)),
        parse::Expr::Concat(es) => es.iter().map(|e| count_inputs_expr(e)).max().unwrap_or(0),
        parse::Expr::Name(_) | parse::Expr::Qualified(_, _) => 0,
    }
}
