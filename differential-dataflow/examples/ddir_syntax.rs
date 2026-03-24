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
//! Reducers: MIN, DISTINCT, COUNT
//! Conditions: $a[i] == $b[j], also !=, <, <=, >, >=
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
        Min, Distinct, Count,
        Ident(String), Int(i64),
        Dollar, LParen, RParen, LBrace, RBrace, LBracket, RBracket,
        Comma, Semi, Colon, ColonColon, Eq, EqEq, NotEq, Lt, LtEq, Gt, GtEq,
        Plus, Minus, Star, Eof,
    }

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
                '!' => { chars.next(); if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::NotEq); } else { panic!("Expected != after !"); } },
                '<' => { chars.next(); if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::LtEq); } else { tokens.push(Token::Lt); } },
                '>' => { chars.next(); if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::GtEq); } else { tokens.push(Token::Gt); } },
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
                        "MIN" => Token::Min, "DISTINCT" => Token::Distinct, "COUNT" => Token::Count,
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

        fn parse_condition(&mut self) -> Condition {
            let l = self.parse_field();
            let op = self.next();
            let r = self.parse_field();
            match op {
                Token::EqEq => Condition::Eq(l, r),
                Token::NotEq => Condition::Ne(l, r),
                Token::Lt => Condition::Lt(l, r),
                Token::LtEq => Condition::Le(l, r),
                Token::Gt => Condition::Gt(l, r),
                Token::GtEq => Condition::Ge(l, r),
                o => panic!("Expected comparison operator, got {:?}", o),
            }
        }
        fn parse_reducer(&mut self) -> Reducer { match self.next() { Token::Min => Reducer::Min, Token::Distinct => Reducer::Distinct, Token::Count => Reducer::Count, o => panic!("Expected reducer, got {:?}", o) } }
    }

    pub fn parse(input: &str) -> Vec<Stmt> { let tokens = tokenize(input); let mut p = Parser::new(tokens); p.parse_program() }
}

/// Pipe-oriented concrete syntax: `expr | op | op`, `+` for concat, `-` for negate.
/// Produces the same parse::Stmt/Expr AST as the original syntax.
mod pipe {
    use super::parse::*;

    #[derive(Debug, Clone, PartialEq)]
    enum Token {
        Let, Var, Result,
        Input, Key, Map, Join, Min, Distinct, Count, Arrange, Negate, Filter, EnterAt, Inspect,
        Ident(String), Int(i64),
        Dollar, LParen, RParen, LBrace, RBrace, LBracket, RBracket,
        Comma, Semi, Colon, ColonColon, Eq, EqEq, NotEq, Lt, LtEq, Gt, GtEq,
        Pipe, Plus, Minus, Eof,
    }

    fn tokenize(input: &str) -> Vec<Token> {
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
                '|' => { chars.next(); tokens.push(Token::Pipe); },
                '+' => { chars.next(); tokens.push(Token::Plus); },
                '=' => { chars.next(); if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::EqEq); } else { tokens.push(Token::Eq); } },
                '!' => { chars.next(); if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::NotEq); } else { panic!("Expected != after !"); } },
                '<' => { chars.next(); if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::LtEq); } else { tokens.push(Token::Lt); } },
                '>' => { chars.next(); if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::GtEq); } else { tokens.push(Token::Gt); } },
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
                        "let" => Token::Let, "var" => Token::Var, "result" => Token::Result,
                        "input" => Token::Input, "key" => Token::Key, "map" => Token::Map,
                        "join" => Token::Join, "min" => Token::Min, "distinct" => Token::Distinct,
                        "count" => Token::Count, "arrange" => Token::Arrange, "negate" => Token::Negate,
                        "filter" => Token::Filter, "enter_at" => Token::EnterAt, "inspect" => Token::Inspect,
                        _ => Token::Ident(ident),
                    });
                },
                other => panic!("Unexpected character: {:?}", other),
            }
        }
        tokens.push(Token::Eof);
        tokens
    }

    struct Parser { tokens: Vec<Token>, pos: usize }

    impl Parser {
        fn new(tokens: Vec<Token>) -> Self { Parser { tokens, pos: 0 } }
        fn peek(&self) -> &Token { &self.tokens[self.pos] }
        fn next(&mut self) -> Token { let t = self.tokens[self.pos].clone(); self.pos += 1; t }
        fn expect(&mut self, expected: &Token) { let t = self.next(); assert_eq!(&t, expected, "Expected {:?}, got {:?}", expected, t); }

        fn parse_program(&mut self) -> Vec<Stmt> {
            let mut stmts = Vec::new();
            while *self.peek() != Token::Eof && *self.peek() != Token::RBrace { stmts.push(self.parse_stmt()); }
            stmts
        }

        fn parse_stmt(&mut self) -> Stmt {
            match self.peek().clone() {
                Token::Let => { self.next(); let n = self.parse_ident(); self.expect(&Token::Eq); let e = self.parse_pipe_expr(); self.expect(&Token::Semi); Stmt::Let(n, e) },
                Token::Var => { self.next(); let n = self.parse_ident(); self.expect(&Token::Eq); let e = self.parse_pipe_expr(); self.expect(&Token::Semi); Stmt::Var(n, e) },
                Token::Result => { self.next(); let e = self.parse_pipe_expr(); self.expect(&Token::Semi); Stmt::Result(e) },
                Token::Ident(_) => {
                    let n = self.parse_ident(); self.expect(&Token::Colon);
                    self.expect(&Token::LBrace); let b = self.parse_program(); self.expect(&Token::RBrace); Stmt::Scope(n, b)
                },
                other => panic!("Unexpected token: {:?}", other),
            }
        }

        fn parse_ident(&mut self) -> String { match self.next() { Token::Ident(s) => s, other => panic!("Expected ident, got {:?}", other) } }

        // pipe_expr := concat_expr ('|' pipe_op)*
        fn parse_pipe_expr(&mut self) -> Expr {
            let mut expr = self.parse_concat_expr();
            while *self.peek() == Token::Pipe {
                self.next();
                expr = self.parse_pipe_op(expr);
            }
            expr
        }

        // concat_expr := atom ('+' atom | '-' atom)*
        fn parse_concat_expr(&mut self) -> Expr {
            let first = self.parse_atom();
            let mut parts = vec![first];
            loop {
                match self.peek() {
                    Token::Plus => { self.next(); parts.push(self.parse_atom()); },
                    Token::Minus => { self.next(); parts.push(Expr::Negate(Box::new(self.parse_atom()))); },
                    _ => break,
                }
            }
            if parts.len() == 1 { parts.pop().unwrap() } else { Expr::Concat(parts) }
        }

        fn parse_atom(&mut self) -> Expr {
            match self.peek().clone() {
                Token::Input => { self.next(); match self.next() { Token::Int(n) => Expr::Input(n as usize), o => panic!("Expected int, got {:?}", o) } },
                Token::Ident(_) => { let n = self.parse_ident(); if *self.peek() == Token::ColonColon { self.next(); let f = self.parse_ident(); Expr::Qualified(n, f) } else { Expr::Name(n) } },
                Token::LParen => { self.next(); let e = self.parse_pipe_expr(); self.expect(&Token::RParen); e },
                other => panic!("Unexpected token in atom: {:?}", other),
            }
        }

        // Parse a join argument: atom with optional pipe ops, no concat.
        fn parse_join_arg(&mut self) -> Expr {
            let mut expr = self.parse_atom();
            while *self.peek() == Token::Pipe { self.next(); expr = self.parse_pipe_op(expr); }
            expr
        }

        // Parse a pipe operator applied to the left-hand expression.
        fn parse_pipe_op(&mut self, lhs: Expr) -> Expr {
            match self.peek().clone() {
                Token::Key => { self.next(); let p = self.parse_projection(); Expr::Map(Box::new(lhs), p) },
                Token::Map => { self.next(); let p = self.parse_projection(); Expr::Map(Box::new(lhs), p) },
                Token::Join => { self.next(); self.expect(&Token::LParen); let r = self.parse_join_arg(); self.expect(&Token::Comma); let p = self.parse_projection(); self.expect(&Token::RParen); Expr::Join(Box::new(lhs), Box::new(r), p) },
                Token::Min => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Min) },
                Token::Distinct => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Distinct) },
                Token::Count => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Count) },
                Token::Arrange => { self.next(); Expr::Arrange(Box::new(lhs)) },
                Token::Negate => { self.next(); Expr::Negate(Box::new(lhs)) },
                Token::Filter => { self.next(); self.expect(&Token::LParen); let c = self.parse_condition(); self.expect(&Token::RParen); Expr::Filter(Box::new(lhs), c) },
                Token::EnterAt => { self.next(); self.expect(&Token::LParen); let f = self.parse_field(); self.expect(&Token::RParen); Expr::EnterAt(Box::new(lhs), f) },
                Token::Inspect => { self.next(); self.expect(&Token::LParen); let l = self.parse_ident(); self.expect(&Token::RParen); Expr::Inspect(Box::new(lhs), l) },
                other => panic!("Expected pipe operator, got {:?}", other),
            }
        }

        // key(proj) and map(proj) take projection without outer parens: key($0[0] ; $1[1])
        fn parse_projection(&mut self) -> Projection {
            self.expect(&Token::LParen);
            self.parse_projection_inner()
        }

        // Shared projection parsing (also used inside join where outer '(' is already consumed).
        fn parse_projection_inner(&mut self) -> Projection {
            if *self.peek() == Token::RParen { self.next(); return Projection { key: vec![], val: vec![] }; }
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

        fn parse_condition(&mut self) -> Condition {
            let l = self.parse_field();
            let op = self.next();
            let r = self.parse_field();
            match op {
                Token::EqEq => Condition::Eq(l, r),
                Token::NotEq => Condition::Ne(l, r),
                Token::Lt => Condition::Lt(l, r),
                Token::LtEq => Condition::Le(l, r),
                Token::Gt => Condition::Gt(l, r),
                Token::GtEq => Condition::Ge(l, r),
                o => panic!("Expected comparison operator, got {:?}", o),
            }
        }
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
        Variable,
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
            Reducer::Count => Arc::new(|_key, vals, output| { let count: Diff = vals.iter().map(|(_, d)| *d).sum(); if count > 0 { output.push((smallvec::smallvec![count], 1)); } }),
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
        let (l, r) = match cond {
            Condition::Eq(l, r) | Condition::Ne(l, r) | Condition::Lt(l, r)
            | Condition::Le(l, r) | Condition::Gt(l, r) | Condition::Ge(l, r) => (l, r),
        };
        let mut a = Row::new(); let mut b = Row::new();
        eval_field_into(l, inputs, &mut a); eval_field_into(r, inputs, &mut b);
        match cond {
            Condition::Eq(..) => a == b, Condition::Ne(..) => a != b,
            Condition::Lt(..) => a < b,  Condition::Le(..) => a <= b,
            Condition::Gt(..) => a > b,  Condition::Ge(..) => a >= b,
        }
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
                Node::Variable => {
                    let step: Product<u64, PointStampSummary<u64>> = Product::new(0, feedback_summary::<u64>(level, 1));
                    let (var, col) = VecVariable::new(scope, step);
                    nodes.insert(id, Rendered::Collection(col)); variables.insert(id, (var, level)); var_levels.insert(id, level);
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

fn run(name: &str, stmts: Vec<parse::Stmt>, n_inputs: usize, nodes: u64, edges: u64, arity: usize, batch: u64, rounds: Option<u64>) {
    let compiled = lower::lower(stmts);
    println!("{}: {} IR nodes, result = {}", name, compiled.nodes.len(), compiled.result);
    let name = name.to_string();
    let result_id = compiled.result;

    timely::execute_from_args(std::env::args().skip(4), move |worker| {
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

        let index = worker.index();
        let peers = worker.peers();

        // Initial load: insert edges 0..edges.
        for e in 0..edges {
            if (e as usize) % peers == index {
                let input_idx = (e as usize) % inputs.len();
                inputs[input_idx].update(gen_row(e, nodes, arity), 1);
            }
        }
        for i in inputs.iter_mut() { i.advance_to(1); i.flush(); }
        while probe.less_than(&1u64) { worker.step(); }
        let elapsed = std::time::Instant::now();
        println!("worker {}: {} loaded ({} edges)", index, name, edges);

        // Sliding window: each round removes and inserts `batch` edges.
        let mut cursor = 0u64;  // next edge index to remove/add
        let mut round = 0u64;
        let limit = rounds.unwrap_or(u64::MAX);
        while round < limit {
            let time = (round + 2) as u64;
            for _ in 0..batch {
                let remove_idx = cursor;
                let add_idx = edges + cursor;
                if (remove_idx as usize) % peers == index {
                    let input_idx = (remove_idx as usize) % inputs.len();
                    inputs[input_idx].update(gen_row(remove_idx, nodes, arity), -1);
                }
                if (add_idx as usize) % peers == index {
                    let input_idx = (add_idx as usize) % inputs.len();
                    inputs[input_idx].update(gen_row(add_idx, nodes, arity), 1);
                }
                cursor += 1;
            }
            for i in inputs.iter_mut() { i.advance_to(time); i.flush(); }
            while probe.less_than(&time) { worker.step(); }

            round += 1;
            if round % 100 == 0 {
                println!("worker {}: {} round {} ({:.2?})", index, name, round, elapsed.elapsed());
            }
        }
        println!("worker {}: {} done ({} rounds, batch {}, {:.2?})", index, name, round, batch, elapsed.elapsed());
    }).unwrap();
}

fn load_program(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("Cannot read {}: {}", path, e))
}

/// Deterministic hash: maps an index to a pseudorandom u64.
fn hash_u64(index: u64) -> u64 {
    // splitmix64
    let mut x = index.wrapping_mul(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

/// Generate one row: arity fields, each hash-derived, magnitude < nodes.
fn gen_row(edge_index: u64, nodes: u64, arity: usize) -> (Row, Row) {
    let mut key = Row::new();
    for col in 0..arity {
        let h = hash_u64(edge_index.wrapping_mul(31).wrapping_add(col as u64));
        key.push((h % nodes) as i64);
    }
    (key, Row::new())
}

const HELP: &str = r#"
DDIR: an interpreted language for iterative computations.

Usage: ddir_syntax <program> <arity> [nodes] [edges] [batch] [rounds]

  program        Path to a program file (.ddir or .ddp).
  arity          Fields per record.
  nodes          Max value for generated data (default: 10).
  edges          Number of records per input (default: 2*nodes).
  batch          Insertions and removals per round (default: 1).
  rounds         Sliding window rounds (default: 0 = one-shot).
                 Each round removes `batch` oldest edges and inserts `batch` new ones.

Records are generated with all fields in the key and an empty value.
Each record is hash(i) for i in 0..edges, reproducible by index.
Set DDIR_PRINT=1 to print output records.

TWO CONCRETE SYNTAXES

  .ddir files use an applicative syntax: MAP(e, proj), JOIN(e1, e2, proj), ...
  .ddp  files use a pipe syntax:        e | key(proj), e | join(e2, proj), ...

  Both syntaxes share the same semantics, IR, and renderer.

LANGUAGE REFERENCE (pipe syntax, .ddp)

  A program computes over collections of (key, val) records, where keys
  and values are tuples of integers. Each record has an integer multiplicity.

  Comments:
    -- This is a comment (to end of line).

  Statements:
    let name = expr;           Bind a collection.
    var name = expr;           Iterative variable (converges to equal its definition).
    name: { ... }              Iterative scope.
    result expr;               Designate output.

  Expressions:
    input n                    External input.
    expr | key(k ; v)          Rearrange fields (alias: map).
    expr | join(e2, (k ; v))   Equijoin on key.
    expr | min                 Reduce by key: keep minimum val.
    expr | distinct            Reduce by key: deduplicate.
    expr | count               Reduce by key: count records.
    expr | arrange             Consolidate: cancel offsetting updates.
    expr | filter(cond)        Keep records matching condition.
    expr | enter_at(field)     Delay introduction by field value.
    expr | inspect(label)      Print updates, pass through.
    a + b                      Union (multiplicities sum).
    a - b                      Subtract (a plus negated b).
    name                       Reference a binding.
    scope::name                Reference a binding from inside a scope.

  Projections (in key and join):
    $0, $1, $2       Whole row (key: $0=key, $1=val; join: $0=key, $1=left, $2=right).
    $n[i]            Element i of row n.
    42, -1           Integer constants.
    (k1, k2 ; v1)   Semicolon separates key fields from value fields.

  Semantics:
    Multiplicities are tracked per (key, val) pair. Two records with the
    same key but different values are independent.

    A var in a scope starts empty and iterates toward a fixed point where
    its value equals its definition. Variable definitions should include
    arrange to ensure convergence; without it, offsetting updates may
    circulate indefinitely.

    Statements within a scope are unordered — execution follows data
    dependencies, not textual order.

  Cost model:
    + and - (concat/negate) are lightweight (no data reorganization).

    join and reduce (min, distinct, count) require arranged input,
    indexed by key. If their input is not already arranged, they will
    arrange it internally. Reduce also produces arranged output.

    arrange produces reusable indexed output. When the same collection
    is used as input to multiple joins or reduces, an explicit arrange
    avoids redundant indexing.

    The key determines grouping for join and reduce. Choose your key/val
    split based on what you need to group by; carry extra fields in the
    val to avoid costly joins to recover them later.

EXAMPLE (strongly connected components, scc.ddp):

    let edges = input 0 | key($0[0] ; $0[1]);
    let trans = edges | key($1 ; $0);

    outer: {
        let scc = edges - trim;

        fwd: {
            let nodes = edges | key($1 ; $1) | enter_at($1[0]);
            let labels = proposals + nodes | min;
            var proposals = labels | join(scc, ($2 ; $1));
        }

        let fwd_arr = fwd::labels | arrange;
        let fwd_kept = edges
            | join(fwd_arr, ($1 ; $0, $2))
            | join(fwd_arr, ($0 ; $1, $2))
            | filter($1[1] == $1[2])
            | key($0 ; $1[0]);

        bwd: {
            let nodes = trans | key($1 ; $1) | enter_at($1[0]);
            let labels = proposals + nodes | min;
            var proposals = labels | join(fwd_kept, ($2 ; $1));
        }

        let bwd_arr = bwd::labels | arrange;
        let bwd_kept = trans
            | join(bwd_arr, ($1 ; $0, $2))
            | join(bwd_arr, ($0 ; $1, $2))
            | filter($1[1] == $1[2])
            | key($0 ; $1[0]);

        var trim = edges - bwd_kept;
    }

    result outer::scc;
"#;

fn main() {
    let program = std::env::args().nth(1).unwrap_or_else(|| { print!("{}", HELP); std::process::exit(0); });
    let arity: usize = std::env::args().nth(2).unwrap_or("2".into()).parse().unwrap();
    let nodes: u64 = std::env::args().nth(3).unwrap_or("10".into()).parse().unwrap();
    let edges: u64 = std::env::args().nth(4).unwrap_or_else(|| (2 * nodes).to_string()).parse().unwrap();
    let batch: u64 = std::env::args().nth(5).unwrap_or("1".into()).parse().unwrap();
    let rounds: Option<u64> = std::env::args().nth(6).map(|s| s.parse().unwrap());

    let source = load_program(&program);

    // Select parser based on file extension.
    let stmts = if program.ends_with(".ddp") {
        pipe::parse(&source)
    } else {
        parse::parse(&source)
    };
    let n_inputs = count_inputs(&stmts);

    let name = std::path::Path::new(&program).file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or(program.clone());
    run(&name, stmts, n_inputs, nodes, edges, arity, batch, rounds);
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
