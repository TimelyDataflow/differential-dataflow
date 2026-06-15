//! Applicative syntax parser for .ddir files.
//!
//! Syntax: `MAP(e, proj)`, `JOIN(e1, e2, proj)`, `REDUCE(e, MIN)`, etc.

use super::*;

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Let, Var, Scope, Export,
    Input, Import, Map, Join, Reduce, Concat, Arrange, Filter, Negate, EnterAt, LiftIter, FlatMap, Inspect,
    Min, Distinct, Count, Collect,
    Ident(String), Int(i64), Str(String),
    Dollar, Caret, LParen, RParen, LBrace, RBrace, LBracket, RBracket,
    Comma, Semi, Colon, ColonColon, Eq, EqEq, NotEq, Lt, LtEq, Gt, GtEq, AndAnd,
    Plus, Minus, Star, Eof,
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
            '=' => { chars.next(); if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::EqEq); } else { tokens.push(Token::Eq); } },
            '!' => { chars.next(); if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::NotEq); } else { panic!("Expected != after !"); } },
            '<' => { chars.next(); if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::LtEq); } else { tokens.push(Token::Lt); } },
            '>' => { chars.next(); if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::GtEq); } else { tokens.push(Token::Gt); } },
            '&' => { chars.next(); if chars.peek() == Some(&'&') { chars.next(); tokens.push(Token::AndAnd); } else { panic!("Expected && after &"); } },
            '+' => { chars.next(); tokens.push(Token::Plus); },
            '*' => { chars.next(); tokens.push(Token::Star); },
            '$' => { chars.next(); tokens.push(Token::Dollar); },
            '^' => { chars.next(); tokens.push(Token::Caret); },
            '"' => {
                // String literal: the quoted name in `IMPORT "..."` / `EXPORT "..." = ...`.
                // Names carry no escapes, so read verbatim up to the closing quote.
                chars.next();
                let mut s = String::new();
                let mut closed = false;
                while let Some(c) = chars.next() {
                    if c == '"' { closed = true; break; }
                    s.push(c);
                }
                if !closed { panic!("Unterminated string literal"); }
                tokens.push(Token::Str(s));
            },
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
                    "let" => Token::Let, "var" => Token::Var, "scope" => Token::Scope,
                    "export" => Token::Export,
                    "INPUT" => Token::Input, "IMPORT" => Token::Import,
                    "MAP" => Token::Map, "JOIN" => Token::Join,
                    "REDUCE" => Token::Reduce, "CONCAT" => Token::Concat, "ARRANGE" => Token::Arrange,
                    "FILTER" => Token::Filter, "NEGATE" => Token::Negate, "ENTER_AT" => Token::EnterAt, "INSPECT" => Token::Inspect,
                    "MIN" => Token::Min, "DISTINCT" => Token::Distinct, "COUNT" => Token::Count,
                    "COLLECT" => Token::Collect,
                    "LIFT_ITER" => Token::LiftIter, "FLATMAP" => Token::FlatMap,
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
            Token::Let => { self.next(); let n = self.parse_ident(); self.expect(&Token::Eq); let e = self.parse_expr(); self.expect(&Token::Semi); Stmt::Let(n, e) },
            Token::Var => { self.next(); let n = self.parse_ident(); self.expect(&Token::Eq); let e = self.parse_expr(); self.expect(&Token::Semi); Stmt::Var(n, e) },
            Token::Export => {
                self.next();
                let name = match self.next() {
                    Token::Str(s) => s,
                    o => panic!("Expected string literal after `export`, got {:?}", o),
                };
                self.expect(&Token::Eq);
                let e = self.parse_expr();
                self.expect(&Token::Semi);
                Stmt::Export(name, e)
            },
            Token::Ident(_) => {
                let n = self.parse_ident(); self.expect(&Token::Colon);
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
            Token::Import => { self.next(); match self.next() { Token::Str(s) => Expr::Import(s), o => panic!("Expected string literal after IMPORT, got {:?}", o) } },
            Token::Map => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::Comma); let p = self.parse_projection(); self.expect(&Token::RParen); Expr::Map(Box::new(i), p) },
            Token::Join => { self.next(); self.expect(&Token::LParen); let l = self.parse_expr(); self.expect(&Token::Comma); let r = self.parse_expr(); self.expect(&Token::Comma); let p = self.parse_projection(); self.expect(&Token::RParen); Expr::Join(Box::new(l), Box::new(r), p) },
            Token::Reduce => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::Comma); let r = self.parse_reducer(); self.expect(&Token::RParen); Expr::Reduce(Box::new(i), r) },
            Token::Filter => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::Comma); let c = self.parse_term(); self.expect(&Token::RParen); Expr::Filter(Box::new(i), c) },
            Token::Negate => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::RParen); Expr::Negate(Box::new(i)) },
            Token::EnterAt => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::Comma); let f = self.parse_term(); self.expect(&Token::RParen); Expr::EnterAt(Box::new(i), f) },
            Token::LiftIter => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::RParen); Expr::LiftIter(Box::new(i)) },
            Token::FlatMap => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::Comma); let l = self.parse_term(); self.expect(&Token::RParen); Expr::FlatMap(Box::new(i), l) },
            Token::Inspect => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::Comma); let label = self.parse_ident(); self.expect(&Token::RParen); Expr::Inspect(Box::new(i), label) },
            Token::Concat => { self.next(); self.expect(&Token::LParen); let mut v = vec![self.parse_expr()]; while *self.peek() == Token::Comma { self.next(); v.push(self.parse_expr()); } self.expect(&Token::RParen); Expr::Concat(v) },
            Token::Arrange => { self.next(); self.expect(&Token::LParen); let i = self.parse_expr(); self.expect(&Token::RParen); Expr::Arrange(Box::new(i)) },
            Token::Ident(_) => { let n = self.parse_ident(); if *self.peek() == Token::ColonColon { self.next(); let f = self.parse_ident(); Expr::Qualified(n, f) } else { Expr::Name(n) } },
            other => panic!("Unexpected token in expr: {:?}", other),
        }
    }

    // A projection `(k1, k2 ; v1, v2)` builds `key = tuple(k1, k2)` and
    // `val = tuple(v1, v2)`. A bare `$n` field splices the whole input row
    // (`Spread`); see `pipe.rs` for the rationale.
    fn parse_projection(&mut self) -> Projection {
        self.expect(&Token::LParen);
        let key = self.parse_field_list_until(&[Token::Semi, Token::RParen]);
        let val = if *self.peek() == Token::Semi {
            self.next();
            self.parse_field_list_until(&[Token::RParen])
        } else {
            vec![]
        };
        self.expect(&Token::RParen);
        Projection { key: Term::Tuple(key), val: Term::Tuple(val) }
    }

    fn parse_field_list_until(&mut self, terminators: &[Token]) -> Vec<Term> {
        if terminators.contains(self.peek()) { return vec![]; }
        let mut fields = vec![self.parse_proj_field()];
        while *self.peek() == Token::Comma { self.next(); fields.push(self.parse_proj_field()); }
        fields
    }

    /// A single projection field. A bare `$n` (no index) splices.
    fn parse_proj_field(&mut self) -> Term {
        if *self.peek() == Token::Dollar
            && matches!(self.tokens.get(self.pos + 1), Some(Token::Int(_)))
            && self.tokens.get(self.pos + 2) != Some(&Token::LBracket)
        {
            self.next();
            let n = match self.next() { Token::Int(n) => n as usize, o => panic!("Expected int, got {:?}", o) };
            return Term::Spread(Box::new(Term::Var(n)));
        }
        self.parse_term()
    }

    // ---- General scalar term grammar (shared shape with pipe.rs). ----

    fn parse_term(&mut self) -> Term { self.parse_logic() }

    fn parse_logic(&mut self) -> Term {
        let mut left = self.parse_cmp();
        while *self.peek() == Token::AndAnd {
            self.next();
            left = Term::Binary(BinOp::And, Box::new(left), Box::new(self.parse_cmp()));
        }
        left
    }

    fn parse_cmp(&mut self) -> Term {
        let left = self.parse_add();
        let op = match self.peek() {
            Token::EqEq => BinOp::Eq, Token::NotEq => BinOp::Ne,
            Token::Lt => BinOp::Lt, Token::LtEq => BinOp::Le,
            Token::Gt => BinOp::Gt, Token::GtEq => BinOp::Ge,
            _ => return left,
        };
        self.next();
        Term::Binary(op, Box::new(left), Box::new(self.parse_add()))
    }

    fn parse_add(&mut self) -> Term {
        let mut left = self.parse_mul();
        loop {
            let op = match self.peek() { Token::Plus => BinOp::Add, Token::Minus => BinOp::Sub, _ => break };
            self.next();
            left = Term::Binary(op, Box::new(left), Box::new(self.parse_mul()));
        }
        left
    }

    fn parse_mul(&mut self) -> Term {
        let mut left = self.parse_unary();
        while *self.peek() == Token::Star {
            self.next();
            left = Term::Binary(BinOp::Mul, Box::new(left), Box::new(self.parse_unary()));
        }
        left
    }

    fn parse_unary(&mut self) -> Term {
        if *self.peek() == Token::Minus { self.next(); return Term::Unary(UnOp::Neg, Box::new(self.parse_unary())); }
        self.parse_primary_term()
    }

    fn parse_primary_term(&mut self) -> Term {
        let mut base = match self.peek().clone() {
            Token::Dollar => {
                self.next();
                let n = match self.next() { Token::Int(n) => n as usize, o => panic!("Expected int after $, got {:?}", o) };
                Term::Var(n)
            }
            Token::Caret => {
                self.next();
                let k = match self.next() { Token::Int(k) => k as usize, o => panic!("Expected int after ^, got {:?}", o) };
                Term::Bound(k)
            }
            Token::Int(n) => { self.next(); Term::Int(n) }
            Token::Minus => { self.next(); Term::Unary(UnOp::Neg, Box::new(self.parse_unary())) }
            Token::LParen => { self.next(); let t = self.parse_term(); self.expect(&Token::RParen); t }
            Token::Ident(name) => { self.next(); self.parse_builtin(&name) }
            other => panic!("Unexpected token in term: {:?}", other),
        };
        // Postfix field projection: `base[i]`, chaining `base[i][j]`.
        while *self.peek() == Token::LBracket {
            self.next();
            let i = match self.next() { Token::Int(i) => i as usize, o => panic!("Expected int index, got {:?}", o) };
            self.expect(&Token::RBracket);
            base = Term::Proj(Box::new(base), i);
        }
        base
    }

    fn parse_args(&mut self) -> Vec<Term> {
        self.expect(&Token::LParen);
        let mut args = Vec::new();
        if *self.peek() != Token::RParen {
            args.push(self.parse_term());
            while *self.peek() == Token::Comma { self.next(); args.push(self.parse_term()); }
        }
        self.expect(&Token::RParen);
        args
    }

    fn parse_builtin(&mut self, name: &str) -> Term {
        let mut args = self.parse_args();
        super::build_builtin(name, &mut args)
    }

    fn parse_reducer(&mut self) -> Reducer { match self.next() { Token::Min => Reducer::Min, Token::Distinct => Reducer::Distinct, Token::Count => Reducer::Count, Token::Collect => Reducer::Collect, o => panic!("Expected reducer, got {:?}", o) } }
}

pub fn parse(input: &str) -> Vec<Stmt> { let tokens = tokenize(input); let mut p = Parser::new(tokens); p.parse_program() }
