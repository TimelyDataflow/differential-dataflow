//! Applicative syntax parser for .ddir files.
//!
//! Syntax: `MAP(e, proj)`, `JOIN(e1, e2, proj)`, `REDUCE(e, MIN)`, etc.

use super::*;

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Let, Var, Scope, Result,
    Input, Map, Join, Reduce, Concat, Arrange, Filter, Negate, EnterAt, Inspect,
    Min, Distinct, Count,
    Ident(String), Int(i64),
    Dollar, LParen, RParen, LBrace, RBrace, LBracket, RBracket,
    Comma, Semi, Colon, ColonColon, Eq, EqEq, NotEq, Lt, LtEq, Gt, GtEq,
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
            Token::Result => { self.next(); let e = self.parse_expr(); self.expect(&Token::Semi); Stmt::Result(e) },
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
    fn parse_reducer(&mut self) -> Reducer { match self.next() { Token::Min => Reducer::Min, Token::Distinct => Reducer::Distinct, Token::Count => Reducer::Count, o => panic!("Expected reducer, got {:?}", o) } }
}

pub fn parse(input: &str) -> Vec<Stmt> { let tokens = tokenize(input); let mut p = Parser::new(tokens); p.parse_program() }
