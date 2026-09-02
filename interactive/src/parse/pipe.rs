//! Pipe syntax parser for .ddp files — and the surface-language reference.
//!
//! A program is a sequence of statements; an expression is a source piped
//! through operators (`expr | op | op`). Two layers: the *collection* language
//! (the dataflow graph) and the *scalar* language ([`Term`], per-row value
//! computation over [`crate::ir::Value`]).
//!
//! # Collection language
//!
//! Sources: `input N` (positional input), `import "name"` (named trace),
//! `name` (a `let`/`var` in scope), `scope::field` (a child scope's export).
//! Operators chain with `|`:
//!
//! - `| key(k… ; v…)` — reshape to `(key ; val)`; `map` is an alias.
//! - `| join(other, (k… ; v…))` — equijoin on the key.
//! - `| min` / `| distinct` / `| count` / `| collect` — reduce; `collect` is
//!   NEST (gather a key's values into a `List`).
//! - `| filter(term)` — keep rows where `term` is truthy (a nonzero `Int`).
//! - `| flatmap(term)` — UNNEST: explode a `List`-valued `term` into one row
//!   per element, value `tuple(pos, element)`.
//! - `| negate` · `| arrange` · `| inspect(label)` · `| enter_at(term)` ·
//!   `| lift_iter`.
//! - `a + b` (concat), `a - b` (concat with `b` negated).
//!
//! Statements: `let x = …;`, `var x = …;` (a feedback variable, for
//! recursion), `name: { … }` (a nested scope), `export "name" = …;` (a
//! program output, root scope only). `type Name = V0 shape? | V1 shape? …;`
//! declares a sum type (see below); it is parse-time only and emits no IR.
//!
//! # Scalar language (`Term`)
//!
//! Evaluated against an environment of input rows: linear ops bind `$0` = key,
//! `$1` = value; a join binds `$0` = key, `$1` = left value, `$2` = right value.
//!
//! - Access: `$n` is a whole row — in a projection it *splices* the row's
//!   fields; `$n[i]` selects field `i`; chains as `$n[i][j]`.
//! - Arithmetic / compare / logic: `+ - *`, `== != < <= > >=`, `&&`,
//!   `or(a, b)`, `not(x)`, unary `-x`.
//! - Products: `tuple(a, …)`; index with `v[i]` or `proj(v, i)`; `len(v)`.
//! - Lists: `list(a, …)`; eliminated by `flatmap` / `collect` / `fold`.
//! - Sums: every sum is a declared type, `type Size = Small u64 | Big (u64, u64)
//!   | Empty;` — tags are positions, scoped to the type; a payload shape is
//!   `u64`/`int`, `()` (the default when omitted), `(a, b, …)`, `List(a)`,
//!   `Option(a)`, `Result(a, b)`, or an earlier type's name. A constructor call
//!   `Small(x)` / `Big(a, b)` / `Empty` builds the sum (`Type::Ctor` when two
//!   types share a name); `variant(Type, tag, payload)` takes a data-driven tag
//!   (every lane must then share the payload's shape); `istag(tag, v)` tests.
//!   The built-ins `Some(x)`/`None` and `Ok(x)`/`Err(e)` build `Option`/`Result`;
//!   a `None`/`Ok`/`Err` learns its other lane from the other branch of an `if`
//!   or the other arms of a `case`.
//! - Pattern match: `case scrut { Ctor(a, b) => arm, …, _ => default }` — an arm
//!   names the payload's tuple fields, or the whole payload with one name.
//! - Fold: `fold(list, init, step)` — in `step`, `^0` is the element and `^1`
//!   the accumulator.
//! - Binders: `^k` refers to the k-th enclosing `case`/`fold` binder (de
//!   Bruijn, innermost = 0); `case` patterns also bind payload fields by name.
//! - Conditional: `if(cond, then, els)`.
//!
//! The applicative front-end ([`super::applicative`], `.ddir` files) parses the
//! same scalar grammar with an S-expression operator syntax (`MAP`, `JOIN`, …).

use super::*;

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Let, Var, Export, Type, Case, Fold,
    Input, Import, Key, Map, Join, Min, Distinct, Count, Collect, Arrange, Negate, Filter, EnterAt, LiftIter, FlatMap, Inspect,
    Ident(String), Int(i64), Str(String),
    Dollar, Caret, LParen, RParen, LBrace, RBrace, LBracket, RBracket,
    Comma, Semi, Colon, ColonColon, Eq, EqEq, NotEq, Lt, LtEq, Gt, GtEq, AndAnd, FatArrow,
    Pipe, Plus, Minus, Star, Eof,
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
            '&' => { chars.next(); if chars.peek() == Some(&'&') { chars.next(); tokens.push(Token::AndAnd); } else { panic!("Expected && after &"); } },
            '+' => { chars.next(); tokens.push(Token::Plus); },
            '*' => { chars.next(); tokens.push(Token::Star); },
            '^' => { chars.next(); tokens.push(Token::Caret); },
            '=' => { chars.next(); match chars.peek() { Some(&'=') => { chars.next(); tokens.push(Token::EqEq); }, Some(&'>') => { chars.next(); tokens.push(Token::FatArrow); }, _ => tokens.push(Token::Eq) } },
            '!' => { chars.next(); if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::NotEq); } else { panic!("Expected != after !"); } },
            '<' => { chars.next(); if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::LtEq); } else { tokens.push(Token::Lt); } },
            '>' => { chars.next(); if chars.peek() == Some(&'=') { chars.next(); tokens.push(Token::GtEq); } else { tokens.push(Token::Gt); } },
            '$' => { chars.next(); tokens.push(Token::Dollar); },
            '"' => {
                // String literal: the quoted name in `import "..."` / `export "..." = ...`.
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
                    "let" => Token::Let, "var" => Token::Var, "export" => Token::Export,
                    "type" => Token::Type, "case" => Token::Case, "fold" => Token::Fold,
                    "input" => Token::Input, "import" => Token::Import,
                    "key" => Token::Key, "map" => Token::Map,
                    "join" => Token::Join, "min" => Token::Min, "distinct" => Token::Distinct,
                    "count" => Token::Count, "collect" => Token::Collect,
                    "flatmap" => Token::FlatMap,
                    "arrange" => Token::Arrange, "negate" => Token::Negate,
                    "filter" => Token::Filter, "enter_at" => Token::EnterAt, "inspect" => Token::Inspect,
                    "lift_iter" => Token::LiftIter,
                    _ => Token::Ident(ident),
                });
            },
            other => panic!("Unexpected character: {:?}", other),
        }
    }
    tokens.push(Token::Eof);
    tokens
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    /// Declared sum types: name -> the variants in tag order, each with its payload shape.
    /// Populated by `type` decls; tags are positions, scoped to their type.
    types: std::collections::HashMap<String, Vec<(String, corgi::Shape)>>,
    /// Constructor name -> every (type, tag) that declares it, for unqualified use. A name
    /// declared by two types must be written `Type::Name`.
    ctors: std::collections::HashMap<String, Vec<(String, usize)>>,
    /// In-scope pattern binders, innermost last: (name, binder-depth, field). A use resolves to
    /// `Bound(cur_depth - binder_depth)`, projected to `field` when the payload is a tuple whose
    /// fields the pattern named, so the de Bruijn index tracks nesting (a fold/case between
    /// binding and use).
    binders: Vec<(String, usize, Option<usize>)>,
    /// Number of `case`/`fold` binders currently in scope.
    depth: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, pos: 0, types: Default::default(), ctors: Default::default(), binders: Vec::new(), depth: 0 }
    }
    fn peek(&self) -> &Token { &self.tokens[self.pos] }
    fn next(&mut self) -> Token { let t = self.tokens[self.pos].clone(); self.pos += 1; t }
    fn expect(&mut self, expected: &Token) { let t = self.next(); assert_eq!(&t, expected, "Expected {:?}, got {:?}", expected, t); }

    fn parse_program(&mut self) -> Vec<Stmt> {
        let mut stmts = Vec::new();
        while *self.peek() != Token::Eof && *self.peek() != Token::RBrace {
            // `type Name = V0 shape? | V1 shape? | …;` is a parse-time declaration (no IR): the
            // sum universe. Tags are positions; an omitted payload shape is `()`.
            if *self.peek() == Token::Type {
                self.next();
                let name = self.parse_ident();
                self.expect(&Token::Eq);
                let mut variants = vec![self.parse_variant_decl()];
                while *self.peek() == Token::Pipe {
                    self.next();
                    variants.push(self.parse_variant_decl());
                }
                self.expect(&Token::Semi);
                assert!(!self.types.contains_key(&name), "type `{name}` declared twice");
                for (tag, (v, _)) in variants.iter().enumerate() {
                    assert!(!variants[..tag].iter().any(|(w, _)| w == v), "type `{name}` declares `{v}` twice");
                    self.ctors.entry(v.clone()).or_default().push((name.clone(), tag));
                }
                self.types.insert(name, variants);
                continue;
            }
            stmts.push(self.parse_stmt());
        }
        stmts
    }

    /// Resolve a bare name to a pattern binder, if any.
    fn resolve_binder(&self, name: &str) -> Option<Term> {
        self.binders.iter().rev().find(|(n, _, _)| n == name).map(|&(_, level, field)| {
            let bound = Term::Bound(self.depth - level);
            match field {
                Some(f) => Term::Proj(Box::new(bound), f),
                None => bound,
            }
        })
    }

    /// One `Name shape?` of a `type` declaration.
    fn parse_variant_decl(&mut self) -> (String, corgi::Shape) {
        let name = self.parse_ident();
        let shape = match self.peek() {
            Token::Pipe | Token::Semi => corgi::Shape::Unit,
            _ => self.parse_shape(),
        };
        (name, shape)
    }

    /// A payload shape: `u64`/`int`, `()`, `(a, b, ..)`, `List(a)`, `Option(a)`, `Result(a, b)`,
    /// or an earlier `type`'s name (so sums nest; never recursively).
    fn parse_shape(&mut self) -> corgi::Shape {
        use corgi::Shape;
        match self.next() {
            Token::LParen => {
                if *self.peek() == Token::RParen {
                    self.next();
                    return Shape::Unit;
                }
                let mut fields = vec![self.parse_shape()];
                while *self.peek() == Token::Comma {
                    self.next();
                    fields.push(self.parse_shape());
                }
                self.expect(&Token::RParen);
                Shape::Prod(fields)
            }
            Token::Ident(k) => match k.as_str() {
                "u64" | "int" => Shape::Prim(64),
                "List" => {
                    self.expect(&Token::LParen);
                    let inner = self.parse_shape();
                    self.expect(&Token::RParen);
                    Shape::List(Box::new(inner))
                }
                "Option" => {
                    self.expect(&Token::LParen);
                    let inner = self.parse_shape();
                    self.expect(&Token::RParen);
                    Shape::Sum(vec![Shape::Unit, inner])
                }
                "Result" => {
                    self.expect(&Token::LParen);
                    let ok = self.parse_shape();
                    self.expect(&Token::Comma);
                    let err = self.parse_shape();
                    self.expect(&Token::RParen);
                    Shape::Sum(vec![ok, err])
                }
                name => Shape::Sum(self.type_lanes(name)),
            },
            other => panic!("expected a shape, got {:?}", other),
        }
    }

    /// The lane shapes of a declared type.
    fn type_lanes(&self, name: &str) -> Vec<corgi::Shape> {
        self.types
            .get(name)
            .unwrap_or_else(|| panic!("unknown type `{name}`"))
            .iter()
            .map(|(_, s)| s.clone())
            .collect()
    }

    /// Resolve a constructor name (optionally qualified by its type) to the sum it builds, its
    /// tag, and its declared payload shape (`None` for a built-in whose lane the payload fixes).
    fn resolve_ctor(&self, ty: Option<&str>, name: &str) -> Option<(SumTy, usize, Option<corgi::Shape>)> {
        let (ty, tag) = match ty {
            Some(t) => {
                let vs = self.types.get(t).unwrap_or_else(|| panic!("unknown type `{t}`"));
                let tag = vs.iter().position(|(v, _)| v == name).unwrap_or_else(|| panic!("type `{t}` has no constructor `{name}`"));
                (t.to_string(), tag)
            }
            None => match self.ctors.get(name).map(Vec::as_slice) {
                Some([(t, tag)]) => (t.clone(), *tag),
                Some(many) => panic!(
                    "constructor `{name}` is declared by {} types ({}); qualify it as `Type::{name}`",
                    many.len(),
                    many.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>().join(", ")
                ),
                None => {
                    // the built-ins, unless a declaration shadows them.
                    return match name {
                        "None" => Some((SumTy::Option, 0, Some(corgi::Shape::Unit))),
                        "Some" => Some((SumTy::Option, 1, None)),
                        "Ok" => Some((SumTy::Result, 0, None)),
                        "Err" => Some((SumTy::Result, 1, None)),
                        _ => None,
                    };
                }
            },
        };
        let shape = self.types[&ty][tag].1.clone();
        Some((SumTy::Declared(self.type_lanes(&ty)), tag, Some(shape)))
    }

    /// A constructor's payload from its call arguments: a tuple-shaped lane takes one argument
    /// per field (or one tuple), a unit lane takes none, any other lane takes exactly one.
    fn ctor_payload(name: &str, shape: Option<&corgi::Shape>, mut args: Vec<Term>) -> Term {
        match shape {
            Some(corgi::Shape::Unit) => {
                assert!(args.is_empty(), "constructor `{name}` takes no payload, got {} args", args.len());
                Term::Tuple(Vec::new())
            }
            Some(corgi::Shape::Prod(fs)) if args.len() == fs.len() && fs.len() != 1 => Term::Tuple(args),
            _ => {
                assert_eq!(args.len(), 1, "constructor `{name}` takes one payload, got {} args", args.len());
                args.pop().unwrap()
            }
        }
    }

    fn parse_stmt(&mut self) -> Stmt {
        match self.peek().clone() {
            Token::Let => { self.next(); let n = self.parse_ident(); self.expect(&Token::Eq); let e = self.parse_pipe_expr(); self.expect(&Token::Semi); Stmt::Let(n, e) },
            Token::Var => { self.next(); let n = self.parse_ident(); self.expect(&Token::Eq); let e = self.parse_pipe_expr(); self.expect(&Token::Semi); Stmt::Var(n, e) },
            Token::Export => {
                self.next();
                let name = match self.next() {
                    Token::Str(s) => s,
                    o => panic!("Expected string literal after `export`, got {:?}", o),
                };
                self.expect(&Token::Eq);
                let e = self.parse_pipe_expr();
                self.expect(&Token::Semi);
                Stmt::Export(name, e)
            },
            Token::Ident(_) => {
                let n = self.parse_ident(); self.expect(&Token::Colon);
                self.expect(&Token::LBrace); let b = self.parse_program(); self.expect(&Token::RBrace); Stmt::Scope(n, b)
            },
            other => panic!("Unexpected token: {:?}", other),
        }
    }

    fn parse_ident(&mut self) -> String { match self.next() { Token::Ident(s) => s, other => panic!("Expected ident, got {:?}", other) } }

    fn parse_pipe_expr(&mut self) -> Expr {
        let mut expr = self.parse_concat_expr();
        while *self.peek() == Token::Pipe {
            self.next();
            expr = self.parse_pipe_op(expr);
        }
        expr
    }

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
            Token::Import => { self.next(); match self.next() { Token::Str(s) => Expr::Import(s), o => panic!("Expected string literal after `import`, got {:?}", o) } },
            Token::Ident(_) => { let n = self.parse_ident(); if *self.peek() == Token::ColonColon { self.next(); let f = self.parse_ident(); Expr::Qualified(n, f) } else { Expr::Name(n) } },
            Token::LParen => { self.next(); let e = self.parse_pipe_expr(); self.expect(&Token::RParen); e },
            other => panic!("Unexpected token in atom: {:?}", other),
        }
    }

    fn parse_join_arg(&mut self) -> Expr {
        let mut expr = self.parse_atom();
        while *self.peek() == Token::Pipe { self.next(); expr = self.parse_pipe_op(expr); }
        expr
    }

    fn parse_pipe_op(&mut self, lhs: Expr) -> Expr {
        match self.peek().clone() {
            Token::Key => { self.next(); let p = self.parse_projection(); Expr::Map(Box::new(lhs), p) },
            Token::Map => { self.next(); let p = self.parse_projection(); Expr::Map(Box::new(lhs), p) },
            Token::Join => { self.next(); self.expect(&Token::LParen); let r = self.parse_join_arg(); self.expect(&Token::Comma); let p = self.parse_projection(); self.expect(&Token::RParen); Expr::Join(Box::new(lhs), Box::new(r), p) },
            Token::Min => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Min) },
            Token::Distinct => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Distinct) },
            Token::Count => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Count) },
            Token::Collect => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Collect) },
            Token::FlatMap => { self.next(); self.expect(&Token::LParen); let l = self.parse_term(); self.expect(&Token::RParen); Expr::FlatMap(Box::new(lhs), l) },
            Token::Arrange => { self.next(); Expr::Arrange(Box::new(lhs)) },
            Token::Negate => { self.next(); Expr::Negate(Box::new(lhs)) },
            Token::Filter => { self.next(); self.expect(&Token::LParen); let c = self.parse_term(); self.expect(&Token::RParen); Expr::Filter(Box::new(lhs), c) },
            Token::EnterAt => { self.next(); self.expect(&Token::LParen); let f = self.parse_term(); self.expect(&Token::RParen); Expr::EnterAt(Box::new(lhs), f) },
            Token::LiftIter => { self.next(); Expr::LiftIter(Box::new(lhs)) },
            Token::Inspect => { self.next(); self.expect(&Token::LParen); let l = self.parse_ident(); self.expect(&Token::RParen); Expr::Inspect(Box::new(lhs), l) },
            other => panic!("Expected pipe operator, got {:?}", other),
        }
    }

    fn parse_projection(&mut self) -> Projection {
        self.expect(&Token::LParen);
        self.parse_projection_inner()
    }

    // A projection `(k1, k2 ; v1, v2)` builds `key = tuple(k1, k2)` and
    // `val = tuple(v1, v2)`. A bare `$n` field splices the whole input row's
    // fields (`Spread`), matching the flat-row concatenation of the original
    // `[i64]` model; any other field is one (possibly nested) element.
    fn parse_projection_inner(&mut self) -> Projection {
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

    /// Parse a comma-separated list of projection fields up to (not consuming)
    /// any token in `terminators`.
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

    // ---- General scalar term grammar (shared shape with applicative.rs). ----

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
            Token::Case => self.parse_pattern_case(),
            Token::Fold => self.parse_fold(),
            Token::Ident(name) => {
                self.next();
                // `Type::Ctor` names a constructor by its type.
                let (ty, name) = if *self.peek() == Token::ColonColon {
                    self.next();
                    (Some(name), self.parse_ident())
                } else {
                    (None, name)
                };
                if let Some(binder) = ty.is_none().then(|| self.resolve_binder(&name)).flatten() {
                    binder
                } else if let Some((sum, tag, shape)) = self.resolve_ctor(ty.as_deref(), &name) {
                    // Constructor: `Ctor(a, b)` / `Ctor(x)` / bare `Ctor` => inject into its type.
                    let args = if *self.peek() == Token::LParen { self.parse_args() } else { Vec::new() };
                    let payload = Self::ctor_payload(&name, shape.as_ref(), args);
                    Term::Inject { tag: Box::new(Term::Int(tag as i64)), payload: Box::new(payload), sum }
                } else if *self.peek() == Token::LParen {
                    self.parse_builtin(&name)
                } else {
                    panic!("unknown name in term: `{}` (not a binder, constructor, or builtin)", name)
                }
            }
            other => panic!("Unexpected token in term: {:?}", other),
        };
        // Postfix field projection: `base[i]` on any primary ($n, a binder, a
        // parenthesized term, …), chaining `base[i][j]`.
        while *self.peek() == Token::LBracket {
            self.next();
            let i = match self.next() { Token::Int(i) => i as usize, o => panic!("Expected int index, got {:?}", o) };
            self.expect(&Token::RBracket);
            base = Term::Proj(Box::new(base), i);
        }
        base
    }

    /// `fold(list, init, step)` — `step` sees `^0` = element, `^1` = accumulator.
    /// Parsed depth-aware so named (case) binders used inside resolve correctly.
    fn parse_fold(&mut self) -> Term {
        self.expect(&Token::Fold);
        self.expect(&Token::LParen);
        let list = Box::new(self.parse_term());
        self.expect(&Token::Comma);
        let init = Box::new(self.parse_term());
        self.expect(&Token::Comma);
        self.depth += 2;                       // acc, element
        let step = Box::new(self.parse_term());
        self.depth -= 2;
        self.expect(&Token::RParen);
        Term::Fold { list, init, step }
    }

    /// `case scrut { Ctor(a, b) => arm, …, _ => default }` — pattern match on a
    /// declared constructor, binding its payload fields by name in the arm.
    fn parse_pattern_case(&mut self) -> Term {
        self.expect(&Token::Case);
        let scrutinee = Box::new(self.parse_term());
        self.expect(&Token::LBrace);
        let mut tagged: Vec<(usize, Term)> = Vec::new();
        let mut default: Option<Box<Term>> = None;
        let mut lanes: Option<usize> = None; // the matched type's arity, from its first arm
        while *self.peek() != Token::RBrace {
            let name = self.parse_ident();
            if name == "_" {
                self.expect(&Token::FatArrow);
                default = Some(Box::new(self.parse_term()));
            } else {
                let (ty, name) = if *self.peek() == Token::ColonColon {
                    self.next();
                    (Some(name), self.parse_ident())
                } else {
                    (None, name)
                };
                let (sum, tag, shape) = self
                    .resolve_ctor(ty.as_deref(), &name)
                    .unwrap_or_else(|| panic!("unknown constructor in pattern: `{}`", name));
                match &lanes {
                    None => lanes = Some(match &sum { SumTy::Declared(ls) => ls.len(), SumTy::Option | SumTy::Result => 2 }),
                    Some(n) => assert_eq!(*n, match &sum { SumTy::Declared(ls) => ls.len(), _ => 2 }, "case arms mix types (`{name}`)"),
                }
                let mut names = Vec::new();
                if *self.peek() == Token::LParen {
                    self.next();
                    if *self.peek() != Token::RParen {
                        names.push(self.parse_ident());
                        while *self.peek() == Token::Comma { self.next(); names.push(self.parse_ident()); }
                    }
                    self.expect(&Token::RParen);
                }
                // The binders: one per field of a tuple payload (each a projection), else the
                // one name is the payload itself; a unit payload binds nothing.
                let fields: Vec<Option<usize>> = match shape {
                    Some(corgi::Shape::Unit) => {
                        assert!(names.is_empty(), "pattern `{name}` binds nothing, got {} names", names.len());
                        Vec::new()
                    }
                    Some(corgi::Shape::Prod(fs)) if names.len() == fs.len() && fs.len() != 1 => (0..fs.len()).map(Some).collect(),
                    _ => {
                        assert_eq!(names.len(), 1, "pattern `{name}` binds its one payload, got {} names", names.len());
                        vec![None]
                    }
                };
                self.expect(&Token::FatArrow);
                self.depth += 1;
                let level = self.depth;
                for (n, f) in names.iter().zip(fields) { self.binders.push((n.clone(), level, f)); }
                let arm = self.parse_term();
                for _ in 0..names.len() { self.binders.pop(); }
                self.depth -= 1;
                tagged.push((tag, arm));
            }
            if *self.peek() == Token::Comma { self.next(); }
        }
        self.expect(&Token::RBrace);
        // `Case` indexes arms by tag over the WHOLE type; a missing arm takes the default,
        // which must then exist.
        let n = lanes.unwrap_or_else(|| panic!("case needs at least one constructor arm"));
        let mut arms: Vec<Term> = Vec::new();
        for t in 0..n {
            match tagged.iter().find(|(tt, _)| *tt == t) {
                Some((_, arm)) => arms.push(arm.clone()),
                None => match &default {
                    Some(d) => arms.push((**d).clone()),
                    None => panic!("case has no arm for tag {} and no `_` default", t),
                },
            }
        }
        Term::Case { scrutinee, arms, default }
    }

    /// Parse the argument list `( t , t , ... )` of a builtin call.
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

    /// Function-call style ADT operators: `tuple`, `list`, `variant`, `case`,
    /// `fold`, `proj`, `len`, `istag`, `not`, `or`, `if`.
    fn parse_builtin(&mut self, name: &str) -> Term {
        if name == "variant" || name == "inject" {
            // `variant(Type, tag, payload)`: a data-driven tag into a declared type. Every lane
            // of the type must share the payload's shape (the columnar form is a demux).
            self.expect(&Token::LParen);
            let ty = self.parse_ident();
            let lanes = self.type_lanes(&ty);
            self.expect(&Token::Comma);
            let tag = Box::new(self.parse_term());
            self.expect(&Token::Comma);
            let payload = Box::new(self.parse_term());
            self.expect(&Token::RParen);
            return Term::Inject { tag, payload, sum: SumTy::Declared(lanes) };
        }
        let mut args = self.parse_args();
        super::build_builtin(name, &mut args)
    }
}

pub fn parse(input: &str) -> Vec<Stmt> { let tokens = tokenize(input); let mut p = Parser::new(tokens); p.parse_program() }

/// Parse a single scalar [`Term`] from a string — the same expression language
/// used in `filter`/`map`/`flatmap` arguments. A *closed* term (no `$n` input
/// references) evaluates to a constant [`crate::ir::Value`], which is how the
/// server accepts structured (ADT) values on input.
pub fn parse_term(input: &str) -> Term { let tokens = tokenize(input); let mut p = Parser::new(tokens); p.parse_term() }
