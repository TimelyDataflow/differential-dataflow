//! Pipe syntax parser for .ddp files — and the surface-language reference.
//!
//! A program is a sequence of statements; an expression is a source piped
//! through operators (`expr | op | op`). The *collection* language describes the
//! dataflow graph; each operator's per-row logic is a *slot*, a corgi lambda.
//!
//! # Collection language
//!
//! Sources: `input N : (key_shape, val_shape)` (positional input), `import "name"`
//! (named trace), `name` (a `let` or `set` in scope), `scope::field` (a child scope's
//! export). An input declares its shape, using the shape syntax below, as a table
//! declares its columns: the server refuses to install a program with an undeclared
//! input. The shape is the column encoding, also for empty lists and inactive sum
//! lanes, which a row cannot reveal. An `import` may declare a shape too, which must
//! match the shape its trace is published at. A fed row is asserted to have its
//! input's shape during execution, where a mismatch can panic a dataflow worker and
//! take down the shared server; there is no per-program failure isolation.
//! Operators chain with `|`; a slot is written `(pat -> body)`:
//!
//! - `| map((k, v) -> (k', v'))` — reshape each row to the `(key, val)` the lambda gives.
//! - `| value((k, v) -> v')` — replace the value, keeping the key.
//! - `| join(other, (k, v, v1) -> (k', v'))` — equijoin on the key.
//! - `| min` / `| distinct` / `| count` / `| collect` — reduce; `distinct` keeps each `(key, val)`
//!   record once, and `collect` is NEST (gather a key's values into a `List`).
//! - `| reduce((k, vals) -> out)` — reduce by a lambda: `vals` is the key's values with their
//!   non-zero diffs, a `List` of `(val, diff)` in value order, and `out` the key's output, a
//!   `List` of `(val, diff)`.
//! - `| filter((k, v) -> mask)` — keep rows where the lambda gives a nonzero `Int`.
//! - `| flatmap((k, v) -> list)` — UNNEST: explode the `List` the lambda gives into one row per
//!   element, value `(pos, element)`.
//! - `| negate` · `| arrange` · `| inspect(label)` · `| enter_at((k, v) -> round)` ·
//!   `| lift_iter`.
//! - `| lift` — pair each value with its whole time, `(key, (val, time))`, the time a tuple
//!   of the epoch and every scope coordinate.
//! - `| join_function((k, v) -> list)` — replace each row by the lambda's list of
//!   `((key, val), time, diff)`, at the row's time joined with `time`, diffs multiplied.
//! - `| half_join(other, strict, (k, v, v1) -> (k', v'))` — each update of a lifted stream,
//!   `(k, (v, time))`, meets `other`'s rows at its key whose times precede the update's own in the
//!   total order on times (strictly when `strict` is 1, or also equal when 0), giving
//!   `(k', (v', time ∨ t2))`. The lambda does not see the times. `other`'s changes produce nothing.
//! - `| delay((k, v) -> time)` — `join_function` with the list `[((key, val), time, 1)]`: advance
//!   each row to at least the time the lambda gives.
//! - `a + b` (concat), `a - b` (concat with `b` negated). `+ b` and `- b` are stages like `| op`,
//!   and each stage applies to everything to its left: `a | map(f) + b | distinct` is
//!   `((a | map(f)) + b) | distinct`. To add a pipeline, parenthesize it or put it first.
//!
//! Statements: `let x = …;`, `set x = …;` (a feedback variable in a named
//! scope), `name: { … }` (a nested scope), `export "name" = …;` (a
//! program output, root scope only). An optional `var x;` or
//! `var x : (key_shape, val_shape);` declares a feedback variable; the latter
//! checks the shape inferred for its `set` value. An `export` may also declare
//! its shape. `type Name = V0 shape? | V1 shape? …;` declares a sum type, an
//! enum of the program's lambdas (below); it emits no IR.
//!
//! A shape is `int`, `float`, `()`, `(a, b, …)`, `List(a)`, `Option(a)`, `Result(a, b)`, or a
//! declared type's name. A variant's payload shape is `()` when omitted, and a type's payloads
//! may name earlier types, never recursively.
//!
//! A collection's shape, as `input`, `import`, `var` and `export` declare it, is the pair
//! `(key_shape, val_shape)`: its root has exactly two fields, the key and the value, as a slot's
//! input `(key, val)` does. `((int, int), ())` is a two-field key with a unit value, and
//! `(int, int)` keys by the first int with the second as the value. The older `(key ; val)` is
//! still accepted, and deprecated.
//!
//! # Lambdas
//!
//! A slot runs `let pat = input in body` in corgi's ML syntax, with `input` the tuple of the
//! slot's rows: `(key, val)` for a linear op, `(key, left val, right val)` for a join, and
//! `(key, vals)` for `reduce`. For example, `map(((src, dst), ()) -> (src, dst))` keys each row
//! of pairs by its first field. Operators are postfix (`(a, b) add`, `xs len`,
//! `(mask, a, b) select`); `x.N` is a tuple's field, `(e)` a one-field tuple and `()` unit;
//! `[1, 2]` is a list of constants and `(xs, ys) append` concatenates. A `--` comment may sit
//! inside a lambda. The language is corgi's (its `frontend/ml.rs` and `NOTES.md`).
//!
//! A lambda is parsed in its program's scope, which names what the program has not defined:
//!
//! - Registered functions and the Float functions are words reached by `call`. A word's input is
//!   its argument, or the tuple of its arguments when it takes several: `x call fsqrt`,
//!   `(x, y) call fpow`, `() call f` for a function of none.
//! - The Float functions, each the Rust `f64` method of the same name: `fabs`, `fsqrt`, `fexp`,
//!   `fln`, `ffloor`, `fceil`, `fround`, `fsin`, `fcos`, `ftan` (Float -> Float); `fpow` (`powf`),
//!   `fpowi` (`powi`, an Int exponent). `fint` is Float -> Int as Rust's `x as i64`: truncate
//!   toward zero, NaN is 0, out of range saturates. `fmin`/`fmax` skip a NaN operand and
//!   otherwise follow the total order (`fmin` of `-0.0` and `0.0` is `-0.0`). `feq fne flt fle fgt
//!   fge` are IEEE comparisons giving Int 0/1: NaN compares false (`fne` true) and `-0.0` equals
//!   `0.0`, where corgi's own `eq lt …` use the total order.
//! - The program's `type` declarations, written before the lambda, are corgi enums of the same
//!   names and variants. A variant two types share is written `Type::Variant`.
//! - `Option` (`None`, `Some`) and `Result` (`Ok`, `Err`) are enums whose payload lanes are open,
//!   `None` aside: `x inject Some` takes its lane's shape from `x`, and `branch`, `map_variant` and
//!   `match` read only their tags, as in `(x, c) branch Option map_variant None (_ -> ())` for
//!   "`Some(x)` when `c`, else `None`".

use super::*;
use crate::ir::{Lambda, Reducer};

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Let, Var, Set, Export, Type,
    Input, Import, Map, Join, Min, Distinct, Count, Collect, Arrange, Negate, Filter, EnterAt, LiftIter, FlatMap, Inspect,
    Ident(String), Int(i64), Str(String),
    /// A slot written as a corgi lambda, `pat -> body`, kept as corgi source.
    Lambda { pat: String, body: String },
    LParen, RParen, LBrace, RBrace,
    Comma, Semi, Colon, ColonColon, Eq,
    Pipe, Plus, Minus, Eof,
}

/// The source as a stream of chars, with the byte offset of the next one.
#[derive(Clone)]
struct Chars<'a> { src: &'a str, at: usize, peeked: Option<char> }

impl Chars<'_> {
    fn peek(&mut self) -> Option<&char> { self.peeked = self.src[self.at..].chars().next(); self.peeked.as_ref() }
}

impl Iterator for Chars<'_> {
    type Item = char;
    fn next(&mut self) -> Option<char> { let c = self.src[self.at..].chars().next()?; self.at += c.len_utf8(); Some(c) }
}

/// A corgi lambda that ends the parenthesized group starting at byte `from` of `src` (just after
/// its `(`): the group holds a `->` outside any nested bracket. Returns the lambda's start (after
/// the group's last comma before the arrow, or `from`), the arrow, and the group's `)`.
///
/// Corgi's text is not DDIR's (it has `.2`, float literals, `#` comments), so the lexer takes the
/// lambda whole rather than lexing it. A pattern holds no comma outside parentheses, so the last
/// comma before the arrow separates the lambda from the arguments before it, as in `join`.
fn lambda_in(src: &str, from: usize) -> Option<(usize, usize, usize)> {
    let bytes = src.as_bytes();
    let (mut depth, mut start, mut arrow) = (0usize, from, None);
    let mut i = from;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' { if bytes[i] == b'\\' { i += 1; } i += 1; }
            }
            b'#' => { while i < bytes.len() && bytes[i] != b'\n' { i += 1; } }
            b'-' if bytes.get(i + 1) == Some(&b'-') => { while i < bytes.len() && bytes[i] != b'\n' { i += 1; } }
            b'-' if bytes.get(i + 1) == Some(&b'>') && depth == 0 && arrow.is_none() => { arrow = Some(i); i += 1; }
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' if depth == 0 => return arrow.map(|a| (start, a, i)),
            b')' | b']' | b'}' => depth -= 1,
            b',' if depth == 0 && arrow.is_none() => start = i + 1,
            _ => {}
        }
        i += 1;
    }
    None
}

/// Lambda text as corgi reads it: a DDIR `--` comment becomes corgi's `#` comment.
fn corgi_comments(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                let from = i;
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' { if bytes[i] == b'\\' { i += 1; } i += 1; }
                i = (i + 1).min(bytes.len());
                out.push_str(&text[from..i]);
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                let from = i + 2;
                while i < bytes.len() && bytes[i] != b'\n' { i += 1; }
                out.push('#');
                out.push_str(&text[from..i]);
            }
            _ => {
                let c = text[i..].chars().next().unwrap();
                out.push(c);
                i += c.len_utf8();
            }
        }
    }
    out
}

fn tokenize(input: &str) -> Vec<Token> {
    let mut tokens = lex(input);
    tokens.push(Token::Eof);
    tokens
}

fn lex(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut chars = Chars { src: input, at: 0, peeked: None };
    while let Some(&ch) = chars.peek() {
        match ch {
            ' ' | '\t' | '\n' | '\r' => { chars.next(); },
            '-' if chars.clone().nth(1).map_or(false, |c| c == '-') => {
                while let Some(&c) = chars.peek() { chars.next(); if c == '\n' { break; } }
            },
            '(' => {
                chars.next();
                tokens.push(Token::LParen);
                if let Some((start, arrow, close)) = lambda_in(input, chars.at) {
                    tokens.extend(lex(&input[chars.at..start]));
                    let (pat, body) = (corgi_comments(input[start..arrow].trim()), corgi_comments(input[arrow + 2..close].trim()));
                    tokens.push(Token::Lambda { pat, body });
                    tokens.push(Token::RParen);
                    chars.at = close + 1;
                }
            },
            ')' => { chars.next(); tokens.push(Token::RParen); },
            '{' => { chars.next(); tokens.push(Token::LBrace); },
            '}' => { chars.next(); tokens.push(Token::RBrace); },
            ',' => { chars.next(); tokens.push(Token::Comma); },
            ';' => { chars.next(); tokens.push(Token::Semi); },
            '|' => { chars.next(); tokens.push(Token::Pipe); },
            '+' => { chars.next(); tokens.push(Token::Plus); },
            '=' => { chars.next(); tokens.push(Token::Eq); },
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
                tokens.push(keyword(&ident).unwrap_or(Token::Ident(ident)));
            },
            other => panic!("Unexpected character: {:?}", other),
        }
    }
    tokens
}

/// The lexer's keywords: an identifier spelled like one lexes as the keyword, never as a name.
/// `crate::ir::KEYWORDS` lists the same names, for code below the parser.
fn keyword(ident: &str) -> Option<Token> {
    Some(match ident {
        "let" => Token::Let, "var" => Token::Var, "set" => Token::Set, "export" => Token::Export,
        "type" => Token::Type,
        "input" => Token::Input, "import" => Token::Import,
        "map" => Token::Map,
        "join" => Token::Join, "min" => Token::Min, "distinct" => Token::Distinct,
        "count" => Token::Count, "collect" => Token::Collect,
        "flatmap" => Token::FlatMap,
        "arrange" => Token::Arrange, "negate" => Token::Negate,
        "filter" => Token::Filter, "enter_at" => Token::EnterAt, "inspect" => Token::Inspect,
        "lift_iter" => Token::LiftIter,
        _ => return None,
    })
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    /// Declared sum types: name -> the variants in tag order, each with its payload shape.
    /// Populated by `type` decls; tags are positions, scoped to their type.
    types: std::collections::HashMap<String, Vec<(String, corgi::Shape)>>,
    /// The declared types, in declaration order (a type's payload may name an earlier one).
    type_order: Vec<String>,
    /// The corgi scope lambdas are parsed in, built at the first lambda and then kept current
    /// with each `type` declaration.
    scope: Option<corgi::Scope>,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, pos: 0, types: Default::default(), type_order: Vec::new(), scope: None }
    }

    /// The corgi scope of this program's lambdas: the registered functions and the Float functions
    /// as words, the built-in `Option` and `Result`, and the types declared so far as enums.
    fn lambda_scope(&mut self) -> &corgi::Scope {
        if self.scope.is_none() {
            let mut scope = crate::lambda::base_scope();
            for f in crate::lambda::registered() {
                scope.word(f.name(), crate::lambda::function_word(&f)).unwrap_or_else(|e| panic!("{e}"));
            }
            for name in &self.type_order {
                declare_enum(&mut scope, name, &self.types[name], &self.types);
            }
            self.scope = Some(scope);
        }
        self.scope.as_ref().unwrap()
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
                }
                if let Some(scope) = self.scope.as_mut() {
                    declare_enum(scope, &name, &variants, &self.types);
                }
                self.type_order.push(name.clone());
                self.types.insert(name, variants);
                continue;
            }
            stmts.push(self.parse_stmt());
        }
        stmts
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

    /// A payload shape: `int`, `float`, `()`, `(a, b, ..)`, `List(a)`, `Option(a)`, `Result(a, b)`,
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
                "int" => Shape::Int,
                "u64" => panic!("`u64` is retired: an integer is `int`, which is signed"),
                "float" => Shape::Float,
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

    fn parse_stmt(&mut self) -> Stmt {
        match self.peek().clone() {
            Token::Let => { self.next(); let n = self.parse_ident(); self.expect(&Token::Eq); let e = self.parse_pipe_expr(); self.expect(&Token::Semi); Stmt::Let(n, e) },
            Token::Var => {
                self.next();
                let n = self.parse_ident();
                let s = self.declared_shape();
                if *self.peek() == Token::Eq {
                    if s.is_some() {
                        panic!("`var {n} : S = e;` is no longer valid; write `var {n} : S;` then `set {n} = e;`");
                    }
                    panic!("`var {n} = e;` is no longer valid; write `set {n} = e;`");
                }
                self.expect(&Token::Semi);
                Stmt::Var(n, s)
            },
            Token::Set => { self.next(); let n = self.parse_ident(); self.expect(&Token::Eq); let e = self.parse_pipe_expr(); self.expect(&Token::Semi); Stmt::Set(n, e) },
            Token::Export => {
                self.next();
                let name = match self.next() {
                    Token::Str(s) => s,
                    o => panic!("Expected string literal after `export`, got {:?}", o),
                };
                let shape = self.declared_shape();
                self.expect(&Token::Eq);
                let e = self.parse_pipe_expr();
                self.expect(&Token::Semi);
                Stmt::Export(name, shape, e)
            },
            Token::Ident(_) => {
                let n = self.parse_ident(); self.expect(&Token::Colon);
                self.expect(&Token::LBrace); let b = self.parse_program(); self.expect(&Token::RBrace); Stmt::Scope(n, b)
            },
            other => panic!("Unexpected token: {:?}", other),
        }
    }

    fn parse_ident(&mut self) -> String { match self.next() { Token::Ident(s) => s, other => panic!("Expected ident, got {:?}", other) } }

    /// An atom followed by stages, applied left to right: `| op`, `+ atom` and `- atom`. A run of
    /// `+`/`-` stages is one `Concat`.
    fn parse_pipe_expr(&mut self) -> Expr {
        let mut expr = self.parse_atom();
        let mut concat = false;
        loop {
            let part = match self.peek() {
                Token::Pipe => { self.next(); expr = self.parse_pipe_op(expr); concat = false; continue },
                Token::Plus => { self.next(); self.parse_atom() },
                Token::Minus => { self.next(); Expr::Negate(Box::new(self.parse_atom())) },
                _ => break,
            };
            expr = match expr {
                Expr::Concat(mut parts) if concat => { parts.push(part); Expr::Concat(parts) },
                lhs => Expr::Concat(vec![lhs, part]),
            };
            concat = true;
        }
        expr
    }

    fn parse_atom(&mut self) -> Expr {
        match self.peek().clone() {
            Token::Input => { self.next(); let source = match self.next() { Token::Int(n) => Expr::Input(n as usize), o => panic!("Expected int, got {:?}", o) }; self.source_shape(source) },
            Token::Import => { self.next(); let source = match self.next() { Token::Str(s) => Expr::Import(s), o => panic!("Expected string literal after `import`, got {:?}", o) }; self.source_shape(source) },
            Token::Ident(_) => { let n = self.parse_ident(); if *self.peek() == Token::ColonColon { self.next(); let f = self.parse_ident(); Expr::Qualified(n, f) } else { Expr::Name(n) } },
            Token::LParen => { self.next(); let e = self.parse_pipe_expr(); self.expect(&Token::RParen); e },
            other => panic!("Unexpected token in atom: {:?}", other),
        }
    }

    fn source_shape(&mut self, source: Expr) -> Expr {
        match self.declared_shape() {
            Some((key, val)) => Expr::TypedSource(Box::new(source), key, val),
            None => source,
        }
    }

    /// An optional `: (key, val)` shape declaration: the root pair is the key and the value.
    /// The older `(key ; val)` is accepted too, and deprecated.
    fn declared_shape(&mut self) -> Option<crate::scope_ir::RowShape> {
        if *self.peek() != Token::Colon { return None; }
        self.next();
        self.expect(&Token::LParen);
        let key = self.parse_shape();
        match self.next() {
            Token::Comma | Token::Semi => {}
            other => panic!("a collection shape is (key, value), got {:?} after its key", other),
        }
        let val = self.parse_shape();
        if *self.peek() != Token::RParen {
            panic!("a collection shape is (key, value), with exactly two fields; got {:?} after its value", self.peek());
        }
        self.next();
        Some((key, val))
    }

    fn parse_pipe_op(&mut self, lhs: Expr) -> Expr {
        match self.peek().clone() {
            Token::Map => { self.next(); Expr::Map(Box::new(lhs), self.slot()) },
            Token::Ident(name) if name == "value" => {
                self.next();
                let val = self.slot();
                Expr::Map(Box::new(lhs), Lambda::compose("(k, v)", &[(&val, "(k, v)".into())], |r| format!("(k, {})", r[0])))
            },
            Token::Join => { self.next(); self.expect(&Token::LParen); let r = self.parse_pipe_expr(); self.expect(&Token::Comma); let p = self.lambda(); self.expect(&Token::RParen); Expr::Join(Box::new(lhs), Box::new(r), p) },
            Token::Min => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Min) },
            Token::Distinct => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Distinct) },
            Token::Count => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Count) },
            Token::Collect => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Collect) },
            Token::Ident(name) if name == "reduce" => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Expr(self.slot())) },
            Token::FlatMap => { self.next(); Expr::FlatMap(Box::new(lhs), self.slot()) },
            Token::Arrange => { self.next(); Expr::Arrange(Box::new(lhs)) },
            Token::Negate => { self.next(); Expr::Negate(Box::new(lhs)) },
            Token::Filter => { self.next(); Expr::Filter(Box::new(lhs), self.slot()) },
            Token::EnterAt => { self.next(); Expr::EnterAt(Box::new(lhs), self.slot()) },
            Token::LiftIter => { self.next(); Expr::LiftIter(Box::new(lhs)) },
            Token::Ident(name) if name == "lift" => { self.next(); Expr::Lift(Box::new(lhs)) },
            Token::Ident(name) if name == "half_join" => {
                self.next(); self.expect(&Token::LParen);
                let r = self.parse_pipe_expr(); self.expect(&Token::Comma);
                let strict = match self.next() {
                    Token::Int(0) => false,
                    Token::Int(1) => true,
                    other => panic!("half_join: strict is 0 or 1, not {other:?}"),
                };
                self.expect(&Token::Comma);
                let p = self.lambda();
                self.expect(&Token::RParen);
                Expr::HalfJoin(Box::new(lhs), Box::new(r), strict, p)
            },
            Token::Ident(name) if name == "join_function" => { self.next(); Expr::JoinFunction(Box::new(lhs), self.slot()) },
            Token::Ident(name) if name == "delay" => {
                self.next();
                let time = self.slot();
                Expr::JoinFunction(Box::new(lhs), Lambda::compose("(k, v)", &[(&time, "(k, v)".into())], |r| format!("((k, v), {}, 1) enlist", r[0])))
            },
            Token::Inspect => { self.next(); self.expect(&Token::LParen); let l = self.parse_ident(); self.expect(&Token::RParen); Expr::Inspect(Box::new(lhs), l) },
            other => panic!("Expected pipe operator, got {:?}", other),
        }
    }

    /// A slot, `(pat -> body)`: a corgi lambda over the tuple of the slot's rows.
    fn slot(&mut self) -> Lambda {
        self.expect(&Token::LParen);
        let lambda = self.lambda();
        self.expect(&Token::RParen);
        lambda
    }

    /// A corgi lambda, `pat -> body`: it runs `let pat = input in body`, in the program's scope.
    fn lambda(&mut self) -> Lambda {
        match self.next() {
            Token::Lambda { pat, body } => Lambda::new(format!("let {pat} = input in {body}"), self.lambda_scope()),
            other => panic!("expected a lambda `pat -> body`, got {other:?}"),
        }
    }
}

/// Declare the DDIR type `name` in a lambda scope, as the corgi enum of the same name and variants.
/// A payload that is itself a sum is written as the name of a declared type with those lanes, or
/// else as a fresh enum declared for it, with names no program writes.
fn declare_enum(scope: &mut corgi::Scope, name: &str, variants: &[(String, corgi::Shape)], types: &std::collections::HashMap<String, Vec<(String, corgi::Shape)>>) {
    fn render(s: &corgi::Shape, scope: &mut corgi::Scope, types: &std::collections::HashMap<String, Vec<(String, corgi::Shape)>>) -> String {
        use corgi::Shape;
        match s {
            Shape::Int => "int".into(),
            Shape::Float => "float".into(),
            Shape::Unit => "()".into(),
            Shape::Prod(fs) => format!("({})", fs.iter().map(|f| render(f, scope, types)).collect::<Vec<_>>().join(", ")),
            Shape::List(e) => format!("List({})", render(e, scope, types)),
            Shape::Sum(lanes) => {
                let named = types.iter()
                    .filter(|(n, vs)| vs.iter().map(|(_, s)| s).eq(lanes.iter()) && scope.variants(n).is_some())
                    .map(|(n, _)| n)
                    .min();
                if let Some(n) = named { return n.clone(); }
                let mut k = 0;
                while scope.variants(&format!("__sum{k}")).is_some() { k += 1; }
                let fresh = format!("__sum{k}");
                let body: Vec<String> = lanes.iter().enumerate().map(|(i, l)| format!("{fresh}_{i} {}", render(l, scope, types))).collect();
                scope.declare(&format!("enum {fresh} = {}", body.join(" | "))).unwrap_or_else(|e| panic!("{e}"));
                fresh
            }
            Shape::Ref(_) => panic!("a type cannot hold a reference"),
        }
    }
    let body: Vec<String> = variants.iter().map(|(v, s)| format!("{v} {}", render(s, scope, types))).collect();
    scope.declare(&format!("enum {name} = {}", body.join(" | "))).unwrap_or_else(|e| panic!("type `{name}`: {e}"));
}

pub fn parse(input: &str) -> Vec<Stmt> { let tokens = tokenize(input); let mut p = Parser::new(tokens); p.parse_program() }

#[cfg(test)]
mod tests {
    use super::{parse, Stmt};

    #[test]
    fn parses_set_and_optional_declarations() {
        let stmts = parse("s: { var x; set x = x; set y = x; var y : ((int), ()); }");
        let Stmt::Scope(_, body) = &stmts[0] else { panic!("expected a scope") };
        assert!(matches!(&body[0], Stmt::Var(n, None) if n == "x"));
        assert!(matches!(&body[1], Stmt::Set(n, _) if n == "x"));
        assert!(matches!(&body[2], Stmt::Set(n, _) if n == "y"));
        assert!(matches!(&body[3], Stmt::Var(n, Some(_)) if n == "y"));
    }

    #[test]
    fn old_var_bindings_explain_the_new_spelling() {
        for (old, expected) in [
            ("s: { var x = x; }", "set x = e;"),
            ("s: { var x : ((int), ()) = x; }", "var x : S;` then `set x = e;"),
        ] {
            let err = std::panic::catch_unwind(|| parse(old)).unwrap_err();
            let message = err.downcast_ref::<String>().expect("string panic");
            assert!(message.contains(expected), "{message}");
        }
    }

    /// Every name `crate::ir::KEYWORDS` lists lexes as a keyword, so a registered function is
    /// refused exactly the names the lexer would claim.
    #[test]
    fn ir_keywords_are_the_lexers() {
        for k in crate::ir::KEYWORDS {
            assert!(super::keyword(k).is_some(), "`{k}` is in ir::KEYWORDS but the lexer does not claim it");
        }
    }
}
