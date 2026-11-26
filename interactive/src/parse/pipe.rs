//! Pipe syntax parser for .ddp files — and the surface-language reference.
//!
//! A program is a sequence of statements; an expression is a source piped
//! through operators (`expr | op | op`). The *collection* language describes the
//! dataflow graph; each operator's per-row logic is a *slot*, a corgi lambda.
//!
//! # Collection language
//!
//! Sources: `input N : (key_shape, val_shape)` or `input "name" : (key_shape, val_shape)` (an
//! input, labelled by a number or by a name that is an identifier), `name` (a `let` or `set` in
//! this or any enclosing scope), or a scope path such as `a::b::f` or `::a::b::f`.
//! A label is the input's identity: a client feeds an input by its label, and a load may bind it
//! to a published trace instead, as `load p from f.ddp edges=graph::edges`. A program does not
//! name the traces it reads. Writing the same label again reads the same input, and the order
//! inputs are written in never matters.
//! A leading `::` reads the rest of the path in the root. Otherwise the first
//! component names one visible scope: the current scope, an ancestor, or a child
//! of one of these. Scope names cannot shadow one another, so that scope is unique.
//! If it encloses the reference, the rest of the path is read in it; otherwise the
//! whole path is read in its parent. Every component except the last descends into
//! a child scope, and the last names a binding that scope exports. The binding is
//! read as converged where the path is read, and carried down to the reference
//! through ordinary import edges. Paths may have any depth. A path cannot descend
//! into a scope that encloses the reference; use the local name there. A scope name
//! cannot shadow an ancestor or a sibling of any ancestor, and a binding cannot
//! share a name with a child scope. Disjoint subtrees may reuse scope names.
//! An input declares its shape, using the shape syntax below, as a table
//! declares its columns: the server refuses to install a program with an undeclared
//! input. The shape is the column encoding, also for empty lists and inactive sum
//! lanes, which a row cannot reveal. A bound input's declared shape must
//! match the shape its trace is published at. A fed row is asserted to have its
//! input's shape during execution, where a mismatch can panic a dataflow worker and
//! take down the shared server; there is no per-program failure isolation.
//! Operators chain with `|`; a slot is written `(pat -> body)`:
//!
//! - `| map((k, v) -> (k', v'))` — reshape each row to the `(key, val)` the lambda gives.
//! - `| join(other)` — equijoin on the key, yielding `(k, (v, v1))`.
//! - `| join(other, f)` — project the joined row, as in `| join(other) | map(f)`.
//! - `| min` / `| distinct` / `| count` / `| collect` — reduce; `distinct` keeps each `(key, val)`
//!   record once, and `collect` is NEST (gather a key's values into a `List`).
//! - `| sum` — sum integer values per key, weighted by their signed diffs. A zero total has no row.
//!   For example, `(deposits - debits) | sum` maintains balances from `(account, amount)` rows.
//! - `| reduce((k, vals) -> out)` — reduce by a lambda: `vals` is the key's values with their
//!   non-zero diffs, a `List` of `(val, diff)` in value order, and `out` the key's output, a
//!   `List` of `(val, diff)`. `| reduce(limit n, (k, vals) -> out)` hands the lambda only each
//!   key's first `n` values (`n` at least one), whatever their diffs; `min` is
//!   `reduce(limit 1, (_, vals) -> (vals head.0, 1) enlist)`.
//! - `| filter((k, v) -> mask)` — keep rows where the lambda gives a nonzero `Int`.
//! - `| flatmap((k, v) -> list)` — any number of complete `(key, val)` rows.
//!   For positions, write `((0, l len) range, l) zip`; keys are chosen by the lambda.
//! - `| negate` · `| arrange` · `| inspect(label)`.
//! - `| lift` — pair each value with its whole time, `(key, (val, time))`, the time a tuple
//!   of the epoch and every scope coordinate.
//! - `| join_function((k, v) -> list)` — replace each row by the lambda's list of
//!   `((key, val), time, diff)`, at the row's time joined with `time`, diffs multiplied.
//! - `| half_join(other, strict)` — each update of a lifted stream,
//!   `(k, (v, time))`, meets `other`'s rows at its key whose times precede the update's own in the
//!   total order on times (strictly when `strict` is 1, or also equal when 0), giving
//!   `(k, ((v, v1), time ∨ t2))`. `half_join(other, strict, f)` projects this whole row.
//!   The output retains the stream update's timestamp. `other`'s changes produce nothing.
//! - `| delay((k, v) -> time)` — `join_function` with the list `[((key, val), time, 1)]`: advance
//!   each row to at least the time the lambda gives. The tuple names the epoch, then each
//!   enclosing loop from outermost to innermost; `0` leaves a coordinate unchanged. Inside one
//!   loop, `delay((_, n) -> (0, (256, n call bit_length) mul))` schedules by integer bit length.
//! - `| semijoin(other)` — the rows whose key is a key of `other`, each once however many rows of
//!   `other` share it: `join` with `other | map((k, _) -> (k, ())) | distinct`, keeping `(k, v)`.
//!   `| antijoin(other)` — the other rows: the input less its semijoin. The two split the input
//!   exactly, where `a - (a | join(b, …))` subtracts a row once per row of `b` with its key.
//! - `a + b` (concat), `a - b` (concat with `b` negated). `+ b` and `- b` are stages like `| op`,
//!   and each stage applies to everything to its left: `a | map(f) + b | distinct` is
//!   `((a | map(f)) + b) | distinct`. To add a pipeline, parenthesize it or put it first.
//!
//! Join fragments are an experimental surface form, not part of the plan language or a stable
//! interface. The parser expands each one into plain DDP as it reads it, so no syntax tree,
//! later stage or emitted plan contains one. They may change or be removed.
//! A fragment is a pipeline source: `a(x, y) * b(y, z) |- (x, z)`.
//! Atoms name existing bindings (including scope paths); they always have key and value arguments.
//! Every later key must mention at least one variable bound by an earlier atom; it may combine
//! earlier variables with literals, tuples, unit and constructors, but not wildcards or new variables.
//! Open built-in constructors (`Ok(x)`, `Err(e)`, `None`) cannot be built as later keys, and
//! untyped empty lists (including `""`) cannot appear in them.
//! A constant key would move every incoming row to one key; write a cross join as plain DDP.
//! Atoms keep their existing arrangements: one new arrangement per `*`, of the rows so far;
//! none at the first `*` when the first two keys are the same unrestricted variables.
//! The first key and every value accept variables, `_`, tuples, integer/list/string literals,
//! and constructors with payload patterns, such as `Some(x)` or `Type::Variant(x)`.
//! A bare constructor such as `None` means its empty payload; a constructor needing a payload
//! must supply one. A name unknown as a constructor is a variable.
//! An empty list pattern `[]` tests emptiness; inside a list literal its element shape must
//! come from sibling constants, otherwise DDIR rejects the literal with a shape error.
//! Reusing a variable tests equality. Plain filters run as soon as their fields are bound,
//! before the next rekey; a direct first join precedes its first atom's tests. Variants bind
//! through typed payload lists in `flatmap`, which can also emit the next lookup key.
//! The head is required. It is a pair of corgi expressions for the key and value.
//! The final join projects the head directly when no tests are pending.
//! An atom `!b(key, value)` keeps rows with no matching row of `b` and binds no variables.
//! The corresponding `?b(key, value)` keeps rows with a match.
//! Both join against a distinct set of matched fields. An antijoin subtracts those rows.
//! A plain atom multiplies the weights.
//! A relation `@name(args)` tests bound values or computes a new value in the written order.
//! Tests run at the first checkpoint that has their arguments. Use `!@name(args)` to negate a test.
//! Even the parsed view shows the expansion.
//!
//! Statements: `let x = …;`, `set x = …;` (a feedback variable: in a named
//! scope it is the previous round's value; at the root, the previous time's), `name: { … }` (a nested scope), `output "name" = …;` (a
//! program output, root scope only). An optional `var x;` or
//! `var x : (key_shape, val_shape);` declares a feedback variable; the latter
//! checks the shape inferred for its `set` value. An `output` may also declare
//! its shape. `enum Name = V0 shape? | V1 shape? …;` declares a sum type, an
//! enum of the program's lambdas (below); `type Name = shape;` declares an erased alias.
//! Neither declaration emits IR.
//!
//! A shape is `int`, `float`, `()`, `(a, b, …)`, `List(a)`, `Option(a)`, `Result(a, b)`, or a
//! declared type's name. A variant's payload shape is `()` when omitted, and a type's payloads
//! may name other types declared anywhere in the program, including inside a scope. Types are
//! global to the program and may appear in any order, but cannot be recursive.
//!
//! A collection's shape, as `input`, `var` and `output` declare it, is the pair
//! `(key_shape, val_shape)`: its root has exactly two fields, the key and the value, as a slot's
//! input `(key, val)` does. `((int, int), ())` is a two-field key with a unit value, and
//! `(int, int)` keys by the first int with the second as the value. The older `(key ; val)` is
//! still accepted, and deprecated.
//!
//! # Lambdas
//!
//! A slot runs `let pat = input in body` in corgi's ML syntax, with `input` the tuple of the
//! slot's rows: `(key, val)` for a linear op, `(key, (left val, right val))` for a join, and
//! `(key, vals)` for `reduce`. For example, `map(((src, dst), ()) -> (src, dst))` keys each row
//! of pairs by its first field. Operators are postfix (`(a, b) add`, `xs len`,
//! `(mask, a, b) select`); `x.N` is a tuple's field, `(e)` a one-field tuple and `()` unit;
//! `[1, 2]` is a list of constants and `(xs, ys) append` concatenates. A `--` comment may sit
//! inside a lambda. The language is corgi's (its `frontend/ml.rs` and `NOTES.md`).
//!
//! A lambda is parsed in its program's scope, which names what the program has not defined:
//!
//! - Registered functions and the base words are reached by `call`. A word's input is
//!   its argument, or the tuple of its arguments when it takes several: `x call fsqrt`,
//!   `(x, y) call fpow`, `() call f` for a function of none.
//! - The Float functions, each the Rust `f64` method of the same name: `fabs`, `fsqrt`, `fexp`,
//!   `fln`, `ffloor`, `fceil`, `fround`, `fsin`, `fcos`, `ftan` (Float -> Float); `fpow` (`powf`),
//!   `fpowi` (`powi`, an Int exponent). `fint` is Float -> Int as Rust's `x as i64`: truncate
//!   toward zero, NaN is 0, out of range saturates. `fmin`/`fmax` skip a NaN operand and
//!   otherwise follow the total order (`fmin` of `-0.0` and `0.0` is `-0.0`). `feq fne flt fle fgt
//!   fge` are IEEE comparisons giving Int 0/1: NaN compares false (`fne` true) and `-0.0` equals
//!   `0.0`, where corgi's own `eq lt …` use the total order.
//! - `n call bit_length` is Int -> Int: `64 - (n as u64).leading_zeros()`, so zero gives 0
//!   and a negative integer gives 64.
//! - The program's `enum` declarations are corgi enums of the same
//!   names and variants. A variant two types share is written `Type::Variant`.
//!   Aliases are not yet available inside lambdas.
//! - `Option` (`None`, `Some`) and `Result` (`Ok`, `Err`) are enums whose payload lanes are open,
//!   `None` aside: `x inject Some` takes its lane's shape from `x`, and `branch`, `map_variant` and
//!   `match` read only their tags, as in `(x, c) branch Option map_variant None (_ -> ())` for
//!   "`Some(x)` when `c`, else `None`".

use super::*;
use crate::ir::Lambda;

mod fragment;

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Let, Var, Set, Output, Type, Enum,
    Input, Import, Map, Join, Min, Distinct, Count, Collect, Arrange, Negate, Filter, FlatMap, Inspect,
    Ident(String), Int(i64), Str(String),
    /// A slot written as a corgi lambda, `pat -> body`, kept as corgi source.
    Lambda { pat: String, body: String },
    Turnstile, Head(String), RelationArgs(Vec<String>),
    LParen, RParen, LBrace, RBrace,
    Comma, Semi, Colon, ColonColon, Eq,
    Pipe, Plus, Minus, Star, Bang, Question, At, LBracket, RBracket, Eof,
}

/// A shape before names in type declarations have been resolved.
enum TypeShape {
    Unit, Int, Float,
    Prod(Vec<TypeShape>), List(Box<TypeShape>),
    Option(Box<TypeShape>), Result(Box<TypeShape>, Box<TypeShape>),
    Named(String),
}

impl TypeShape {
    fn dependencies(&self, names: &mut Vec<String>) {
        match self {
            Self::Prod(fields) => { for field in fields { field.dependencies(names); } },
            Self::List(inner) | Self::Option(inner) => inner.dependencies(names),
            Self::Result(ok, err) => { ok.dependencies(names); err.dependencies(names); },
            Self::Named(name) => names.push(name.clone()),
            Self::Unit | Self::Int | Self::Float => {},
        }
    }
}

struct TypeDecl {
    name: String,
    body: TypeBody,
}

enum TypeBody {
    Enum(Vec<(String, TypeShape)>),
    Alias(TypeShape),
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

/// Scan the contents of a parenthesized group without lexing corgi text as DDIR.
struct CorgiGroup {
    close: usize,
    lambda: Option<(usize, usize)>,
}

fn corgi_group(src: &str, from: usize, mut comma: impl FnMut(usize)) -> Option<CorgiGroup> {
    let bytes = src.as_bytes();
    let (mut depth, mut start, mut arrow) = (0usize, from, None);
    let mut i = from;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' { i += 1; }
            }
            b'#' => { while i < bytes.len() && bytes[i] != b'\n' { i += 1; } }
            b'-' if bytes.get(i + 1) == Some(&b'-') => { while i < bytes.len() && bytes[i] != b'\n' { i += 1; } }
            b'-' if bytes.get(i + 1) == Some(&b'>') && depth == 0 && arrow.is_none() => { arrow = Some(i); i += 1; }
            b'(' | b'[' | b'{' => depth += 1,
            b')' if depth == 0 => return Some(CorgiGroup { close: i, lambda: arrow.map(|a| (start, a)) }),
            b']' | b'}' if depth == 0 => return None,
            b')' | b']' | b'}' => depth -= 1,
            b',' if depth == 0 => {
                comma(i);
                if arrow.is_none() { start = i + 1; }
            },
            _ => {}
        }
        i += 1;
    }
    None
}

/// A DDIR `--` comment becomes corgi's `#` comment when retaining source text.
fn corgi_text(text: &str, keep_comments: bool) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                let from = i;
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' { i += 1; }
                i = (i + 1).min(bytes.len());
                out.push_str(&text[from..i]);
            }
            b'#' | b'-' if bytes[i] == b'#' || bytes.get(i + 1) == Some(&b'-') => {
                let from = i + if bytes[i] == b'#' { 1 } else { 2 };
                while i < bytes.len() && bytes[i] != b'\n' { i += 1; }
                if keep_comments {
                    out.push('#');
                    out.push_str(&text[from..i]);
                }
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
                if matches!(tokens.last(), Some(Token::Turnstile)) {
                    let mut commas = 0;
                    let group = corgi_group(input, chars.at, |_| commas += 1).expect("unclosed fragment head; write `|- (key, value)`");
                    assert!(commas == 1 && group.lambda.is_none(), "a fragment head must be a tuple `|- (key, value)`");
                    tokens.push(Token::Head(corgi_text(&input[chars.at - 1..=group.close], true)));
                    chars.at = group.close + 1;
                    continue;
                }
                if matches!(&tokens[..], [.., Token::At, Token::Ident(_)]) {
                    let mut start = chars.at;
                    let mut args = Vec::new();
                    let group = corgi_group(input, start, |end| {
                        args.push(corgi_text(&input[start..end], false));
                        start = end + 1;
                    }).expect("unclosed computed relation arguments");
                    let last = corgi_text(&input[start..group.close], false);
                    if !args.is_empty() || !last.trim().is_empty() { args.push(last); }
                    tokens.push(Token::RelationArgs(args));
                    chars.at = group.close + 1;
                    continue;
                }
                tokens.push(Token::LParen);
                // The last top-level comma before an arrow separates a lambda from
                // preceding arguments, as in join.
                if let Some(CorgiGroup { close, lambda: Some((start, arrow)), .. }) = corgi_group(input, chars.at, |_| {}) {
                    tokens.extend(lex(&input[chars.at..start]));
                    let (pat, body) = (corgi_text(input[start..arrow].trim(), true), corgi_text(input[arrow + 2..close].trim(), true));
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
            '|' => {
                chars.next();
                if chars.peek() == Some(&'-') && chars.clone().nth(1) != Some('-') {
                    chars.next();
                    tokens.push(Token::Turnstile);
                } else {
                    tokens.push(Token::Pipe);
                }
            },
            '+' => { chars.next(); tokens.push(Token::Plus); },
            '*' => { chars.next(); tokens.push(Token::Star); },
            '!' => { chars.next(); tokens.push(Token::Bang); },
            '?' => { chars.next(); tokens.push(Token::Question); },
            '@' => { chars.next(); tokens.push(Token::At); },
            '[' => { chars.next(); tokens.push(Token::LBracket); },
            ']' => { chars.next(); tokens.push(Token::RBracket); },
            '=' => { chars.next(); tokens.push(Token::Eq); },
            '"' => {
                // String literal: the quoted name in `input "..."` / `output "..." = ...`.
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
                let token = match tokens.last() {
                    Some(Token::At) => Token::Ident(ident),
                    _ => keyword(&ident).unwrap_or(Token::Ident(ident)),
                };
                tokens.push(token);
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
        "let" => Token::Let, "var" => Token::Var, "set" => Token::Set, "output" => Token::Output,
        "type" => Token::Type, "enum" => Token::Enum,
        "input" => Token::Input, "import" => Token::Import,
        "map" => Token::Map,
        "join" => Token::Join, "min" => Token::Min, "distinct" => Token::Distinct,
        "count" => Token::Count, "collect" => Token::Collect,
        "flatmap" => Token::FlatMap,
        "arrange" => Token::Arrange, "negate" => Token::Negate,
        "filter" => Token::Filter, "inspect" => Token::Inspect,
        _ => return None,
    })
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    /// Declared sum types: name -> the variants in tag order, each with its payload shape.
    /// Populated before statements are parsed; tags are positions, scoped to their type.
    types: std::collections::HashMap<String, Vec<(String, corgi::Shape)>>,
    /// Erased aliases, resolved in the same dependency order as enums.
    aliases: std::collections::HashMap<String, corgi::Shape>,
    /// The declared types, in dependency order.
    type_order: Vec<String>,
    /// The corgi scope lambdas are parsed in, built at the first lambda.
    scope: Option<corgi::Scope>,
    /// Rows a fragment reads twice, bound to fresh names just before the statement that uses them,
    /// so that a chain of antijoins does not copy its left side at every step.
    hoisted: Vec<Stmt>,
    /// The next fresh name's number.
    fresh: usize,
    /// Every name the source writes, so that a fresh name neither collides with a binding nor
    /// captures a reference to one in any scope.
    written: std::collections::HashSet<String>,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        let written = tokens.iter().filter_map(|t| match t { Token::Ident(name) => Some(name.clone()), _ => None }).collect();
        Parser { tokens, pos: 0, types: Default::default(), aliases: Default::default(), type_order: Vec::new(), scope: None, hoisted: Vec::new(), fresh: 0, written }
    }

    /// The corgi scope of this program's lambdas: the registered functions and the base words
    /// as words, the built-in `Option` and `Result`, and all declared enums.
    fn lambda_scope(&mut self) -> &corgi::Scope {
        if self.scope.is_none() {
            let mut scope = crate::lambda::base_scope();
            for f in crate::lambda::registered() {
                scope.word(f.name(), crate::lambda::function_word(&f)).unwrap_or_else(|e| panic!("{e}"));
            }
            for name in &self.type_order {
                if let Some(variants) = self.types.get(name) {
                    declare_enum(&mut scope, name, variants, &self.types);
                }
            }
            self.scope = Some(scope);
        }
        self.scope.as_ref().unwrap()
    }
    fn peek(&self) -> &Token { &self.tokens[self.pos] }
    fn next(&mut self) -> Token { let t = self.tokens[self.pos].clone(); self.pos += 1; t }
    fn expect(&mut self, expected: &Token) { let t = self.next(); assert_eq!(&t, expected, "Expected {:?}, got {:?}", expected, t); }

    /// Collect declarations at every brace depth, then remove them from the statement stream.
    fn collect_types(&mut self) {
        let mut decls = Vec::new();
        let mut indices = std::collections::HashMap::new();
        let mut ranges = Vec::new();
        while *self.peek() != Token::Eof {
            if !matches!(self.peek(), Token::Type | Token::Enum) { self.next(); continue; }
            let start = self.pos;
            let kind = self.next();
            let name = self.parse_ident();
            assert!(!matches!(name.as_str(), "int" | "float" | "List" | "Option" | "Result"),
                "`{name}` is a built-in shape and cannot be declared");
            assert!(!indices.contains_key(&name), "type `{name}` declared twice");
            self.expect(&Token::Eq);
            let body = if kind == Token::Enum {
                let mut variants = vec![self.parse_variant_decl()];
                while *self.peek() == Token::Pipe {
                    self.next();
                    variants.push(self.parse_variant_decl());
                }
                for (tag, (variant, _)) in variants.iter().enumerate() {
                    assert!(!variants[..tag].iter().any(|(other, _)| other == variant),
                        "enum `{name}` declares `{variant}` twice");
                }
                TypeBody::Enum(variants)
            } else {
                let shape = self.parse_type_shape();
                // Anything but `;` after the shape, such as a `|` or a variant's payload, was meant as a sum.
                assert!(*self.peek() == Token::Semi, "type `{name}` must be one shape followed by `;`; sums are declared with `enum`, not `type`");
                TypeBody::Alias(shape)
            };
            self.expect(&Token::Semi);
            indices.insert(name.clone(), decls.len());
            ranges.push((start, self.pos));
            decls.push(TypeDecl { name, body });
        }
        let mut states = vec![0; decls.len()];
        let mut stack = Vec::new();
        for index in 0..decls.len() {
            declare_type(index, &decls, &indices, &mut states, &mut stack, self);
        }
        for (start, end) in ranges.into_iter().rev() { self.tokens.drain(start..end); }
        self.pos = 0;
    }

    fn parse_program(&mut self) -> Vec<Stmt> {
        let mut stmts = Vec::new();
        while *self.peek() != Token::Eof && *self.peek() != Token::RBrace {
            let stmt = self.parse_stmt();
            stmts.append(&mut self.hoisted);
            stmts.push(stmt);
        }
        stmts
    }

    /// One `Name shape?` of an `enum` declaration.
    fn parse_variant_decl(&mut self) -> (String, TypeShape) {
        let name = self.parse_ident();
        let shape = match self.peek() {
            Token::Pipe | Token::Semi => TypeShape::Unit,
            _ => self.parse_type_shape(),
        };
        (name, shape)
    }

    /// A payload shape: `int`, `float`, `()`, `(a, b, ..)`, `List(a)`, `Option(a)`, `Result(a, b)`,
    /// or a declared type's name.
    fn parse_type_shape(&mut self) -> TypeShape {
        use TypeShape as Shape;
        match self.next() {
            Token::LParen => {
                if *self.peek() == Token::RParen {
                    self.next();
                    return Shape::Unit;
                }
                let mut fields = vec![self.parse_type_shape()];
                while *self.peek() == Token::Comma {
                    self.next();
                    fields.push(self.parse_type_shape());
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
                    let inner = self.parse_type_shape();
                    self.expect(&Token::RParen);
                    Shape::List(Box::new(inner))
                }
                "Option" => {
                    self.expect(&Token::LParen);
                    let inner = self.parse_type_shape();
                    self.expect(&Token::RParen);
                    Shape::Option(Box::new(inner))
                }
                "Result" => {
                    self.expect(&Token::LParen);
                    let ok = self.parse_type_shape();
                    self.expect(&Token::Comma);
                    let err = self.parse_type_shape();
                    self.expect(&Token::RParen);
                    Shape::Result(Box::new(ok), Box::new(err))
                }
                name => Shape::Named(name.to_owned()),
            },
            other => panic!("expected a shape, got {:?}", other),
        }
    }

    fn parse_shape(&mut self) -> corgi::Shape {
        let shape = self.parse_type_shape();
        self.resolve_shape(&shape)
    }

    fn resolve_shape(&self, shape: &TypeShape) -> corgi::Shape {
        use corgi::Shape;
        match shape {
            TypeShape::Unit => Shape::Unit,
            TypeShape::Int => Shape::Int,
            TypeShape::Float => Shape::Float,
            TypeShape::Prod(fields) => Shape::Prod(fields.iter().map(|s| self.resolve_shape(s)).collect()),
            TypeShape::List(inner) => Shape::List(Box::new(self.resolve_shape(inner))),
            TypeShape::Option(inner) => Shape::Sum(vec![Shape::Unit, self.resolve_shape(inner)]),
            TypeShape::Result(ok, err) => Shape::Sum(vec![self.resolve_shape(ok), self.resolve_shape(err)]),
            TypeShape::Named(name) => self.aliases.get(name).cloned()
                .unwrap_or_else(|| Shape::Sum(self.type_lanes(name))),
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
            Token::Output => {
                self.next();
                let name = match self.next() {
                    Token::Str(s) => s,
                    o => panic!("Expected string literal after `output`, got {:?}", o),
                };
                let shape = self.declared_shape();
                self.expect(&Token::Eq);
                let e = self.parse_pipe_expr();
                self.expect(&Token::Semi);
                Stmt::Output(name, shape, e)
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
        let mut expr = self.parse_product();
        let mut concat = false;
        loop {
            let part = match self.peek() {
                Token::Pipe => { self.next(); expr = self.parse_pipe_op(expr); concat = false; continue },
                Token::Plus => { self.next(); self.parse_product() },
                Token::Minus => { self.next(); Expr::Negate(Box::new(self.parse_product())) },
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

    fn parse_product(&mut self) -> Expr {
        assert!(*self.peek() != Token::At, "a computed relation cannot be the first atom");
        assert!(!matches!(self.peek(), Token::Bang | Token::Question), "a fragment test cannot be the first atom");
        let source = self.parse_atom();
        if *self.peek() == Token::LParen { self.fragment(source) } else { source }
    }

    fn parse_atom(&mut self) -> Expr {
        match self.peek().clone() {
            Token::Input => {
                self.next();
                let input = match self.next() {
                    Token::Int(n) => crate::scope_ir::InputId::Number(n as usize),
                    Token::Str(name) => {
                        assert!(crate::scope_ir::InputId::is_name(&name), "input name {name:?} must be an identifier");
                        crate::scope_ir::InputId::Name(name)
                    }
                    other => panic!("`input` takes a number or a name, got {other:?}"),
                };
                self.source_shape(Expr::Input(input))
            },
            Token::Import => panic!("`import` is gone: write `input \"name\" : shape`, and bind the name to a trace at load, as `name=prog::output`"),
            Token::Ident(_) | Token::ColonColon => {
                let root = *self.peek() == Token::ColonColon;
                if root { self.next(); }
                let mut parts = vec![self.parse_ident()];
                while *self.peek() == Token::ColonColon {
                    self.next();
                    parts.push(self.parse_ident());
                }
                if root || parts.len() > 1 { Expr::Path(parts, root) }
                else { Expr::Name(parts.pop().unwrap()) }
            },
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

    fn join_projection(&mut self) -> Lambda {
        if *self.peek() == Token::Comma {
            self.next();
            self.lambda()
        } else {
            Lambda::new("let row = input in row".into(), &corgi::Scope::default())
        }
    }

    fn parse_pipe_op(&mut self, lhs: Expr) -> Expr {
        match self.peek().clone() {
            Token::Map => { self.next(); Expr::Map(Box::new(lhs), self.slot()) },
            Token::Join => {
                self.next();
                self.expect(&Token::LParen);
                let rhs = self.parse_pipe_expr();
                let projection = self.join_projection();
                self.expect(&Token::RParen);
                Expr::Join(Box::new(lhs), Box::new(rhs), projection)
            },
            Token::Min => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Min) },
            Token::Distinct => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Distinct) },
            Token::Count => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Count) },
            Token::Collect => { self.next(); Expr::Reduce(Box::new(lhs), Reducer::Collect) },
            Token::Ident(name) if name == "sum" => {
                self.next();
                // Move the integer value into the diff before arranging by key. Joining the
                // update's time with itself preserves every coordinate, including inside loops.
                let weight = Lambda::new(
                    "let (k, (v, t)) = input in ((k, ()), t, v) enlist".into(),
                    &corgi::Scope::default(),
                );
                let timed = Expr::Lift(Box::new(lhs));
                let weighted = Expr::JoinFunction(Box::new(timed), weight);
                Expr::Reduce(Box::new(weighted), Reducer::Count)
            },
            Token::Ident(name) if name == "reduce" => {
                self.next(); self.expect(&Token::LParen);
                // `reduce(limit k, (k, vals) -> …)`: the lambda is handed each key's first `k` values.
                let limit = if matches!(self.peek(), Token::Ident(word) if word == "limit") {
                    self.next();
                    let k = match self.next() {
                        Token::Int(k) if k > 0 => k as usize,
                        other => panic!("reduce: a limit is a positive integer, not {other:?}"),
                    };
                    self.expect(&Token::Comma);
                    Some(k)
                } else { None };
                let lambda = self.lambda();
                self.expect(&Token::RParen);
                Expr::Reduce(Box::new(lhs), Reducer::Expr(lambda, limit))
            },
            Token::FlatMap => { self.next(); Expr::FlatMap(Box::new(lhs), self.slot()) },
            Token::Arrange => { self.next(); Expr::Arrange(Box::new(lhs)) },
            Token::Negate => { self.next(); Expr::Negate(Box::new(lhs)) },
            Token::Filter => { self.next(); Expr::Filter(Box::new(lhs), self.slot()) },
            Token::Ident(name) if name == "lift" => { self.next(); Expr::Lift(Box::new(lhs)) },
            Token::Ident(name) if name == "half_join" => {
                self.next(); self.expect(&Token::LParen);
                let r = self.parse_pipe_expr(); self.expect(&Token::Comma);
                let strict = match self.next() {
                    Token::Int(0) => false,
                    Token::Int(1) => true,
                    other => panic!("half_join: strict is 0 or 1, not {other:?}"),
                };
                let p = self.join_projection();
                self.expect(&Token::RParen);
                Expr::HalfJoin(Box::new(lhs), Box::new(r), strict, p)
            },
            Token::Ident(name) if name == "semijoin" || name == "antijoin" => {
                let anti = name == "antijoin";
                self.next(); self.expect(&Token::LParen);
                let right = self.parse_pipe_expr();
                self.expect(&Token::RParen);
                let key = Lambda::new("let (k, _) = input in (k, ())".into(), self.lambda_scope());
                let keys = Expr::Reduce(Box::new(Expr::Map(Box::new(right), key)), Reducer::Distinct);
                let matched = Lambda::new("let (k, v, _) = input in (k, v)".into(), self.lambda_scope());
                let semi = Expr::Join(Box::new(lhs.clone()), Box::new(keys), matched);
                if anti { Expr::Concat(vec![lhs, Expr::Negate(Box::new(semi))]) } else { semi }
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
            Token::Lambda { pat, body } => {
                let mut lambda = Lambda::new(format!("let {pat} = input in {body}"), self.lambda_scope());
                if let Err(error) = &mut lambda.graph {
                    if let Some(name) = self.aliases.keys().find(|name| error.ends_with(&format!("unknown enum '{name}'"))) {
                        *error += &format!("; alias `{name}`: aliases are not yet available inside lambdas");
                    }
                }
                lambda
            },
            other => panic!("expected a lambda `pat -> body`, got {other:?}"),
        }
    }
}

/// Visit dependencies before declaring a type. The active stack gives a useful cycle path.
fn declare_type(
    index: usize,
    decls: &[TypeDecl],
    indices: &std::collections::HashMap<String, usize>,
    states: &mut [u8],
    stack: &mut Vec<usize>,
    parser: &mut Parser,
) {
    if states[index] == 2 { return; }
    if states[index] == 1 {
        let from = stack.iter().position(|&i| i == index).unwrap();
        let path = stack[from..].iter().chain(std::iter::once(&index))
            .map(|&i| decls[i].name.as_str()).collect::<Vec<_>>().join(" -> ");
        panic!("recursive types: {path}");
    }
    states[index] = 1;
    stack.push(index);
    let decl = &decls[index];
    let mut dependencies = Vec::new();
    match &decl.body {
        TypeBody::Enum(variants) => for (_, shape) in variants { shape.dependencies(&mut dependencies); },
        TypeBody::Alias(shape) => shape.dependencies(&mut dependencies),
    }
    for name in dependencies {
        let dependency = *indices.get(&name).unwrap_or_else(|| panic!("unknown type `{name}`"));
        declare_type(dependency, decls, indices, states, stack, parser);
    }
    match &decl.body {
        TypeBody::Enum(variants) => {
            let variants = variants.iter().map(|(name, shape)| (name.clone(), parser.resolve_shape(shape))).collect();
            parser.types.insert(decl.name.clone(), variants);
        }
        TypeBody::Alias(shape) => { parser.aliases.insert(decl.name.clone(), parser.resolve_shape(shape)); }
    }
    parser.type_order.push(decl.name.clone());
    stack.pop();
    states[index] = 2;
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
    scope.declare(&format!("enum {name} = {}", body.join(" | "))).unwrap_or_else(|e| panic!("enum `{name}`: {e}"));
}

pub fn parse(input: &str) -> Vec<Stmt> { let tokens = tokenize(input); let mut p = Parser::new(tokens); p.collect_types(); p.parse_program() }

/// The resolved type declarations and lambda scope, retained by the plan printer.
pub(crate) fn print_context(input: &str) -> (Vec<(String, Vec<(String, corgi::Shape)>)>, corgi::Scope, std::collections::BTreeSet<String>) {
    let mut p = Parser::new(tokenize(input));
    p.collect_types();
    let scope = p.lambda_scope().clone();
    let types = p.type_order.iter().filter_map(|name| p.types.get(name).map(|vs| (name.clone(), vs.clone()))).collect();
    let sources = p.tokens.iter().filter_map(|token| match token {
        Token::Lambda { pat, body } => Some(format!("let {pat} = input in {body}")),
        _ => None,
    }).collect();
    (types, scope, sources)
}

#[cfg(test)]
mod tests {
    use super::{parse, Stmt};

    fn panic_text(src: &str) -> String {
        let err = std::panic::catch_unwind(|| parse(src)).unwrap_err();
        err.downcast_ref::<String>().cloned()
            .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string())).expect("string panic")
    }

    #[test]
    fn types_resolve_after_uses_and_in_dependency_order() {
        let stmts = parse(r#"
            output "out" = input 0 : (int, Inner)
                | map((k, v) -> (k, v inject Outer::Wrap));
            enum Outer = Wrap Inner | Empty;
            enum Nested = Lane List(Option(Inner));
            enum Inner = Leaf int;
        "#);
        assert_eq!(stmts.len(), 1);
        assert!(matches!(&stmts[0], Stmt::Output(name, _, _) if name == "out"));
    }

    #[test]
    fn a_type_inside_a_scope_is_visible_at_the_root() {
        let stmts = parse(r#"
            output "out" = input 0 : (int, Member)
                | map((k, v) -> (k, v inject Group));
            inner: { enum Member = User int; }
            enum Group = Group Member;
        "#);
        assert_eq!(stmts.len(), 2);
        assert!(matches!(&stmts[1], Stmt::Scope(_, body) if body.is_empty()));
    }

    #[test]
    fn recursive_types_name_the_cycle() {
        let message = panic_text("enum A = Wrap B; enum B = Wrap A;");
        assert!(message.contains("A -> B -> A"), "{message}");
    }

    #[test]
    fn recursive_aliases_name_the_cycle() {
        for src in ["type A = B; type B = A;", "type A = B; enum B = Wrap A;"] {
            let message = panic_text(src);
            assert!(message.contains("recursive types: A -> B -> A"), "{message}");
        }
    }

    #[test]
    fn aliases_and_enums_share_a_namespace() {
        for src in ["type A = int; enum A = One;", "enum A = One; type A = int;", "type A = int; type A = float;"] {
            let message = panic_text(src);
            assert!(message.contains("type `A` declared twice"), "{message}");
        }
    }

    #[test]
    fn sums_require_enum() {
        for src in ["type X = A | B;", "type Node = Node List(int);"] {
            let message = panic_text(src);
            assert!(message.contains("sums are declared with `enum`"), "{message}");
        }
    }

    #[test]
    fn built_in_shapes_cannot_be_declared() {
        for src in ["type int = float;", "enum float = F;", "type List = int;", "enum Option = O;", "type Result = (int, int);"] {
            let message = panic_text(src);
            assert!(message.contains("is a built-in shape and cannot be declared"), "{message}");
        }
    }

    #[test]
    fn unknown_types_keep_their_error() {
        let message = panic_text("enum A = Wrap Missing;");
        assert!(message.contains("unknown type `Missing`"), "{message}");
    }

    #[test]
    fn duplicate_types_and_variants_keep_their_errors() {
        let name = panic_text("enum A = One; s: { enum A = Two; }");
        assert!(name.contains("type `A` declared twice"), "{name}");
        let variant = panic_text("enum A = One | One;");
        assert!(variant.contains("enum `A` declares `One` twice"), "{variant}");
    }

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
