//! Experimental join fragments, lowered from surface text to ordinary DDP text.
//! Only fragment source ranges are replaced; the core language has no knowledge of them.
//! Written join order, existing indexes and first-appearance binding order are preserved.
//! Literal and equality tests run at the first legal checkpoint as plain filters.
//! Variants bind through unweave lists, and may emit the next lookup key directly.
//! Later keys must use an earlier binding; constant-key cross joins belong in plain DDP.
//! This surface form may change or be removed.

use std::{cell::Cell, collections::BTreeMap, panic::{catch_unwind, AssertUnwindSafe}};
use crate::{ir::Lambda, parse::Expr};

/// Expand experimental fragments before handing the returned text to the core parser.
/// Errors retain the original message, prefixed by the surface line and column.
pub fn expand(text: &str) -> Result<String, String> {
    let at = Cell::new(0);
    catch_unwind(AssertUnwindSafe(|| expand_inner(text, &at))).map_err(|panic| {
        let message = panic.downcast_ref::<String>().map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied()).unwrap_or("surface expansion panicked");
        let prefix = &text[..at.get().min(text.len())];
        let line = prefix.bytes().filter(|b| *b == b'\n').count() + 1;
        let column = prefix.rsplit('\n').next().unwrap().chars().count() + 1;
        format!("{line}:{column}: {message}")
    })
}

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Let, Var, Set, Output, Type, Enum, Input, Import, Map, Join, Min, Distinct, Count,
    Collect, Arrange, Negate, Filter, LiftIter, FlatMap, Inspect,
    Ident(String), Int(i64), Str(String), Lambda,
    LParen, RParen, LBrace, RBrace, Comma, Semi, Colon, ColonColon, Eq,
    Pipe, Plus, Minus, Star, LBracket, RBracket, Eof, Other(char),
}
struct Span { kind: Token, start: usize, end: usize }
struct Source { tokens: Vec<Span>, comments: Vec<(usize, usize)> }

// The scanner leaves Corgi opaque, using the same lambda boundaries as the core lexer.
fn scan(text: &str, from: usize, to: usize, source: &mut Source, at: &Cell<usize>) {
    let bytes = text.as_bytes();
    let mut i = from;
    while i < to {
        let start = i;
        at.set(start);
        let ch = text[i..].chars().next().unwrap();
        if ch.is_ascii_whitespace() { i += 1; continue; }
        if bytes[i..].starts_with(b"--") {
            while i < to && bytes[i] != b'\n' { i += 1; }
            source.comments.push((start, i));
            continue;
        }
        if ch == '(' {
            source.tokens.push(Span { kind: Token::LParen, start, end: i + 1 });
            if let Some((begin, _, close)) = crate::parse::pipe::lambda_in(text, i + 1).filter(|(_, _, close)| *close < to) {
                scan(text, i + 1, begin, source, at);
                source.tokens.push(Span { kind: Token::Lambda, start: begin, end: close });
                source.tokens.push(Span { kind: Token::RParen, start: close, end: close + 1 });
                i = close + 1;
            } else { i += 1; }
            continue;
        }
        let kind = if ch == '"' {
            i += 1;
            let begin = i;
            while i < to && bytes[i] != b'"' { i += 1; }
            assert!(i < to, "Unterminated string literal");
            let s = text[begin..i].to_string();
            i += 1;
            Token::Str(s)
        } else if ch.is_ascii_digit() {
            while i < to && bytes[i].is_ascii_digit() { i += 1; }
            Token::Int(text[start..i].parse().unwrap())
        } else if ch.is_ascii_alphabetic() || ch == '_' {
            while i < to && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') { i += 1; }
            match &text[start..i] {
                "let" => Token::Let, "var" => Token::Var, "set" => Token::Set, "output" => Token::Output,
                "type" => Token::Type, "enum" => Token::Enum, "input" => Token::Input, "import" => Token::Import,
                "map" => Token::Map, "join" => Token::Join, "min" => Token::Min, "distinct" => Token::Distinct,
                "count" => Token::Count, "collect" => Token::Collect, "arrange" => Token::Arrange,
                "negate" => Token::Negate, "filter" => Token::Filter, "lift_iter" => Token::LiftIter,
                "flatmap" => Token::FlatMap, "inspect" => Token::Inspect,
                name => Token::Ident(name.into()),
            }
        } else {
            i += ch.len_utf8();
            match ch {
                ')' => Token::RParen, '{' => Token::LBrace, '}' => Token::RBrace,
                ',' => Token::Comma, ';' => Token::Semi, '=' => Token::Eq, '|' => Token::Pipe,
                '+' => Token::Plus, '-' => Token::Minus, '*' => Token::Star, '[' => Token::LBracket, ']' => Token::RBracket,
                ':' if bytes.get(i) == Some(&b':') => { i += 1; Token::ColonColon },
                ':' => Token::Colon,
                other => Token::Other(other),
            }
        };
        source.tokens.push(Span { kind, start, end: i });
    }
}

fn group_end(tokens: &[Span], mut i: usize) -> usize {
    let mut depth = 0;
    loop {
        match tokens[i].kind {
            Token::LParen => depth += 1,
            Token::RParen => { depth -= 1; if depth == 0 { return i + 1; } }
            Token::Eof => return i,
            _ => {},
        }
        i += 1;
    }
}
fn declaration_end(tokens: &[Span], mut i: usize) -> usize {
    while !matches!(tokens[i].kind, Token::Semi | Token::Eof) { i += 1; }
    if tokens[i].kind == Token::Semi { i + 1 } else { i }
}
fn next_fragment(tokens: &[Span], mut i: usize, at: &Cell<usize>) -> Option<usize> {
    while i + 1 < tokens.len() {
        match tokens[i].kind {
            Token::Type | Token::Enum => { i = declaration_end(tokens, i); continue; }
            Token::Colon if tokens[i + 1].kind == Token::LParen => {
                let end = group_end(tokens, i + 1);
                if tokens[end].kind == Token::LParen { at.set(tokens[end - 1].start); panic!("a fragment atom names an existing binding"); }
                i = end; continue;
            }
            Token::LParen if tokens[group_end(tokens, i)].kind == Token::LParen => return Some(i),
            Token::Input | Token::Import | Token::Int(_) | Token::Str(_) | Token::RParen if tokens[i + 1].kind == Token::LParen => {
                at.set(tokens[i].start);
                panic!("a fragment atom names an existing binding");
            }
            Token::Ident(_) | Token::ColonColon => {
                let start = i;
                if tokens[i].kind == Token::ColonColon { i += 1; }
                if !matches!(tokens[i].kind, Token::Ident(_)) { i += 1; continue; }
                i += 1;
                while tokens[i].kind == Token::ColonColon {
                    i += 1;
                    if !matches!(tokens[i].kind, Token::Ident(_)) { break; }
                    i += 1;
                }
                if tokens[i].kind == Token::LParen && (start == 0 || tokens[start - 1].kind != Token::Pipe) { return Some(start); }
                continue;
            }
            _ => i += 1,
        }
    }
    None
}
fn expand_inner(text: &str, at: &Cell<usize>) -> String {
    let mut source = Source { tokens: Vec::new(), comments: Vec::new() };
    scan(text, 0, text.len(), &mut source, at);
    source.tokens.push(Span { kind: Token::Eof, start: text.len(), end: text.len() });
    let tokens = &source.tokens;
    let Some(mut start) = next_fragment(tokens, 0, at) else { return text.to_string(); };
    // Type declarations are global. Reuse their existing resolver without parsing collection text.
    let mut declarations = text.bytes().map(|b| if b == b'\n' { b } else { b' ' }).collect::<Vec<_>>();
    let mut i = 0;
    let mut first_decl = None;
    while i + 1 < tokens.len() {
        if matches!(tokens[i].kind, Token::Type | Token::Enum) {
            let end = declaration_end(tokens, i);
            let (a, b) = (tokens[i].start, tokens[end.saturating_sub(1)].end);
            declarations[a..b].copy_from_slice(&text.as_bytes()[a..b]);
            first_decl.get_or_insert(a);
            i = end;
        } else { i += 1; }
    }
    at.set(first_decl.unwrap_or(tokens[start].start));
    let (types, scope, _) = crate::parse::pipe::print_context(&String::from_utf8(declarations).unwrap());
    let mut out = String::new();
    let mut copied = 0;
    loop {
        let begin = tokens[start].start;
        at.set(begin);
        let mut parser = Parser { tokens, pos: start, scope: &scope, types: &types, at };
        let fragment = parser.fragment();
        let end = tokens[parser.pos - 1].end;
        let expr = fragment.expansion(at);
        out.push_str(&text[copied..begin]);
        out.push('(');
        for &(a, b) in &source.comments {
            if begin <= a && b <= end { out.push('\n'); out.push_str(&text[a..b]); out.push('\n'); }
        }
        out.push_str(&expression(&expr));
        out.push(')');
        copied = end;
        match next_fragment(tokens, parser.pos, at) { Some(next) => start = next, None => break }
    }
    out.push_str(&text[copied..]);
    out
}

// Only the ordinary expression kinds produced by this expansion need printing.
fn expression(expr: &Expr) -> String {
    fn slot(l: &Lambda) -> String {
        let (pat, body) = l.src.strip_prefix("let ").unwrap().split_once(" = input in ").unwrap();
        format!("{pat} -> {body}")
    }
    match expr {
        Expr::Name(n) => n.clone(),
        Expr::Path(parts, root) => format!("{}{}", if *root { "::" } else { "" }, parts.join("::")),
        Expr::Join(l, r, f) => format!("{}\n    | join({}, {})", expression(l), expression(r), slot(f)),
        Expr::Map(e, f) | Expr::Filter(e, f) | Expr::FlatMap(e, f) => {
            let word = match expr { Expr::Map(..) => "map", Expr::Filter(..) => "filter", _ => "flatmap" };
            format!("{}\n    | {word}({})", expression(e), slot(f))
        }
        _ => unreachable!("fragment expansion emits names, joins and linear stages"),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Pattern {
    Var(String), Wild, Tuple(Vec<Pattern>), Literal(String),
    Variant { name: String, tag: usize, payload: Box<Pattern> },
}

impl Pattern {
    fn shape_pattern(&self) -> String {
        match self {
            Self::Tuple(ps) => tuple(&ps.iter().map(Self::shape_pattern).collect::<Vec<_>>()),
            Self::Variant { name, .. } => name.split("::").next().unwrap().to_string(),
            _ => "_".into(),
        }
    }
    fn has_variant(&self) -> bool {
        match self {
            Self::Variant { .. } => true,
            Self::Tuple(ps) => ps.iter().any(Self::has_variant),
            _ => false,
        }
    }
    fn constant(&self) -> bool {
        match self { Self::Literal(_) => true, Self::Tuple(ps) => ps.iter().all(Self::constant), _ => false }
    }
    fn check_lists(&self) {
        match self {
            // Check the whole literal so sibling constants can supply an empty list's shape.
            Self::Literal(s) if s != "[]" && s.contains("[]") => {
                corgi::parse_ml(s).unwrap_or_else(|e| panic!("DDIR fragment list literal `{s}` needs a complete element shape; use plain DDP with an explicit list shape: {e}"));
            }
            Self::Tuple(ps) => for p in ps { p.check_lists(); },
            Self::Variant { payload, .. } => payload.check_lists(),
            _ => {},
        }
    }
    fn text(&self) -> String {
        match self {
            Self::Var(n) | Self::Literal(n) => n.clone(), Self::Wild => "_".into(),
            Self::Tuple(ps) => tuple(&ps.iter().map(Self::text).collect::<Vec<_>>()),
            Self::Variant { name, payload, .. } => format!("{name}({})", payload.text()),
        }
    }
}


struct Atom { source: Expr, key: Pattern, val: Pattern, at: usize, key_at: usize }
struct Fragment { atoms: Vec<Atom>, scope: corgi::Scope }
struct Parser<'a> {
    tokens: &'a [Span], pos: usize, scope: &'a corgi::Scope,
    types: &'a [(String, Vec<(String, corgi::Shape)>)], at: &'a Cell<usize>,
}
impl Parser<'_> {
    fn peek(&self) -> &Token { &self.tokens[self.pos].kind }
    fn next(&mut self) -> Token {
        let span = &self.tokens[self.pos]; self.at.set(span.start); self.pos += 1; span.kind.clone()
    }
    fn expect(&mut self, expected: &Token) {
        let t = self.next(); assert_eq!(&t, expected, "Expected {:?}, got {:?}", expected, t);
    }
    fn parse_ident(&mut self) -> String {
        match self.next() { Token::Ident(n) => n, t => panic!("Expected ident, got {:?}", t) }
    }
    fn lambda_scope(&self) -> &corgi::Scope { self.scope }
    fn source(&mut self) -> Expr {
        if *self.peek() == Token::LParen {
            self.next();
            let source = self.source();
            assert!(*self.peek() == Token::RParen, "a fragment atom names an existing binding");
            self.next();
            return source;
        }
        assert!(matches!(self.peek(), Token::Ident(_) | Token::ColonColon), "a fragment atom names an existing binding");
        let root = *self.peek() == Token::ColonColon;
        if root { self.next(); }
        let mut parts = vec![self.parse_ident()];
        while *self.peek() == Token::ColonColon { self.next(); parts.push(self.parse_ident()); }
        if root || parts.len() > 1 { Expr::Path(parts, root) } else { Expr::Name(parts.remove(0)) }
    }
    fn fragment(&mut self) -> Fragment {
        let mut atoms = Vec::new();
        loop {
            let at = self.tokens[self.pos].start;
            let source = self.source();
            self.expect(&Token::LParen);
            let key_at = self.tokens[self.pos].start;
            let key = self.fragment_pattern();
            self.expect(&Token::Comma);
            let val = self.fragment_pattern();
            if atoms.is_empty() { key.check_lists(); }
            val.check_lists();
            self.expect(&Token::RParen);
            atoms.push(Atom { source, key, val, at, key_at });
            if *self.peek() != Token::Star { break; }
            self.next();
        }
        Fragment { atoms, scope: self.scope.clone() }
    }
    fn fragment_pattern(&mut self) -> Pattern {
        match self.next() {
            Token::LParen => {
                let mut ps = Vec::new();
                if *self.peek() != Token::RParen {
                    loop {
                        ps.push(self.fragment_pattern());
                        if *self.peek() != Token::Comma { break; }
                        self.next();
                    }
                }
                self.expect(&Token::RParen);
                Pattern::Tuple(ps)
            }
            Token::Ident(n) if n == "_" => Pattern::Wild,
            Token::Ident(mut n) => {
                if *self.peek() == Token::ColonColon { self.next(); n += &format!("::{}", self.parse_ident()); }
                let scope = self.lambda_scope();
                let constructor = scope.variant(&n).map(|(tag, ty)| {
                    let empty = matches!(scope.variants(ty).unwrap()[tag].1.as_ref(), Some(corgi::Shape::Unit));
                    (tag, ty.to_string(), empty)
                });
                // An ambiguous declared constructor must not silently become a variable either.
                let known = constructor.is_ok() || n.contains("::") || self.types.iter().any(|(_, vs)| vs.iter().any(|(v, _)| v == &n));
                if *self.peek() == Token::LParen || known {
                    let (tag, ty, empty) = constructor.unwrap_or_else(|e| panic!("fragment constructor `{n}`: {e}"));
                    let payload = if *self.peek() == Token::LParen {
                        self.next();
                        let p = if *self.peek() == Token::RParen { Pattern::Tuple(vec![]) } else { self.fragment_pattern() };
                        self.expect(&Token::RParen);
                        p
                    } else {
                        assert!(empty, "fragment constructor `{n}` requires a payload; write `{n}(pattern)`");
                        Pattern::Tuple(vec![])
                    };
                    Pattern::Variant { name: format!("{ty}::{}", n.rsplit("::").next().unwrap()), tag, payload: Box::new(payload) }
                } else {
                    Pattern::Var(n)
                }
            }
            Token::Int(n) => Pattern::Literal(n.to_string()),
            Token::Minus => match self.next() { Token::Int(n) => Pattern::Literal(format!("-{n}")), t => panic!("expected integer after -, got {t:?}") },
            Token::Str(s) => Pattern::Literal(format!("[{}]", s.bytes().map(|b| b.to_string()).collect::<Vec<_>>().join(", "))),
            Token::LBracket => {
                let mut xs = Vec::new();
                if *self.peek() != Token::RBracket {
                    loop {
                        let x = self.fragment_pattern();
                        assert!(x.constant(), "a list pattern is a literal");
                        xs.push(x.text());
                        if *self.peek() != Token::Comma { break; }
                        self.next();
                    }
                }
                self.expect(&Token::RBracket);
                Pattern::Literal(format!("[{}]", xs.join(", ")))
            }
            t => panic!("expected fragment pattern, got {t:?}"),
        }
    }
}
fn tuple(xs: &[String]) -> String { format!("({})", xs.join(", ")) }
// Emit a join's next key directly, as the design specifies. This is syntax
// composition, with no demand analysis or choice of index.
fn project(input: Expr, map: Lambda) -> Expr {
    match input {
        Expr::Join(left, right, projection) => Expr::Join(left, right,
            Lambda::compose("row", &[(&projection, "row".into()), (&map, "_call0".into())], |r| r[1].clone())),
        other => Expr::Map(Box::new(other), map),
    }
}

/// Logical fields stay fixed while checkpoints change their physical locations.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Access {
    Slot(usize), Field(Box<Access>, usize), Payload(Box<Access>, usize),
}

/// A lookup key's fields are read from k, including constructed variant payloads.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum KeyRead {
    Value(String), Field(Box<KeyRead>, usize), Payload(Box<KeyRead>, usize),
}
impl KeyRead {
    fn scalar(&self) -> Option<String> {
        match self {
            Self::Value(s) => Some(s.clone()),
            Self::Field(a, i) => Some(format!("{}.{i}", a.scalar()?)),
            Self::Payload(..) => None,
        }
    }
    fn bind(&self, bindings: &mut Vec<(String, String)>, names: &mut BTreeMap<KeyRead, String>) -> String {
        if let Some(name) = names.get(self) { return name.clone(); }
        match self {
            Self::Value(s) => s.clone(),
            Self::Field(a, i) => format!("{}.{i}", a.bind(bindings, names)),
            Self::Payload(a, tag) => {
                let source = a.bind(bindings, names);
                let name = format!("key_payload{}", bindings.len());
                bindings.push((format!("{source} enlist unweave .{}", tag + 1), name.clone()));
                names.insert(self.clone(), name.clone());
                name
            }
        }
    }
}

enum Test { Equal(Access, Access), Literal(Access, String) }
#[derive(Clone)]
struct Variant { at: Access, tag: usize, shape: String }
#[derive(Clone, Copy)]
enum Target<'a> { Key(&'a Pattern), Current, Output }
struct State<'a> {
    scope: &'a corgi::Scope,
    next: usize,
    names: Vec<String>,
    bound: BTreeMap<String, Access>,
    locations: BTreeMap<Access, String>,
    key: BTreeMap<Access, KeyRead>,
    variants: Vec<Variant>,
    tests: Vec<Test>,
}
impl State<'_> {
    fn lambda(&self, pat: &str, body: String) -> Lambda { Lambda::new(format!("let {pat} = input in {body}"), self.scope) }
    fn read(&self, at: &Access) -> String {
        if let Some(expr) = self.locations.get(at) { return expr.clone(); }
        match at {
            Access::Field(a, i) => format!("let field = {} in field.{i}", self.read(a)),
            _ => panic!("fragment payload or field is not bound: {at:?}"),
        }
    }
    fn ready(&self, at: &Access) -> bool {
        self.locations.contains_key(at) || matches!(at, Access::Field(a, _) if self.ready(a))
    }
    fn test(&self, test: &Test) -> String {
        match test {
            Test::Equal(a, b) => format!("({}, {}) eq", self.read(a), self.read(b)),
            Test::Literal(a, lit) if lit == "[]" => format!("({} len, 0) eq", self.read(a)),
            Test::Literal(a, lit) => format!("({}, {lit}) eq", self.read(a)),
        }
    }
    fn pattern(&mut self, p: &Pattern, at: Access) {
        match p {
            Pattern::Wild => {},
            Pattern::Var(n) => {
                if let Some(old) = self.bound.get(n) { self.tests.push(Test::Equal(old.clone(), at)); }
                else { self.names.push(n.clone()); self.bound.insert(n.clone(), at); }
            }
            Pattern::Literal(lit) => self.tests.push(Test::Literal(at, lit.clone())),
            Pattern::Tuple(ps) => for (i, p) in ps.iter().enumerate() {
                self.pattern(p, Access::Field(Box::new(at.clone()), i));
            },
            Pattern::Variant { tag, payload, .. } => {
                self.variants.push(Variant { at: at.clone(), tag: *tag, shape: payload.shape_pattern() });
                self.pattern(payload, Access::Payload(Box::new(at), *tag));
            }
        }
    }
    fn ingest(&mut self, p: &Pattern, expr: String) {
        if let Pattern::Tuple(ps) = p {
            for (i, p) in ps.iter().enumerate() { self.ingest(p, format!("{expr}.{i}")); }
        } else if !matches!(p, Pattern::Wild) {
            let at = Access::Slot(self.next);
            self.next += 1;
            self.locations.insert(at.clone(), expr);
            self.pattern(p, at);
        }
    }
    fn validate_key(&self, p: &Pattern) {
        fn mentions_bound(s: &State<'_>, p: &Pattern) -> bool {
            match p {
                Pattern::Var(n) => {
                    assert!(s.bound.contains_key(n), "fragment key variable `{n}` is not bound by an earlier atom");
                    true
                }
                Pattern::Tuple(ps) => ps.iter().fold(false, |found, p| mentions_bound(s, p) || found),
                Pattern::Variant { payload, .. } => mentions_bound(s, payload),
                Pattern::Literal(_) => false,
                Pattern::Wild => panic!("a later fragment key cannot contain a wildcard; use a variable bound by an earlier atom"),
            }
        }
        assert!(mentions_bound(self, p), "a constant fragment key would move every incoming row to one key; write a cross join as plain DDP");
        p.check_lists();
    }
    fn key_expr(&self, p: &Pattern) -> String {
        match p {
            Pattern::Var(n) => self.read(&self.bound[n]),
            Pattern::Tuple(ps) => tuple(&ps.iter().map(|p| self.key_expr(p)).collect::<Vec<_>>()),
            Pattern::Variant { name, payload, .. } => format!("{} inject {name}", self.key_expr(payload)),
            Pattern::Literal(s) => s.clone(),
            _ => unreachable!("lookup keys were validated"),
        }
    }
    fn key_locations(&self, p: &Pattern, expr: KeyRead, out: &mut BTreeMap<Access, KeyRead>) {
        match p {
            Pattern::Var(n) => { out.entry(self.bound[n].clone()).or_insert(expr); },
            Pattern::Tuple(ps) => for (i, p) in ps.iter().enumerate() {
                self.key_locations(p, KeyRead::Field(Box::new(expr.clone()), i), out);
            },
            Pattern::Variant { tag, payload, .. } => self.key_locations(payload, KeyRead::Payload(Box::new(expr), *tag), out),
            Pattern::Literal(_) => {},
            _ => unreachable!("lookup keys were validated"),
        }
    }
    /// Keep named fields and pending tests, retaining an enclosing sum only until it is bound.
    fn required(&self) -> std::collections::BTreeSet<Access> {
        fn resident(s: &State<'_>, at: &Access) -> Access {
            if s.ready(at) || s.key.contains_key(at) { return at.clone(); }
            match at {
                Access::Field(a, _) | Access::Payload(a, _) => resident(s, a),
                _ => panic!("missing fragment field {at:?}"),
            }
        }
        self.inputs(&self.variants).into_iter().map(|at| resident(self, &at)).collect()
    }
    fn inputs(&self, variants: &[Variant]) -> Vec<Access> {
        let mut inputs = self.bound.values().cloned().collect::<Vec<_>>();
        for test in &self.tests { match test {
            Test::Equal(a, b) => { inputs.push(a.clone()); inputs.push(b.clone()); }
            Test::Literal(a, _) => inputs.push(a.clone()),
        } }
        inputs.extend(variants.iter().map(|v| v.at.clone()));
        inputs
    }
    fn row(&mut self, key_expr: String, key: BTreeMap<Access, KeyRead>) -> String {
        let carried = self.required().into_iter().filter(|a| !key.contains_key(a)).collect::<Vec<_>>();
        let values = carried.iter().map(|a| self.read(a)).collect::<Vec<_>>();
        let mut locations = key.iter().filter_map(|(a, r)| r.scalar().map(|r| (a.clone(), r))).collect::<BTreeMap<_, _>>();
        for (i, at) in carried.into_iter().enumerate() { locations.insert(at, format!("v.{i}")); }
        self.locations = locations;
        self.key = key;
        format!("({key_expr}, {})", tuple(&values))
    }
    fn layout(&mut self, target: Target<'_>) -> String {
        match target {
            Target::Output => {
                let values = self.names.iter().map(|n| self.read(&self.bound[n])).collect::<Vec<_>>();
                let key = if values.len() == 1 { values[0].clone() } else { tuple(&values) };
                format!("({key}, ())")
            }
            Target::Key(p) => {
                self.validate_key(p);
                let expr = self.key_expr(p);
                let mut key = BTreeMap::new();
                self.key_locations(p, KeyRead::Value("k".into()), &mut key);
                self.row(expr, key)
            }
            // Decoded key payloads remain scalar while a following plain filter uses them.
            Target::Current => self.row("k".into(), self.key.iter().filter(|(_, r)| r.scalar().is_some()).map(|(a, r)| (a.clone(), r.clone())).collect()),
        }
    }
    fn filters(&mut self, mut expr: Expr, pat: &str) -> Expr {
        let mut ready = Vec::new();
        let tests = std::mem::take(&mut self.tests);
        for t in tests {
            let available = match &t {
                Test::Equal(a, b) => self.ready(a) && self.ready(b),
                Test::Literal(a, _) => self.ready(a),
            };
            if available { ready.push(self.test(&t)); } else { self.tests.push(t); }
        }
        if !ready.is_empty() {
            let test = if ready.len() == 1 { ready.pop().unwrap() } else { format!("{} all", tuple(&ready)) };
            expr = Expr::Filter(Box::new(expr), self.lambda(pat, test));
        }
        expr
    }
    fn capture(&mut self, variants: &[Variant], payload: &Access, prefix: &str, preserve_key: bool) -> (String, String) {
        fn nearest(s: &State<'_>, at: Access) -> Access {
            if s.locations.contains_key(&at) { return at; }
            match at {
                Access::Field(a, _) | Access::Payload(a, _) => nearest(s, *a),
                _ => panic!("missing fragment capture {at:?}"),
            }
        }
        let mut inputs = self.inputs(variants).into_iter().map(|a| nearest(self, a)).collect::<std::collections::BTreeSet<_>>();
        inputs.remove(payload);
        let mut values = inputs.iter().map(|a| self.read(a)).collect::<Vec<_>>();
        let mut names = inputs.into_iter().enumerate().map(|(i, a)| {
            let name = format!("{prefix}_{i}");
            self.locations.insert(a, name.clone());
            name
        }).collect::<Vec<_>>();
        if preserve_key { values.insert(0, "k".into()); names.insert(0, "k".into()); }
        (tuple(&values), tuple(&names))
    }
    fn payload_lists(&mut self, variants: &[Variant], i: usize, target: Target<'_>) -> String {
        let v = &variants[i];
        let list = format!("{} enlist unweave .{}", self.read(&v.at), v.tag + 1);
        let payload = Access::Payload(Box::new(v.at.clone()), v.tag);
        let name = format!("payload{i}");
        self.locations.insert(payload.clone(), name.clone());
        let (values, pat) = self.capture(&variants[i + 1..], &payload, &format!("capture{i}"), matches!(target, Target::Current));
        let last = i + 1 == variants.len();
        let body = if last { self.layout(target) } else { self.payload_lists(variants, i + 1, target) };
        let binder = if v.shape == "_" { name } else { format!("{name} : {}", v.shape) };
        let mapped = format!("({values}, {list}) cap_list map (({pat}, {binder}) -> {body})");
        if last { mapped } else { format!("{mapped} flatten .1") }
    }
    /// Finish one atom, before the next arrangement. A payload list can emit that next key.
    fn checkpoint(&mut self, expr: Expr, target: Target<'_>, pat: &str) -> Expr {
        if let Target::Key(p) = target { self.validate_key(p); }
        let mut expr = self.filters(expr, pat);
        let mut bindings = Vec::new();
        let mut names = BTreeMap::new();
        for (at, read) in &self.key { self.locations.insert(at.clone(), read.bind(&mut bindings, &mut names)); }
        let variants = std::mem::take(&mut self.variants);
        if variants.is_empty() && bindings.is_empty() {
            let body = self.layout(target);
            return project(expr, self.lambda(pat, body));
        }
        let defer = !self.tests.is_empty();
        // A plain key remains readable by the following filter. Final layouts and
        // constructed payload keys wait until that filter has consumed its fields.
        let output = if defer && !matches!(target, Target::Key(p) if !p.has_variant()) { Target::Current } else { target };
        let mut list = !variants.is_empty();
        let mut body = if list { self.payload_lists(&variants, 0, output) } else { self.layout(output) };
        for (i, (payloads, name)) in bindings.iter().enumerate().rev() {
            let mut context = vec!["k".into(), "v".into()];
            context.extend(bindings[..i].iter().map(|(_, n)| n.clone()));
            let context = tuple(&context);
            body = format!("({context}, {payloads}) cap_list map (({context}, {name}) -> {body}){}", if list { " flatten .1" } else { "" });
            list = true;
        }
        expr = Expr::FlatMap(Box::new(expr), self.lambda(pat, body));
        if defer {
            expr = self.filters(expr, "(k, v)");
            assert!(self.tests.is_empty(), "fragment tests must be bound at their checkpoint");
            let body = self.layout(target);
            expr = project(expr, self.lambda("(k, v)", body));
        }
        expr
    }
}

impl Fragment {
    fn expansion(&self, at: &Cell<usize>) -> Expr {
        let mut s = State { scope: &self.scope, next: 0, names: vec![], bound: BTreeMap::new(),
            locations: BTreeMap::new(), key: BTreeMap::new(), variants: vec![], tests: vec![] };
        let first = &self.atoms[0];
        s.ingest(&first.key, "k".into());
        s.key = s.locations.iter().map(|(a, expr)| (a.clone(), KeyRead::Value(expr.clone()))).collect();
        s.ingest(&first.val, "v".into());
        let direct = self.atoms.get(1).is_some_and(|next| first.key == next.key);
        let mut expr = first.source.clone();
        if !direct {
            at.set(self.atoms.get(1).map_or(first.at, |a| a.key_at));
            let target = self.atoms.get(1).map_or(Target::Output, |a| Target::Key(&a.key));
            expr = s.checkpoint(expr, target, &format!("(k : {}, v : {})", first.key.shape_pattern(), first.val.shape_pattern()));
        }
        for i in 1..self.atoms.len() {
            let atom = &self.atoms[i];
            at.set(atom.key_at);
            s.validate_key(&atom.key);
            s.ingest(&atom.val, "r".into());
            let body = s.row("k".into(), s.key.clone());
            let pat = if i == 1 && direct {
                format!("(k : {}, v : {}, r : {})", first.key.shape_pattern(), first.val.shape_pattern(), atom.val.shape_pattern())
            } else { format!("(k, v, r : {})", atom.val.shape_pattern()) };
            expr = Expr::Join(Box::new(expr), Box::new(atom.source.clone()), s.lambda(&pat, body));
            let target = self.atoms.get(i + 1).map_or(Target::Output, |a| Target::Key(&a.key));
            at.set(self.atoms.get(i + 1).map_or(atom.at, |a| a.key_at));
            expr = s.checkpoint(expr, target, "(k, v)");
        }
        expr
    }
}
