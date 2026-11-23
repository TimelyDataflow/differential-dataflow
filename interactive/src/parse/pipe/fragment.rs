//! Deliberately ordered, arrangement-preserving source-to-source expansion.
//! Tests and payload bindings run at the first legal checkpoint, before the next rekey.

use super::*;
use std::collections::BTreeMap;

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
    /// Distinct variables in tuples: a key that restricts nothing, so joining on it needs no test first.
    fn unrestricted(&self) -> bool {
        fn walk<'a>(p: &'a Pattern, seen: &mut Vec<&'a str>) -> bool {
            match p {
                Pattern::Var(n) if !seen.contains(&n.as_str()) => { seen.push(n); true }
                Pattern::Tuple(ps) => ps.iter().all(|p| walk(p, seen)),
                _ => false,
            }
        }
        walk(self, &mut Vec::new())
    }
    fn leaves(&self, accept: impl Fn(&Self) -> bool + Copy) -> bool {
        match self {
            Self::Tuple(ps) => ps.iter().all(|p| p.leaves(accept)),
            leaf => accept(leaf),
        }
    }
    fn visit_vars(&self, visit: &mut impl FnMut(&str)) {
        match self {
            Self::Var(name) => visit(name),
            Self::Tuple(ps) => ps.iter().for_each(|p| p.visit_vars(visit)),
            Self::Variant { payload, .. } => payload.visit_vars(visit),
            _ => {},
        }
    }
    fn number_matches(&mut self, names: &mut Vec<String>) {
        match self {
            Self::Var(name) => {
                let position = format!("match{}", names.len());
                names.push(std::mem::replace(name, position));
            }
            Self::Tuple(ps) => ps.iter_mut().for_each(|p| p.number_matches(names)),
            Self::Variant { payload, .. } => payload.number_matches(names),
            _ => {},
        }
    }
    fn constant(&self) -> bool {
        match self { Self::Literal(_) => true, Self::Tuple(ps) => ps.iter().all(Self::constant), _ => false }
    }
    fn has_empty_list(&self) -> bool {
        match self {
            Self::Literal(s) => s == "[]",
            Self::Tuple(ps) => ps.iter().any(Self::has_empty_list),
            Self::Variant { payload, .. } => payload.has_empty_list(),
            _ => false,
        }
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
            Self::Literal(n) => n.clone(),
            Self::Tuple(ps) => tuple(&ps.iter().map(Self::text).collect::<Vec<_>>()),
            _ => unreachable!("only constants in list literals need text"),
        }
    }
}

struct Atom {
    source: Expr,
    key: Pattern,
    val: Pattern,
    membership: Option<Membership>,
}

struct Membership {
    names: Vec<String>,
    negated: bool,
}

impl Atom {
    fn target(&self, state: &State<'_>, keep: bool) -> Target<'_> {
        if let Some(test) = &self.membership {
            for name in &test.names {
                assert!(state.bound.contains_key(name), "fragment test variable `{name}` is not bound by an earlier atom");
            }
        }
        if keep { Target::Keep(&self.key) } else { Target::Key(&self.key) }
    }

    fn membership(source: Expr, mut key: Pattern, mut val: Pattern, negated: bool, scope: &corgi::Scope) -> Self {
        let full_key = key.leaves(|p| matches!(p, Pattern::Var(_))) && val.leaves(|p| matches!(p, Pattern::Wild));
        let original_key = key.clone();
        // Number occurrences by position. Two uses of the same pattern can share
        // the right side even if they give their variables different names.
        let mut names = Vec::new();
        key.number_matches(&mut names);
        val.number_matches(&mut names);
        assert!(!names.is_empty(), "a fragment test needs at least one matched variable");

        let mut state = State::new(scope);
        let (source, key) = if full_key {
            let pat = format!("(k, vals : List(({}, int)))", val.shape_pattern());
            let reducer = state.lambda(&pat, "((), 1) enlist".into());
            (Expr::Reduce(Box::new(source), Reducer::Expr(reducer, Some(1))), original_key)
        } else {
            let pat = state.bind_first(&key, &val);
            let projected = state.checkpoint(source, Target::Output, &pat);
            let fields = names.iter().cloned().map(Pattern::Var).collect::<Vec<_>>();
            let key = if let [field] = &fields[..] { field.clone() } else { Pattern::Tuple(fields) };
            (Expr::Reduce(Box::new(projected), Reducer::Distinct), key)
        };
        Self { source, key, val: Pattern::Wild, membership: Some(Membership { names, negated }) }
    }
}

enum Mode {
    Test,
    ComputeLast,
}
impl Mode {
    fn description(&self) -> &str {
        match self {
            Self::Test => "test (all arguments bound)",
            Self::ComputeLast => "compute (arguments 1 and 2 bound, argument 3 new)",
        }
    }
}

struct Relation {
    name: &'static str,
    arity: usize,
    modes: &'static [Mode],
}

const RELATIONS: &[Relation] = &[
    Relation { name: "eq", arity: 2, modes: &[Mode::Test] },
    Relation { name: "ne", arity: 2, modes: &[Mode::Test] },
    Relation { name: "lt", arity: 2, modes: &[Mode::Test] },
    Relation { name: "le", arity: 2, modes: &[Mode::Test] },
    Relation { name: "gt", arity: 2, modes: &[Mode::Test] },
    Relation { name: "ge", arity: 2, modes: &[Mode::Test] },
    Relation { name: "add", arity: 3, modes: &[Mode::Test, Mode::ComputeLast] },
    Relation { name: "sub", arity: 3, modes: &[Mode::Test, Mode::ComputeLast] },
    Relation { name: "mul", arity: 3, modes: &[Mode::Test, Mode::ComputeLast] },
];

enum Argument {
    Name(String),
    Literal(String),
}
impl Argument {
    fn parse(text: String) -> Self {
        let text = text.trim();
        assert!(text != "_", "a computed relation cannot use a wildcard");
        let mut chars = text.chars();
        if chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Self::Name(text.into());
        }
        // A corgi list accepts only constants, including nested tuples and lists.
        corgi::parse_ml(&format!("[\n{text}\n]")).unwrap_or_else(|error|
            panic!("computed relation argument must be a variable or literal: {error}"));
        Self::Literal(text.into())
    }
}

struct Evaluation {
    relation: &'static Relation,
    args: Vec<Argument>,
    negated: bool,
}

enum Action {
    Read(Atom),
    Compute(String, Evaluation),
}
struct Step {
    action: Action,
    tests: Vec<Evaluation>,
}

/// A fragment's atoms, expanded as soon as they are parsed: no syntax tree holds a fragment.
struct Fragment { steps: Vec<Step>, head: Option<String> }

impl Parser {
    pub(super) fn fragment(&mut self, source: Expr) -> Expr {
        let first = self.collection_atom(source, None, true);
        let mut bound = BTreeMap::new();
        first.key.visit_vars(&mut |name| { bound.insert(name.to_string(), 0); });
        first.val.visit_vars(&mut |name| { bound.insert(name.to_string(), 0); });
        let mut steps = vec![Step { action: Action::Read(first), tests: vec![] }];
        while *self.peek() == Token::Star {
            self.next();
            let membership = match self.peek() {
                Token::Bang => Some(true),
                Token::Question => Some(false),
                _ => None,
            };
            if membership.is_some() { self.next(); }
            if *self.peek() == Token::At {
                assert!(membership != Some(false), "write a computed relation with `@` or `!@`");
                let eval = self.evaluation(membership == Some(true));
                let mut ready = 0;
                let mut missing = Vec::new();
                for (i, arg) in eval.args.iter().enumerate() {
                    if let Argument::Name(name) = arg {
                        if let Some(&at) = bound.get(name) {
                            ready = ready.max(at);
                        } else {
                            missing.push((i, name));
                        }
                    }
                }
                let output = match &missing[..] {
                    [(2, name)] if !eval.negated && eval.relation.modes.iter().any(|mode| matches!(mode, Mode::ComputeLast)) => Some((*name).clone()),
                    _ => None,
                };
                if missing.is_empty() {
                    steps[ready].tests.push(eval);
                } else if let Some(name) = output {
                    bound.insert(name.clone(), steps.len());
                    steps.push(Step { action: Action::Compute(name, eval), tests: vec![] });
                } else {
                    let unbound = missing.iter().map(|(i, name)| format!("argument {} `{name}`", i + 1)).collect::<Vec<_>>();
                    let modes = eval.relation.modes.iter().map(Mode::description).collect::<Vec<_>>();
                    let note = if eval.negated { ". Negation needs every argument bound" } else { "" };
                    panic!("relation `@{}` has unbound {}. Supported modes are {}{note}",
                        eval.relation.name, unbound.join(", "), modes.join(" and "));
                }
            } else {
                let source = self.parse_atom();
                let atom = self.collection_atom(source, membership, false);
                if atom.membership.is_none() {
                    atom.val.visit_vars(&mut |name| { bound.entry(name.to_string()).or_insert(steps.len()); });
                }
                steps.push(Step { action: Action::Read(atom), tests: vec![] });
            }
        }
        let head = if *self.peek() == Token::Turnstile {
            self.next();
            let Token::Head(head) = self.next() else { panic!("a fragment head must be a tuple `|- (key, value)`") };
            Some(head)
        } else {
            None
        };
        let (mut fresh, mut hoisted) = (self.fresh, Vec::new());
        let expr = Fragment { steps, head }.expansion(self.lambda_scope(), &mut |rows| {
            let name = format!("fragment_rows{fresh}");
            fresh += 1;
            hoisted.push(Stmt::Let(name.clone(), rows));
            Expr::Name(name)
        });
        self.fresh = fresh;
        self.hoisted.extend(hoisted);
        expr
    }

    fn collection_atom(&mut self, source: Expr, membership: Option<bool>, first: bool) -> Atom {
        assert!(matches!(source, Expr::Name(_) | Expr::Path(..)), "a fragment atom names an existing binding");
        self.expect(&Token::LParen);
        let key = self.fragment_pattern();
        self.expect(&Token::Comma);
        let val = self.fragment_pattern();
        if first || membership.is_some() { key.check_lists(); }
        val.check_lists();
        self.expect(&Token::RParen);
        match membership {
            Some(negated) => Atom::membership(source, key, val, negated, self.lambda_scope()),
            None => Atom { source, key, val, membership: None },
        }
    }

    fn evaluation(&mut self, negated: bool) -> Evaluation {
        self.expect(&Token::At);
        let name = self.parse_ident();
        let relation = RELATIONS.iter().find(|r| r.name == name)
            .unwrap_or_else(|| panic!("unknown computed relation `@{name}`"));
        let Token::RelationArgs(args) = self.next() else { panic!("expected arguments for relation `@{name}`") };
        assert!(args.len() == relation.arity, "relation `@{name}` expects {} arguments, got {}", relation.arity, args.len());
        Evaluation { relation, args: args.into_iter().map(Argument::parse).collect(), negated }
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
                let known = constructor.is_ok() || n.contains("::") || self.types.values().any(|vs| vs.iter().any(|(v, _)| v == &n));
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
/// A row's key or value from its fields: one field is itself, not a tuple of one.
fn fields(xs: &[String]) -> String { if let [x] = xs { x.clone() } else { tuple(xs) } }
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

enum Test<'a> { Equal(Access, Access), Literal(Access, String), Eval(&'a Evaluation) }
#[derive(Clone)]
struct Variant { at: Access, tag: usize, shape: String }
#[derive(Clone, Copy)]
enum Target<'a> { Key(&'a Pattern), Keep(&'a Pattern), Current, Head(&'a str), Output }
struct State<'a> {
    scope: &'a corgi::Scope,
    next: usize,
    names: Vec<String>,
    bound: BTreeMap<String, Access>,
    locations: BTreeMap<Access, String>,
    key: BTreeMap<Access, KeyRead>,
    variants: Vec<Variant>,
    tests: Vec<Test<'a>>,
}
impl<'a> State<'a> {
    fn new(scope: &'a corgi::Scope) -> Self {
        Self { scope, next: 0, names: vec![], bound: BTreeMap::new(), locations: BTreeMap::new(),
            key: BTreeMap::new(), variants: vec![], tests: vec![] }
    }
    fn bind_first(&mut self, key: &Pattern, val: &Pattern) -> String {
        self.ingest(key, "k".into());
        self.key = self.locations.iter().map(|(at, expr)| (at.clone(), KeyRead::Value(expr.clone()))).collect();
        self.ingest(val, "v".into());
        format!("(k : {}, v : {})", key.shape_pattern(), val.shape_pattern())
    }
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
    fn argument(&self, arg: &Argument) -> String {
        match arg {
            Argument::Name(name) => self.read(&self.bound[name]),
            Argument::Literal(literal) => literal.clone(),
        }
    }
    fn evaluate(&self, eval: &Evaluation) -> String {
        format!("({}, {}) {}", self.argument(&eval.args[0]), self.argument(&eval.args[1]), eval.relation.name)
    }
    fn test(&self, test: &Test<'_>) -> String {
        match test {
            Test::Equal(a, b) => format!("({}, {}) eq", self.read(a), self.read(b)),
            Test::Literal(a, lit) if lit == "[]" => format!("({} len, 0) eq", self.read(a)),
            Test::Literal(a, lit) => format!("({}, {lit}) eq", self.read(a)),
            Test::Eval(eval) => {
                let mut body = self.evaluate(eval);
                if let [_, _, result] = &eval.args[..] {
                    body = format!("({body}, {}) eq", self.argument(result));
                }
                if eval.negated { format!("({body}, 0) eq") } else { body }
            }
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
        assert!(!p.has_empty_list(), "DDIR fragment key literal `[]` needs a complete element shape; use plain DDP with an explicit list shape");
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
            Test::Eval(_) => {}, // Every argument is already in bound.
        } }
        inputs.extend(variants.iter().map(|v| v.at.clone()));
        inputs
    }
    fn row(&mut self, key_expr: String, key: BTreeMap<Access, KeyRead>) -> String {
        let carried = self.required().into_iter().filter(|a| !key.contains_key(a)).collect::<Vec<_>>();
        let values = carried.iter().map(|a| self.read(a)).collect::<Vec<_>>();
        let mut locations = key.iter().filter_map(|(a, r)| r.scalar().map(|r| (a.clone(), r))).collect::<BTreeMap<_, _>>();
        let one = carried.len() == 1;
        for (i, at) in carried.into_iter().enumerate() { locations.insert(at, if one { "v".into() } else { format!("v.{i}") }); }
        self.locations = locations;
        self.key = key;
        format!("({key_expr}, {})", fields(&values))
    }
    fn layout(&mut self, target: Target<'_>) -> String {
        match target {
            Target::Head(head) => {
                let names = tuple(&self.names);
                // A closed corgi lambda catches free names, including an accidental
                // reference to the generated slot's input.
                corgi::parse_ml_in(&format!("input map ({names} -> {head})"), self.scope)
                    .unwrap_or_else(|error| panic!("fragment head: {error}"));
                let values = self.names.iter().map(|n| self.read(&self.bound[n])).collect::<Vec<_>>();
                format!("let {names} = {} in {head}", tuple(&values))
            }
            Target::Output => {
                let values = self.names.iter().map(|n| self.read(&self.bound[n])).collect::<Vec<_>>();
                let key = fields(&values);
                format!("({key}, ())")
            }
            Target::Key(p) | Target::Keep(p) => {
                self.validate_key(p);
                let expr = if matches!(target, Target::Keep(_)) { "k".into() } else { self.key_expr(p) };
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
                Test::Eval(eval) => eval.args.iter().all(|arg| match arg {
                    Argument::Name(name) => self.ready(&self.bound[name]),
                    Argument::Literal(_) => true,
                }),
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
        let (values, pat) = self.capture(&variants[i + 1..], &payload, &format!("capture{i}"), matches!(target, Target::Current | Target::Keep(_)));
        let last = i + 1 == variants.len();
        let body = if last { self.layout(target) } else { self.payload_lists(variants, i + 1, target) };
        let binder = if v.shape == "_" { name } else { format!("{name} : {}", v.shape) };
        let mapped = format!("({values}, {list}) cap_list map (({pat}, {binder}) -> {body})");
        if last { mapped } else { format!("{mapped} flatten") }
    }
    /// Finish one atom, before the next arrangement. A payload list can emit that next key.
    fn checkpoint(&mut self, expr: Expr, target: Target<'_>, pat: &str) -> Expr {
        if let Target::Key(p) | Target::Keep(p) = target { self.validate_key(p); }
        let mut expr = self.filters(expr, pat);
        let mut bindings = Vec::new();
        let mut names = BTreeMap::new();
        for (at, read) in &self.key { self.locations.insert(at.clone(), read.bind(&mut bindings, &mut names)); }
        let variants = std::mem::take(&mut self.variants);
        if variants.is_empty() && bindings.is_empty() {
            if matches!(target, Target::Current) && pat == "(k, v)" {
                return expr;
            }
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
            body = format!("({context}, {payloads}) cap_list map (({context}, {name}) -> {body}){}", if list { " flatten" } else { "" });
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

impl Step {
    fn target(&self, state: &State<'_>, keep: bool) -> Target<'_> {
        match &self.action {
            Action::Read(atom) => atom.target(state, keep),
            Action::Compute(..) => Target::Current,
        }
    }
}

impl Fragment {
    /// `share` binds rows read twice to a name and returns that name.
    fn expansion(&self, scope: &corgi::Scope, share: &mut dyn FnMut(Expr) -> Expr) -> Expr {
        let mut s = State::new(scope);
        let output = self.head.as_deref().map_or(Target::Output, Target::Head);
        let Action::Read(first) = &self.steps[0].action else { unreachable!() };
        let first_pat = s.bind_first(&first.key, &first.val);
        s.tests.extend(self.steps[0].tests.iter().map(Test::Eval));
        // A restricted first key is tested before a new arrangement, so rows it rejects are never paired.
        let same_key = matches!(self.steps.get(1), Some(Step { action: Action::Read(next), .. }) if first.key == next.key);
        let direct = first.key.unrestricted() && same_key;
        let mut expr = first.source.clone();
        if !direct {
            // Tests may restrict the key, but an identical lookup must retain its original value.
            let target = self.steps.get(1).map_or(output, |step| step.target(&s, same_key));
            expr = s.checkpoint(expr, target, &first_pat);
        }
        for (i, step) in self.steps.iter().enumerate().skip(1) {
            let mut checkpoint_pat = "(k, v)";
            match &step.action {
                Action::Read(atom) => {
                    if let Some(test) = &atom.membership {
                        // An antijoin reads the rows so far twice; a name keeps them from being copied.
                        if test.negated && !matches!(expr, Expr::Name(_) | Expr::Path(..)) { expr = share(expr); }
                        let left = test.negated.then(|| expr.clone());
                        let joined = Expr::Join(Box::new(expr), Box::new(atom.source.clone()),
                            s.lambda("(k, (v, _))", "(k, v)".into()));
                        expr = if let Some(left) = left {
                            let difference = Expr::Concat(vec![left, Expr::Negate(Box::new(joined))]);
                            if i == 1 && direct && self.steps.len() == 2 && s.tests.is_empty() && self.keeps_first_row(first) {
                                return difference;
                            }
                            difference
                        } else { joined };
                        if i == 1 && direct { checkpoint_pat = &first_pat; }
                    } else {
                        s.validate_key(&atom.key);
                        s.ingest(&atom.val, "r".into());
                        let body = s.row("k".into(), s.key.clone());
                        let pat = if i == 1 && direct {
                            format!("(k : {}, (v : {}, r : {}))", first.key.shape_pattern(), first.val.shape_pattern(), atom.val.shape_pattern())
                        } else { format!("(k, (v, r : {}))", atom.val.shape_pattern()) };
                        expr = Expr::Join(Box::new(expr), Box::new(atom.source.clone()), s.lambda(&pat, body));
                    }
                }
                Action::Compute(name, eval) => {
                    let value = s.evaluate(eval);
                    s.ingest(&Pattern::Var(name.clone()), "computed".into());
                    let target = if step.tests.is_empty() {
                        self.steps.get(i + 1).map_or(output, |next| next.target(&s, false))
                    } else {
                        Target::Current
                    };
                    let row = s.layout(target);
                    expr = project(expr, s.lambda("(k, v)", format!("let computed = {value} in {row}")));
                    if step.tests.is_empty() { continue; }
                }
            }
            s.tests.extend(step.tests.iter().map(Test::Eval));
            let target = self.steps.get(i + 1).map_or(output, |next| next.target(&s, false));
            expr = s.checkpoint(expr, target, checkpoint_pat);
        }
        expr
    }

    /// A direct first antijoin with this head already has the requested row.
    fn keeps_first_row(&self, first: &Atom) -> bool {
        let (Pattern::Var(key), Pattern::Var(val), Some(head)) =
            (&first.key, &first.val, &self.head) else { return false; };
        key != val && head.strip_prefix('(').and_then(|s| s.strip_suffix(')')).and_then(|s| s.split_once(','))
            .is_some_and(|(k, v)| k.trim() == key && v.trim() == val)
    }
}
