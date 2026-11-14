//! Deliberately ordered, arrangement-preserving source-to-source expansion.
//! Tests and payload bindings run at the first legal checkpoint, before the next rekey.

use super::*;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Atom { source: Expr, key: Pattern, val: Pattern }

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Fragment {
    pub(crate) text: String,
    atoms: Vec<Atom>,
    // Scope is compilation context, like the scope captured in ordinary parsed lambdas.
    #[serde(skip)]
    scope: corgi::Scope,
}
impl std::fmt::Debug for Fragment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.text.fmt(f) }
}

impl Parser {
    pub(super) fn fragment(&mut self, source: Expr) -> Expr {
        let mut atoms = Vec::new();
        let mut source = source;
        loop {
            assert!(matches!(source, Expr::Name(_) | Expr::Path(..)), "a fragment atom names an existing binding");
            self.expect(&Token::LParen);
            let key = self.fragment_pattern();
            self.expect(&Token::Comma);
            let val = self.fragment_pattern();
            if atoms.is_empty() { key.check_lists(); }
            val.check_lists();
            self.expect(&Token::RParen);
            atoms.push(Atom { source, key, val });
            if *self.peek() != Token::Star { break; }
            self.next();
            source = self.parse_atom();
        }
        let text = atoms.iter().map(|a| {
            let name = match &a.source {
                Expr::Name(n) => n.clone(),
                Expr::Path(ps, root) => format!("{}{}", if *root { "::" } else { "" }, ps.join("::")),
                _ => unreachable!(),
            };
            format!("{name}({}, {})", a.key.text(), a.val.text())
        }).collect::<Vec<_>>().join(" * ");
        Expr::Fragment(Box::new(Fragment { text, atoms, scope: self.lambda_scope().clone() }))
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
    fn expansion(&self) -> Expr {
        let mut s = State { scope: &self.scope, next: 0, names: vec![], bound: BTreeMap::new(),
            locations: BTreeMap::new(), key: BTreeMap::new(), variants: vec![], tests: vec![] };
        let first = &self.atoms[0];
        s.ingest(&first.key, "k".into());
        s.key = s.locations.iter().map(|(a, expr)| (a.clone(), KeyRead::Value(expr.clone()))).collect();
        s.ingest(&first.val, "v".into());
        let direct = self.atoms.get(1).is_some_and(|next| first.key == next.key);
        let mut expr = first.source.clone();
        if !direct {
            let target = self.atoms.get(1).map_or(Target::Output, |a| Target::Key(&a.key));
            expr = s.checkpoint(expr, target, &format!("(k : {}, v : {})", first.key.shape_pattern(), first.val.shape_pattern()));
        }
        for i in 1..self.atoms.len() {
            let atom = &self.atoms[i];
            s.validate_key(&atom.key);
            s.ingest(&atom.val, "r".into());
            let body = s.row("k".into(), s.key.clone());
            let pat = if i == 1 && direct {
                format!("(k : {}, v : {}, r : {})", first.key.shape_pattern(), first.val.shape_pattern(), atom.val.shape_pattern())
            } else { format!("(k, v, r : {})", atom.val.shape_pattern()) };
            expr = Expr::Join(Box::new(expr), Box::new(atom.source.clone()), s.lambda(&pat, body));
            let target = self.atoms.get(i + 1).map_or(Target::Output, |a| Target::Key(&a.key));
            expr = s.checkpoint(expr, target, "(k, v)");
        }
        expr
    }
}

/// Rewrite syntax before scope/path resolution; no arrangement or demand decisions here.
pub(crate) fn expand(stmts: &mut [Stmt]) {
    fn expression(e: &mut Expr) {
        match e {
            Expr::Fragment(f) => { *e = f.expansion(); },
            Expr::TypedSource(e, _, _) | Expr::Map(e, _) | Expr::Reduce(e, _) | Expr::Filter(e, _)
            | Expr::Negate(e) | Expr::LiftIter(e) | Expr::FlatMap(e, _) | Expr::Lift(e)
            | Expr::JoinFunction(e, _) | Expr::Inspect(e, _) | Expr::Arrange(e) => expression(e),
            Expr::Join(l, r, _) | Expr::HalfJoin(l, r, _, _) => { expression(l); expression(r); },
            Expr::Concat(es) => for e in es { expression(e); },
            _ => {},
        }
    }
    for stmt in stmts {
        match stmt {
            Stmt::Let(_, e) | Stmt::Set(_, e) | Stmt::Output(_, _, e) => expression(e),
            Stmt::Scope(_, body) => expand(body), Stmt::Var(..) => {},
        }
    }
}
