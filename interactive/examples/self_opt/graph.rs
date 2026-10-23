//! Shared export walk and graph encoding for the self optimizer and demand examples.
use corgi::Shape;
use interactive::corgi::container::Updates;
use interactive::corgi::feed::FeedDecoder;
use interactive::ir::{Lambda, LinearOp, Reducer};
use interactive::scope_ir::{Item, Node, Ref, Scope, Source};
use std::collections::HashMap;

pub const SCOPE_STRIDE: i64 = 1 << 20;
pub const HOLDER_BASE: i64 = 1 << 40;

pub const LINEAR: i64 = 0;
pub const CONCAT: i64 = 1;
pub const ARRANGE: i64 = 2;
pub const JOIN: i64 = 3;
pub const REDUCE: i64 = 4;
pub const INSPECT: i64 = 5;
pub const HALF_JOIN: i64 = 6;
pub const HOLDER: i64 = 9;

/// Payloads, interned by their `Debug` text (which is what the Rust dedup compares).
#[derive(Default)]
pub struct Payloads {
    pub ids: HashMap<String, i64>,
    pub ops: HashMap<i64, LinearOp>,
    pub projections: HashMap<i64, Lambda>,
    pub half_joins: HashMap<i64, (bool, Lambda)>,
    pub reducers: HashMap<i64, Reducer>,
    pub labels: HashMap<i64, String>,
}

impl Payloads {
    pub fn intern(&mut self, text: String) -> i64 {
        let next = self.ids.len() as i64 + 1;
        *self.ids.entry(text).or_insert(next)
    }
}

pub type Edge = (i64, i64, i64);

/// Rows for one program, plus the numbering needed to read the result back.
#[derive(Default)]
pub struct Rows {
    pub nodes: Vec<(i64, (i64, i64, i64, Vec<i64>))>,
    pub edges: Vec<((i64, i64), Edge)>,
}

/// Scope and holder numbering, shared by the export walk and the read-back walk:
/// both visit the tree in the same order and so allocate the same numbers.
pub struct Numbering {
    pub next_scope: i64,
    pub next_holder: i64,
}

impl Numbering {
    pub fn scope(&mut self) -> i64 {
        let s = self.next_scope;
        self.next_scope += 1;
        s
    }
    pub fn holder(&mut self) -> i64 {
        let h = self.next_holder;
        self.next_holder += 1;
        h
    }
}

pub fn enc(r: &Ref, base: i64) -> Edge {
    match r {
        Ref::Local(i) => (0, base + *i as i64, 0),
        Ref::Import(k) => (1, *k as i64, 0),
        Ref::Var(v) => (2, *v as i64, 0),
        Ref::ChildExport(c, j) => (3, base + *c as i64, *j as i64),
    }
}

pub fn dec((tag, a, b): Edge, base: i64) -> Ref {
    match tag {
        0 => Ref::Local((a - base) as usize),
        1 => Ref::Import(a as usize),
        2 => Ref::Var(a as usize),
        3 => Ref::ChildExport((a - base) as usize, b as usize),
        _ => panic!("bad ref tag {tag}"),
    }
}

/// `sources`, when present, marks `s` as a root scope in *shared* mode: the root scopes of
/// all programs form one domain (so dedup can merge across programs), and a root import is
/// named by its source rather than by its position.
pub fn export_scope(
    s: &Scope,
    num: &mut Numbering,
    pay: &mut Payloads,
    rows: &mut Rows,
    sources: Option<&[i64]>,
) {
    let scope = num.scope();
    let base = scope * SCOPE_STRIDE;
    let domain = if sources.is_some() { -1 } else { scope };
    let encode = |r: &Ref| match (r, sources) {
        (Ref::Import(k), Some(src)) => (1, src[*k], 0),
        _ => enc(r, base),
    };
    let holder = |numbering: &mut Numbering, out: &mut Rows, r: &Ref| {
        let h = numbering.holder();
        out.nodes.push((h, (domain, HOLDER, h, Vec::new())));
        out.edges.push(((h, 0), encode(r)));
    };
    for (i, item) in s.items.iter().enumerate() {
        let id = base + i as i64;
        match item {
            Item::Op(node) => {
                let (kind, pid, ops, refs): (i64, i64, Vec<i64>, Vec<&Ref>) = match node {
                    Node::Linear { input, ops } => {
                        let ids = ops
                            .iter()
                            .map(|op| {
                                let op_id = pay.intern(format!("op {:?}", op));
                                pay.ops.insert(op_id, op.clone());
                                op_id
                            })
                            .collect();
                        (LINEAR, 0, ids, vec![input])
                    }
                    Node::Concat(refs) => (CONCAT, 0, Vec::new(), refs.iter().collect()),
                    Node::Arrange(input) => (ARRANGE, 0, Vec::new(), vec![input]),
                    Node::Join {
                        left,
                        right,
                        projection,
                    } => {
                        let pid = pay.intern(format!("join {:?}", projection));
                        pay.projections.insert(pid, projection.clone());
                        (JOIN, pid, Vec::new(), vec![left, right])
                    }
                    Node::HalfJoin {
                        stream,
                        arrangement,
                        strict,
                        projection,
                    } => {
                        let pid = pay.intern(format!("half_join {strict} {:?}", projection));
                        pay.half_joins.insert(pid, (*strict, projection.clone()));
                        (HALF_JOIN, pid, Vec::new(), vec![stream, arrangement])
                    }
                    Node::Reduce { input, reducer } => {
                        let pid = pay.intern(format!("reduce {:?}", reducer));
                        pay.reducers.insert(pid, reducer.clone());
                        (REDUCE, pid, Vec::new(), vec![input])
                    }
                    Node::Inspect { input, label } => {
                        let pid = pay.intern(format!("inspect {:?}", label));
                        pay.labels.insert(pid, label.clone());
                        (INSPECT, pid, Vec::new(), vec![input])
                    }
                };
                rows.nodes.push((id, (domain, kind, pid, ops)));
                for (pos, r) in refs.into_iter().enumerate() {
                    rows.edges.push(((id, pos as i64), encode(r)));
                }
            }
            Item::Sub(child) => {
                for imp in &child.imports {
                    if let Source::Parent(r) = &imp.from {
                        holder(num, rows, r);
                    }
                }
                export_scope(child, num, pay, rows, None);
            }
        }
    }
    for b in &s.binds {
        holder(num, rows, &b.value);
    }
    for e in &s.exports {
        holder(num, rows, &e.value);
    }
}

fn columns(
    rows: impl IntoIterator<Item = (String, String)>,
    shape: (Shape, Shape),
    diff: i64,
) -> Updates {
    let mut decoder = FeedDecoder::new(&shape.0, &shape.1);
    for (key, val) in rows {
        decoder.push(&key, Some(&val), diff).unwrap();
    }
    decoder.finish()
}

pub fn updates(rows: &Rows, diff: i64) -> (Updates, Updates) {
    let nodes = columns(
        rows.nodes.iter().map(|(id, (scope, kind, pid, ops))| {
            (
                format!("tuple({id})"),
                format!(
                    "tuple({scope},{kind},{pid},list({}))",
                    ops.iter().map(i64::to_string).collect::<Vec<_>>().join(",")
                ),
            )
        }),
        (
            Shape::Prod(vec![Shape::Int]),
            Shape::Prod(vec![
                Shape::Int,
                Shape::Int,
                Shape::Int,
                Shape::List(Box::new(Shape::Int)),
            ]),
        ),
        diff,
    );
    let edges = columns(
        rows.edges.iter().map(|((h, pos), (tag, a, b))| {
            (format!("tuple({h},{pos})"), format!("tuple({tag},{a},{b})"))
        }),
        (
            Shape::Prod(vec![Shape::Int, Shape::Int]),
            Shape::Prod(vec![Shape::Int, Shape::Int, Shape::Int]),
        ),
        diff,
    );
    (nodes, edges)
}
