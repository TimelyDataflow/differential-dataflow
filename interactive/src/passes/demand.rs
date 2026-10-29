//! Field demand: which top-level key and value fields of each collection something reads.
//! An analysis only; `narrow` is the transform that acts on it.
//!
//! Every collection in the tree (an import, a variable, an operator's output, each step of a
//! linear chain, an export) is a *site* with one fact per top-level field of its key and its
//! value, and a *presence* fact for the collection itself. An edge `(consumer, producer)` says
//! that demanding the first fact demands the second. Seeds are the program's interface: root
//! imports and exports, declared shapes, and inspections. The closure from the seeds is the
//! set of live facts; a dead field is one nothing reads.
//!
//! Lambda input demand comes from Corgi's demand analysis of the compiled slot. A slot that
//! Corgi cannot analyze reads its whole input. A reduce reads its whole input, except the
//! value fields a sum-only reducer ignores (`summed_fields`), whose coalescing cannot change
//! the sum.

use std::collections::VecDeque;

use corgi::{Demand as Request, Shape};

use crate::ir::{Lambda, LinearOp, Reducer};
use crate::scope_ir::{self as st, Item, Node, Ref, Source};
use crate::shapes::{self, CollShape, ItemShapes, ScopeShapes};

/// The facts of one collection: its presence, then one fact per top-level field of its key
/// and of its value (none for `()`, one for a scalar or any other non-product shape).
#[derive(Clone, Debug)]
pub(super) struct Site {
    pub shape: (Shape, Shape),
    pub presence: usize,
    pub key: Vec<usize>,
    pub val: Vec<usize>,
}

impl Site {
    pub(super) fn all(&self) -> impl Iterator<Item = usize> + '_ {
        std::iter::once(self.presence).chain(self.key.iter().copied()).chain(self.val.iter().copied())
    }

    fn fields(&self, side: usize) -> &[usize] { if side == 0 { &self.key } else { &self.val } }

    /// Each field fact with its path in the lambda's output `(key, val)`.
    fn field_paths(&self) -> Vec<(usize, Vec<usize>)> {
        let mut fields = Vec::new();
        for (side, shape) in [&self.shape.0, &self.shape.1].into_iter().enumerate() {
            for (index, &id) in self.fields(side).iter().enumerate() {
                fields.push((id, match shape { Shape::Prod(_) => vec![side, index], _ => vec![side] }));
            }
        }
        fields
    }

    /// The shape with every dead field replaced by `()`.
    pub(super) fn narrowed(&self, live: &[bool]) -> (Shape, Shape) {
        (narrow_shape(&self.shape.0, &self.key, live), narrow_shape(&self.shape.1, &self.val, live))
    }
}

pub(super) fn width(shape: &Shape) -> usize {
    match shape { Shape::Unit => 0, Shape::Prod(fields) => fields.len(), _ => 1 }
}

fn narrow_shape(shape: &Shape, facts: &[usize], live: &[bool]) -> Shape {
    match shape {
        Shape::Unit => Shape::Unit,
        Shape::Prod(fields) => Shape::Prod(fields.iter().zip(facts).map(|(f, &id)| if live[id] { f.clone() } else { Shape::Unit }).collect()),
        other => if live[facts[0]] { other.clone() } else { Shape::Unit },
    }
}

pub(super) enum ItemSites {
    /// An operator's output, and for a linear chain the output of each of its steps
    /// (the last of them is the output itself).
    Op { output: Site, stages: Vec<Site> },
    Sub(Box<ScopeSites>),
}

pub(super) struct ScopeSites {
    pub imports: Vec<Site>,
    pub vars: Vec<Site>,
    pub items: Vec<ItemSites>,
    pub exports: Vec<Site>,
}

impl ScopeSites {
    pub(super) fn at(&self, r: &Ref) -> &Site {
        match r {
            Ref::Import(i) => &self.imports[*i],
            Ref::Var(i) => &self.vars[*i],
            Ref::Local(i) => match &self.items[*i] { ItemSites::Op { output, .. } => output, _ => panic!("demand: {r:?} is a scope") },
            Ref::ChildExport(i, e) => match &self.items[*i] { ItemSites::Sub(scope) => &scope.exports[*e], _ => panic!("demand: {r:?} is not a scope") },
        }
    }
}

/// The analysis: sites, edges and seeds. `live` computes the closure, with extra seeds.
pub(super) struct Demand {
    pub root: ScopeSites,
    seeds: Vec<usize>,
    /// For each fact, the facts it demands.
    next: Vec<Vec<usize>>,
}

impl Demand {
    /// Analyze a typed program. `shapes` is its inference, without problems.
    pub(super) fn analyze(program: &st::Program, shapes: &ScopeShapes) -> Result<Self, String> {
        let mut builder = Builder::default();
        let root = builder.scope(&program.root, shapes, None, 0)?;
        for export in &root.exports { builder.seeds.extend(export.all()); }
        let mut next = vec![Vec::new(); builder.facts];
        for (from, to) in builder.edges { next[from].push(to); }
        Ok(Demand { root, seeds: builder.seeds, next })
    }

    /// The live facts: everything the seeds and `pinned` reach.
    pub(super) fn live(&self, pinned: &[usize]) -> Vec<bool> {
        let mut live = vec![false; self.next.len()];
        let mut queue = VecDeque::new();
        for &id in self.seeds.iter().chain(pinned) {
            if !live[id] { live[id] = true; queue.push_back(id); }
        }
        while let Some(id) = queue.pop_front() {
            for &target in &self.next[id] {
                if !live[target] { live[target] = true; queue.push_back(target); }
            }
        }
        live
    }
}

#[derive(Default)]
struct Builder {
    facts: usize,
    edges: Vec<(usize, usize)>,
    seeds: Vec<usize>,
}

impl Builder {
    fn fact(&mut self) -> usize { self.facts += 1; self.facts - 1 }

    /// A site whose fields each demand the site's presence.
    fn site(&mut self, key: Shape, val: Shape) -> Site {
        let presence = self.fact();
        let key_ids: Vec<_> = (0..width(&key)).map(|_| self.fact()).collect();
        let val_ids: Vec<_> = (0..width(&val)).map(|_| self.fact()).collect();
        for &id in key_ids.iter().chain(&val_ids) { self.edges.push((id, presence)); }
        Site { shape: (key, val), presence, key: key_ids, val: val_ids }
    }

    fn shape_site(&mut self, shape: &CollShape) -> Site { self.site(shape.key.clone(), shape.val.clone()) }

    fn edge(&mut self, from: usize, to: usize) { self.edges.push((from, to)); }

    /// Presence demands the input's presence.
    fn presence(&mut self, from: &Site, input: &Site) { self.edge(from.presence, input.presence); }

    /// Presence demands every fact of the input.
    fn opaque(&mut self, from: &Site, input: &Site) {
        for id in input.all() { self.edge(from.presence, id); }
    }

    /// Field by field, on both sides when the widths agree, else everything.
    fn identity(&mut self, from: &Site, input: &Site) {
        self.presence(from, input);
        for side in 0..2 {
            let (out, src) = (from.fields(side), input.fields(side));
            if out.len() == src.len() {
                for (&o, &i) in out.iter().zip(src) { self.edge(o, i); }
            } else {
                for &i in src { self.edge(from.presence, i); }
            }
        }
    }

    fn key_identity(&mut self, from: &Site, input: &Site) {
        self.presence(from, input);
        for (&out, &src) in from.key.iter().zip(&input.key) { self.edge(out, src); }
    }

    /// Each demanded output path demands the input fields the lambda reads for it.
    fn lambda(&mut self, from: &Site, slots: &[Shape], lambda: &Lambda, fallback: &[&Site]) {
        let Some(analysis) = SlotDemand::new(lambda, slots) else {
            for input in fallback { self.opaque(from, input); }
            return;
        };
        for (output_id, path) in from.field_paths() {
            for (slot, field) in analysis.reads(&path) {
                if let Some(&id) = slot_fields(fallback, slot).and_then(|fields| fields.get(field)) { self.edge(output_id, id); }
            }
        }
    }

    /// Presence demands the input fields a scalar (predicate, delay, list) reads.
    fn scalar(&mut self, from: &Site, slots: &[Shape], lambda: &Lambda, input: &Site) {
        let Some(analysis) = SlotDemand::new(lambda, slots) else { self.opaque(from, input); return };
        for (slot, field) in analysis.reads(&[]) {
            if let Some(&id) = slot_fields(&[input], slot).and_then(|fields| fields.get(field)) { self.edge(from.presence, id); }
        }
    }

    fn scope(&mut self, scope: &st::Scope, shapes: &ScopeShapes, parent: Option<&ScopeSites>, depth: usize) -> Result<ScopeSites, String> {
        let known = |shape: &Option<CollShape>, what: String| shape.clone().ok_or_else(|| format!("demand: no shape for {what}"));
        let mut sites = ScopeSites { imports: Vec::new(), vars: Vec::new(), items: Vec::new(), exports: Vec::new() };
        for (i, (import, shape)) in scope.imports.iter().zip(&shapes.imports).enumerate() {
            let site = self.shape_site(&known(shape, format!("import {i}"))?);
            match &import.from {
                Source::Parent(r) => {
                    let source = parent.ok_or("demand: a root import names its parent")?.at(r);
                    self.identity(&site, source);
                    // A declared shape is a contract with the parent.
                    if import.shape.is_some() { self.seeds.extend(site.all()); }
                }
                // Root sources keep their shapes: they are the program's interface.
                Source::Input(_) | Source::Trace(_) => self.seeds.extend(site.all()),
            }
            sites.imports.push(site);
        }
        for (i, (var, shape)) in scope.vars.iter().zip(&shapes.vars).enumerate() {
            let site = self.shape_site(&known(shape, format!("var {i}"))?);
            // A declaration constrains every feedback binding.
            if var.shape.is_some() { self.seeds.extend(site.all()); }
            sites.vars.push(site);
        }
        for (i, item) in scope.items.iter().enumerate() {
            let item = match (item, &shapes.items[i]) {
                (Item::Op(node), ItemShapes::Op(shape)) => {
                    let output = self.shape_site(&known(shape, format!("item {i}"))?);
                    let stages = self.node(node, &output, &sites, depth)?;
                    ItemSites::Op { output, stages }
                }
                (Item::Sub(child), ItemShapes::Sub(child_shapes)) => {
                    ItemSites::Sub(Box::new(self.scope(child, child_shapes, Some(&sites), depth + 1)?))
                }
                _ => return Err(format!("demand: item {i} disagrees with shape inference")),
            };
            sites.items.push(item);
        }
        for bind in &scope.binds {
            let (var, source) = (sites.vars[bind.var].clone(), sites.at(&bind.value).clone());
            self.identity(&var, &source);
        }
        for (i, export) in scope.exports.iter().enumerate() {
            let site = self.shape_site(&known(&shapes.exports[i], format!("export {i}"))?);
            let source = sites.at(&export.value).clone();
            self.identity(&site, &source);
            if export.shape.is_some() { self.seeds.extend(site.all()); }
            sites.exports.push(site);
        }
        Ok(sites)
    }

    /// The edges of one operator. Returns the sites of a linear chain's steps.
    fn node(&mut self, node: &Node, output: &Site, sites: &ScopeSites, depth: usize) -> Result<Vec<Site>, String> {
        let mut stages = Vec::new();
        match node {
            Node::Linear { input, ops } => {
                let mut before = sites.at(input).clone();
                let steps = shapes::linear_shapes(ops, before.shape.0.clone(), before.shape.1.clone(), depth)?;
                if ops.is_empty() { self.identity(output, &before); }
                for (j, op) in ops.iter().enumerate() {
                    let (key, val) = steps[j + 1].clone();
                    let after = if j + 1 == ops.len() { output.clone() } else { self.site(key, val) };
                    let slots = [before.shape.0.clone(), before.shape.1.clone()];
                    match op {
                        LinearOp::Project(lambda) => {
                            self.presence(&after, &before);
                            self.lambda(&after, &slots, lambda, &[&before]);
                        }
                        LinearOp::Filter(lambda) | LinearOp::EnterAt(lambda) => {
                            self.identity(&after, &before);
                            self.scalar(&after, &slots, lambda, &before);
                        }
                        LinearOp::Negate => self.identity(&after, &before),
                        // The value gains a trailing coordinate; the time is not data of the input.
                        LinearOp::LiftIter => {
                            self.key_identity(&after, &before);
                            for (&out, &src) in after.val.iter().zip(&before.val) { self.edge(out, src); }
                        }
                        // `(key, val)` becomes `(key, (val, time))`: the value is one fact of the output.
                        LinearOp::Lift => {
                            self.key_identity(&after, &before);
                            if let Some(&value) = after.val.first() {
                                for &field in &before.val { self.edge(value, field); }
                            }
                        }
                        // The list decides output presence and positions, whatever is read of its elements.
                        LinearOp::FlatMap(lambda) => {
                            self.key_identity(&after, &before);
                            self.scalar(&after, &slots, lambda, &before);
                        }
                        LinearOp::JoinFunction(lambda) => {
                            self.presence(&after, &before);
                            self.scalar(&after, &slots, lambda, &before);
                        }
                    }
                    stages.push(after.clone());
                    before = after;
                }
            }
            // Arms share one narrowing: every arm must keep what any arm keeps.
            Node::Concat(refs) => {
                for r in refs {
                    let arm = sites.at(r);
                    self.identity(output, arm);
                    for (&out, &src) in output.key.iter().chain(&output.val).zip(arm.key.iter().chain(&arm.val)) { self.edge(src, out); }
                    self.edge(arm.presence, output.presence);
                }
            }
            // An arrangement is keyed by its whole key.
            Node::Arrange(r) => {
                let input = sites.at(r);
                self.identity(output, input);
                for &key in &input.key { self.edge(output.presence, key); }
            }
            Node::Join { left, right, projection } => {
                let (left, right) = (sites.at(left), sites.at(right));
                self.presence(output, left);
                self.presence(output, right);
                for &key in left.key.iter().chain(&right.key) { self.edge(output.presence, key); }
                let slots = [left.shape.0.clone(), left.shape.1.clone(), right.shape.1.clone()];
                self.lambda(output, &slots, projection, &[left, right]);
            }
            Node::HalfJoin { stream, arrangement, projection, .. } => {
                let (stream, arrangement) = (sites.at(stream), sites.at(arrangement));
                self.presence(output, stream);
                self.presence(output, arrangement);
                // Keys decide matching, and the stream's lifted time decides whether a match is
                // admissible and supplies the output's data time.
                for &key in stream.key.iter().chain(&arrangement.key) { self.edge(output.presence, key); }
                self.edge(output.presence, *stream.val.get(1).ok_or("demand: half_join has no time field")?);
                let Shape::Prod(parts) = &stream.shape.1 else { return Err("demand: half_join stream value is not (value, time)".into()) };
                let value = parts.first().ok_or("demand: half_join has no value field")?.clone();
                let slots = [stream.shape.0.clone(), value, arrangement.shape.1.clone()];
                // The lambda gives `(key, val)`; the operator appends time after `val`, so the
                // output value's first fact is the lambda's whole value and its second is free.
                let Some(analysis) = SlotDemand::new(projection, &slots) else {
                    self.opaque(output, stream);
                    self.opaque(output, arrangement);
                    return Ok(stages);
                };
                for (output_id, path) in output.field_paths() {
                    if path == [1, 1] { continue; }
                    let projected = if path == [1, 0] { vec![1] } else { path };
                    for (slot, field) in analysis.reads(&projected) {
                        let source = match slot { 0 => stream.key.get(field), 1 => stream.val.first(), 2 => arrangement.val.get(field), _ => None };
                        if let Some(&source) = source { self.edge(output_id, source); }
                    }
                }
            }
            Node::Reduce { input, reducer, limit } => {
                let input = sites.at(input);
                self.presence(output, input);
                for &key in &input.key { self.edge(output.presence, key); }
                let summed = match (limit, reducer) {
                    (None, Reducer::Expr(lambda)) => summed_fields(lambda, &input.shape.0, &input.shape.1),
                    _ => None,
                };
                match summed {
                    Some(fields) => for field in fields { self.edge(output.presence, input.val[field]); },
                    None => for &value in &input.val { self.edge(output.presence, value); },
                }
            }
            // Inspection is observable even with no downstream consumer.
            Node::Inspect { input, .. } => {
                let input = sites.at(input);
                self.seeds.extend(output.all());
                self.opaque(output, input);
            }
        }
        Ok(stages)
    }
}

/// Which slot a lambda's input field belongs to: `(key, val)` for a linear op, `(key, left
/// val, right val)` for a join.
fn slot_fields<'a>(inputs: &'a [&Site], slot: usize) -> Option<&'a [usize]> {
    match (inputs, slot) {
        ([site], 0) | ([site, _], 0) => Some(&site.key),
        ([site], 1) | ([site, _], 1) => Some(&site.val),
        ([_, right], 2) => Some(&right.val),
        _ => None,
    }
}

/// A slot compiled at its input, asked which top-level input fields each output path reads.
struct SlotDemand {
    compiled: corgi::Compiled,
    input: Shape,
    output: Shape,
    slots: Vec<Shape>,
}

impl SlotDemand {
    fn new(lambda: &Lambda, slots: &[Shape]) -> Option<Self> {
        let graph = lambda.graph().ok()?;
        let input = Shape::Prod(slots.to_vec());
        let output = corgi::shape_of(graph, &input).ok()?;
        // Prove patterns at the original type before asking which input fields are read.
        let compiled = corgi::compile(graph.as_ref().clone(), &input).ok()?;
        Some(SlotDemand { compiled, input, output, slots: slots.to_vec() })
    }

    /// The `(slot, field)` pairs read for the output at `path`; everything if Corgi cannot say.
    fn reads(&self, path: &[usize]) -> Vec<(usize, usize)> {
        let all = || self.slots.iter().enumerate().flat_map(|(slot, shape)| (0..width(shape)).map(move |field| (slot, field))).collect();
        let Some(request) = output_request(&self.output, path) else { return all() };
        let Ok(analysis) = corgi::analyze_demand(self.compiled.graph(), &self.input, &request) else { return all() };
        // Requests keep original field positions. Inspect each original field separately so
        // dropping an earlier field cannot renumber the facts.
        fn field(request: &Request, index: usize) -> &Request {
            match request { Request::Prod(fields) => &fields[index], other => other }
        }
        let mut fields = Vec::new();
        for (slot, original) in self.slots.iter().enumerate() {
            let request = field(analysis.input_demand(), slot);
            let parts = match original { Shape::Prod(parts) => parts.as_slice(), other => std::slice::from_ref(other) };
            for (index, part) in parts.iter().enumerate() {
                let need = if matches!(original, Shape::Prod(_)) { field(request, index) } else { request };
                match need.projected_shape(part) {
                    Ok(Shape::Unit) => {}
                    Ok(_) => fields.push((slot, index)),
                    Err(_) => return all(),
                }
            }
        }
        fields
    }
}

/// The request for one output path: all of that field, none of its siblings.
fn output_request(shape: &Shape, path: &[usize]) -> Option<Request> {
    let Some((&field, tail)) = path.split_first() else { return Some(Request::All) };
    let Shape::Prod(parts) = shape else { return None };
    let chosen = output_request(parts.get(field)?, tail)?;
    Some(Request::Prod((0..parts.len()).map(|i| if i == field { chosen.clone() } else { Request::None }).collect()))
}

// ---------------------------------------------------------------------------
// Reducers that only sum. Rule and proof by GPT-6 Sol (Codex), 2026-10-28.

/// The top-level value fields a reducer reads, when coalescing values that agree on them
/// cannot change its output: the lambda reads `vals` only as one `map` whose element is
/// `diff * g(value)` (the bare diff when `g = 1`), consumed by one integer `fold_add`, and it
/// gives no output for an empty list. Coalescing by the read fields preserves that sum,
/// retractions included, and a group whose diffs cancel disappears either way. Any other read
/// of the list (its length, its first value, a filter on the diff) can see entry boundaries.
pub(super) fn summed_fields(lambda: &Lambda, key: &Shape, val: &Shape) -> Option<Vec<usize>> {
    let input = Shape::Prod(vec![key.clone(), Shape::List(Box::new(Shape::Prod(vec![val.clone(), Shape::Int])))]);
    let compiled = corgi::compile(lambda.graph().ok()?.as_ref().clone(), &input).ok()?;
    sum_only(&compiled, key, val)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Term { Neutral, Diff, Linear, Bad }

/// A map element must be `diff * g(value)`, with `g = 1` for the bare diff.
fn weighted(body: &corgi::Graph<corgi::NumOp>) -> bool {
    use corgi::{ArithOp, BinOp, NodeRef, NumOp, Op};
    let nodes: Vec<_> = body.nodes().collect();
    let mut terms = Vec::with_capacity(nodes.len());
    for node in &nodes {
        let term = match node {
            NodeRef::Input => Term::Bad,
            NodeRef::Tuple(fields) => if fields.iter().any(|&i| terms[i] != Term::Neutral) { Term::Bad } else { Term::Neutral },
            NodeRef::Op(NumOp::Core(Op::Field(field)), i) => match &nodes[*i] {
                NodeRef::Input => match field { 0 => Term::Neutral, 1 => Term::Diff, _ => Term::Bad },
                NodeRef::Tuple(fields) => fields.get(*field).map(|&j| terms[j]).unwrap_or(Term::Bad),
                _ if terms[*i] == Term::Neutral => Term::Neutral,
                _ => Term::Bad,
            },
            NodeRef::Op(NumOp::Arith(ArithOp::Bin(BinOp::Mul)), i) => match &nodes[*i] {
                NodeRef::Input => Term::Linear,
                NodeRef::Tuple(fields) if fields.len() == 2 => match (terms[fields[0]], terms[fields[1]]) {
                    (Term::Diff | Term::Linear, Term::Neutral) | (Term::Neutral, Term::Diff | Term::Linear) => Term::Linear,
                    _ => Term::Bad,
                },
                _ => Term::Bad,
            },
            NodeRef::Op(NumOp::Core(Op::Unit | Op::Lit(_)), _) => Term::Neutral,
            NodeRef::Op(_, i) => if terms[*i] == Term::Neutral { Term::Neutral } else { Term::Bad },
        };
        terms.push(term);
    }
    matches!(terms[body.output()], Term::Diff | Term::Linear)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ListUse { Input, Vals, Mapped, Other }

fn sum_only(compiled: &corgi::Compiled, key: &Shape, val: &Shape) -> Option<Vec<usize>> {
    use corgi::{ArithOp, Bounds, NodeRef, NumOp, Op, Red, Value};
    let graph = compiled.graph();
    let nodes: Vec<_> = graph.nodes().collect();
    let mut uses = Vec::with_capacity(nodes.len());
    let (mut maps, mut sums) = (0, 0);
    for node in &nodes {
        let use_ = match node {
            NodeRef::Input => ListUse::Input,
            NodeRef::Tuple(fields) => {
                if fields.iter().any(|&i| uses[i] != ListUse::Other) { return None; }
                ListUse::Other
            }
            NodeRef::Op(NumOp::Core(Op::Field(field)), i) if uses[*i] == ListUse::Input => match field { 0 => ListUse::Other, 1 => ListUse::Vals, _ => return None },
            NodeRef::Op(NumOp::Core(Op::MapList(body)), i) if uses[*i] == ListUse::Vals => {
                if !weighted(body) { return None; }
                maps += 1;
                ListUse::Mapped
            }
            NodeRef::Op(NumOp::Arith(ArithOp::Reduce(Red::Add)), i) if uses[*i] == ListUse::Mapped => { sums += 1; ListUse::Other }
            NodeRef::Op(NumOp::Core(Op::Unit | Op::Lit(_)), _) => ListUse::Other,
            NodeRef::Op(_, i) => { if uses[*i] != ListUse::Other { return None; } ListUse::Other }
        };
        uses.push(use_);
    }
    if maps != 1 || sums != 1 || uses[graph.output()] != ListUse::Other { return None; }
    // A coalesced group can disappear when its values cancel. The reducer must give nothing
    // for an empty list, or coalescing would change which keys have output.
    let empty = Value::List(Bounds::offsets(vec![0]), Box::new(Value::empty(&Shape::Prod(vec![val.clone(), Shape::Int]))));
    let argument = Value::Prod(vec![zero_row(key)?, empty]);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        corgi::eval_graph(graph, argument).into_list("empty reduce").ok().map(|(bounds, _)| bounds.to_vec())
    })).ok()??;
    if result != [0] { return None; }
    let analysis = corgi::analyze_demand(graph, compiled.input(), &Request::All).ok()?;
    let Request::Prod(root) = analysis.input_demand() else { return None };
    // One representative key stands for all in the empty-list check, so the key must be unread.
    if root[0] != Request::None { return None; }
    let Request::List(element) = &root[1] else { return None };
    let Request::Prod(pair) = &**element else { return None };
    let parts = match val { Shape::Prod(parts) => parts.as_slice(), Shape::Unit => &[], _ => std::slice::from_ref(val) };
    let mut fields = Vec::new();
    for (i, part) in parts.iter().enumerate() {
        let need = match &pair[0] { Request::Prod(needs) => needs.get(i)?, other => other };
        if need.projected_shape(part).ok()? != Shape::Unit { fields.push(i); }
    }
    Some(fields)
}

fn zero_row(shape: &Shape) -> Option<corgi::Value> {
    use corgi::{Bounds, Value};
    Some(match shape {
        Shape::Unit => Value::Unit(1),
        Shape::Int => Value::i64(vec![0]),
        Shape::Float => Value::f64(vec![0.0]),
        Shape::Prod(parts) => Value::Prod(parts.iter().map(zero_row).collect::<Option<_>>()?),
        Shape::List(element) => Value::List(Bounds::offsets(vec![0]), Box::new(Value::empty(element))),
        Shape::Sum(parts) if !parts.is_empty() => {
            let lanes = parts.iter().enumerate().map(|(i, p)| if i == 0 { zero_row(p) } else { Some(Value::empty(p)) }).collect::<Option<_>>()?;
            Value::sum(vec![0], lanes)
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{lower, parse};

    fn demand(text: &str) -> (st::Program, Demand, Vec<bool>) {
        let mut program = lower::lower_tree(parse::pipe::parse(text));
        crate::passes::optimize(&mut program);
        let (shapes, problems) = shapes::infer(&program, &|_| None);
        assert!(problems.conflicts.is_empty() && problems.unknown.is_empty(), "{problems:?}");
        let demand = Demand::analyze(&program, &shapes).unwrap();
        let live = demand.live(&[]);
        (program, demand, live)
    }

    #[test]
    fn a_lambda_reads_the_fields_its_live_output_uses() {
        let (_, demand, live) = demand("let x = input 0 : (int, (int, int)) | map((k, (a, b)) -> (k, ((a, 1) add, b)));
            export \"out\" = x | map((k, (a, _)) -> (k, a));
            export \"two\" = x | map((k, (a, _)) -> (k, (a, 1) add));");
        let ItemSites::Op { output, .. } = &demand.root.items[0] else { panic!() };
        assert_eq!(output.val.iter().map(|&id| live[id]).collect::<Vec<_>>(), [true, false]);
    }

    #[test]
    fn sum_only_reducers_ignore_unread_values_but_min_and_distinct_read_them() {
        for (text, expected) in [
            ("export \"out\" = input 0 : (int, (int, int)) | count;", Some(vec![])),
            ("export \"out\" = input 0 : (int, (int, int)) | reduce((_, vals) -> let n = vals map ((v, d) -> (v.1, d) mul) fold_add in (n, (n, 1)) enlist filter);", Some(vec![1])),
            ("export \"out\" = input 0 : (int, (int, int)) | reduce((_, vals) -> (vals map ((v, d) -> (v.0, d) mul) fold_add, 1) enlist);", None),
            ("export \"out\" = input 0 : (int, (int, int)) | reduce((_, vals) -> (vals len, 1) enlist);", None),
            ("export \"out\" = input 0 : (int, (int, int)) | min;", None),
            ("export \"out\" = input 0 : (int, (int, int)) | distinct;", None),
        ] {
            let program = lower::lower_tree(parse::pipe::parse(text));
            let reduce = program.root.items.iter().find_map(|item| match item {
                Item::Op(Node::Reduce { reducer: Reducer::Expr(lambda), limit: None, .. }) => Some(lambda),
                _ => None,
            });
            let fields = reduce.and_then(|lambda| summed_fields(lambda, &Shape::Int, &Shape::Prod(vec![Shape::Int, Shape::Int])));
            assert_eq!(fields, expected, "{text}");
        }
    }

    #[test]
    fn concat_arms_share_one_narrowing() {
        let (_, demand, live) = demand("let rows = input 0 : (int, (int, int));
            let a = rows | map((k, (a, b)) -> (k, (a, b)));
            let b = rows | map((k, (a, b)) -> (k, (b, a)));
            export \"wide\" = a | map((k, (a, b)) -> (k, (a, b)));
            export \"narrow\" = (a + b) | map((k, (a, _)) -> (k, a));");
        let ItemSites::Op { output, .. } = &demand.root.items[1] else { panic!() };
        assert_eq!(output.val.iter().map(|&id| live[id]).collect::<Vec<_>>(), [true, true], "the wide export keeps both fields of the arm, and so of its sibling");
    }
}
