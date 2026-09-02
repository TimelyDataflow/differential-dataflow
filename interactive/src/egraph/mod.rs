//! The DDIR optimizer as a DDIR program: each scope of a program is reified into a term table,
//! an equality-saturation program (`optimize.ddp`, itself DDIR) computes the e-classes and the
//! cheapest node per class, and the scope is rebuilt from that choice.
//!
//! What the e-graph sees is the scope's operator DAG at the grain of one node per linear step,
//! with everything an operator closes over — a projection, a predicate, a reducer, a child
//! scope's body — interned to an opaque id, and each node priced by the width of the row it
//! produces. Two nodes are equal when their kinds, payloads, and children's classes agree
//! (congruence), or when a rule says so. Rules come in two kinds: those the program states over
//! the term table alone (arrange idempotence), and those the host instantiates because they need
//! a fact about a scalar operator ([`scalar`]) — demand pushdown mints, for a reducer or a join
//! that reads only some of an input's fields, the same operator over a projection of that input
//! keeping just those fields, and asserts the two equal; join commutativity mints each join with
//! its inputs swapped.
//! Extraction picks, per class, the node with the least tree cost, and the host regroups
//! single-consumer chains of steps back into `Linear` operators.
//!
//! Child scopes are opaque leaves of their parent's e-graph, optimized on their own first: a scope
//! boundary is a semantic barrier, so nothing merges across one.

pub mod scalar;

use std::collections::{BTreeMap, HashMap};

use crate::backend::vec;
use crate::ir::{LinearOp, Value};
use crate::parse::{Projection, Reducer};
use crate::scope_ir::{Bind, Export, Item, Node, Program, Ref, Scope, Source};
use scalar::Width;

const KIND_IMPORT: i64 = 0;
const KIND_VAR: i64 = 1;
const KIND_CHILD_EXPORT: i64 = 2;
const KIND_OP: i64 = 3;
const KIND_CONCAT: i64 = 4;
const KIND_ARRANGE: i64 = 5;
const KIND_JOIN: i64 = 6;
const KIND_REDUCE: i64 = 7;
const KIND_INSPECT: i64 = 8;
const KIND_SUB: i64 = 9;

/// The op weight by kind — the same table `optimize.ddp` prices with.
fn weight(kind: i64) -> i64 {
    match kind {
        KIND_ARRANGE => 3,
        KIND_JOIN => 5,
        KIND_REDUCE => 4,
        KIND_SUB => 8,
        _ => 1,
    }
}

/// The width a node is priced by: the rows it holds for an Arrange or a Reduce (its input's),
/// the rows it produces for everything else.
fn priced_width(kind: i64, input: Option<Width>, output: Option<Width>) -> Option<Width> {
    match kind {
        KIND_ARRANGE | KIND_REDUCE => input,
        _ => output,
    }
}

/// A node's price: its weight times one plus its priced width.
fn price(kind: i64, priced: Option<Width>) -> i64 {
    weight(kind) * (1 + priced.map_or(0, |(k, v)| (k + v) as i64))
}

/// The optimizer program, parsed and lowered once.
fn optimizer() -> &'static Program {
    static PROGRAM: std::sync::OnceLock<Program> = std::sync::OnceLock::new();
    PROGRAM.get_or_init(|| {
        let src = include_str!("optimize.ddp");
        let mut tree = crate::lower::lower_tree(crate::parse::pipe::parse(src));
        tree.optimize();
        tree
    })
}

fn root_widths(program: &Program, source_widths: &[Option<Width>]) -> Vec<Option<Width>> {
    (0..program.root.imports.len()).map(|k| source_widths.get(k).copied().flatten()).collect()
}

/// Optimize every scope of `program`, innermost first, through the e-graph. `source_widths`
/// gives each root import's row width, so rows can be priced; `None` for an input whose width is
/// not known.
pub fn optimize(program: &Program, source_widths: &[Option<Width>]) -> Program {
    Program { root: optimize_scope(&program.root, &root_widths(program, source_widths)).0 }
}

/// The DAG cost of a program under the optimizer's model: every operator priced by kind and by
/// the width of the row it produces. What `optimize` minimizes per class, summed over the program
/// as it will run (shared subterms once).
pub fn cost(program: &Program, source_widths: &[Option<Width>]) -> i64 {
    fn scope_cost(s: &Scope, import_widths: &[Option<Width>]) -> i64 {
        let w = scope_widths(s, import_widths);
        let mut total = 0;
        for (i, item) in s.items.iter().enumerate() {
            match item {
                Item::Op(Node::Linear { ops, .. }) => {
                    let mut cur = w.inputs[i];
                    for op in ops {
                        cur = cur.and_then(|c| scalar::width_after(op, c));
                        total += price(KIND_OP, cur);
                    }
                }
                Item::Op(node) => {
                    let (kind, input) = match node {
                        Node::Concat(_) => (KIND_CONCAT, None),
                        Node::Arrange(input) => (KIND_ARRANGE, Some(input)),
                        Node::Join { .. } => (KIND_JOIN, None),
                        Node::Reduce { input, .. } => (KIND_REDUCE, Some(input)),
                        Node::Inspect { .. } => (KIND_INSPECT, None),
                        Node::Linear { .. } => unreachable!(),
                    };
                    total += price(kind, priced_width(kind, input.and_then(|rf| w.of_ref(rf)), w.items[i]));
                }
                Item::Sub(child) => {
                    total += price(KIND_SUB, None) + scope_cost(child, &w.child_imports(child));
                }
            }
        }
        total
    }
    scope_cost(&program.root, &root_widths(program, source_widths))
}

/// Row widths through one scope: per item (its output), per linear item's input, per var, per
/// import, and per child scope's exports.
struct Widths {
    items: Vec<Option<Width>>,
    inputs: Vec<Option<Width>>,
    vars: Vec<Option<Width>>,
    imports: Vec<Option<Width>>,
    child_exports: HashMap<usize, Vec<Option<Width>>>,
}

impl Widths {
    fn of_ref(&self, rf: &Ref) -> Option<Width> {
        match rf {
            Ref::Local(i) => self.items[*i],
            Ref::Import(k) => self.imports[*k],
            Ref::Var(v) => self.vars[*v],
            Ref::ChildExport(i, k) => self.child_exports.get(i).and_then(|ws| ws.get(*k).copied().flatten()),
        }
    }
    /// A child scope's import widths, as its parent sees them.
    fn child_imports(&self, child: &Scope) -> Vec<Option<Width>> {
        child
            .imports
            .iter()
            .map(|imp| match &imp.from {
                Source::Parent(rf) => self.of_ref(rf),
                _ => None,
            })
            .collect()
    }
}

/// The forward width pass over a scope; feedback vars take their bound value's width, to a
/// fixpoint (a var read before it is known stays unknown for that pass).
fn scope_widths(s: &Scope, import_widths: &[Option<Width>]) -> Widths {
    let mut w = Widths {
        items: vec![None; s.items.len()],
        inputs: vec![None; s.items.len()],
        vars: vec![None; s.vars.len()],
        imports: import_widths.to_vec(),
        child_exports: HashMap::new(),
    };
    for _pass in 0..4 {
        for (i, item) in s.items.iter().enumerate() {
            let out = match item {
                Item::Op(Node::Linear { input, ops }) => {
                    let mut cur = w.of_ref(input);
                    w.inputs[i] = cur;
                    for op in ops {
                        cur = cur.and_then(|c| scalar::width_after(op, c));
                    }
                    cur
                }
                Item::Op(Node::Concat(refs)) => refs.first().and_then(|r| w.of_ref(r)),
                Item::Op(Node::Arrange(input)) | Item::Op(Node::Inspect { input, .. }) => w.of_ref(input),
                Item::Op(Node::Join { left, right, projection }) => match (w.of_ref(left), w.of_ref(right)) {
                    (Some(l), Some(r)) => scalar::join_width(projection, l.0, l.1, r.1),
                    _ => None,
                },
                Item::Op(Node::Reduce { input, reducer }) => w.of_ref(input).map(|c| scalar::reducer_width(reducer, c)),
                Item::Sub(child) => {
                    let cw = scope_widths(child, &w.child_imports(child));
                    let exports: Vec<Option<Width>> = child.exports.iter().map(|e| cw.of_ref(&e.value)).collect();
                    w.child_exports.insert(i, exports);
                    None
                }
            };
            w.items[i] = out;
        }
        let mut changed = false;
        for b in &s.binds {
            let nw = w.of_ref(&b.value);
            if nw.is_some() && w.vars[b.var] != nw {
                w.vars[b.var] = nw;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    w
}

/// One node of a reified scope.
#[derive(Clone, Debug)]
struct RNode {
    kind: i64,
    payload: i64,
    children: Vec<usize>,
    /// the width of the row the node produces
    width: Option<Width>,
}

impl RNode {
    /// The width the node is priced by (see `priced_width`), given its children.
    fn priced(&self, nodes: &[RNode]) -> Option<Width> {
        priced_width(self.kind, self.children.first().and_then(|&c| nodes[c].width), self.width)
    }
}

/// A scope reified for the e-graph: its nodes, the tables its payloads intern into, and the
/// equalities the host asserted from scalar facts.
#[derive(Default)]
struct Reified {
    nodes: Vec<RNode>,
    equal: Vec<(usize, usize)>,
    ops: Vec<LinearOp>,
    projections: Vec<Projection>,
    reducers: Vec<Reducer>,
    labels: Vec<String>,
    subs: Vec<Scope>,
    /// interned payloads by kind: (kind, debug rendering) -> payload id
    interned: HashMap<(i64, String), i64>,
}

impl Reified {
    fn push(&mut self, kind: i64, payload: i64, children: Vec<usize>, width: Option<Width>) -> usize {
        self.nodes.push(RNode { kind, payload, children, width });
        self.nodes.len() - 1
    }
    fn intern<T: std::fmt::Debug + Clone>(&mut self, kind: i64, value: &T, table: fn(&mut Self) -> &mut Vec<T>) -> i64 {
        let key = (kind, format!("{value:?}"));
        if let Some(&id) = self.interned.get(&key) {
            return id;
        }
        let t = table(self);
        t.push(value.clone());
        let id = (t.len() - 1) as i64;
        self.interned.insert(key, id);
        id
    }
}

/// A child scope's body, as the parent's e-graph should see it: everything but where its imports
/// come from (those are its children edges).
fn sub_body(s: &Scope) -> String {
    let imports: Vec<&str> = s.imports.iter().map(|i| i.name.as_str()).collect();
    format!("{:?} {:?} {:?} {:?} {:?}", imports, s.vars, s.items, s.binds, s.exports)
}

/// The reifier's view of where a scope's refs land: node ids for imports, vars, items, and
/// (minted on demand) child exports.
struct Refs {
    import_nodes: Vec<usize>,
    var_nodes: Vec<usize>,
    item_nodes: Vec<usize>,
    child_exports: HashMap<(usize, usize), usize>,
    sub_export_widths: HashMap<usize, Vec<Option<Width>>>,
}

impl Refs {
    fn resolve(&mut self, rf: &Ref, r: &mut Reified) -> usize {
        match rf {
            Ref::Local(i) => self.item_nodes[*i],
            Ref::Import(k) => self.import_nodes[*k],
            Ref::Var(v) => self.var_nodes[*v],
            Ref::ChildExport(i, k) => *self.child_exports.entry((*i, *k)).or_insert_with(|| {
                let width = self.sub_export_widths.get(i).and_then(|ws| ws.get(*k).copied().flatten());
                r.push(KIND_CHILD_EXPORT, *k as i64, vec![self.item_nodes[*i]], width)
            }),
        }
    }
}

/// Optimize one scope; returns it with its exports' widths (what its parent needs of it).
fn optimize_scope(scope: &Scope, import_widths: &[Option<Width>]) -> (Scope, Vec<Option<Width>>) {
    let w = scope_widths(scope, import_widths);

    // Reify. Children scopes are optimized first and enter as opaque `Sub` nodes.
    let mut r = Reified::default();
    let mut refs = Refs {
        import_nodes: (0..scope.imports.len()).map(|k| r.push(KIND_IMPORT, k as i64, vec![], w.imports[k])).collect(),
        var_nodes: (0..scope.vars.len()).map(|v| r.push(KIND_VAR, v as i64, vec![], w.vars[v])).collect(),
        item_nodes: Vec::with_capacity(scope.items.len()),
        child_exports: HashMap::new(),
        sub_export_widths: HashMap::new(),
    };
    for (i, item) in scope.items.iter().enumerate() {
        let node = match item {
            Item::Op(Node::Linear { input, ops }) => {
                let mut prev = refs.resolve(input, &mut r);
                let mut cur = w.inputs[i];
                for op in ops {
                    cur = cur.and_then(|c| scalar::width_after(op, c));
                    let payload = r.intern(KIND_OP, op, |r| &mut r.ops);
                    prev = r.push(KIND_OP, payload, vec![prev], cur);
                }
                prev
            }
            Item::Op(Node::Concat(xs)) => {
                let kids: Vec<usize> = xs.iter().map(|x| refs.resolve(x, &mut r)).collect();
                r.push(KIND_CONCAT, 0, kids, w.items[i])
            }
            Item::Op(Node::Arrange(input)) => {
                let kid = refs.resolve(input, &mut r);
                r.push(KIND_ARRANGE, 0, vec![kid], w.items[i])
            }
            Item::Op(Node::Join { left, right, projection }) => {
                let l = refs.resolve(left, &mut r);
                let rr = refs.resolve(right, &mut r);
                let payload = r.intern(KIND_JOIN, projection, |r| &mut r.projections);
                r.push(KIND_JOIN, payload, vec![l, rr], w.items[i])
            }
            Item::Op(Node::Reduce { input, reducer }) => {
                let kid = refs.resolve(input, &mut r);
                let payload = r.intern(KIND_REDUCE, reducer, |r| &mut r.reducers);
                r.push(KIND_REDUCE, payload, vec![kid], w.items[i])
            }
            Item::Op(Node::Inspect { input, label }) => {
                let kid = refs.resolve(input, &mut r);
                let payload = r.intern(KIND_INSPECT, label, |r| &mut r.labels);
                r.push(KIND_INSPECT, payload, vec![kid], w.items[i])
            }
            Item::Sub(child) => {
                let (optimized, export_widths) = optimize_scope(child, &w.child_imports(child));
                refs.sub_export_widths.insert(i, export_widths);
                let kids: Vec<usize> = optimized
                    .imports
                    .iter()
                    .map(|imp| match &imp.from {
                        Source::Parent(rf) => refs.resolve(rf, &mut r),
                        other => panic!("a child scope imports from {other:?}, not its parent"),
                    })
                    .collect();
                let body = sub_body(&optimized);
                let payload = r.intern(KIND_SUB, &body, |r| &mut r.labels);
                r.subs.push(optimized);
                r.push(KIND_SUB, payload, kids, None)
            }
        };
        refs.item_nodes.push(node);
    }
    // the child scope behind each Sub node, by node id.
    let sub_of: HashMap<usize, usize> =
        r.nodes.iter().enumerate().filter(|(_, n)| n.kind == KIND_SUB).map(|(id, _)| id).enumerate().map(|(k, id)| (id, k)).collect();
    let bind_roots: Vec<usize> = scope.binds.iter().map(|b| refs.resolve(&b.value, &mut r)).collect();
    let export_roots: Vec<usize> = scope.exports.iter().map(|e| refs.resolve(&e.value, &mut r)).collect();

    // Host-instantiated rules, from scalar facts.
    //
    // Demand pushdown: a reducer that reads no values equals the same reducer over its input
    // with the values projected away. The narrowed twin is cheaper by the values' width, so
    // extraction takes it wherever there were values to drop.
    let reduce_nodes: Vec<usize> = (0..r.nodes.len()).filter(|&n| r.nodes[n].kind == KIND_REDUCE).collect();
    for n in reduce_nodes {
        let reducer = r.reducers[r.nodes[n].payload as usize].clone();
        let input = r.nodes[n].children[0];
        let Some((k, v)) = r.nodes[input].width else { continue };
        if scalar::reducer_reads_values(&reducer) || v == 0 {
            continue;
        }
        let narrow_op = scalar::keep_key_only();
        let payload = r.intern(KIND_OP, &narrow_op, |r| &mut r.ops);
        let narrowed = r.push(KIND_OP, payload, vec![input], Some((k, 0)));
        let (reduce_payload, width) = (r.nodes[n].payload, r.nodes[n].width);
        let twin = r.push(KIND_REDUCE, reduce_payload, vec![narrowed], width);
        r.equal.push((n, twin));
    }
    // Demand pushdown through a join: a join that reads only some fields of an input's values
    // equals the join over that input with the other fields projected away, reading the kept
    // fields by their new positions. Each side alone, and both together, are minted.
    let join_nodes: Vec<usize> = (0..r.nodes.len()).filter(|&n| r.nodes[n].kind == KIND_JOIN).collect();
    for &n in &join_nodes {
        let projection = r.projections[r.nodes[n].payload as usize].clone();
        let children = r.nodes[n].children.clone();
        let mut narrowings: Vec<(usize, usize, BTreeMap<usize, usize>)> = Vec::new(); // (side, narrowed node, map)
        for side in [1usize, 2] {
            // narrow below an Arrange, which is where the rows are held
            let (input, arranged) = match r.nodes[children[side - 1]].kind {
                KIND_ARRANGE => (r.nodes[children[side - 1]].children[0], true),
                _ => (children[side - 1], false),
            };
            let Some((k, v)) = r.nodes[input].width else { continue };
            let scalar::Demand::Fields(fields) = scalar::projection_demand(&projection, side) else { continue };
            if fields.len() >= v {
                continue;
            }
            let (op, map) = scalar::keep_fields(&fields);
            let payload = r.intern(KIND_OP, &op, |r| &mut r.ops);
            let mut narrowed = r.push(KIND_OP, payload, vec![input], Some((k, fields.len())));
            if arranged {
                narrowed = r.push(KIND_ARRANGE, 0, vec![narrowed], Some((k, fields.len())));
            }
            narrowings.push((side, narrowed, map));
        }
        let mut variants: Vec<Vec<&(usize, usize, BTreeMap<usize, usize>)>> = narrowings.iter().map(|x| vec![x]).collect();
        if narrowings.len() == 2 {
            variants.push(narrowings.iter().collect());
        }
        for chosen in variants {
            let mut kids = children.clone();
            let mut p = projection.clone();
            for (side, narrowed, map) in chosen {
                kids[*side - 1] = *narrowed;
                p = scalar::narrow_join(&p, *side, map);
            }
            let payload = r.intern(KIND_JOIN, &p, |r| &mut r.projections);
            let width = r.nodes[n].width;
            let twin = r.push(KIND_JOIN, payload, kids, width);
            r.equal.push((n, twin));
        }
    }
    // Join commutativity: a join equals the join of its inputs swapped, reading them swapped.
    for &n in &join_nodes {
        let projection = r.projections[r.nodes[n].payload as usize].clone();
        let swapped = scalar::swap_join(&projection);
        let payload = r.intern(KIND_JOIN, &swapped, |r| &mut r.projections);
        let kids = vec![r.nodes[n].children[1], r.nodes[n].children[0]];
        let width = r.nodes[n].width;
        let twin = r.push(KIND_JOIN, payload, kids, width);
        r.equal.push((n, twin));
    }

    // Saturate and extract, as a DDIR program on the vec backend.
    let int = |n: usize| Value::Int(n as i64);
    let node_rows: Vec<(Value, Value)> = r
        .nodes
        .iter()
        .enumerate()
        .map(|(id, n)| {
            let priced = n.priced(&r.nodes).map_or(0, |(k, v)| (k + v) as i64);
            (Value::Tuple(vec![int(id), Value::Int(n.kind), Value::Int(n.payload), Value::Int(priced)]), Value::unit())
        })
        .collect();
    let edge_rows: Vec<(Value, Value)> = r
        .nodes
        .iter()
        .enumerate()
        .flat_map(|(id, n)| n.children.iter().enumerate().map(move |(pos, &c)| (Value::Tuple(vec![int(id), int(pos), int(c)]), Value::unit())))
        .collect();
    let equal_rows: Vec<(Value, Value)> = r.equal.iter().map(|&(a, b)| (Value::Tuple(vec![int(a), int(b)]), Value::unit())).collect();
    let out = vec::evaluate(optimizer(), &[node_rows, edge_rows, equal_rows]);
    let ints = |v: &Value| -> Vec<i64> {
        match v {
            Value::Tuple(xs) => xs.iter().map(|x| x.as_int()).collect(),
            other => vec![other.as_int()],
        }
    };
    let classes: BTreeMap<usize, usize> =
        out["classes"].iter().filter(|(_, d)| *d > 0).map(|((k, v), _)| (ints(k)[0] as usize, ints(v)[0] as usize)).collect();
    let best: BTreeMap<usize, usize> =
        out["best"].iter().filter(|(_, d)| *d > 0).map(|((k, v), _)| (ints(k)[0] as usize, ints(v)[0] as usize)).collect();
    let chosen = |node: usize| -> usize {
        let class = classes[&node];
        *best.get(&class).unwrap_or_else(|| panic!("class {class} has no extracted node"))
    };

    // Consumers per class in the extracted DAG (reachable from the roots), so a chain of steps
    // with one consumer folds into one `Linear`. The roots (binds, exports) consume their classes
    // too, so a chain ending at a root is not folded into the step that follows it.
    let mut consumers: HashMap<usize, usize> = HashMap::new();
    let mut seen: std::collections::HashSet<usize> = Default::default();
    let mut stack: Vec<usize> = bind_roots.iter().chain(&export_roots).map(|&n| chosen(n)).collect();
    for &n in bind_roots.iter().chain(&export_roots) {
        *consumers.entry(classes[&n]).or_insert(0) += 1;
    }
    while let Some(n) = stack.pop() {
        if !seen.insert(n) {
            continue;
        }
        for &c in &r.nodes[n].children {
            let cc = chosen(c);
            *consumers.entry(classes[&cc]).or_insert(0) += 1;
            stack.push(cc);
        }
    }

    // Rebuild the scope from the chosen nodes, children before parents.
    let mut out_scope = Scope { name: scope.name.clone(), imports: scope.imports.clone(), vars: scope.vars.clone(), ..Scope::default() };
    let ctx = Rebuild { r: &r, classes: &classes, best: &best, consumers: &consumers, sub_of: &sub_of };
    let mut emitted: HashMap<usize, Ref> = HashMap::new(); // by class
    for (b, &root) in scope.binds.iter().zip(&bind_roots) {
        let value = ctx.emit(root, &mut emitted, &mut out_scope);
        out_scope.binds.push(Bind { var: b.var, value });
    }
    for (e, &root) in scope.exports.iter().zip(&export_roots) {
        let value = ctx.emit(root, &mut emitted, &mut out_scope);
        out_scope.exports.push(Export { name: e.name.clone(), value });
    }
    let export_widths: Vec<Option<Width>> = export_roots.iter().map(|&n| r.nodes[n].width).collect();
    (out_scope, export_widths)
}

/// What rebuilding a scope reads: the reified nodes, the classes, the choice per class, the
/// consumer counts per class, and which child scope each Sub node stands for.
struct Rebuild<'a> {
    r: &'a Reified,
    classes: &'a BTreeMap<usize, usize>,
    best: &'a BTreeMap<usize, usize>,
    consumers: &'a HashMap<usize, usize>,
    sub_of: &'a HashMap<usize, usize>,
}

impl Rebuild<'_> {
    fn local(out: &mut Scope, node: Node) -> Ref {
        out.items.push(Item::Op(node));
        Ref::Local(out.items.len() - 1)
    }

    /// Emit the chosen node of `node`'s class into `out` (its children first), memoized by class.
    fn emit(&self, node: usize, emitted: &mut HashMap<usize, Ref>, out: &mut Scope) -> Ref {
        let class = self.classes[&node];
        if let Some(rf) = emitted.get(&class) {
            return rf.clone();
        }
        let n = self.best[&class];
        let rn = &self.r.nodes[n];
        let rf = match rn.kind {
            KIND_IMPORT => Ref::Import(rn.payload as usize),
            KIND_VAR => Ref::Var(rn.payload as usize),
            KIND_CHILD_EXPORT => match self.emit(rn.children[0], emitted, out) {
                Ref::Local(i) => Ref::ChildExport(i, rn.payload as usize),
                other => panic!("a child export's scope emitted as {other:?}"),
            },
            KIND_OP => {
                let input = self.emit(rn.children[0], emitted, out);
                let op = self.r.ops[rn.payload as usize].clone();
                // extend a single-consumer `Linear` in place, else start one
                let input_class = self.classes[&rn.children[0]];
                let extend = match &input {
                    Ref::Local(i) => {
                        self.consumers.get(&input_class).copied().unwrap_or(0) == 1 && matches!(out.items[*i], Item::Op(Node::Linear { .. }))
                    }
                    _ => false,
                };
                if extend {
                    let Ref::Local(i) = input else { unreachable!() };
                    let Item::Op(Node::Linear { ops, .. }) = &mut out.items[i] else { unreachable!() };
                    ops.push(op);
                    emitted.insert(class, Ref::Local(i));
                    return Ref::Local(i);
                }
                Self::local(out, Node::Linear { input, ops: vec![op] })
            }
            KIND_CONCAT => {
                let kids: Vec<Ref> = rn.children.iter().map(|&c| self.emit(c, emitted, out)).collect();
                Self::local(out, Node::Concat(kids))
            }
            KIND_ARRANGE => {
                let input = self.emit(rn.children[0], emitted, out);
                Self::local(out, Node::Arrange(input))
            }
            KIND_JOIN => {
                let left = self.emit(rn.children[0], emitted, out);
                let right = self.emit(rn.children[1], emitted, out);
                Self::local(out, Node::Join { left, right, projection: self.r.projections[rn.payload as usize].clone() })
            }
            KIND_REDUCE => {
                let input = self.emit(rn.children[0], emitted, out);
                Self::local(out, Node::Reduce { input, reducer: self.r.reducers[rn.payload as usize].clone() })
            }
            KIND_INSPECT => {
                let input = self.emit(rn.children[0], emitted, out);
                Self::local(out, Node::Inspect { input, label: self.r.labels[rn.payload as usize].clone() })
            }
            KIND_SUB => {
                let kids: Vec<Ref> = rn.children.iter().map(|&c| self.emit(c, emitted, out)).collect();
                let mut child = self.r.subs[self.sub_of[&n]].clone();
                for (imp, rf) in child.imports.iter_mut().zip(kids) {
                    imp.from = Source::Parent(rf);
                }
                out.items.push(Item::Sub(child));
                Ref::Local(out.items.len() - 1)
            }
            other => panic!("unknown node kind {other}"),
        };
        emitted.insert(class, rf.clone());
        rf
    }
}
