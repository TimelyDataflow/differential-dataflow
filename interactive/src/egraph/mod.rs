//! The DDIR optimizer as a DDIR program: each scope of a program is reified into a term table,
//! an equality-saturation program (`optimize.ddp`, itself DDIR) computes the e-classes and the
//! cheapest node per class, and the scope is rebuilt from that choice.
//!
//! What the e-graph sees is the scope's operator DAG at the grain of one node per linear step,
//! with everything an operator closes over — a projection, a predicate, a reducer, a child
//! scope's body — interned to an opaque id, and each node priced by the host: an op weight, times
//! the width of the rows it holds or produces, times a relative row volume (a filter halves it).
//! Two nodes are equal when their kinds, payloads, and children's classes agree (congruence,
//! with a concatenation's children taken as the unordered multiset of its leaves through nested
//! concatenations), or when a rule says so. Rules come in two kinds: those the program states
//! over the term table alone (arrange idempotence), and those the host instantiates because
//! they need a fact about a scalar operator ([`scalar`]), handed to the program as asserted
//! equalities — demand pushdown (a reducer or a join that reads only some of an input's fields
//! equals itself over that input narrowed to them), join commutativity, and filter pushdown (a
//! filter equals itself composed through the projection, negation, concatenation, or join below
//! it, when its predicate can be expressed over the rows there). The host applies its rules to
//! the nodes they mint too, so a filter sinks as far as its predicate allows.
//!
//! Extraction picks one node per class: the program computes the least *tree* cost per class
//! (a dynamic program, which counts a shared subterm once per use), and the host refines that
//! choice against the *DAG* cost — the weight of what the choices materialize from the roots,
//! shared subterms once — by hill-climbing to a local optimum (optimal DAG extraction is
//! NP-hard; this is the standard greedy, seeded by the tree optimum). The host then regroups
//! single-consumer chains of steps back into `Linear` operators, and single-consumer nested
//! concatenations into one.
//!
//! Child scopes are opaque leaves of their parent's e-graph, optimized on their own first: a scope
//! boundary is a semantic barrier, so nothing merges across one.

pub mod scalar;

use std::collections::{BTreeMap, HashMap, HashSet};

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

/// The relative volume of an input's rows; a filter halves whatever it sees.
const BASE_VOLUME: i64 = 64;

/// The op weight by kind.
fn weight(kind: i64) -> i64 {
    match kind {
        KIND_ARRANGE => 3,
        KIND_JOIN => 5,
        KIND_REDUCE => 4,
        KIND_SUB => 8,
        _ => 1,
    }
}

/// What the optimizer knows of a row: its width, when the language can tell, and a relative
/// volume, an estimate with no claim beyond ordering the alternatives a rule proposes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Row {
    width: Option<Width>,
    volume: i64,
}

impl Row {
    const UNKNOWN: Row = Row { width: None, volume: BASE_VOLUME };
    fn fields(&self) -> i64 {
        self.width.map_or(0, |(k, v)| (k + v) as i64)
    }
}

/// A node's price: its weight, times one plus the width of the rows it holds (its input's, for
/// an Arrange or a Reduce) or produces (everything else), times their volume.
fn price(kind: i64, input: Option<Row>, output: Row) -> i64 {
    let priced = match kind {
        KIND_ARRANGE | KIND_REDUCE => input.unwrap_or(output),
        _ => output,
    };
    weight(kind) * (1 + priced.fields()) * priced.volume
}

/// The optimizer program, parsed and lowered once.
fn optimizer() -> &'static Program {
    static PROGRAM: std::sync::OnceLock<Program> = std::sync::OnceLock::new();
    PROGRAM.get_or_init(|| {
        let src = include_str!("optimize.ddp");
        let mut tree = crate::lower::lower_tree(crate::parse::pipe::parse(src));
        tree.root.optimize(); // the hand-written optimizer: the e-graph cannot optimize itself
        tree
    })
}

fn root_rows(program: &Program, source_widths: &[Option<Width>]) -> Vec<Row> {
    (0..program.root.imports.len()).map(|k| Row { width: source_widths.get(k).copied().flatten(), volume: BASE_VOLUME }).collect()
}

/// Optimize every scope of `program`, innermost first, through the e-graph. `source_widths`
/// gives each root import's row width, so rows can be priced; `None` for an input whose width is
/// not known.
pub fn optimize(program: &Program, source_widths: &[Option<Width>]) -> Program {
    optimize_with(program, source_widths, Extraction::Cheapest)
}

/// How to choose a node per class once the classes are known. The cost model lives entirely
/// here: saturation is the same whatever the choice, and any acyclic choice is a correct
/// program — `Random` exists to check exactly that, over alternatives the cost never picks.
#[derive(Clone, Copy, Debug)]
pub enum Extraction {
    /// The cheapest DAG the hill-climb finds from the tree optimum.
    Cheapest,
    /// From the tree optimum, a few hundred random moves, each kept if the choices stay acyclic.
    Random(u64),
}

/// `optimize`, with the extraction chosen.
pub fn optimize_with(program: &Program, source_widths: &[Option<Width>], extraction: Extraction) -> Program {
    Program { root: optimize_scope(&program.root, &root_rows(program, source_widths), extraction).0 }
}

/// The DAG cost of a program under the optimizer's model: every operator priced by kind, row
/// width, and row volume. What `optimize` minimizes, summed over the program as it will run
/// (shared subterms once).
pub fn cost(program: &Program, source_widths: &[Option<Width>]) -> i64 {
    fn scope_cost(s: &Scope, import_rows: &[Row]) -> i64 {
        let f = scope_facts(s, import_rows);
        let mut total = 0;
        for (i, item) in s.items.iter().enumerate() {
            match item {
                Item::Op(Node::Linear { ops, .. }) => {
                    let mut cur = f.inputs[i];
                    for op in ops {
                        cur = scalar_step(op, cur);
                        total += price(KIND_OP, None, cur);
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
                    total += price(kind, input.map(|rf| f.of_ref(rf)), f.items[i]);
                }
                Item::Sub(child) => {
                    total += weight(KIND_SUB) + scope_cost(child, &f.child_imports(child));
                }
            }
        }
        total
    }
    scope_cost(&program.root, &root_rows(program, source_widths))
}

/// The row after a linear step.
fn scalar_step(op: &LinearOp, row: Row) -> Row {
    Row { width: row.width.and_then(|w| scalar::width_after(op, w)), volume: scalar::volume_after(op, row.volume) }
}

/// Row facts through one scope: per item (its output), per linear item's input, per var, per
/// import, and per child scope's exports.
struct Facts {
    items: Vec<Row>,
    inputs: Vec<Row>,
    vars: Vec<Row>,
    imports: Vec<Row>,
    child_exports: HashMap<usize, Vec<Row>>,
}

impl Facts {
    fn of_ref(&self, rf: &Ref) -> Row {
        match rf {
            Ref::Local(i) => self.items[*i],
            Ref::Import(k) => self.imports[*k],
            Ref::Var(v) => self.vars[*v],
            Ref::ChildExport(i, k) => self.child_exports.get(i).and_then(|ws| ws.get(*k).copied()).unwrap_or(Row::UNKNOWN),
        }
    }
    /// A child scope's import rows, as its parent sees them.
    fn child_imports(&self, child: &Scope) -> Vec<Row> {
        child
            .imports
            .iter()
            .map(|imp| match &imp.from {
                Source::Parent(rf) => self.of_ref(rf),
                _ => Row::UNKNOWN,
            })
            .collect()
    }
}

/// The forward pass over a scope. Feedback vars take their bound value's width, to a fixpoint (a
/// var read before it is known stays unknown for that pass), and the base volume: a feedback
/// volume has no fixpoint worth estimating.
fn scope_facts(s: &Scope, import_rows: &[Row]) -> Facts {
    let mut f = Facts {
        items: vec![Row::UNKNOWN; s.items.len()],
        inputs: vec![Row::UNKNOWN; s.items.len()],
        vars: vec![Row::UNKNOWN; s.vars.len()],
        imports: import_rows.to_vec(),
        child_exports: HashMap::new(),
    };
    for _pass in 0..4 {
        for (i, item) in s.items.iter().enumerate() {
            let out = match item {
                Item::Op(Node::Linear { input, ops }) => {
                    let mut cur = f.of_ref(input);
                    f.inputs[i] = cur;
                    for op in ops {
                        cur = scalar_step(op, cur);
                    }
                    cur
                }
                Item::Op(Node::Concat(refs)) => {
                    let rows: Vec<Row> = refs.iter().map(|r| f.of_ref(r)).collect();
                    Row { width: rows.first().and_then(|r| r.width), volume: rows.iter().map(|r| r.volume).sum() }
                }
                Item::Op(Node::Arrange(input)) | Item::Op(Node::Inspect { input, .. }) => f.of_ref(input),
                Item::Op(Node::Join { left, right, projection }) => {
                    let (l, r) = (f.of_ref(left), f.of_ref(right));
                    join_row(projection, l, r)
                }
                Item::Op(Node::Reduce { input, reducer }) => {
                    let row = f.of_ref(input);
                    Row { width: row.width.map(|w| scalar::reducer_width(reducer, w)), volume: row.volume }
                }
                Item::Sub(child) => {
                    let cf = scope_facts(child, &f.child_imports(child));
                    let exports: Vec<Row> = child.exports.iter().map(|e| cf.of_ref(&e.value)).collect();
                    f.child_exports.insert(i, exports);
                    Row::UNKNOWN
                }
            };
            f.items[i] = out;
        }
        let mut changed = false;
        for b in &s.binds {
            let width = f.of_ref(&b.value).width;
            if width.is_some() && f.vars[b.var].width != width {
                f.vars[b.var].width = width;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    f
}

/// The row a join produces from its inputs' rows.
fn join_row(projection: &Projection, l: Row, r: Row) -> Row {
    let width = match (l.width, r.width) {
        (Some(l), Some(r)) => scalar::join_width(projection, l.0, l.1, r.1),
        _ => None,
    };
    Row { width, volume: l.volume.max(r.volume) }
}

/// One node of a reified scope.
#[derive(Clone, Debug)]
struct RNode {
    kind: i64,
    payload: i64,
    children: Vec<usize>,
    /// the row the node produces
    row: Row,
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
    /// nodes by signature, so a rule minting a node that exists gets that node
    index: HashMap<(i64, i64, Vec<usize>), usize>,
}

impl Reified {
    fn push(&mut self, kind: i64, payload: i64, children: Vec<usize>, row: Row) -> usize {
        if kind != KIND_SUB {
            if let Some(&id) = self.index.get(&(kind, payload, children.clone())) {
                return id;
            }
        }
        self.nodes.push(RNode { kind, payload, children: children.clone(), row });
        let id = self.nodes.len() - 1;
        self.index.insert((kind, payload, children), id);
        id
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
    fn price_of(&self, n: usize) -> i64 {
        let node = &self.nodes[n];
        price(node.kind, node.children.first().map(|&c| self.nodes[c].row), node.row)
    }
    /// The step a node stands for, if it is one.
    fn op_of(&self, n: usize) -> Option<&LinearOp> {
        (self.nodes[n].kind == KIND_OP).then(|| &self.ops[self.nodes[n].payload as usize])
    }
    /// A node's input for narrowing or filtering: below its Arrange, if it is one, since that is
    /// where the rows are held. Returns the input and whether to re-arrange.
    fn below_arrange(&self, n: usize) -> (usize, bool) {
        match self.nodes[n].kind {
            KIND_ARRANGE => (self.nodes[n].children[0], true),
            _ => (n, false),
        }
    }
    /// A step over `input`, then the same arrangement of it as `like` had.
    fn step_below(&mut self, input: usize, rearrange: bool, op: &LinearOp) -> usize {
        let row = scalar_step(op, self.nodes[input].row);
        let payload = self.intern(KIND_OP, op, |r| &mut r.ops);
        let stepped = self.push(KIND_OP, payload, vec![input], row);
        if rearrange {
            self.push(KIND_ARRANGE, 0, vec![stepped], row)
        } else {
            stepped
        }
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
    sub_export_rows: HashMap<usize, Vec<Row>>,
}

impl Refs {
    fn resolve(&mut self, rf: &Ref, r: &mut Reified) -> usize {
        match rf {
            Ref::Local(i) => self.item_nodes[*i],
            Ref::Import(k) => self.import_nodes[*k],
            Ref::Var(v) => self.var_nodes[*v],
            Ref::ChildExport(i, k) => *self.child_exports.entry((*i, *k)).or_insert_with(|| {
                let row = self.sub_export_rows.get(i).and_then(|ws| ws.get(*k).copied()).unwrap_or(Row::UNKNOWN);
                r.push(KIND_CHILD_EXPORT, *k as i64, vec![self.item_nodes[*i]], row)
            }),
        }
    }
}

/// Reify a scope: one node per import, var, linear step, and operator; children scopes optimized
/// first and entered as opaque `Sub` nodes. Returns the table with the nodes of the binds and of
/// the exports (the roots).
fn reify(scope: &Scope, f: &Facts, extraction: Extraction) -> (Reified, Vec<usize>, Vec<usize>) {
    let mut r = Reified::default();
    let mut refs = Refs {
        import_nodes: (0..scope.imports.len()).map(|k| r.push(KIND_IMPORT, k as i64, vec![], f.imports[k])).collect(),
        var_nodes: (0..scope.vars.len()).map(|v| r.push(KIND_VAR, v as i64, vec![], f.vars[v])).collect(),
        item_nodes: Vec::with_capacity(scope.items.len()),
        child_exports: HashMap::new(),
        sub_export_rows: HashMap::new(),
    };
    for (i, item) in scope.items.iter().enumerate() {
        let node = match item {
            Item::Op(Node::Linear { input, ops }) => {
                let mut prev = refs.resolve(input, &mut r);
                let mut cur = f.inputs[i];
                for op in ops {
                    cur = scalar_step(op, cur);
                    let payload = r.intern(KIND_OP, op, |r| &mut r.ops);
                    prev = r.push(KIND_OP, payload, vec![prev], cur);
                }
                prev
            }
            Item::Op(Node::Concat(xs)) => {
                let kids: Vec<usize> = xs.iter().map(|x| refs.resolve(x, &mut r)).collect();
                r.push(KIND_CONCAT, 0, kids, f.items[i])
            }
            Item::Op(Node::Arrange(input)) => {
                let kid = refs.resolve(input, &mut r);
                r.push(KIND_ARRANGE, 0, vec![kid], f.items[i])
            }
            Item::Op(Node::Join { left, right, projection }) => {
                let l = refs.resolve(left, &mut r);
                let rr = refs.resolve(right, &mut r);
                let payload = r.intern(KIND_JOIN, projection, |r| &mut r.projections);
                r.push(KIND_JOIN, payload, vec![l, rr], f.items[i])
            }
            Item::Op(Node::Reduce { input, reducer }) => {
                let kid = refs.resolve(input, &mut r);
                let payload = r.intern(KIND_REDUCE, reducer, |r| &mut r.reducers);
                r.push(KIND_REDUCE, payload, vec![kid], f.items[i])
            }
            Item::Op(Node::Inspect { input, label }) => {
                let kid = refs.resolve(input, &mut r);
                let payload = r.intern(KIND_INSPECT, label, |r| &mut r.labels);
                r.push(KIND_INSPECT, payload, vec![kid], f.items[i])
            }
            Item::Sub(child) => {
                let (optimized, export_rows) = optimize_scope(child, &f.child_imports(child), extraction);
                refs.sub_export_rows.insert(i, export_rows);
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
                r.push(KIND_SUB, payload, kids, Row::UNKNOWN)
            }
        };
        refs.item_nodes.push(node);
    }
    let bind_roots: Vec<usize> = scope.binds.iter().map(|b| refs.resolve(&b.value, &mut r)).collect();
    let export_roots: Vec<usize> = scope.exports.iter().map(|e| refs.resolve(&e.value, &mut r)).collect();
    (r, bind_roots, export_roots)
}

/// The host's rules, instantiated from scalar facts over every node — the ones the rules mint
/// included, to a fixpoint (each rule mints something structurally below what it matched, so
/// there is one; the round cap is a guard).
fn instantiate_rules(r: &mut Reified) {
    let mut seen: HashSet<(usize, usize)> = HashSet::new();
    let mut done = 0;
    for _round in 0..32 {
        let end = r.nodes.len();
        for n in done..end {
            let mut found: Vec<(usize, usize)> = Vec::new();
            match r.nodes[n].kind {
                KIND_REDUCE => reduce_narrowing(r, n, &mut found),
                KIND_JOIN => {
                    join_narrowing(r, n, &mut found);
                    join_commutativity(r, n, &mut found);
                }
                KIND_OP => filter_pushdown(r, n, &mut found),
                _ => {}
            }
            for (a, b) in found {
                if a != b && seen.insert((a.min(b), a.max(b))) {
                    r.equal.push((a, b));
                }
            }
        }
        done = end;
        if r.nodes.len() == end {
            break;
        }
    }
}

/// Demand pushdown: a reducer that reads no values equals the same reducer over its input with
/// the values projected away. The narrowed twin holds narrower rows, so it prices lower.
fn reduce_narrowing(r: &mut Reified, n: usize, found: &mut Vec<(usize, usize)>) {
    let reducer = r.reducers[r.nodes[n].payload as usize].clone();
    let input = r.nodes[n].children[0];
    let Some((_, v)) = r.nodes[input].row.width else { return };
    if scalar::reducer_reads_values(&reducer) || v == 0 {
        return;
    }
    let narrowed = r.step_below(input, false, &scalar::keep_key_only());
    let (payload, row) = (r.nodes[n].payload, r.nodes[n].row);
    let twin = r.push(KIND_REDUCE, payload, vec![narrowed], row);
    found.push((n, twin));
}

/// Demand pushdown through a join: a join that reads only some fields of an input's values
/// equals the join over that input with the other fields projected away (below the input's
/// arrange), reading the kept fields by their new positions. Each side alone, and both together.
fn join_narrowing(r: &mut Reified, n: usize, found: &mut Vec<(usize, usize)>) {
    let projection = r.projections[r.nodes[n].payload as usize].clone();
    let children = r.nodes[n].children.clone();
    let mut narrowings: Vec<(usize, usize, BTreeMap<usize, usize>)> = Vec::new(); // (side, narrowed node, map)
    for side in [1usize, 2] {
        let (input, arranged) = r.below_arrange(children[side - 1]);
        let Some((_, v)) = r.nodes[input].row.width else { continue };
        let scalar::Demand::Fields(fields) = scalar::projection_demand(&projection, side) else { continue };
        if fields.len() >= v {
            continue;
        }
        let (op, map) = scalar::keep_fields(&fields);
        let narrowed = r.step_below(input, arranged, &op);
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
        let row = r.nodes[n].row;
        let twin = r.push(KIND_JOIN, payload, kids, row);
        found.push((n, twin));
    }
}

/// Join commutativity: a join equals the join of its inputs swapped, reading them swapped.
fn join_commutativity(r: &mut Reified, n: usize, found: &mut Vec<(usize, usize)>) {
    let projection = r.projections[r.nodes[n].payload as usize].clone();
    let swapped = scalar::swap_join(&projection);
    let payload = r.intern(KIND_JOIN, &swapped, |r| &mut r.projections);
    let kids = vec![r.nodes[n].children[1], r.nodes[n].children[0]];
    let row = r.nodes[n].row;
    let twin = r.push(KIND_JOIN, payload, kids, row);
    found.push((n, twin));
}

/// Filter pushdown: a filter over a projection equals the projection over the filter composed
/// through it; over a negation or an entry, the same filter below; over a concatenation, the
/// concatenation of the filter over each part; over a join, the join with the filter composed
/// through its projection on whichever input alone the composed predicate reads (below that
/// input's arrange). Each is minted only when the predicate can be expressed over the rows below.
fn filter_pushdown(r: &mut Reified, n: usize, found: &mut Vec<(usize, usize)>) {
    let Some(LinearOp::Filter(p)) = r.op_of(n).cloned() else { return };
    let child = r.nodes[n].children[0];
    match r.nodes[child].kind {
        KIND_OP => {
            let below = r.nodes[child].children[0];
            let op = r.op_of(child).cloned().unwrap();
            let pushed = match &op {
                LinearOp::Project(q) => {
                    let Some((k, v)) = r.nodes[below].row.width else { return };
                    let Some(composed) = scalar::compose(&p, q, &[k, v]) else { return };
                    LinearOp::Filter(composed)
                }
                LinearOp::Negate | LinearOp::EnterAt(_) => LinearOp::Filter(p),
                _ => return,
            };
            let filtered = r.step_below(below, false, &pushed);
            let payload = r.nodes[child].payload;
            let row = scalar_step(&op, r.nodes[filtered].row);
            let twin = r.push(KIND_OP, payload, vec![filtered], row);
            found.push((n, twin));
        }
        KIND_CONCAT => {
            let parts = r.nodes[child].children.clone();
            let op = LinearOp::Filter(p);
            let filtered: Vec<usize> = parts.iter().map(|&x| r.step_below(x, false, &op)).collect();
            let row = Row { width: r.nodes[child].row.width, volume: filtered.iter().map(|&x| r.nodes[x].row.volume).sum() };
            let twin = r.push(KIND_CONCAT, 0, filtered, row);
            found.push((n, twin));
        }
        KIND_JOIN => {
            let q = r.projections[r.nodes[child].payload as usize].clone();
            let [l, rr] = r.nodes[child].children[..] else { return };
            let (Some((k, wl)), Some((_, wr))) = (r.nodes[l].row.width, r.nodes[rr].row.width) else { return };
            let Some(composed) = scalar::compose(&p, &q, &[k, wl, wr]) else { return };
            let reads = |row: usize| !matches!(scalar::demand(&composed, row), scalar::Demand::Fields(ref f) if f.is_empty());
            let (side, predicate) = match (reads(1), reads(2)) {
                (_, false) => (0, composed),
                (false, true) => (1, scalar::rename_rows(&composed, &[(2, 1)].into_iter().collect())),
                _ => return,
            };
            let (input, arranged) = r.below_arrange(r.nodes[child].children[side]);
            let filtered = r.step_below(input, arranged, &LinearOp::Filter(predicate));
            let mut kids = r.nodes[child].children.clone();
            kids[side] = filtered;
            let row = join_row(&q, r.nodes[kids[0]].row, r.nodes[kids[1]].row);
            let payload = r.nodes[child].payload;
            let twin = r.push(KIND_JOIN, payload, kids, row);
            found.push((n, twin));
        }
        _ => {}
    }
}

/// Optimize one scope; returns it with its exports' rows (what its parent needs of it).
fn optimize_scope(scope: &Scope, import_rows: &[Row], extraction: Extraction) -> (Scope, Vec<Row>) {
    let f = scope_facts(scope, import_rows);
    let (mut r, bind_roots, export_roots) = reify(scope, &f, extraction);
    instantiate_rules(&mut r);

    // the child scope behind each Sub node, by node id.
    let sub_of: HashMap<usize, usize> =
        r.nodes.iter().enumerate().filter(|(_, n)| n.kind == KIND_SUB).map(|(id, _)| id).enumerate().map(|(k, id)| (id, k)).collect();

    // Saturate and extract, as a DDIR program on the vec backend.
    let int = |n: usize| Value::Int(n as i64);
    let node_rows: Vec<(Value, Value)> = r
        .nodes
        .iter()
        .enumerate()
        .map(|(id, n)| (Value::Tuple(vec![int(id), Value::Int(n.kind), Value::Int(n.payload), Value::Int(r.price_of(id))]), Value::unit()))
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
    let roots: Vec<usize> = bind_roots.iter().chain(&export_roots).copied().collect();
    let best = match extraction {
        Extraction::Cheapest => refine_dag(&r, &classes, &roots, best),
        Extraction::Random(seed) => random_dag(&r, &classes, &roots, best, seed),
    };
    let chosen = |node: usize| -> usize {
        let class = classes[&node];
        *best.get(&class).unwrap_or_else(|| panic!("class {class} has no extracted node"))
    };

    // Consumers per class in the extracted DAG (reachable from the roots), so a chain of steps
    // with one consumer folds into one `Linear`, and a nested concatenation with one consumer
    // into its parent. The roots (binds, exports) consume their classes too.
    let mut consumers: HashMap<usize, usize> = HashMap::new();
    let mut seen: HashSet<usize> = Default::default();
    let mut stack: Vec<usize> = roots.iter().map(|&n| chosen(n)).collect();
    for &n in &roots {
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
    let export_rows: Vec<Row> = export_roots.iter().map(|&n| r.nodes[n].row).collect();
    (out_scope, export_rows)
}

/// DAG-aware extraction: starting from a choice per class (the tree-cost optimum), hill-climb on
/// the weight of the DAG the choices materialize from the roots — every reached class's node
/// once, however many consumers it has. A move changes one reached class's node and is taken
/// when the whole DAG gets strictly lighter (so a move that makes the choices cyclic, or that
/// pays for a second copy of something a sibling already materializes, is never taken).
/// Optimal DAG extraction is NP-hard; this is the standard greedy, seeded by the tree optimum.
fn refine_dag(r: &Reified, classes: &BTreeMap<usize, usize>, roots: &[usize], mut pick: BTreeMap<usize, usize>) -> BTreeMap<usize, usize> {
    let walk = Walk::new(r, classes, roots);
    let mut members: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for (&node, &class) in classes {
        members.entry(class).or_default().push(node);
    }
    let (mut best, mut reached) = walk.total(&pick).expect("the tree-cost extraction is acyclic and complete");
    loop {
        let mut improved = false;
        'moves: for &class in &reached {
            let cur = pick[&class];
            for &node in &members[&class] {
                if node == cur {
                    continue;
                }
                pick.insert(class, node);
                if let Some((t, rr)) = walk.total(&pick) {
                    if t < best {
                        best = t;
                        reached = rr;
                        improved = true;
                        break 'moves;
                    }
                }
                pick.insert(class, cur);
            }
        }
        if !improved {
            return pick;
        }
    }
}

/// Extraction by coin: from the tree optimum, a few hundred moves to a random member of a
/// random reached class, each kept if the choices stay acyclic, whatever it costs.
fn random_dag(r: &Reified, classes: &BTreeMap<usize, usize>, roots: &[usize], mut pick: BTreeMap<usize, usize>, seed: u64) -> BTreeMap<usize, usize> {
    let walk = Walk::new(r, classes, roots);
    let mut members: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for (&node, &class) in classes {
        members.entry(class).or_default().push(node);
    }
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let (_, mut reached) = walk.total(&pick).expect("the tree-cost extraction is acyclic and complete");
    for _ in 0..256 {
        let class = reached[(next() % reached.len() as u64) as usize];
        let nodes = &members[&class];
        let node = nodes[(next() % nodes.len() as u64) as usize];
        let cur = pick[&class];
        if node == cur {
            continue;
        }
        pick.insert(class, node);
        match walk.total(&pick) {
            Some((_, rr)) => reached = rr,
            None => {
                pick.insert(class, cur);
            }
        }
    }
    pick
}

/// The chosen DAG's walker: from the roots, sum prices and list the classes reached; nothing on
/// a cycle or a class without a choice.
struct Walk<'a> {
    r: &'a Reified,
    classes: &'a BTreeMap<usize, usize>,
    root_classes: Vec<usize>,
    w: Vec<i64>,
}

impl<'a> Walk<'a> {
    fn new(r: &'a Reified, classes: &'a BTreeMap<usize, usize>, roots: &[usize]) -> Self {
        Walk { r, classes, root_classes: roots.iter().map(|&n| classes[&n]).collect(), w: (0..r.nodes.len()).map(|n| r.price_of(n)).collect() }
    }
    fn total(&self, choice: &BTreeMap<usize, usize>) -> Option<(i64, Vec<usize>)> {
        let mut state = HashMap::new();
        let (mut sum, mut reached) = (0, Vec::new());
        for &rc in &self.root_classes {
            if !self.visit(rc, choice, &mut state, &mut sum, &mut reached) {
                return None;
            }
        }
        Some((sum, reached))
    }
    fn visit(&self, class: usize, choice: &BTreeMap<usize, usize>, state: &mut HashMap<usize, u8>, sum: &mut i64, reached: &mut Vec<usize>) -> bool {
        match state.get(&class) {
            Some(1) => return false,
            Some(_) => return true,
            None => {}
        }
        let Some(&p) = choice.get(&class) else { return false };
        state.insert(class, 1);
        *sum += self.w[p];
        reached.push(class);
        for &k in &self.r.nodes[p].children {
            if !self.visit(self.classes[&k], choice, state, sum, reached) {
                return false;
            }
        }
        state.insert(class, 2);
        true
    }
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

    fn chosen(&self, node: usize) -> usize {
        self.best[&self.classes[&node]]
    }

    /// A concatenation's parts, with single-consumer nested concatenations spliced in.
    fn concat_parts(&self, children: &[usize], out: &mut Vec<usize>) {
        for &c in children {
            let cc = self.chosen(c);
            if self.r.nodes[cc].kind == KIND_CONCAT && self.consumers.get(&self.classes[&cc]).copied().unwrap_or(0) == 1 {
                self.concat_parts(&self.r.nodes[cc].children, out);
            } else {
                out.push(c);
            }
        }
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
                let mut parts = Vec::new();
                self.concat_parts(&rn.children, &mut parts);
                let kids: Vec<Ref> = parts.iter().map(|&c| self.emit(c, emitted, out)).collect();
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
