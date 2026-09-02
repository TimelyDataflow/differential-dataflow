//! The DDIR optimizer as a DDIR program: each scope of a program is reified into a term table,
//! an equality-saturation program (`optimize.ddp`, itself DDIR) computes the e-classes and the
//! cheapest node per class, and the scope is rebuilt from that choice.
//!
//! What the e-graph sees is the scope's operator DAG at the grain of one node per linear step,
//! with everything an operator closes over — a projection, a predicate, a reducer, a child
//! scope's body — interned to an opaque id. Two nodes are equal when their kinds, payloads, and
//! children's classes agree (congruence), or when a rule says so; today the one rule is arrange
//! idempotence. Extraction picks, per class, the node with the least tree cost, and the host
//! regroups single-consumer chains of steps back into `Linear` operators.
//!
//! Child scopes are opaque leaves of their parent's e-graph, optimized on their own first: a scope
//! boundary is a semantic barrier, so nothing merges across one.

use std::collections::{BTreeMap, HashMap};

use crate::backend::vec;
use crate::ir::{LinearOp, Value};
use crate::parse::{Projection, Reducer};
use crate::scope_ir::{Bind, Export, Item, Node, Program, Ref, Scope, Source};

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

/// Optimize every scope of `program`, innermost first, through the e-graph.
pub fn optimize(program: &Program) -> Program {
    Program { root: optimize_scope(&program.root) }
}

/// One node of a reified scope.
#[derive(Clone, Debug)]
struct RNode {
    kind: i64,
    payload: i64,
    children: Vec<usize>,
}

/// A scope reified for the e-graph: its nodes, and the tables its payloads intern into.
#[derive(Default)]
struct Reified {
    nodes: Vec<RNode>,
    ops: Vec<LinearOp>,
    projections: Vec<Projection>,
    reducers: Vec<Reducer>,
    labels: Vec<String>,
    subs: Vec<Scope>,
    /// interned payloads by kind: (kind, debug rendering) -> payload id
    interned: HashMap<(i64, String), i64>,
}

impl Reified {
    fn push(&mut self, kind: i64, payload: i64, children: Vec<usize>) -> usize {
        self.nodes.push(RNode { kind, payload, children });
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

fn optimize_scope(scope: &Scope) -> Scope {
    // Reify. Children scopes are optimized first and enter as opaque `Sub` nodes.
    let mut r = Reified::default();
    let import_nodes: Vec<usize> = (0..scope.imports.len()).map(|k| r.push(KIND_IMPORT, k as i64, vec![])).collect();
    let var_nodes: Vec<usize> = (0..scope.vars.len()).map(|v| r.push(KIND_VAR, v as i64, vec![])).collect();
    let mut item_nodes: Vec<usize> = Vec::with_capacity(scope.items.len());
    let mut child_exports: HashMap<(usize, usize), usize> = HashMap::new();
    let resolve = |rf: &Ref, rr: &mut Reified, items: &[usize], exports: &mut HashMap<(usize, usize), usize>| -> usize {
        match rf {
            Ref::Local(i) => items[*i],
            Ref::Import(k) => import_nodes[*k],
            Ref::Var(v) => var_nodes[*v],
            Ref::ChildExport(i, k) => *exports
                .entry((*i, *k))
                .or_insert_with(|| rr.push(KIND_CHILD_EXPORT, *k as i64, vec![items[*i]])),
        }
    };
    for item in &scope.items {
        let node = match item {
            Item::Op(Node::Linear { input, ops }) => {
                let mut prev = resolve(input, &mut r, &item_nodes, &mut child_exports);
                for op in ops {
                    let payload = r.intern(KIND_OP, op, |r| &mut r.ops);
                    prev = r.push(KIND_OP, payload, vec![prev]);
                }
                prev
            }
            Item::Op(Node::Concat(refs)) => {
                let kids: Vec<usize> = refs.iter().map(|x| resolve(x, &mut r, &item_nodes, &mut child_exports)).collect();
                r.push(KIND_CONCAT, 0, kids)
            }
            Item::Op(Node::Arrange(input)) => {
                let kid = resolve(input, &mut r, &item_nodes, &mut child_exports);
                r.push(KIND_ARRANGE, 0, vec![kid])
            }
            Item::Op(Node::Join { left, right, projection }) => {
                let l = resolve(left, &mut r, &item_nodes, &mut child_exports);
                let rr = resolve(right, &mut r, &item_nodes, &mut child_exports);
                let payload = r.intern(KIND_JOIN, projection, |r| &mut r.projections);
                r.push(KIND_JOIN, payload, vec![l, rr])
            }
            Item::Op(Node::Reduce { input, reducer }) => {
                let kid = resolve(input, &mut r, &item_nodes, &mut child_exports);
                let payload = r.intern(KIND_REDUCE, reducer, |r| &mut r.reducers);
                r.push(KIND_REDUCE, payload, vec![kid])
            }
            Item::Op(Node::Inspect { input, label }) => {
                let kid = resolve(input, &mut r, &item_nodes, &mut child_exports);
                let payload = r.intern(KIND_INSPECT, label, |r| &mut r.labels);
                r.push(KIND_INSPECT, payload, vec![kid])
            }
            Item::Sub(child) => {
                let optimized = optimize_scope(child);
                let kids: Vec<usize> = optimized
                    .imports
                    .iter()
                    .map(|imp| match &imp.from {
                        Source::Parent(rf) => resolve(rf, &mut r, &item_nodes, &mut child_exports),
                        other => panic!("a child scope imports from {other:?}, not its parent"),
                    })
                    .collect();
                let body = sub_body(&optimized);
                let payload = r.intern(KIND_SUB, &body, |r| &mut r.labels);
                r.subs.push(optimized);
                // the payload interns the body; `subs` keeps every child (one per Sub item) so the
                // rebuilt scope can take the body from the node that survives extraction.
                r.push(KIND_SUB, payload, kids)
            }
        };
        item_nodes.push(node);
    }
    // the child scope behind each Sub node, by node id.
    let mut sub_of: HashMap<usize, usize> = HashMap::new();
    let mut next_sub = 0;
    for (id, n) in r.nodes.iter().enumerate() {
        if n.kind == KIND_SUB {
            sub_of.insert(id, next_sub);
            next_sub += 1;
        }
    }
    let bind_roots: Vec<usize> = scope.binds.iter().map(|b| resolve(&b.value, &mut r, &item_nodes, &mut child_exports)).collect();
    let export_roots: Vec<usize> = scope.exports.iter().map(|e| resolve(&e.value, &mut r, &item_nodes, &mut child_exports)).collect();

    // Saturate and extract, as a DDIR program on the vec backend.
    let node_rows: Vec<(Value, Value)> = r
        .nodes
        .iter()
        .enumerate()
        .map(|(id, n)| (Value::Tuple(vec![Value::Int(id as i64), Value::Int(n.kind), Value::Int(n.payload)]), Value::unit()))
        .collect();
    let edge_rows: Vec<(Value, Value)> = r
        .nodes
        .iter()
        .enumerate()
        .flat_map(|(id, n)| {
            n.children.iter().enumerate().map(move |(pos, &c)| {
                (Value::Tuple(vec![Value::Int(id as i64), Value::Int(pos as i64), Value::Int(c as i64)]), Value::unit())
            })
        })
        .collect();
    let out = vec::evaluate(optimizer(), &[node_rows, edge_rows]);
    let ints = |v: &Value| -> Vec<i64> {
        match v {
            Value::Tuple(xs) => xs.iter().map(|x| x.as_int()).collect(),
            other => vec![other.as_int()],
        }
    };
    let classes: BTreeMap<usize, usize> = out["classes"]
        .iter()
        .filter(|(_, d)| *d > 0)
        .map(|((k, v), _)| (ints(k)[0] as usize, ints(v)[0] as usize))
        .collect();
    let best: BTreeMap<usize, usize> = out["best"]
        .iter()
        .filter(|(_, d)| *d > 0)
        .map(|((k, v), _)| (ints(k)[0] as usize, ints(v)[0] as usize))
        .collect();
    let chosen = |node: usize| -> usize {
        let class = classes[&node];
        *best.get(&class).unwrap_or_else(|| panic!("class {class} has no extracted node"))
    };

    // Consumers per class in the extracted DAG (reachable from the roots), so a chain of steps
    // with one consumer folds into one `Linear`.
    let mut consumers: HashMap<usize, usize> = HashMap::new();
    let mut seen: std::collections::HashSet<usize> = Default::default();
    let mut stack: Vec<usize> = bind_roots.iter().chain(&export_roots).map(|&n| chosen(n)).collect();
    // the roots (binds, exports) consume their classes too, so a chain ending at a root is not
    // folded into the step that follows it.
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
    let mut out_scope = Scope {
        name: scope.name.clone(),
        imports: scope.imports.clone(),
        vars: scope.vars.clone(),
        ..Scope::default()
    };
    let mut emitted: HashMap<usize, Ref> = HashMap::new(); // by class
    fn emit(
        node: usize,
        r: &Reified,
        classes: &BTreeMap<usize, usize>,
        best: &BTreeMap<usize, usize>,
        consumers: &HashMap<usize, usize>,
        sub_of: &HashMap<usize, usize>,
        emitted: &mut HashMap<usize, Ref>,
        out: &mut Scope,
    ) -> Ref {
        let class = classes[&node];
        if let Some(rf) = emitted.get(&class) {
            return rf.clone();
        }
        let n = best[&class];
        let rn = &r.nodes[n];
        let kid = |k: usize, emitted: &mut HashMap<usize, Ref>, out: &mut Scope| {
            emit(rn.children[k], r, classes, best, consumers, sub_of, emitted, out)
        };
        let rf = match rn.kind {
            KIND_IMPORT => Ref::Import(rn.payload as usize),
            KIND_VAR => Ref::Var(rn.payload as usize),
            KIND_CHILD_EXPORT => match kid(0, emitted, out) {
                Ref::Local(i) => Ref::ChildExport(i, rn.payload as usize),
                other => panic!("a child export's scope emitted as {other:?}"),
            },
            KIND_OP => {
                let input = kid(0, emitted, out);
                let op = r.ops[rn.payload as usize].clone();
                // extend a single-consumer `Linear` in place, else start one
                let input_class = classes[&rn.children[0]];
                let extend = match &input {
                    Ref::Local(i) => {
                        consumers.get(&input_class).copied().unwrap_or(0) == 1
                            && matches!(out.items[*i], Item::Op(Node::Linear { .. }))
                    }
                    _ => false,
                };
                if extend {
                    let Ref::Local(i) = input else { unreachable!() };
                    let Item::Op(Node::Linear { ops, .. }) = &mut out.items[i] else { unreachable!() };
                    ops.push(op);
                    // the extended item now stands for this class too
                    emitted.insert(class, Ref::Local(i));
                    return Ref::Local(i);
                }
                out.items.push(Item::Op(Node::Linear { input, ops: vec![op] }));
                Ref::Local(out.items.len() - 1)
            }
            KIND_CONCAT => {
                let kids: Vec<Ref> = (0..rn.children.len()).map(|k| kid(k, emitted, out)).collect();
                out.items.push(Item::Op(Node::Concat(kids)));
                Ref::Local(out.items.len() - 1)
            }
            KIND_ARRANGE => {
                let input = kid(0, emitted, out);
                out.items.push(Item::Op(Node::Arrange(input)));
                Ref::Local(out.items.len() - 1)
            }
            KIND_JOIN => {
                let left = kid(0, emitted, out);
                let right = kid(1, emitted, out);
                out.items.push(Item::Op(Node::Join { left, right, projection: r.projections[rn.payload as usize].clone() }));
                Ref::Local(out.items.len() - 1)
            }
            KIND_REDUCE => {
                let input = kid(0, emitted, out);
                out.items.push(Item::Op(Node::Reduce { input, reducer: r.reducers[rn.payload as usize].clone() }));
                Ref::Local(out.items.len() - 1)
            }
            KIND_INSPECT => {
                let input = kid(0, emitted, out);
                out.items.push(Item::Op(Node::Inspect { input, label: r.labels[rn.payload as usize].clone() }));
                Ref::Local(out.items.len() - 1)
            }
            KIND_SUB => {
                let kids: Vec<Ref> = (0..rn.children.len()).map(|k| kid(k, emitted, out)).collect();
                let mut child = r.subs[sub_of[&n]].clone();
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
    for (b, &root) in scope.binds.iter().zip(&bind_roots) {
        let value = emit(root, &r, &classes, &best, &consumers, &sub_of, &mut emitted, &mut out_scope);
        out_scope.binds.push(Bind { var: b.var, value });
    }
    for (e, &root) in scope.exports.iter().zip(&export_roots) {
        let value = emit(root, &r, &classes, &best, &consumers, &sub_of, &mut emitted, &mut out_scope);
        out_scope.exports.push(Export { name: e.name.clone(), value });
    }
    out_scope
}
