//! Compare DDIR's operator demand rules with the installed Rust pass.
//!
//! Run from `interactive/` with paths to `.ddp` programs. The DDIR program
//! consumes the self optimizer's graph encoding plus typed site and Corgi-read
//! relations. Rust allocates facts and extracts lambda reads; it does not build
//! dependency edges or seeds for the DDIR side.

use std::collections::BTreeSet;

use corgi::Shape;
use interactive::corgi::container::Updates;
use interactive::corgi::feed::FeedDecoder;
use interactive::ir::{Lambda, LinearOp, Reducer};
use interactive::projection::{demand_graph, lambda_input_fields};
use interactive::scope_ir::{self as st, Item, Node, Source};
use interactive::server::Server;
use interactive::shapes::{self, CollShape, ItemShapes, ScopeShapes};
use interactive::{lower, parse};

#[allow(dead_code)] // `dec` belongs to the shared optimizer encoding.
#[path = "self_opt/graph.rs"]
mod graph;

const PROJECT: i64 = 0;
const FILTER: i64 = 1;
const ENTER_AT: i64 = 2;
const NEGATE: i64 = 3;
const LIFT_ITER: i64 = 4;
const LIFT: i64 = 5;
const FLATMAP: i64 = 6;
const JOIN_FUNCTION: i64 = 7;

fn width(shape: &Shape) -> usize {
    match shape {
        Shape::Unit => 0,
        Shape::Prod(fields) => fields.len(),
        _ => 1,
    }
}

#[derive(Clone)]
struct Site {
    id: i64,
    key: Shape,
    val: Shape,
    key_facts: Vec<i64>,
    val_facts: Vec<i64>,
}

impl Site {
    fn output_fields(&self) -> Vec<(i64, i64, Vec<usize>)> {
        let mut fields = Vec::new();
        for (side, shape, facts) in [
            (0, &self.key, &self.key_facts),
            (1, &self.val, &self.val_facts),
        ] {
            for (index, _) in facts.iter().enumerate() {
                let path = if matches!(shape, Shape::Prod(_)) {
                    vec![side as usize, index]
                } else {
                    vec![side as usize]
                };
                fields.push((side, index as i64, path));
            }
        }
        fields
    }
}

enum ReadMode {
    Fields,
    Scalar,
    HalfJoin,
}

#[derive(Default)]
struct Relations {
    next_fact: i64,
    facts: Vec<((i64, i64, i64), i64)>,
    widths: Vec<((i64, i64), i64)>,
    targets: Vec<((i64, i64, i64, i64), i64)>,
    node_sites: Vec<(i64, i64)>,
    holder_sites: Vec<(i64, i64)>,
    marks: Vec<(i64, i64)>,
    stages: Vec<((i64, i64), (i64, i64))>,
    reducers: Vec<(i64, i64)>,
    reads: Vec<((i64, i64, i64, i64), (i64, i64))>,
}

impl Relations {
    fn site(&mut self, key: Shape, val: Shape) -> Site {
        let id = self.next_fact;
        self.next_fact += 1;
        self.facts.push(((id, 2, 0), id));
        let mut key_facts = Vec::new();
        for index in 0..width(&key) {
            let fact = self.next_fact;
            self.next_fact += 1;
            self.facts.push(((id, 0, index as i64), fact));
            key_facts.push(fact);
        }
        let mut val_facts = Vec::new();
        for index in 0..width(&val) {
            let fact = self.next_fact;
            self.next_fact += 1;
            self.facts.push(((id, 1, index as i64), fact));
            val_facts.push(fact);
        }
        self.widths.push(((id, 0), key_facts.len() as i64));
        self.widths.push(((id, 1), val_facts.len() as i64));
        Site {
            id,
            key,
            val,
            key_facts,
            val_facts,
        }
    }
    fn shaped(&mut self, shape: &Option<CollShape>) -> Result<Site, String> {
        let shape = shape.as_ref().ok_or("unknown collection shape")?;
        Ok(self.site(shape.key.clone(), shape.val.clone()))
    }
    fn read(
        &mut self,
        node: i64,
        stage: i64,
        out: &Site,
        lambda: &Lambda,
        inputs: &[Shape],
        mode: ReadMode,
    ) {
        // A scalar lambda is queried once. DDIR distributes its reads to
        // every output fact; that operator rule does not belong in Rust.
        let fields = if matches!(mode, ReadMode::Scalar) {
            vec![(-1, -1, Vec::new())]
        } else {
            out.output_fields()
        };
        for (side, field, path) in fields {
            if matches!(mode, ReadMode::HalfJoin) && side == 1 && field == 1 {
                continue;
            }
            let path = if matches!(mode, ReadMode::HalfJoin) && side == 1 && field == 0 {
                vec![1]
            } else {
                path
            };
            for (slot, input_field) in lambda_input_fields(lambda, inputs, &path) {
                self.reads.push((
                    (node, stage, side, field),
                    (slot as i64, input_field as i64),
                ));
            }
        }
    }
}

fn op_code(op: &LinearOp) -> i64 {
    match op {
        LinearOp::Project(_) => PROJECT,
        LinearOp::Filter(_) => FILTER,
        LinearOp::EnterAt(_) => ENTER_AT,
        LinearOp::Negate => NEGATE,
        LinearOp::LiftIter => LIFT_ITER,
        LinearOp::Lift => LIFT,
        LinearOp::FlatMap(_) => FLATMAP,
        LinearOp::JoinFunction(_) => JOIN_FUNCTION,
    }
}

/// Replay the export walk's numbering while attaching inferred sites to its
/// existing nodes, holders, and encoded references. No dependency is decided here.
fn describe_scope(
    scope: &st::Scope,
    shapes: &ScopeShapes,
    num: &mut graph::Numbering,
    rel: &mut Relations,
    parent_holders: &[(usize, i64)],
    depth: usize,
    root: bool,
) -> Result<Vec<Site>, String> {
    let scope_id = num.scope();
    let base = scope_id * graph::SCOPE_STRIDE;
    let imports = shapes
        .imports
        .iter()
        .map(|s| rel.shaped(s))
        .collect::<Result<Vec<_>, _>>()?;
    let vars = shapes
        .vars
        .iter()
        .map(|s| rel.shaped(s))
        .collect::<Result<Vec<_>, _>>()?;
    for (index, site) in imports.iter().enumerate() {
        rel.targets.push(((scope_id, 1, index as i64, 0), site.id));
        if let Some((_, holder)) = parent_holders.iter().find(|(i, _)| *i == index) {
            rel.holder_sites.push((*holder, site.id));
            if scope.imports[index].shape.is_some() {
                rel.marks.push((site.id, 1));
            }
        }
    }
    for (index, site) in vars.iter().enumerate() {
        rel.targets.push(((scope_id, 2, index as i64, 0), site.id));
        if scope.vars[index].shape.is_some() {
            rel.marks.push((site.id, 1));
        }
    }
    for (index, item) in scope.items.iter().enumerate() {
        let node_id = base + index as i64;
        match (item, &shapes.items[index]) {
            (Item::Op(node), ItemShapes::Op(shape)) => {
                let out = rel.shaped(shape)?;
                rel.node_sites.push((node_id, out.id));
                rel.targets.push(((scope_id, 0, node_id, 0), out.id));
                match node {
                    Node::Linear { input, ops } => {
                        let before = shapes.of(input).ok_or("unknown linear input shape")?;
                        let steps = shapes::linear_shapes(ops, before.key, before.val, depth)?;
                        for (step, op) in ops.iter().enumerate() {
                            let after = if step + 1 == ops.len() {
                                out.clone()
                            } else {
                                let (key, val) = steps[step + 1].clone();
                                rel.site(key, val)
                            };
                            rel.stages
                                .push(((node_id, step as i64), (after.id, op_code(op))));
                            let inputs = [steps[step].0.clone(), steps[step].1.clone()];
                            match op {
                                LinearOp::Project(lambda) => rel.read(
                                    node_id,
                                    step as i64,
                                    &after,
                                    lambda,
                                    &inputs,
                                    ReadMode::Fields,
                                ),
                                LinearOp::Filter(lambda)
                                | LinearOp::EnterAt(lambda)
                                | LinearOp::FlatMap(lambda)
                                | LinearOp::JoinFunction(lambda) => rel.read(
                                    node_id,
                                    step as i64,
                                    &after,
                                    lambda,
                                    &inputs,
                                    ReadMode::Scalar,
                                ),
                                _ => {}
                            }
                        }
                    }
                    Node::Join {
                        left,
                        right,
                        projection,
                    } => {
                        let left = shapes.of(left).ok_or("unknown join left shape")?;
                        let right = shapes.of(right).ok_or("unknown join right shape")?;
                        rel.read(
                            node_id,
                            -1,
                            &out,
                            projection,
                            &[left.key, left.val, right.val],
                            ReadMode::Fields,
                        );
                    }
                    Node::HalfJoin {
                        stream,
                        arrangement,
                        projection,
                        ..
                    } => {
                        let stream = shapes.of(stream).ok_or("unknown half join stream shape")?;
                        let arrangement = shapes
                            .of(arrangement)
                            .ok_or("unknown half join arrangement shape")?;
                        let Shape::Prod(parts) = stream.val else {
                            return Err("half join stream value is not a tuple".into());
                        };
                        let value = parts
                            .first()
                            .ok_or("half join stream has no value")?
                            .clone();
                        rel.read(
                            node_id,
                            -1,
                            &out,
                            projection,
                            &[stream.key, value, arrangement.val],
                            ReadMode::HalfJoin,
                        );
                    }
                    Node::Reduce { reducer, .. } => rel.reducers.push((
                        node_id,
                        if matches!(reducer, Reducer::Count) {
                            0
                        } else {
                            1
                        },
                    )),
                    _ => {}
                }
            }
            (Item::Sub(child), ItemShapes::Sub(child_shapes)) => {
                let mut holders = Vec::new();
                for (imp, source) in child.imports.iter().enumerate() {
                    if matches!(source.from, Source::Parent(_)) {
                        holders.push((imp, num.holder()));
                    }
                }
                let exports =
                    describe_scope(child, child_shapes, num, rel, &holders, depth + 1, false)?;
                for (export, site) in exports.iter().enumerate() {
                    rel.targets
                        .push(((scope_id, 3, node_id, export as i64), site.id));
                }
            }
            _ => return Err(format!("item {node_id} disagrees with inferred shapes")),
        }
    }
    for bind in &scope.binds {
        rel.holder_sites.push((num.holder(), vars[bind.var].id));
    }
    let mut exports = Vec::new();
    for (index, export) in scope.exports.iter().enumerate() {
        let site = rel.shaped(&shapes.exports[index])?;
        rel.holder_sites.push((num.holder(), site.id));
        if export.shape.is_some() {
            rel.marks.push((site.id, 1));
        }
        if root {
            rel.marks.push((site.id, 2));
        }
        exports.push(site);
    }
    Ok(exports)
}

fn feed_rows(
    rows: impl IntoIterator<Item = (String, String)>,
    key_shape: Shape,
    val_shape: Shape,
    diff: i64,
) -> Updates {
    let mut decoder = FeedDecoder::new(&key_shape, &val_shape);
    for (key, val) in rows {
        decoder.push(&key, Some(&val), diff).unwrap();
    }
    decoder.finish()
}
fn tup(xs: &[i64]) -> String {
    format!(
        "tuple({})",
        xs.iter().map(i64::to_string).collect::<Vec<_>>().join(",")
    )
}
fn ints(n: usize) -> Shape {
    Shape::Prod(vec![Shape::Int; n])
}

fn source_shape(source: &Source) -> Option<(Shape, Shape)> {
    let Source::Trace(name) = source else {
        return None;
    };
    if name == "clock"
        || name
            .strip_prefix("iota:")
            .is_some_and(|n| n.parse::<u64>().is_ok())
    {
        return Some((ints(1), Shape::Unit));
    }
    if name == "edges" {
        // Published by examples/server/programs/producer.ddp.
        return Some((ints(1), ints(1)));
    }
    let params = name.strip_prefix("random:")?;
    let mut arity = 2usize;
    let mut nodes = false;
    let mut edges = false;
    for param in params.split(',') {
        let (key, value) = param.split_once('=')?;
        match key.trim() {
            "nodes" => nodes = value.trim().parse::<u64>().is_ok(),
            "edges" => edges = value.trim().parse::<u64>().is_ok(),
            "arity" => arity = value.trim().parse().ok()?,
            "seed" | "churn" => {
                value.trim().parse::<u64>().ok()?;
            }
            _ => return None,
        }
    }
    (nodes && edges).then(|| {
        (
            if arity == 0 { Shape::Unit } else { ints(arity) },
            Shape::Unit,
        )
    })
}

struct SignatureKernel {
    name: &'static str,
    input: Shape,
    output: Shape,
}

impl corgi::HostKernel for SignatureKernel {
    fn name(&self) -> &str {
        self.name
    }
    fn input(&self) -> &Shape {
        &self.input
    }
    fn output(&self) -> &Shape {
        &self.output
    }
    fn eval(&self, _: corgi::Value) -> Result<corgi::Value, String> {
        Err("signature-only kernel is never evaluated by demand analysis".into())
    }
}

fn register_corpus_signatures() {
    use std::sync::Arc;
    let int = Shape::Int;
    let list = |shape| Shape::List(Box::new(shape));
    let register = |name: &'static str, input: Shape, output: Shape| {
        interactive::ir::register(Arc::new(SignatureKernel {
            name,
            input,
            output,
        }));
    };
    register("grow", ints(1), list(ints(2)));
    register("blend", Shape::Prod(vec![ints(2)]), Shape::Float);
    register(
        "describe",
        ints(1),
        Shape::Prod(vec![ints(1), list(int.clone())]),
    );
    register("seven", Shape::Unit, int.clone());
    register("double", ints(1), int);
}

fn feed(rel: &Relations, graph: &graph::Rows, diff: i64) -> Vec<Updates> {
    let (nodes, edges) = graph::updates(graph, diff);
    vec![
        nodes,
        edges,
        feed_rows(
            rel.facts
                .iter()
                .map(|((s, side, pos), f)| (tup(&[*s, *side, *pos]), f.to_string())),
            ints(3),
            Shape::Int,
            diff,
        ),
        feed_rows(
            rel.widths
                .iter()
                .map(|((s, side), w)| (tup(&[*s, *side]), w.to_string())),
            ints(2),
            Shape::Int,
            diff,
        ),
        feed_rows(
            rel.targets
                .iter()
                .map(|((scope, tag, a, b), s)| (tup(&[*scope, *tag, *a, *b]), s.to_string())),
            ints(4),
            Shape::Int,
            diff,
        ),
        feed_rows(
            rel.node_sites
                .iter()
                .map(|(n, s)| (n.to_string(), s.to_string())),
            Shape::Int,
            Shape::Int,
            diff,
        ),
        feed_rows(
            rel.holder_sites
                .iter()
                .map(|(h, s)| (h.to_string(), s.to_string())),
            Shape::Int,
            Shape::Int,
            diff,
        ),
        feed_rows(
            rel.marks
                .iter()
                .map(|(s, k)| (s.to_string(), k.to_string())),
            Shape::Int,
            Shape::Int,
            diff,
        ),
        feed_rows(
            rel.stages
                .iter()
                .map(|((n, j), (s, k))| (tup(&[*n, *j]), tup(&[*s, *k]))),
            ints(2),
            ints(2),
            diff,
        ),
        feed_rows(
            rel.reducers
                .iter()
                .map(|(n, k)| (n.to_string(), k.to_string())),
            Shape::Int,
            Shape::Int,
            diff,
        ),
        feed_rows(
            rel.reads
                .iter()
                .map(|((n, j, side, field), (slot, input))| {
                    (tup(&[*n, *j, *side, *field]), tup(&[*slot, *input]))
                }),
            ints(4),
            ints(2),
            diff,
        ),
    ]
}

fn one(
    path: &str,
    text: &str,
    server: &mut Server,
    worker: &mut timely::worker::Worker,
) -> Result<Option<(usize, usize)>, String> {
    let mut program = lower::lower_tree(parse::pipe::parse(text));
    program.optimize();
    let oracle = match demand_graph(&program, &source_shape) {
        Ok(graph) => graph,
        Err(err) => {
            eprintln!("{path}: Rust demand graph unavailable: {err}");
            return Ok(None);
        }
    };
    let (shapes, problems) = shapes::infer(&program, &source_shape);
    if !problems.conflicts.is_empty() || !problems.unknown.is_empty() {
        return Err(format!("{path}: oracle accepted shape errors"));
    }
    let mut graph_rows = graph::Rows::default();
    graph::export_scope(
        &program.root,
        &mut graph::Numbering {
            next_scope: 0,
            next_holder: graph::HOLDER_BASE,
        },
        &mut graph::Payloads::default(),
        &mut graph_rows,
        None,
    );
    let mut relations = Relations::default();
    describe_scope(
        &program.root,
        &shapes,
        &mut graph::Numbering {
            next_scope: 0,
            next_holder: graph::HOLDER_BASE,
        },
        &mut relations,
        &[],
        0,
        true,
    )?;
    if oracle.facts != relations.next_fact as usize {
        return Err(format!(
            "{path}: fact numbering differs: Rust {} DDIR {}",
            oracle.facts, relations.next_fact
        ));
    }
    let inserts = feed(&relations, &graph_rows, 1);
    for (input, update) in inserts.into_iter().enumerate() {
        server
            .feed_columns("rules", input, update, None)
            .map_err(|e| format!("{path}: {e}"))?;
    }
    server.tick(worker);
    let snapshot = server
        .snapshot(worker, "live")
        .map_err(|e| format!("{path}: {e}"))?;
    let mut got = vec![false; oracle.facts];
    for (key, _, diff) in snapshot.rows() {
        let fact = key.parse::<usize>().map_err(|e| e.to_string())?;
        if fact >= got.len() || diff != 1 {
            return Err(format!("{path}: invalid live fact {fact} with diff {diff}"));
        }
        got[fact] = true;
    }
    let want = oracle.closure();
    let disagreement = want.iter().zip(&got).filter(|(a, b)| a != b).count();
    if disagreement != 0 {
        let missing: BTreeSet<_> = want
            .iter()
            .zip(&got)
            .enumerate()
            .filter_map(|(i, (a, b))| (*a && !*b).then_some(i))
            .collect();
        let extra: BTreeSet<_> = want
            .iter()
            .zip(&got)
            .enumerate()
            .filter_map(|(i, (a, b))| (!*a && *b).then_some(i))
            .collect();
        eprintln!(
            "{path}: {disagreement} live-fact disagreements; missing {missing:?}; extra {extra:?}"
        );
        eprintln!("--- candidate program ---\n{text}");
    }
    let retracts = feed(&relations, &graph_rows, -1);
    for (input, update) in retracts.into_iter().enumerate() {
        server
            .feed_columns("rules", input, update, None)
            .map_err(|e| format!("{path}: {e}"))?;
    }
    server.tick(worker);
    for export in ["live", "dependencies"] {
        if !server
            .snapshot(worker, export)
            .map_err(|e| e.to_string())?
            .is_empty()
        {
            return Err(format!("{path}: {export} relation did not retract"));
        }
    }
    Ok(Some((oracle.facts, disagreement)))
}

fn main() {
    register_corpus_signatures();
    let paths: Vec<String> = std::env::args().skip(1).collect();
    let rules = std::fs::read_to_string("examples/self_opt/demand_rules.ddp")
        .expect("run from interactive/");
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        let mut rules = lower::lower_tree(parse::pipe::parse(&rules));
        rules.optimize();
        server.install(worker, "rules", &rules).unwrap();
        let (mut checked, mut skipped, mut mismatched) = (0, 0, 0);
        for path in &paths {
            let text = match std::fs::read_to_string(path) {
                Ok(text) => text,
                Err(err) => {
                    eprintln!("{path}: {err}");
                    skipped += 1;
                    continue;
                }
            };
            match one(path, &text, &mut server, worker) {
                Ok(Some((facts, n))) => {
                    checked += 1;
                    mismatched += n;
                    println!("{path}: {facts} facts, {n} disagreements");
                }
                Ok(None) => {
                    skipped += 1;
                    println!("{path}: skipped (Rust demand graph unavailable)");
                }
                Err(err) => {
                    mismatched += 1;
                    eprintln!("{err}");
                }
            }
        }
        println!("{checked} checked, {skipped} skipped, {mismatched} disagreements");
        assert_eq!(mismatched, 0);
    });
}
