//! DD IR vec-backed backend: parse, lower, render, execute.
//!
//! With `--explain`, applies `interactive::explain::explain` after lowering
//! and treats the last input handle as the query input (seeded from the
//! `QUERY` env var, format `"key_fields:val_fields"`).

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::collections::HashMap;
use std::sync::Arc;
use timely::order::Product;
use timely::dataflow::Scope;
use differential_dataflow::VecCollection;
use differential_dataflow::operators::iterate::VecVariable;
use differential_dataflow::dynamic::pointstamp::{PointStamp, PointStampSummary};
use differential_dataflow::dynamic::feedback_summary;
use differential_dataflow::trace::implementations::ValSpine;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::input::Input;
use smallvec::SmallVec;
use smallvec::smallvec as svec;

use interactive::parse;
use interactive::lower;
use interactive::scope_ir as st;
use interactive::ir::{Node, LinearOp, Program, Diff, Id, Time, eval_fields, eval_field_into, eval_condition};

type Row = SmallVec<[i64; 2]>;
type DdirTime = Product<u64, PointStamp<u64>>;
type Col<'scope, T> = VecCollection<'scope, T, (Row, Row), Diff>;
type Arr<'scope, T> = Arranged<'scope, TraceAgent<ValSpine<Row, Row, T, Diff>>>;

enum Rendered<'scope, T: timely::progress::Timestamp + differential_dataflow::lattice::Lattice> {
    Collection(Col<'scope, T>),
    Arrangement(Arr<'scope, T>),
}

impl<'scope, T: timely::progress::Timestamp + differential_dataflow::lattice::Lattice> Rendered<'scope, T> {
    fn collection(&self) -> Col<'scope, T> { match self { Rendered::Collection(c) => c.clone(), Rendered::Arrangement(a) => a.clone().as_collection(|k, v| (k.clone(), v.clone())) } }
    fn arrange(&self) -> Arr<'scope, T> { match self { Rendered::Arrangement(a) => a.clone(), Rendered::Collection(c) => c.clone().arrange_by_key() } }
}


/// Render a Linear chain: one flat_map applying the ops in sequence. `level` is
/// the op's scope depth — it locates the iteration coord for LiftIter and the
/// coordinate position EnterAt's delay lands in.
fn render_linear<'scope>(c: Col<'scope, DdirTime>, ops: Vec<LinearOp>, level: usize) -> Col<'scope, DdirTime> {
    use differential_dataflow::AsCollection;
    use differential_dataflow::lattice::Lattice;
    use timely::dataflow::operators::core::Map;
    c.inner.flat_map(move |((key, val), t_in, d_in)| {
        use timely::progress::Timestamp;
        let iter_at_level: i64 = level
            .checked_sub(1)
            .and_then(|idx| t_in.inner.get(idx).copied())
            .unwrap_or(0) as i64;
        let mut results: smallvec::SmallVec<[((Row, Row), Time, Diff); 2]> = svec![((key, val), Time::minimum(), 1)];
        for op in &ops {
            let mut next = smallvec::SmallVec::new();
            for ((k, v), t, d) in results {
                match op {
                    LinearOp::Project(proj) => {
                        let i = [k.as_slice(), v.as_slice()];
                        next.push(((eval_fields(&proj.key, &i), eval_fields(&proj.val, &i)), t, d));
                    },
                    LinearOp::Filter(cond) => {
                        let i = [k.as_slice(), v.as_slice()];
                        if eval_condition(cond, &i) { next.push(((k, v), t, d)); }
                    },
                    LinearOp::Negate => {
                        next.push(((k, v), t, -d));
                    },
                    LinearOp::EnterAt(field) => {
                        let delay = {
                            let mut r = Row::new();
                            eval_field_into(field, &[k.as_slice(), v.as_slice()], &mut r);
                            256 * (64 - (r.as_slice().first().copied().unwrap_or(0) as u64).leading_zeros() as u64)
                        };
                        let mut coords = smallvec::SmallVec::<[u64; 1]>::new();
                        for _ in 0..level.saturating_sub(1) { coords.push(0); }
                        coords.push(delay);
                        next.push(((k, v), Product::new(0u64, PointStamp::new(coords)), d));
                    },
                    LinearOp::LiftIter => {
                        let mut new_v = v.clone();
                        new_v.push(iter_at_level);
                        next.push(((k, new_v), t, d));
                    },
                }
            }
            results = next;
        }
        results.into_iter().map(move |((k, v), t_delta, d)| ((k, v), t_in.join(&t_delta), d_in * d))
    }).as_collection()
}

// ===== Scope-tree renderer =====
//
// Walks the scope tree (scope_ir) in item order — no scans, no topological
// re-analysis. Each `{}` scope renders as a timely region (imports enter it,
// exports leave it), with the dynamic PointStamp coordinate riding inside.

/// A rendered item: an operator's value, or a child scope's surrendered exports
/// (already returned to this scope's depth via `leave_dynamic`).
enum RItem<'scope> {
    Op(Rendered<'scope, DdirTime>),
    Sub(Vec<Col<'scope, DdirTime>>),
}

fn resolve<'scope>(
    items: &[RItem<'scope>],
    imports: &[Col<'scope, DdirTime>],
    var_cols: &[Col<'scope, DdirTime>],
    r: &st::Ref,
) -> Rendered<'scope, DdirTime> {
    match r {
        st::Ref::Local(i) => match &items[*i] {
            RItem::Op(Rendered::Collection(c)) => Rendered::Collection(c.clone()),
            RItem::Op(Rendered::Arrangement(a)) => Rendered::Arrangement(a.clone()),
            RItem::Sub(_) => panic!("Ref::Local points at a child scope"),
        },
        st::Ref::Import(i) => Rendered::Collection(imports[*i].clone()),
        st::Ref::Var(i) => Rendered::Collection(var_cols[*i].clone()),
        st::Ref::ChildExport(i, j) => match &items[*i] {
            RItem::Sub(exports) => Rendered::Collection(exports[*j].clone()),
            RItem::Op(_) => panic!("Ref::ChildExport points at an operator"),
        },
    }
}

/// Render one scope at `depth` (root = 0): feedback vars first (they're listed,
/// not scanned for), then items in order, then binds close the loops, then the
/// exports are surrendered. The returned collections are at this scope's depth;
/// popping the coordinate (`leave_dynamic`) is the caller's job.
fn render_tree<'scope>(
    s: &st::Scope,
    scope: Scope<'scope, DdirTime>,
    depth: usize,
    imports: Vec<Col<'scope, DdirTime>>,
) -> Vec<Col<'scope, DdirTime>> {
    let mut var_handles: Vec<Option<VecVariable<'scope, DdirTime, (Row, Row), Diff>>> = Vec::new();
    let mut var_cols: Vec<Col<'scope, DdirTime>> = Vec::new();
    for _ in &s.vars {
        let step: Product<u64, PointStampSummary<u64>> = Product::new(0, feedback_summary::<u64>(depth, 1));
        let (var, col) = VecVariable::new(scope, step);
        var_handles.push(Some(var));
        var_cols.push(col);
    }

    let mut items: Vec<RItem<'scope>> = Vec::new();
    for item in &s.items {
        match item {
            st::Item::Op(node) => {
                let rendered = match node {
                    st::Node::Linear { input, ops } => {
                        let c = resolve(&items, &imports, &var_cols, input).collection();
                        Rendered::Collection(render_linear(c, ops.clone(), depth))
                    },
                    st::Node::Concat(refs) => {
                        let mut c = resolve(&items, &imports, &var_cols, &refs[0]).collection();
                        for r in &refs[1..] { c = c.concat(resolve(&items, &imports, &var_cols, r).collection()); }
                        Rendered::Collection(c)
                    },
                    st::Node::Arrange(r) => Rendered::Arrangement(resolve(&items, &imports, &var_cols, r).arrange()),
                    st::Node::Join { left, right, projection } => {
                        let l = resolve(&items, &imports, &var_cols, left).arrange();
                        let r = resolve(&items, &imports, &var_cols, right).arrange();
                        let proj = projection.clone();
                        let f: Arc<dyn Fn(&Row, &Row, &Row) -> smallvec::SmallVec<[(Row, Row); 2]> + Send + Sync> =
                            Arc::new(move |key, left, right| { let i = [key.as_slice(), left.as_slice(), right.as_slice()]; svec![(eval_fields(&proj.key, &i), eval_fields(&proj.val, &i))] });
                        Rendered::Collection(l.join_core(r, move |k, v1, v2| f(k, v1, v2)))
                    },
                    st::Node::Reduce { input, reducer } => {
                        let a = resolve(&items, &imports, &var_cols, input).arrange();
                        let f: Arc<dyn Fn(&Row, &[(&Row, Diff)], &mut Vec<(Row, Diff)>) + Send + Sync> = match reducer {
                            parse::Reducer::Min => Arc::new(|_key, vals, output| { if let Some(min) = vals.iter().map(|(v, _)| *v).min() { output.push((min.clone(), 1)); } }),
                            parse::Reducer::Distinct => Arc::new(|_key, _vals, output| { output.push((Row::new(), 1)); }),
                            parse::Reducer::Count => Arc::new(|_key, vals, output| { let count: Diff = vals.iter().map(|(_, d)| *d).sum(); if count > 0 { let mut r = Row::new(); r.push(count); output.push((r, 1)); } }),
                        };
                        let reduced = a.reduce_abelian::<_, differential_dataflow::trace::implementations::ValBuilder<_,_,_,_>, ValSpine<_,_,_,_>, _>(
                            "Reduce",
                            move |k, v, o| f(k, v, o),
                            |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v,t,r)| ((key.clone(), v),t,r))); },
                        );
                        Rendered::Arrangement(reduced)
                    },
                    st::Node::Inspect { input, label } => {
                        let col = resolve(&items, &imports, &var_cols, input).collection();
                        let label = label.clone();
                        Rendered::Collection(col.inspect(move |x| eprintln!("  [{}] {:?}", label, x.clone())))
                    },
                };
                items.push(RItem::Op(rendered));
            },
            st::Item::Sub(child) => {
                let child_imports: Vec<Col<'scope, DdirTime>> = child.imports.iter().map(|imp| match &imp.from {
                    st::Source::Parent(r) => resolve(&items, &imports, &var_cols, r).collection(),
                    other => panic!("non-root scope with external source {:?}", other),
                }).collect();
                // Each `{}` scope is a real timely region: imports enter it,
                // the child renders inside, exports leave it structurally —
                // and then pop the child's dynamic coordinate.
                let exported = scope.region_named(&child.name, |region| {
                    let entered: Vec<_> = child_imports.iter().map(|c| c.clone().enter(region)).collect();
                    let exports = render_tree(child, region, depth + 1, entered);
                    exports.into_iter().map(|c| c.leave(scope)).collect::<Vec<_>>()
                });
                let left: Vec<Col<'scope, DdirTime>> = exported.into_iter().map(|c| c.leave_dynamic(depth + 1)).collect();
                items.push(RItem::Sub(left));
            },
        }
    }

    for bind in &s.binds {
        let c = resolve(&items, &imports, &var_cols, &bind.value).collection();
        var_handles[bind.var].take().expect("bind: variable already bound").set(c);
    }

    s.exports.iter().map(|e| resolve(&items, &imports, &var_cols, &e.value).collection()).collect()
}

fn render_program<'scope>(program: &Program, scope: Scope<'scope, DdirTime>, inputs: &[Col<'scope, DdirTime>]) -> HashMap<Id, Col<'scope, DdirTime>>
{
    let mut nodes: HashMap<Id, Rendered<'scope, DdirTime>> = HashMap::new();
    let mut level: usize = 0;
    let mut variables: HashMap<Id, (VecVariable<'scope, DdirTime, (Row, Row), Diff>, usize)> = HashMap::new();
    let mut var_levels: HashMap<Id, usize> = HashMap::new();

    for (&id, node) in program.nodes.iter() {
        match node {
            Node::Input(i) => { nodes.insert(id, Rendered::Collection(inputs[*i].clone())); },
            Node::Import { name } => panic!("ddir_vec: Import {:?} not supported in this harness (no trace registry).", name),
            Node::Linear { input, ops } => {
                let c = nodes[input].collection();
                let r = render_linear(c, ops.clone(), level);
                nodes.insert(id, Rendered::Collection(r));
            },
            Node::Concat(ids) => { let mut r = nodes[&ids[0]].collection(); for i in &ids[1..] { r = r.concat(nodes[i].collection()); } nodes.insert(id, Rendered::Collection(r)); },
            Node::Arrange(input) => { nodes.insert(id, Rendered::Arrangement(nodes[input].arrange())); },
            Node::Join { left, right, projection } => {
                let Rendered::Arrangement(l) = &nodes[left] else { panic!("Join: left input must be an Arrangement") };
                let Rendered::Arrangement(r) = &nodes[right] else { panic!("Join: right input must be an Arrangement") };
                let l = l.clone();
                let r = r.clone();
                let proj = projection.clone();
                let f: Arc<dyn Fn(&Row, &Row, &Row) -> smallvec::SmallVec<[(Row, Row); 2]> + Send + Sync> =
                    Arc::new(move |key, left, right| { let i = [key.as_slice(), left.as_slice(), right.as_slice()]; svec![(eval_fields(&proj.key, &i), eval_fields(&proj.val, &i))] });
                let result = l.join_core(r, move |k, v1, v2| f(k, v1, v2));
                nodes.insert(id, Rendered::Collection(result));
            },
            Node::Reduce { input, reducer } => {
                let Rendered::Arrangement(a) = &nodes[input] else { panic!("Reduce: input must be an Arrangement") };
                let a = a.clone();
                let f: Arc<dyn Fn(&Row, &[(&Row, Diff)], &mut Vec<(Row, Diff)>) + Send + Sync> = match reducer {
                    parse::Reducer::Min => Arc::new(|_key, vals, output| { if let Some(min) = vals.iter().map(|(v, _)| *v).min() { output.push((min.clone(), 1)); } }),
                    parse::Reducer::Distinct => Arc::new(|_key, _vals, output| { output.push((Row::new(), 1)); }),
                    parse::Reducer::Count => Arc::new(|_key, vals, output| { let count: Diff = vals.iter().map(|(_, d)| *d).sum(); if count > 0 { let mut r = Row::new(); r.push(count); output.push((r, 1)); } }),
                };
                let reduced = a.reduce_abelian::<_, differential_dataflow::trace::implementations::ValBuilder<_,_,_,_>, ValSpine<_,_,_,_>, _>(
                    "Reduce",
                    move |k, v, o| f(k, v, o),
                    |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v,t,r)| ((key.clone(), v),t,r))); },
                );
                nodes.insert(id, Rendered::Arrangement(reduced));
            },
            Node::Variable => {
                let step: Product<u64, PointStampSummary<u64>> = Product::new(0, feedback_summary::<u64>(level, 1));
                let (var, col) = VecVariable::new(scope, step);
                nodes.insert(id, Rendered::Collection(col)); variables.insert(id, (var, level)); var_levels.insert(id, level);
            },
            Node::Inspect { input, label } => {
                let col = nodes[input].collection();
                let label = label.clone();
                nodes.insert(id, Rendered::Collection(col.inspect(move |x| eprintln!("  [{}] {:?}", label, x.clone()))));
            },
            Node::Leave(inner_id, scope_level) => { nodes.insert(id, Rendered::Collection(nodes[inner_id].collection().leave_dynamic(*scope_level))); },
            Node::Scope => { level += 1; },
            Node::EndScope => { level -= 1; },
            Node::Bind { variable, value } => { let c = nodes[value].collection(); let (var, _) = variables.remove(variable).expect("Bind: variable not found"); var.set(c); },
        }
    }

    nodes.into_iter().filter_map(|(id, r)| match r { Rendered::Collection(c) => Some((id, c)), _ => None }).collect()
}

fn run(
    name: &str,
    stmts: Vec<parse::Stmt>,
    n_inputs: usize,
    nodes: u64,
    edges: u64,
    arity: usize,
    batch: u64,
    rounds: Option<u64>,
    explain: bool,
) {
    // The scope-tree IR (lower_tree + render_tree, one timely region per scope)
    // is the default path, including for `--explain` (explain_tree). FLAT=1
    // forces the flat path for A/B comparison; outputs must match either way.
    let tree_mode = std::env::var("FLAT").is_err();
    let (tree, compiled, result_id, tree_export_idx);
    if tree_mode {
        let mut t = lower::lower_tree(stmts);
        // CLONE_RT=1 routes the program through the explain rewrite's
        // clone-with-lifts as an identity check: outputs must be unchanged.
        if std::env::var("CLONE_RT").is_ok() {
            t = interactive::explain_tree::clone_identity(&t);
        }
        // --explain: rewrite for self-explanation before optimization (the
        // rules assume single-op Linears). Sources are the root's imports.
        if explain {
            let source_shapes: Vec<(usize, usize)> = t.root.imports.iter().map(|imp| match &imp.from {
                interactive::scope_ir::Source::Input(_) => (arity, 0usize),
                other => panic!("ddir_vec --explain: unsupported source {:?}", other),
            }).collect();
            t = interactive::explain_tree::explain_tree(&t, &source_shapes);
        }
        let ops_before = t.op_count();
        t.optimize();
        tree_export_idx = t.root.exports.iter().position(|e| e.name == "result").unwrap_or(0);
        println!("{}: tree mode; {} ops before optimize, {} after; driving export {:?}",
            name, ops_before, t.op_count(), t.root.exports[tree_export_idx].name);
        tree = Some(t);
        compiled = Program { nodes: std::collections::BTreeMap::new(), export: vec![] };
        result_id = 0;
    } else {
        let mut c: Program = lower::lower(stmts);
        // When --explain is set, rewrite the program for self-explanation
        // before optimization. The transformed program declares one extra
        // input (the query); the last handle below is reserved for it and
        // seeded from `QUERY=`.
        if explain {
            let input_arities = vec![(arity, 0usize); n_inputs];
            let import_arities = std::collections::BTreeMap::new();
            c = interactive::explain::explain(&c, &input_arities, &import_arities);
        }
        println!("{}: {} IR nodes (before optimize)", name, c.nodes.len());
        c.optimize();
        println!("{}: {} IR nodes (after optimize), exports = {:?}",
            name, c.nodes.len(),
            c.export.iter().map(|(n, id)| (n.as_str(), *id)).collect::<Vec<_>>());
        c.dump();
        // Drive one export: prefer `$result`, else the first declared.
        let (driven_name, id) = {
            let pick = c.export.iter().find(|(n, _)| n == "result")
                .or_else(|| c.export.first())
                .expect("ddir_vec: program declares no exports");
            (pick.0.clone(), pick.1)
        };
        println!("{}: driving export {:?} (id {})", name, driven_name, id);
        tree = None;
        compiled = c;
        result_id = id;
        tree_export_idx = 0;
    }
    let name = name.to_string();
    let total_inputs = if explain { n_inputs + 1 } else { n_inputs };
    let query_input_idx = if explain { Some(n_inputs) } else { None };

    timely::execute_from_args(std::env::args().skip(4), move |worker| {

        // DIAG=1 registers timely/DD logging and serves the diagnostics
        // WebSocket on worker 0 (port 51371) — see diagnostics/README.md.
        let _diag = if std::env::var("DIAG").is_ok() {
            let state = diagnostics::logging::register(worker, false);
            if worker.index() == 0 {
                Some(diagnostics::server::Server::start(51371, state.sink))
            } else {
                drop(state.sink);
                None
            }
        } else { None };

        let (mut inputs, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let mut handles = Vec::new();
            let mut collections = Vec::new();
            for _ in 0..total_inputs {
                let (h, c) = scope.new_collection::<(Row, Row), Diff>();
                handles.push(h); collections.push(c);
            }
            let mut probe = timely::dataflow::ProbeHandle::new();
            let output = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered: Vec<_> = collections.iter().map(|c| c.clone().enter(inner)).collect();
                if let Some(tree) = &tree {
                    let root_imports: Vec<_> = tree.root.imports.iter().map(|imp| match &imp.from {
                        st::Source::Input(n) => entered[*n].clone(),
                        st::Source::Trace(name) => panic!("ddir_vec: Import {:?} not supported in this harness (no trace registry).", name),
                        st::Source::Parent(_) => unreachable!("root scope cannot import from a parent"),
                    }).collect();
                    let exports = render_tree(&tree.root, inner, 0, root_imports);
                    exports[tree_export_idx].clone().leave(scope)
                } else {
                    let rendered = render_program(&compiled, inner, &entered);
                    rendered[&result_id].clone().leave(scope)
                }
            });
            output.probe_with(&mut probe);
            (handles, probe)
        });

        let index = worker.index();
        let peers = worker.peers();

        let timer = std::time::Instant::now();
        let timer_load = std::time::Instant::now();
        // Real inputs are 0..n_inputs. The query input (if any) is at
        // n_inputs and is seeded separately below.
        for e in 0..edges {
            if (e as usize) % peers == index {
                let input_idx = (e as usize) % n_inputs;
                inputs[input_idx].update(interactive::gen_row::<Row>(e, nodes, arity), 1);
            }
        }
        // Seed the query input (worker 0 only) from $QUERY = "k:v[,q]".
        if let Some(q_idx) = query_input_idx {
            if index == 0 {
                if let Ok(qstr) = std::env::var("QUERY") {
                    let parse_row = |s: &str| -> Row {
                        if s.is_empty() {
                            SmallVec::new()
                        } else {
                            s.split(',').map(|t| t.trim().parse::<i64>().unwrap()).collect()
                        }
                    };
                    let (k_str, vq_str) = qstr.split_once(':').unwrap_or((qstr.as_str(), ""));
                    let q_key: Row = parse_row(k_str);
                    let mut q_val: Row = parse_row(vq_str);
                    if q_val.is_empty() { q_val.push(0); }
                    eprintln!("seeding query: key={:?} val_with_q={:?}", q_key, q_val);
                    inputs[q_idx].update((q_key, q_val), 1);
                }
            }
        }
        for i in inputs.iter_mut() { i.advance_to(1); i.flush(); }
        while probe.less_than(&1u64) { worker.step(); }
        println!("worker {}: {} loaded ({} edges, total {:.2?}, load {:.2?})", index, name, edges, timer.elapsed(), timer_load.elapsed());

        let mut cursor = 0u64;
        let mut round = 0u64;
        let limit = rounds.unwrap_or(u64::MAX);
        while round < limit {
            let timer_round = std::time::Instant::now();
            let time = (round + 2) as u64;
            for _ in 0..batch {
                let remove_idx = cursor;
                let add_idx = edges + cursor;
                if (remove_idx as usize) % peers == index {
                    let input_idx = (remove_idx as usize) % n_inputs;
                    inputs[input_idx].update(interactive::gen_row::<Row>(remove_idx, nodes, arity), -1);
                }
                if (add_idx as usize) % peers == index {
                    let input_idx = (add_idx as usize) % n_inputs;
                    inputs[input_idx].update(interactive::gen_row::<Row>(add_idx, nodes, arity), 1);
                }
                cursor += 1;
            }
            for i in inputs.iter_mut() { i.advance_to(time); i.flush(); }
            while probe.less_than(&time) { worker.step(); }

            round += 1;
            if round % 100 == 0 {
                println!("worker {}: {} round {} (total {:.2?}, round {:.2?})", index, name, round, timer.elapsed(), timer_round.elapsed());
            }
        }
        println!("worker {}: {} done ({} rounds, batch {}, total {:.2?})", index, name, round, batch, timer.elapsed());
    }).unwrap();
}

fn main() {
    // Strip an optional leading --explain flag.
    let raw_args: Vec<String> = std::env::args().collect();
    let (explain, args): (bool, Vec<String>) = {
        let mut it = raw_args.into_iter();
        let prog = it.next().unwrap();
        let mut explain = false;
        let mut rest: Vec<String> = Vec::new();
        for a in it {
            if a == "--explain" { explain = true; } else { rest.push(a); }
        }
        let mut out = vec![prog]; out.extend(rest);
        (explain, out)
    };
    let program = args.get(1).cloned().unwrap_or_else(|| { std::process::exit(0); });
    let arity: usize = args.get(2).cloned().unwrap_or("2".into()).parse().unwrap();
    let nodes: u64 = args.get(3).cloned().unwrap_or("10".into()).parse().unwrap();
    let edges: u64 = args.get(4).cloned().unwrap_or_else(|| (2 * nodes).to_string()).parse().unwrap();
    let batch: u64 = args.get(5).cloned().unwrap_or("1".into()).parse().unwrap();
    let rounds: Option<u64> = args.get(6).map(|s| s.parse().unwrap());

    let source = interactive::load_program(&program);
    let stmts = if program.ends_with(".ddp") {
        parse::pipe::parse(&source)
    } else {
        parse::applicative::parse(&source)
    };
    let (n_inputs, imports) = interactive::survey_sources(&stmts);
    if !imports.is_empty() {
        panic!("ddir_vec: program references imports {:?} but this harness has no trace registry.", imports);
    }
    let name = std::path::Path::new(&program).file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or(program.clone());
    run(&name, stmts, n_inputs, nodes, edges, arity, batch, rounds, explain);
}
