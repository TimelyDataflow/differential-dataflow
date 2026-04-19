//! DD IR vec-backed backend: parse, lower, render, execute.

use std::collections::HashMap;
use std::sync::Arc;
use timely::order::Product;
use timely::dataflow::Scope;
use differential_dataflow::VecCollection;
use differential_dataflow::operators::iterate::VecVariable;
use differential_dataflow::dynamic::pointstamp::{PointStamp, PointStampSummary};
use differential_dataflow::dynamic::feedback_summary;
use differential_dataflow::trace::implementations::ValSpine;
use differential_dataflow::operators::arrange::{Arranged, TraceIntra};
use differential_dataflow::input::Input;
use smallvec::smallvec as svec;

use interactive::parse;
use interactive::lower;
use interactive::ir::{Node, LinearOp, Program, Diff, Id, Time, eval_fields, eval_field_into, eval_condition};

type Row = Vec<i64>;
type DdirTime = Product<u64, PointStamp<u64>>;
type Col<'scope, T> = VecCollection<'scope, T, (Row, Row), Diff>;
type Arr<'scope, T> = Arranged<'scope, TraceIntra<ValSpine<Row, Row, T, Diff>>>;

enum Rendered<'scope, T: timely::progress::Timestamp + differential_dataflow::lattice::Lattice> {
    Collection(Col<'scope, T>),
    Arrangement(Arr<'scope, T>),
}

impl<'scope, T: timely::progress::Timestamp + differential_dataflow::lattice::Lattice> Rendered<'scope, T> {
    fn collection(&self) -> Col<'scope, T> { match self { Rendered::Collection(c) => c.clone(), Rendered::Arrangement(a) => a.clone().as_collection(|k, v| (k.clone(), v.clone())) } }
    fn arrange(&self) -> Arr<'scope, T> { match self { Rendered::Arrangement(a) => a.clone(), Rendered::Collection(c) => c.clone().arrange_by_key() } }
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
            Node::Linear { input, ops } => {
                let c = nodes[input].collection();
                let ops = ops.clone();
                let level = level;
                let r = c.join_function(move |(key, val)| {
                    use timely::progress::Timestamp;
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
                            }
                        }
                        results = next;
                    }
                    results
                });
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

fn run(name: &str, stmts: Vec<parse::Stmt>, n_inputs: usize, nodes: u64, edges: u64, arity: usize, batch: u64, rounds: Option<u64>) {
    let mut compiled: Program = lower::lower(stmts);
    println!("{}: {} IR nodes (before optimize)", name, compiled.nodes.len());
    compiled.optimize();
    println!("{}: {} IR nodes (after optimize), result = {}", name, compiled.nodes.len(), compiled.result);
    compiled.dump();
    let name = name.to_string();
    let result_id = compiled.result;

    timely::execute_from_args(std::env::args().skip(4), move |worker| {

        let (mut inputs, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let mut handles = Vec::new();
            let mut collections = Vec::new();
            for _ in 0..n_inputs {
                let (h, c) = scope.new_collection::<(Row, Row), Diff>();
                handles.push(h); collections.push(c);
            }
            let mut probe = timely::dataflow::ProbeHandle::new();
            let output = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered: Vec<_> = collections.iter().map(|c| c.clone().enter(inner)).collect();
                let rendered = render_program(&compiled, inner, &entered);
                rendered[&result_id].clone().leave(scope)
            });
            output.probe_with(&mut probe);
            (handles, probe)
        });

        let index = worker.index();
        let peers = worker.peers();

        for e in 0..edges {
            if (e as usize) % peers == index {
                let input_idx = (e as usize) % inputs.len();
                inputs[input_idx].update(interactive::gen_row::<Row>(e, nodes, arity), 1);
            }
        }
        for i in inputs.iter_mut() { i.advance_to(1); i.flush(); }
        while probe.less_than(&1u64) { worker.step(); }
        let elapsed = std::time::Instant::now();
        println!("worker {}: {} loaded ({} edges)", index, name, edges);

        let mut cursor = 0u64;
        let mut round = 0u64;
        let limit = rounds.unwrap_or(u64::MAX);
        while round < limit {
            let time = (round + 2) as u64;
            for _ in 0..batch {
                let remove_idx = cursor;
                let add_idx = edges + cursor;
                if (remove_idx as usize) % peers == index {
                    let input_idx = (remove_idx as usize) % inputs.len();
                    inputs[input_idx].update(interactive::gen_row::<Row>(remove_idx, nodes, arity), -1);
                }
                if (add_idx as usize) % peers == index {
                    let input_idx = (add_idx as usize) % inputs.len();
                    inputs[input_idx].update(interactive::gen_row::<Row>(add_idx, nodes, arity), 1);
                }
                cursor += 1;
            }
            for i in inputs.iter_mut() { i.advance_to(time); i.flush(); }
            while probe.less_than(&time) { worker.step(); }

            round += 1;
            if round % 100 == 0 {
                println!("worker {}: {} round {} ({:.2?})", index, name, round, elapsed.elapsed());
            }
        }
        println!("worker {}: {} done ({} rounds, batch {}, {:.2?})", index, name, round, batch, elapsed.elapsed());
    }).unwrap();
}

fn main() {
    let program = std::env::args().nth(1).unwrap_or_else(|| { std::process::exit(0); });
    let arity: usize = std::env::args().nth(2).unwrap_or("2".into()).parse().unwrap();
    let nodes: u64 = std::env::args().nth(3).unwrap_or("10".into()).parse().unwrap();
    let edges: u64 = std::env::args().nth(4).unwrap_or_else(|| (2 * nodes).to_string()).parse().unwrap();
    let batch: u64 = std::env::args().nth(5).unwrap_or("1".into()).parse().unwrap();
    let rounds: Option<u64> = std::env::args().nth(6).map(|s| s.parse().unwrap());

    let source = interactive::load_program(&program);
    let stmts = if program.ends_with(".ddp") {
        parse::pipe::parse(&source)
    } else {
        parse::applicative::parse(&source)
    };
    let n_inputs = interactive::count_inputs(&stmts);
    let name = std::path::Path::new(&program).file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or(program.clone());
    run(&name, stmts, n_inputs, nodes, edges, arity, batch, rounds);
}
