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
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::input::Input;

use ddir::parse;
use ddir::lower;
use ddir::ir::{Node, Program, Diff, Id};

type Row = Vec<i64>;
type Col<G> = VecCollection<G, (Row, Row), Diff>;
type Arr<G> = Arranged<G, TraceAgent<ValSpine<Row, Row, <G as timely::dataflow::scopes::ScopeParent>::Timestamp, Diff>>>;

enum Rendered<G: Scope<Timestamp: differential_dataflow::lattice::Lattice>> {
    Collection(Col<G>),
    Arrangement(Arr<G>),
}

impl<G: Scope<Timestamp: differential_dataflow::lattice::Lattice>> Rendered<G> {
    fn collection(&self) -> Col<G> { match self { Rendered::Collection(c) => c.clone(), Rendered::Arrangement(a) => a.clone().as_collection(|k, v| (k.clone(), v.clone())) } }
    fn arrange(&self) -> Arr<G> { match self { Rendered::Arrangement(a) => a.clone(), Rendered::Collection(c) => c.clone().arrange_by_key() } }
}


fn render_program<G>(program: &Program<Row>, scope: &mut G, inputs: &[Col<G>]) -> HashMap<Id, Col<G>>
where G: Scope<Timestamp = Product<u64, PointStamp<u64>>>
{
    let mut nodes: HashMap<Id, Rendered<G>> = HashMap::new();
    let mut level: usize = 0;
    let mut variables: HashMap<Id, (VecVariable<G, (Row, Row), Diff>, usize)> = HashMap::new();
    let mut var_levels: HashMap<Id, usize> = HashMap::new();

    for (id, node) in program.nodes.iter().enumerate() {
        match node {
            Node::Input(i) => { nodes.insert(id, Rendered::Collection(inputs[*i].clone())); },
            Node::Map { input, logic } => { let c = nodes[input].collection(); let l = Arc::clone(logic); let r = c.join_function(move |kv| l(kv)); nodes.insert(id, Rendered::Collection(r)); },
            Node::Concat(ids) => { let mut r = nodes[&ids[0]].collection(); for i in &ids[1..] { r = r.concat(nodes[i].collection()); } nodes.insert(id, Rendered::Collection(r)); },
            Node::Arrange(input) => { nodes.insert(id, Rendered::Arrangement(nodes[input].arrange())); },
            Node::Join { left, right, logic } => {
                let l = nodes[left].arrange();
                let r = nodes[right].arrange();
                let f = Arc::clone(logic);
                let result = l.join_core(r, move |k, v1, v2| f(k, v1, v2));
                nodes.insert(id, Rendered::Collection(result));
            },
            Node::Reduce { input, logic } => {
                let a = nodes[input].arrange();
                let f = Arc::clone(logic);
                let reduced = a.reduce_abelian::<_, differential_dataflow::trace::implementations::ValBuilder<_,_,_,_>, ValSpine<_,_,_,_>>("Reduce", move |k, v, o| f(k, v, o));
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
    let compiled: Program<Row> = lower::lower(stmts);
    println!("{}: {} IR nodes, result = {}", name, compiled.nodes.len(), compiled.result);
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
                rendered[&result_id].clone().leave()
            });
            output.probe_with(&mut probe);
            (handles, probe)
        });

        let index = worker.index();
        let peers = worker.peers();

        for e in 0..edges {
            if (e as usize) % peers == index {
                let input_idx = (e as usize) % inputs.len();
                inputs[input_idx].update(ddir::gen_row::<Row>(e, nodes, arity), 1);
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
                    inputs[input_idx].update(ddir::gen_row::<Row>(remove_idx, nodes, arity), -1);
                }
                if (add_idx as usize) % peers == index {
                    let input_idx = (add_idx as usize) % inputs.len();
                    inputs[input_idx].update(ddir::gen_row::<Row>(add_idx, nodes, arity), 1);
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

    let source = ddir::load_program(&program);
    let stmts = if program.ends_with(".ddp") {
        parse::pipe::parse(&source)
    } else {
        parse::applicative::parse(&source)
    };
    let n_inputs = ddir::count_inputs(&stmts);
    let name = std::path::Path::new(&program).file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or(program.clone());
    run(&name, stmts, n_inputs, nodes, edges, arity, batch, rounds);
}
