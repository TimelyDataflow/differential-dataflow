//! DD IR columnar backend: parse, lower, render, execute.

mod types {
    /// A row type backed by Vec but using Strides for columnar bounds.
    /// This ensures uniform-length rows (common in the IR) get compact
    /// stride-based offset encoding rather than per-element u64 bounds.
    #[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Row(pub Vec<i64>);

    impl Row {
        pub fn new() -> Self { Row(Vec::new()) }
        pub fn push(&mut self, v: i64) { self.0.push(v); }
    }

    impl std::ops::Deref for Row {
        type Target = [i64];
        fn deref(&self) -> &[i64] { &self.0 }
    }

    impl std::iter::FromIterator<i64> for Row {
        fn from_iter<I: IntoIterator<Item = i64>>(iter: I) -> Self {
            Row(iter.into_iter().collect())
        }
    }

    impl<'a> IntoIterator for &'a Row {
        type Item = &'a i64;
        type IntoIter = std::slice::Iter<'a, i64>;
        fn into_iter(self) -> Self::IntoIter { self.0.iter() }
    }

    impl IntoIterator for Row {
        type Item = i64;
        type IntoIter = std::vec::IntoIter<i64>;
        fn into_iter(self) -> Self::IntoIter { self.0.into_iter() }
    }

    impl columnar::Columnar for Row {
        type Container = columnar::Vecs<Vec<i64>, columnar::primitive::offsets::Strides>;

        fn into_owned<'a>(other: columnar::Ref<'a, Self>) -> Self {
            Row(other.into_iter().copied().collect())
        }

        fn copy_from<'a>(&mut self, other: columnar::Ref<'a, Self>) {
            self.0.clear();
            self.0.extend(other.into_iter().copied());
        }
    }

    impl interactive::ir::RowLike for Row {
        fn new() -> Self { Row::new() }
        fn push(&mut self, v: i64) { Row::push(self, v); }
        fn as_slice(&self) -> &[i64] { &self.0 }
        fn extend_from_slice(&mut self, other: &[i64]) { self.0.extend_from_slice(other); }
    }

    pub type Diff = i64;
    pub type Id = usize;
    pub type Time = timely::order::Product<u64, differential_dataflow::dynamic::pointstamp::PointStamp<u64>>;
}
use types::*;

use interactive::parse;
use interactive::lower;
use interactive::ir::Program;

#[path = "../../differential-dataflow/examples/columnar/columnar_support.rs"]
mod columnar_support;
use columnar_support::*;

mod columnar {
    use super::types::*;

    pub use super::columnar_support::*;
    pub use super::columnar_support::{ValSpine, ValBatcher, ValBuilder};

    pub type DdirUpdate = (Row, Row, Time, Diff);
    pub type DdirRecordedUpdates = RecordedUpdates<DdirUpdate>;

    pub type ColValSpine<K, V, T, R> = ValSpine<K, V, T, R>;
    pub type ColValBatcher<K, V, T, R> = ValBatcher<K, V, T, R>;
    pub type ColValBuilder<K, V, T, R> = ValBuilder<K, V, T, R>;
}

mod render {
    use std::collections::HashMap;
    use std::sync::Arc;
    use timely::order::Product;
    use timely::dataflow::Scope;
    use differential_dataflow::Collection;
    use differential_dataflow::operators::iterate::Variable;
    use differential_dataflow::dynamic::pointstamp::{PointStamp, PointStampSummary};
    use differential_dataflow::dynamic::feedback_summary;
    use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
    use columnar::Columnar;
    use super::types::*;
    use interactive::ir::{Node, LinearOp, Program, RowLike, eval_fields, eval_field_into, eval_condition};

    use super::columnar::{DdirUpdate, DdirRecordedUpdates};
    use super::columnar::{ColValSpine, ColValBuilder};

    type ConcreteTime = Product<u64, PointStamp<u64>>;

    pub type Col<'scope> = Collection<'scope, ConcreteTime, DdirRecordedUpdates>;
    type Arr<'scope> = Arranged<'scope, TraceAgent<ColValSpine<Row, Row, ConcreteTime, Diff>>>;

    enum Rendered<'scope> {
        Collection(Col<'scope>),
        Arrangement(Arr<'scope>),
    }

    impl<'scope> Rendered<'scope> {
        fn collection(&self) -> Col<'scope> {
            match self {
                Rendered::Collection(c) => c.clone(),
                Rendered::Arrangement(a) => {
                    super::columnar_support::as_recorded_updates::<DdirUpdate>(a.clone())
                }
            }
        }
        fn arrange(&self) -> Arr<'scope> {
            match self {
                Rendered::Arrangement(a) => a.clone(),
                Rendered::Collection(c) => {
                    use differential_dataflow::operators::arrange::arrangement::arrange_core;
                    use super::columnar::ColValBatcher;
                    arrange_core::<_, ColValBatcher<Row,Row,Time,Diff>, ColValBuilder<Row,Row,Time,Diff>, ColValSpine<Row,Row,Time,Diff>>(c.inner.clone(), timely::dataflow::channels::pact::Pipeline, "Arrange")
                }
            }
        }
    }

    pub fn render_program<'scope>(program: &Program, scope: &mut Scope<'scope, ConcreteTime>, inputs: &[Col<'scope>]) -> HashMap<Id, Col<'scope>>
    {
        let mut nodes: HashMap<Id, Rendered<'scope>> = HashMap::new();
        let mut level: usize = 0;
        let mut variables: HashMap<Id, (Variable<'scope, ConcreteTime, DdirRecordedUpdates>, usize)> = HashMap::new();
        let mut var_levels: HashMap<Id, usize> = HashMap::new();

        for (&id, node) in program.nodes.iter() {
            match node {
                Node::Input(i) => {
                    nodes.insert(id, Rendered::Collection(inputs[*i].clone()));
                },
                Node::Linear { input, ops } => {
                    let c = nodes[input].collection();
                    let ops = ops.clone();
                    let level = level;
                    let result = super::columnar::join_function(c, move |k, v, _t, _d| {
                        use timely::progress::Timestamp;
                        let k: Row = Columnar::into_owned(k);
                        let v: Row = Columnar::into_owned(v);
                        let mut results: Vec<(Row, Row, Time, Diff)> = vec![(k, v, Time::minimum(), 1i64)];
                        for op in &ops {
                            let mut next = Vec::new();
                            for (k, v, t, d) in results {
                                match op {
                                    LinearOp::Project(proj) => {
                                        let i = [k.as_slice(), v.as_slice()];
                                        next.push((eval_fields(&proj.key, &i), eval_fields(&proj.val, &i), t, d));
                                    },
                                    LinearOp::Filter(cond) => {
                                        let i = [k.as_slice(), v.as_slice()];
                                        if eval_condition(cond, &i) { next.push((k, v, t, d)); }
                                    },
                                    LinearOp::Negate => {
                                        next.push((k, v, t, -d));
                                    },
                                    LinearOp::EnterAt(field) => {
                                        let delay = {
                                            let mut r = Row::new();
                                            eval_field_into(field, &[k.as_slice(), v.as_slice()], &mut r);
                                            256 * (64 - (r.as_slice().first().copied().unwrap_or(0) as u64).leading_zeros() as u64)
                                        };
                                        let mut coords = smallvec::SmallVec::<[u64; 2]>::new();
                                        for _ in 0..level.saturating_sub(1) { coords.push(0); }
                                        coords.push(delay);
                                        next.push((k, v, Product::new(0u64, PointStamp::new(coords)), d));
                                    },
                                }
                            }
                            results = next;
                        }
                        results.into_iter()
                    });
                    nodes.insert(id, Rendered::Collection(result));
                },
                Node::Concat(ids) => {
                    let mut r = nodes[&ids[0]].collection();
                    for i in &ids[1..] { r = r.concat(nodes[i].collection()); }
                    nodes.insert(id, Rendered::Collection(r));
                },
                Node::Arrange(input) => {
                    nodes.insert(id, Rendered::Arrangement(nodes[input].arrange()));
                },
                Node::Join { left, right, projection } => {
                    let Rendered::Arrangement(l) = &nodes[left] else { panic!("Join: left input must be an Arrangement") };
                    let Rendered::Arrangement(r) = &nodes[right] else { panic!("Join: right input must be an Arrangement") };
                    let l = l.clone();
                    let r = r.clone();
                    let proj = projection.clone();
                    use differential_dataflow::operators::join::join_traces;
                    use differential_dataflow::collection::AsCollection;
                    use super::columnar::ValColBuilder;
                    let stream = join_traces::<_, _, _, ValColBuilder<DdirUpdate>>(l, r, move |k, v1, v2, t, d1, d2, c| {
                        use differential_dataflow::difference::Multiply;
                        let k: Row = Columnar::into_owned(k);
                        let v1: Row = Columnar::into_owned(v1);
                        let v2: Row = Columnar::into_owned(v2);
                        let d = d1.clone().multiply(d2);
                        let i = [k.as_slice(), v1.as_slice(), v2.as_slice()];
                        let (k2, v2): (Row, Row) = (eval_fields(&proj.key, &i), eval_fields(&proj.val, &i));
                        c.give((k2, v2, t.clone(), d));
                    });
                    nodes.insert(id, Rendered::Collection(stream.as_collection()));
                },
                Node::Reduce { input, reducer } => {
                    let Rendered::Arrangement(a) = &nodes[input] else { panic!("Reduce: input must be an Arrangement") };
                    let a = a.clone();
                    let reducer = reducer.clone();
                    let f: Arc<dyn Fn(&Row, &[(&Row, Diff)], &mut Vec<(Row, Diff)>) + Send + Sync> = match reducer {
                        interactive::parse::Reducer::Min => Arc::new(|_key, vals, output| { if let Some(min) = vals.iter().map(|(v, _)| *v).min() { output.push((min.clone(), 1)); } }),
                        interactive::parse::Reducer::Distinct => Arc::new(|_key, _vals, output| { output.push((Row::new(), 1)); }),
                        interactive::parse::Reducer::Count => Arc::new(|_key, vals, output| { let count: Diff = vals.iter().map(|(_, d)| *d).sum(); if count > 0 { let mut r = Row::new(); r.push(count); output.push((r, 1)); } }),
                    };
                    let reduced = a.reduce_abelian::<_, ColValBuilder<_,_,_,_>, ColValSpine<_,_,_,_>, _>(
                        "Reduce",
                        move |k, vals, output| {
                            let k: Row = Columnar::into_owned(k);
                            let owned_vals: Vec<(Row, Diff)> = vals.iter().map(|(v, d)| {
                                (Columnar::into_owned(*v), *d)
                            }).collect();
                            let refs: Vec<(&Row, Diff)> = owned_vals.iter().map(|(v, d)| (v, *d)).collect();
                            f(&k, &refs, output);
                        },
                        |col, key, upds| {
                            use columnar::{Clear, Push};
                            col.keys.clear();
                            col.vals.clear();
                            col.times.clear();
                            col.diffs.clear();
                            for (val, time, diff) in upds.drain(..) { col.push((key, &val, &time, &diff)); }
                            // NOTE: required because push above doesn't group by key, val.
                            *col = std::mem::take(col).consolidate();
                        },
                    );
                    nodes.insert(id, Rendered::Arrangement(reduced));
                },
                Node::Variable => {
                    let step: Product<u64, PointStampSummary<u64>> = Product::new(0, feedback_summary::<u64>(level, 1));
                    let (var, col) = Variable::new(scope, step);
                    nodes.insert(id, Rendered::Collection(col));
                    variables.insert(id, (var, level));
                    var_levels.insert(id, level);
                },
                Node::Inspect { input, label } => {
                    let col = nodes[input].collection();
                    let label = label.clone();
                    nodes.insert(id, Rendered::Collection(col.inspect_container(move |event| {
                        if let Ok((_time, container)) = event {
                            for (k, v, t, d) in container.updates.iter() {
                                eprintln!("  [{}] ({:?}, {:?}, {:?}, {:?})", label, <Row as Columnar>::into_owned(k), <Row as Columnar>::into_owned(v), <Time as Columnar>::into_owned(t), <Diff as Columnar>::into_owned(d));
                            }
                        }
                    })));
                },
                Node::Leave(inner_id, scope_level) => {
                    let c = nodes[inner_id].collection();
                    nodes.insert(id, Rendered::Collection(super::columnar::leave_dynamic(c, *scope_level)));
                },
                Node::Scope => { level += 1; },
                Node::EndScope => { level -= 1; },
                Node::Bind { variable, value } => {
                    let c = nodes[value].collection();
                    let (var, _) = variables.remove(variable).expect("Bind: variable not found");
                    var.set(c);
                },
            }
        }

        nodes.into_iter().filter_map(|(id, r)| match r { Rendered::Collection(c) => Some((id, c)), _ => None }).collect()
    }
}

use differential_dataflow::dynamic::pointstamp::PointStamp;

type DdirOuterUpdate = (Row, Row, u64, Diff);

fn run(name: &str, stmts: Vec<parse::Stmt>, n_inputs: usize, nodes: u64, edges: u64, arity: usize, batch: u64, rounds: Option<u64>) {
    let mut compiled: Program = lower::lower(stmts);
    println!("{}: {} IR nodes (before optimize)", name, compiled.nodes.len());
    compiled.optimize();
    println!("{}: {} IR nodes (after optimize), result = {}", name, compiled.nodes.len(), compiled.result);
    compiled.dump();
    let name = name.to_string();
    let result_id = compiled.result;

    timely::execute_from_args(std::env::args().skip(4), move |worker| {
        use timely::dataflow::InputHandle;
        use timely::container::PushInto;
        use columnar_support::ValColBuilder;

        type OuterBuilder = ValColBuilder<DdirOuterUpdate>;

        let (mut inputs, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let mut handles = Vec::new();
            let mut collections = Vec::new();
            for _ in 0..n_inputs {
                let mut h = <InputHandle<_, OuterBuilder>>::new_with_builder();
                let stream = h.to_stream(scope);
                handles.push(h);
                collections.push(differential_dataflow::Collection::new(stream));
            }
            let mut probe = timely::dataflow::ProbeHandle::new();
            let output = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered: Vec<_> = collections.iter().map(|c| c.clone().enter(inner)).collect();
                let rendered = render::render_program(&compiled, inner, &entered);
                rendered[&result_id].clone().leave(scope)
            });
            output.probe_with(&mut probe);
            (handles, probe)
        });

        let index = worker.index();
        let peers = worker.peers();

        let mut builders: Vec<OuterBuilder> = (0..n_inputs).map(|_| OuterBuilder::default()).collect();

        for e in 0..edges {
            if (e as usize) % peers == index {
                let input_idx = (e as usize) % inputs.len();
                let (key, val) = interactive::gen_row::<Row>(e, nodes, arity);
                let time = *inputs[input_idx].time();
                builders[input_idx].push_into((key, val, time, 1i64));
            }
        }
        for (i, h) in inputs.iter_mut().enumerate() {
            use timely::container::ContainerBuilder;
            while let Some(container) = builders[i].finish() { h.send_batch(container); }
            h.advance_to(1);
            h.flush();
        }
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
                    let (key, val) = interactive::gen_row::<Row>(remove_idx, nodes, arity);
                    builders[input_idx].push_into((key, val, time, -1i64));
                }
                if (add_idx as usize) % peers == index {
                    let input_idx = (add_idx as usize) % inputs.len();
                    let (key, val) = interactive::gen_row::<Row>(add_idx, nodes, arity);
                    builders[input_idx].push_into((key, val, time, 1i64));
                }
                cursor += 1;
            }
            for (i, h) in inputs.iter_mut().enumerate() {
                use timely::container::ContainerBuilder;
                while let Some(container) = builders[i].finish() { h.send_batch(container); }
                h.advance_to(time);
                h.flush();
            }
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
