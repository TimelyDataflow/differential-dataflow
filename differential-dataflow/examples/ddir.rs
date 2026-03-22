//! A small IR for differential dataflow computations.
//!
//! DAG-based representation with globally unique IDs. All names are in scope
//! once introduced. Scopes are ambient (push/pop). LEAVE projects bindings
//! out of a scope. Dead store elimination removes unused computations.
//!
//! Rendering uses a single `Product<u64, PointStamp<u64>>` scope — all iteration
//! is encoded dynamically in the PointStamp coordinates.

use std::sync::Arc;
use smallvec::smallvec as svec;  // short alias for closure returns
use std::collections::HashMap;

use timely::order::Product;
use timely::dataflow::Scope;

use differential_dataflow::VecCollection;
use differential_dataflow::input::Input;
use differential_dataflow::operators::iterate::VecVariable;
use differential_dataflow::dynamic::pointstamp::{PointStamp, PointStampSummary};
use differential_dataflow::dynamic::feedback_summary;

/// The diff type. Currently `i64`; intended to become arbitrary-precision.
pub type Diff = i64;

/// The timestamp used for all scopes.
type Time = Product<u64, PointStamp<u64>>;

/// A collection of `((R, R), Time, Diff)` triples.
type Col<G, R> = VecCollection<G, (R, R), Diff>;

/// Node identifier — index into the program's node list.
type Id = usize;

/// A node in the IR. Nodes that produce collections get an implicit ID (their index).
/// Structural nodes (Scope, EndScope, Bind) do not produce collections.
pub enum Node<R> {
    /// An external input collection.
    Input(usize),
    /// `join_function`: `(key, val) → [(new_kv, time, diff)]`.
    /// Times are joined, diffs are multiplied. Subsumes map/filter/negate/delay.
    Map {
        input: Id,
        logic: Arc<dyn Fn((R, R)) -> smallvec::SmallVec<[((R, R), Time, Diff); 1]> + Send + Sync>,
    },
    /// Concatenate collections.
    Concat(Vec<Id>),
    /// Arrange a collection by key. Collection → Arrangement.
    Arrange(Id),
    /// Join two arrangements on key. Arrangement × Arrangement → Collection.
    /// `(key, val_left, val_right) → [(new_key, new_val)]`
    Join {
        left: Id,
        right: Id,
        logic: Arc<dyn Fn(&R, &R, &R) -> smallvec::SmallVec<[(R, R); 1]> + Send + Sync>,
    },
    /// Reduce an arrangement by key. Arrangement → Arrangement.
    /// `(key, &[(val, diff)], &mut Vec<(new_val, new_diff)>)`
    Reduce {
        input: Id,
        logic: Arc<dyn Fn(&R, &[(&R, Diff)], &mut Vec<(R, Diff)>) + Send + Sync>,
    },
    /// Feedback variable in the ambient scope. Optionally initialized.
    Variable { init: Option<Id> },
    /// Project a binding out of its scope (truncate timestamps).
    Leave(Id),
    /// Inspect: print records with a label, pass through unchanged.
    Inspect { input: Id, label: &'static str },

    // --- Structural (no collection ID produced) ---

    /// Push a new scope level (iteration boundary).
    Scope,
    /// Pop the current scope level.
    EndScope,
    /// Bind a variable to its definition (connects feedback).
    Bind { variable: Id, value: Id },
}

/// A program is a list of nodes.
pub struct Program<R> {
    pub nodes: Vec<Node<R>>,
}

// ---- Builder API ----

/// Builder for constructing programs.
pub struct Builder<R> {
    nodes: Vec<Node<R>>,
}

impl<R: 'static> Builder<R> {
    pub fn new() -> Self { Builder { nodes: Vec::new() } }

    fn push(&mut self, node: Node<R>) -> Id {
        let id = self.nodes.len();
        self.nodes.push(node);
        id
    }

    pub fn input(&mut self, index: usize) -> Id { self.push(Node::Input(index)) }

    pub fn map(&mut self, input: Id, logic: impl Fn(&R, &R) -> smallvec::SmallVec<[(R, R); 1]> + Send + Sync + 'static) -> Id {
        use timely::progress::Timestamp;
        self.push(Node::Map {
            input,
            logic: Arc::new(move |(k, v)| {
                logic(&k, &v).into_iter().map(|d| (d, Time::minimum(), 1)).collect()
            }),
        })
    }

    pub fn map_raw(&mut self, input: Id, logic: impl Fn((R, R)) -> smallvec::SmallVec<[((R, R), Time, Diff); 1]> + Send + Sync + 'static) -> Id {
        self.push(Node::Map { input, logic: Arc::new(logic) })
    }

    pub fn concat(&mut self, inputs: Vec<Id>) -> Id { self.push(Node::Concat(inputs)) }

    pub fn arrange(&mut self, input: Id) -> Id { self.push(Node::Arrange(input)) }

    pub fn join(&mut self, left: Id, right: Id, logic: impl Fn(&R, &R, &R) -> smallvec::SmallVec<[(R, R); 1]> + Send + Sync + 'static) -> Id {
        self.push(Node::Join { left, right, logic: Arc::new(logic) })
    }

    pub fn reduce(&mut self, input: Id, logic: impl Fn(&R, &[(&R, Diff)], &mut Vec<(R, Diff)>) + Send + Sync + 'static) -> Id {
        self.push(Node::Reduce { input, logic: Arc::new(logic) })
    }

    pub fn variable(&mut self, init: Option<Id>) -> Id { self.push(Node::Variable { init }) }
    pub fn leave(&mut self, inner: Id) -> Id { self.push(Node::Leave(inner)) }

    #[allow(dead_code)]
    pub fn inspect(&mut self, input: Id, label: &'static str) -> Id {
        self.push(Node::Inspect { input, label })
    }

    pub fn scope(&mut self) { self.nodes.push(Node::Scope); }
    pub fn end_scope(&mut self) { self.nodes.push(Node::EndScope); }
    pub fn bind(&mut self, variable: Id, value: Id) { self.nodes.push(Node::Bind { variable, value }); }

    pub fn build(self) -> Program<R> { Program { nodes: self.nodes } }
}

// ---- Rendering ----

use differential_dataflow::trace::implementations::ValSpine;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
type Arr<G, R> = Arranged<G, TraceAgent<ValSpine<R, R, <G as timely::dataflow::scopes::ScopeParent>::Timestamp, Diff>>>;

enum Rendered<G: Scope<Timestamp: differential_dataflow::lattice::Lattice>, R: differential_dataflow::Data + Ord> {
    Collection(Col<G, R>),
    Arrangement(Arr<G, R>),
}

impl<G: Scope<Timestamp: differential_dataflow::lattice::Lattice>, R: differential_dataflow::ExchangeData + Ord> Rendered<G, R> {
    fn collection(&self) -> Col<G, R> {
        match self {
            Rendered::Collection(c) => c.clone(),
            Rendered::Arrangement(a) => a.clone().as_collection(|k, v| (k.clone(), v.clone())),
        }
    }
    fn arrangement(&self) -> Arr<G, R> {
        match self {
            Rendered::Arrangement(a) => a.clone(),
            Rendered::Collection(_) => panic!("Expected arrangement, got collection"),
        }
    }
}

fn render_program<G, R>(
    program: &Program<R>,
    scope: &mut G,
    inputs: &[Col<G, R>],
) -> HashMap<Id, Rendered<G, R>>
where
    G: Scope<Timestamp = Time>,
    R: differential_dataflow::ExchangeData + Ord + Default + std::hash::Hash,
{
    let mut nodes: HashMap<Id, Rendered<G, R>> = HashMap::new();
    let mut level: usize = 0;
    let mut variables: HashMap<Id, (VecVariable<G, (R, R), Diff>, usize)> = HashMap::new();

    for (id, node) in program.nodes.iter().enumerate() {
        match node {
            Node::Input(i) => {
                nodes.insert(id, Rendered::Collection(inputs[*i].clone()));
            },
            Node::Map { input, logic } => {
                let col = nodes[input].collection();
                let logic = Arc::clone(logic);
                nodes.insert(id, Rendered::Collection(col.join_function(move |kv| logic(kv))));
            },
            Node::Concat(ids) => {
                let mut iter = ids.iter();
                let mut result = nodes[iter.next().unwrap()].collection();
                for other_id in iter {
                    result = result.concat(nodes[other_id].collection());
                }
                nodes.insert(id, Rendered::Collection(result));
            },
            Node::Arrange(input) => {
                let col = nodes[input].collection();
                nodes.insert(id, Rendered::Arrangement(col.arrange_by_key()));
            },
            Node::Join { left, right, logic } => {
                let left = nodes[left].arrangement();
                let right = nodes[right].arrangement();
                let logic = Arc::clone(logic);
                nodes.insert(id, Rendered::Collection(left.join_core(right, move |k, v1, v2| logic(k, v1, v2))));
            },
            Node::Reduce { input, logic } => {
                let arr = nodes[input].arrangement();
                let logic = Arc::clone(logic);
                let reduced = arr.reduce_abelian::<_, differential_dataflow::trace::implementations::ValBuilder<_,_,_,_>, ValSpine<_,_,_,_>>(
                    "Reduce",
                    move |k, vals, output| logic(k, vals, output),
                );
                nodes.insert(id, Rendered::Arrangement(reduced));
            },
            Node::Variable { init } => {
                let step: Product<u64, PointStampSummary<u64>> =
                    Product::new(0, feedback_summary::<u64>(level, 1));

                if let Some(init_id) = init {
                    let init_col = nodes[init_id].collection();
                    let (var, col) = VecVariable::new_from(init_col, step);
                    nodes.insert(id, Rendered::Collection(col));
                    variables.insert(id, (var, level));
                } else {
                    let (var, col) = VecVariable::new(scope, step);
                    nodes.insert(id, Rendered::Collection(col));
                    variables.insert(id, (var, level));
                }
            },
            Node::Leave(inner_id) => {
                let col = nodes[inner_id].collection();
                let var_level = variables.get(inner_id)
                    .map(|(_, lvl)| *lvl)
                    .unwrap_or(level);
                nodes.insert(id, Rendered::Collection(col.leave_dynamic(var_level)));
            },
            Node::Inspect { input, label } => {
                let col = nodes[input].collection();
                let label = *label;
                nodes.insert(id, Rendered::Collection(col.inspect(move |x| eprintln!("  [{}] {:?}", label, x))));
            },
            Node::Scope => {
                level += 1;
            },
            Node::EndScope => {
                level -= 1;
            },
            Node::Bind { variable, value } => {
                let col = nodes[value].collection();
                let (var, _lvl) = variables.remove(variable)
                    .expect("Bind: variable not found");
                var.set(col);
            },
        }
    }

    nodes
}

// ---- Row type ----
type Row = smallvec::SmallVec<[i64; 2]>;
fn row(vals: &[i64]) -> Row { vals.into() }

// ---- Helper: run a program ----

fn run_program(
    name: &'static str,
    program: Program<Row>,
    n_inputs: usize,
    output_id: Id,
    setup: impl Fn(usize, &mut Vec<differential_dataflow::input::InputSession<u64, (Row, Row), Diff>>) + Send + Sync + 'static,
) {
    timely::execute_from_args(std::env::args().skip(3), move |worker| {
        let (mut inputs, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let mut handles = Vec::new();
            let mut collections = Vec::new();
            for _ in 0..n_inputs {
                let (handle, col) = scope.new_collection::<(Row, Row), Diff>();
                handles.push(handle);
                collections.push(col);
            }

            let mut probe = timely::dataflow::ProbeHandle::new();

            let output = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered: Vec<_> = collections.iter().map(|c| c.clone().enter(inner)).collect();
                let rendered = render_program(&program, inner, &entered);
                rendered[&output_id].collection().leave()
            });
            if std::env::var("DDIR_PRINT").is_ok() {
                output.inspect(|x| println!("{:?}", x)).probe_with(&mut probe);
            } else {
                output.probe_with(&mut probe);
            }

            (handles, probe)
        });

        setup(worker.index(), &mut inputs);

        for input in inputs.iter_mut() {
            input.advance_to(1);
            input.flush();
        }
        while probe.less_than(&1u64) {
            worker.step();
        }

        println!("worker {}: {} complete", worker.index(), name);
    }).unwrap();
}

// ---- Programs ----

fn reachability_program() -> (Program<Row>, Id) {
    let mut b = Builder::new();

    let edges = b.input(0);   // ([src], [dst])
    let roots = b.input(1);   // ([node], [])

    let edges_arr = b.arrange(edges);

    b.scope();
    let reach = b.variable(None);
    let reach_arr = b.arrange(reach);
    let proposals = b.join(reach_arr, edges_arr, |_src: &Row, _unit: &Row, dst: &Row| svec![(dst.clone(), Row::new())]);
    let combined = b.concat(vec![roots, proposals]);
    b.bind(reach, combined);
    let result = b.leave(reach);
    b.end_scope();

    (b.build(), result)
}

fn scc_program() -> (Program<Row>, Id) {
    let mut b = Builder::new();

    let edges = b.input(0);
    let edges_arr = b.arrange(edges);
    let trans = b.map(edges, |src: &Row, dst: &Row| svec![(dst.clone(), src.clone())]);
    let trans_arr = b.arrange(trans);

    // Outer SCC iteration
    b.scope();
    let scc = b.variable(Some(edges));
    let scc_arr = b.arrange(scc);

    // Forward propagate
    let fwd_labels_arr = {
        let nodes = b.map_raw(edges, |(_src, dst): (Row, Row)| {
            let delay = 256 * (64 - dst.first().copied().unwrap_or(0).leading_zeros() as u64);
            let time = Product::new(0u64, PointStamp::new(smallvec::smallvec![0u64, delay]));
            svec![((dst.clone(), dst), time, 1)]
        });
        b.scope();
        let labels = b.variable(None);
        let labels_arr = b.arrange(labels);
        let proposals = b.join(labels_arr, scc_arr, |_node, label, dst| svec![(dst.clone(), label.clone())]);
        let combined = b.concat(vec![proposals, nodes]);
        let combined_arr = b.arrange(combined);
        let reduced = b.reduce(combined_arr, |_key, vals, output| {
            if let Some(min_label) = vals.iter().map(|(v, _d)| *v).min() {
                output.push((min_label.clone(), 1));
            }
        });
        // reduced is already an arrangement (reduce produces arrangement)
        b.bind(labels, reduced);
        let out = b.leave(labels);
        b.end_scope();
        // Arrange the left labels for reuse in both trim joins
        b.arrange(out)
    };

    // Forward trim: filter `edges` by matching labels, output reversed.
    let trim_fwd = {
        let with_src = b.join(edges_arr, fwd_labels_arr, |src, dst, label_src| {
            let mut val = src.clone();
            val.extend_from_slice(label_src);
            svec![(dst.clone(), val)]
        });
        let with_src_arr = b.arrange(with_src);
        b.join(with_src_arr, fwd_labels_arr, |dst, src_and_label, label_dst| {
            let mid = src_and_label.len() / 2;
            let src = &src_and_label[..mid];
            let label_src = &src_and_label[mid..];
            if label_src == label_dst.as_slice() {
                svec![(dst.clone(), Row::from(src))]
            } else {
                svec![]
            }
        })
    };
    let trim_fwd_arr = b.arrange(trim_fwd);

    // Backward propagate: propagate labels through trim_fwd (reversed forward-trimmed).
    let bwd_labels_arr = {
        let nodes = b.map_raw(trans, |(_src, dst): (Row, Row)| {
            let delay = 256 * (64 - dst.first().copied().unwrap_or(0).leading_zeros() as u64);
            let time = Product::new(0u64, PointStamp::new(smallvec::smallvec![0u64, delay]));
            svec![((dst.clone(), dst), time, 1)]
        });
        b.scope();
        let labels = b.variable(None);
        let labels_arr = b.arrange(labels);
        let proposals = b.join(labels_arr, trim_fwd_arr, |_node, label, dst| svec![(dst.clone(), label.clone())]);
        let combined = b.concat(vec![proposals, nodes]);
        let combined_arr = b.arrange(combined);
        let reduced = b.reduce(combined_arr, |_key, vals, output| {
            if let Some(min_label) = vals.iter().map(|(v, _d)| *v).min() {
                output.push((min_label.clone(), 1));
            }
        });
        b.bind(labels, reduced);
        let out = b.leave(labels);
        b.end_scope();
        b.arrange(out)
    };

    // Backward trim: filter `trans` by matching labels, output reversed.
    let trim_bwd = {
        let with_src = b.join(trans_arr, bwd_labels_arr, |src, dst, label_src| {
            let mut val = src.clone();
            val.extend_from_slice(label_src);
            svec![(dst.clone(), val)]
        });
        let with_src_arr = b.arrange(with_src);
        b.join(with_src_arr, bwd_labels_arr, |dst, src_and_label, label_dst| {
            let mid = src_and_label.len() / 2;
            let src = &src_and_label[..mid];
            let label_src = &src_and_label[mid..];
            if label_src == label_dst.as_slice() {
                svec![(dst.clone(), Row::from(src))]
            } else {
                svec![]
            }
        })
    };

    b.bind(scc, trim_bwd);
    let result = b.leave(scc);
    b.end_scope();

    (b.build(), result)
}

// ---- Main ----

fn main() {
    let example = std::env::args().nth(1).unwrap_or("reach".into());
    let n: usize = std::env::args().nth(2).unwrap_or("10".into()).parse().unwrap();

    match example.as_str() {
        "reach" => {
            println!("Reachability: chain of {} nodes, root at 0", n);
            let (program, output) = reachability_program();
            run_program("reachable", program, 2, output, move |_worker, inputs| {
                for i in 0..n.saturating_sub(1) {
                    inputs[0].update((row(&[i as i64]), row(&[(i + 1) as i64])), 1);
                }
                inputs[1].update((row(&[0]), row(&[])), 1);
            });
        },
        "scc" => {
            let edges = 2 * n;
            println!("SCC: {} nodes, {} random edges", n, edges);
            let (program, output) = scc_program();
            run_program("scc_edge", program, 1, output, move |worker, inputs| {
                if worker == 0 {
                    let mut rng = n as u64;
                    let lcg = |r: &mut u64| { *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); *r };
                    for _ in 0..edges {
                        let src = (lcg(&mut rng) >> 32) % (n as u64);
                        let dst = (lcg(&mut rng) >> 32) % (n as u64);
                        inputs[0].update((row(&[src as i64]), row(&[dst as i64])), 1);
                    }
                }
            });
        },
        "scc-cycle" => {
            println!("SCC: {}-cycle (all edges in one SCC)", n);
            let (program, output) = scc_program();
            run_program("scc_edge", program, 1, output, move |worker, inputs| {
                if worker == 0 {
                    for i in 0..n {
                        inputs[0].update((row(&[i as i64]), row(&[((i + 1) % n) as i64])), 1);
                    }
                }
            });
        },
        "scc-chain" => {
            println!("SCC: {}-cycle + 1 tail", n);
            let (program, output) = scc_program();
            run_program("scc_edge", program, 1, output, move |worker, inputs| {
                if worker == 0 {
                    for i in 0..n {
                        inputs[0].update((row(&[i as i64]), row(&[((i + 1) % n) as i64])), 1);
                    }
                    inputs[0].update((row(&[n as i64]), row(&[0])), 1);
                }
            });
        },
        _ => println!("Usage: ddir [reach|scc|scc-cycle|scc-chain] [nodes]"),
    }
}
