//! Tree-based DD IR with single-trim SCC and enter_at.
//! This is the version that previously worked — recreated as a standalone test.

use std::sync::Arc;

use timely::order::Product;
use timely::dataflow::Scope;

use differential_dataflow::VecCollection;
use differential_dataflow::input::Input;
use differential_dataflow::operators::iterate::VecVariable;
use differential_dataflow::dynamic::pointstamp::{PointStamp, PointStampSummary};
use differential_dataflow::dynamic::feedback_summary;

type Diff = i64;
type Time = Product<u64, PointStamp<u64>>;
type Row = Vec<u64>;
type Col<G> = VecCollection<G, (Row, Row), Diff>;

// ---- Tree IR ----

#[derive(Clone)]
enum Expr {
    Input(usize),
    Get(usize, usize), // (depth, index)
    Map {
        input: Box<Expr>,
        logic: Arc<dyn Fn((Row, Row)) -> Vec<((Row, Row), Time, Diff)> + Send + Sync>,
    },
    Concat(Vec<Expr>),
    Join {
        left: Box<Expr>,
        right: Box<Expr>,
        logic: Arc<dyn Fn(&Row, &Row, &Row) -> Vec<(Row, Row)> + Send + Sync>,
    },
    Reduce {
        input: Box<Expr>,
        logic: Arc<dyn Fn(&Row, &[(&Row, Diff)], &mut Vec<(Row, Diff)>) + Send + Sync>,
    },
    LetRec {
        inits: Vec<Option<Expr>>,
        bindings: Vec<Expr>,
        body: Box<Expr>,
    },
}

fn input(i: usize) -> Box<Expr> { Box::new(Expr::Input(i)) }
fn get(i: usize) -> Box<Expr> { Box::new(Expr::Get(0, i)) }
fn get_outer(depth: usize, i: usize) -> Box<Expr> { Box::new(Expr::Get(depth, i)) }

fn map(inp: Box<Expr>, logic: impl Fn(&Row, &Row) -> Vec<(Row, Row)> + Send + Sync + 'static) -> Box<Expr> {
    use timely::progress::Timestamp;
    Box::new(Expr::Map {
        input: inp,
        logic: Arc::new(move |(k, v)| {
            logic(&k, &v).into_iter().map(|d| (d, Time::minimum(), 1)).collect()
        }),
    })
}

fn map_raw(inp: Box<Expr>, logic: impl Fn((Row, Row)) -> Vec<((Row, Row), Time, Diff)> + Send + Sync + 'static) -> Box<Expr> {
    Box::new(Expr::Map { input: inp, logic: Arc::new(logic) })
}

fn concat(exprs: Vec<Box<Expr>>) -> Box<Expr> {
    Box::new(Expr::Concat(exprs.into_iter().map(|b| *b).collect()))
}

fn join(left: Box<Expr>, right: Box<Expr>, logic: impl Fn(&Row, &Row, &Row) -> Vec<(Row, Row)> + Send + Sync + 'static) -> Box<Expr> {
    Box::new(Expr::Join { left, right, logic: Arc::new(logic) })
}

fn reduce(inp: Box<Expr>, logic: impl Fn(&Row, &[(&Row, Diff)], &mut Vec<(Row, Diff)>) + Send + Sync + 'static) -> Box<Expr> {
    Box::new(Expr::Reduce { input: inp, logic: Arc::new(logic) })
}

fn let_rec_from(inits_and_bindings: Vec<(Option<Box<Expr>>, Box<Expr>)>, body: Box<Expr>) -> Box<Expr> {
    let (inits, bindings): (Vec<_>, Vec<_>) = inits_and_bindings.into_iter()
        .map(|(i, b)| (i.map(|e| *e), *b))
        .unzip();
    Box::new(Expr::LetRec { inits, bindings, body })
}

fn let_rec(bindings: Vec<Box<Expr>>, body: Box<Expr>) -> Box<Expr> {
    let n = bindings.len();
    Box::new(Expr::LetRec {
        inits: (0..n).map(|_| None).collect(),
        bindings: bindings.into_iter().map(|b| *b).collect(),
        body,
    })
}

// ---- Rendering ----

struct Context<'a, G: Scope> {
    inputs: &'a [Col<G>],
    scopes: Vec<&'a [Col<G>]>,
}

fn render<G>(
    expr: &Expr,
    scope: &mut G,
    ctx: &Context<G>,
    level: usize,
) -> Col<G>
where
    G: Scope<Timestamp = Time>,
{
    match expr {
        Expr::Input(i) => ctx.inputs[*i].clone(),
        Expr::Get(depth, index) => ctx.scopes[*depth][*index].clone(),

        Expr::Map { input, logic } => {
            let col = render(input, scope, ctx, level);
            let logic = Arc::clone(logic);
            col.join_function(move |kv| logic(kv))
        },

        Expr::Concat(exprs) => {
            let cols: Vec<_> = exprs.iter().map(|e| render(e, scope, ctx, level)).collect();
            let mut iter = cols.into_iter();
            let mut result = iter.next().expect("Concat needs at least one input");
            for other in iter {
                result = result.concat(other);
            }
            result
        },

        Expr::Join { left, right, logic } => {
            let left = render(left, scope, ctx, level).arrange_by_key();
            let right = render(right, scope, ctx, level).arrange_by_key();
            let logic = Arc::clone(logic);
            left.join_core(right, move |k, v1, v2| logic(k, v1, v2))
        },

        Expr::Reduce { input, logic } => {
            let col = render(input, scope, ctx, level);
            let logic = Arc::clone(logic);
            col.reduce(move |k, vals, output| logic(k, vals, output))
        },

        Expr::LetRec { inits, bindings, body } => {
            let new_level = level + 1;
            let step: Product<u64, PointStampSummary<u64>> =
                Product::new(0, feedback_summary::<u64>(new_level, 1));

            let mut vars = Vec::new();
            let mut cols = Vec::new();
            for init_expr in inits.iter() {
                if let Some(init) = init_expr {
                    let init_col = render(init, scope, ctx, level);
                    let (var, col) = VecVariable::new_from(init_col, step.clone());
                    vars.push(var);
                    cols.push(col);
                } else {
                    let (var, col) = VecVariable::new(scope, step.clone());
                    vars.push(var);
                    cols.push(col);
                }
            }

            let mut scopes = vec![cols.as_slice()];
            scopes.extend_from_slice(&ctx.scopes);
            let inner_ctx = Context {
                inputs: ctx.inputs,
                scopes,
            };

            let rendered: Vec<Col<G>> = bindings.iter()
                .map(|b| render(b, scope, &inner_ctx, new_level))
                .collect();

            for (var, col) in vars.into_iter().zip(rendered) {
                var.set(col);
            }

            render(body, scope, &inner_ctx, new_level)
                .leave_dynamic(new_level)
        },
    }
}

// ---- SCC program (single trim, with enter_at) ----

fn scc_program() -> Box<Expr> {

    fn propagate(cycle_ref: Box<Expr>, nodes_ref: Box<Expr>) -> Box<Expr> {
        let_rec(
            vec![
                reduce(
                    concat(vec![
                        join(get(0), cycle_ref, |_node, label, dst| vec![(dst.clone(), label.clone())]),
                        nodes_ref,
                    ]),
                    |_key, vals, output| {
                        if let Some(min_label) = vals.iter().map(|(v, _d)| *v).min() {
                            output.push((min_label.clone(), 1));
                        }
                    },
                ),
            ],
            get(0),
        )
    }

    fn trim(
        cycle_ref_inner: Box<Expr>,
        edge_ref: Box<Expr>,
    ) -> Box<Expr> {
        let mk_nodes = || map_raw(
            edge_ref.clone(),
            |(_src, dst): (Row, Row)| {
                let delay = 256 * (64 - dst.first().copied().unwrap_or(0).leading_zeros() as u64);
                let time = Product::new(0u64, PointStamp::new(smallvec::smallvec![0u64, delay]));
                vec![((dst.clone(), dst), time, 1)]
            },
        );

        let labels_for_src = propagate(cycle_ref_inner.clone(), mk_nodes());
        let labels_for_dst = propagate(cycle_ref_inner, mk_nodes());

        let edges_with_src_label = join(
            edge_ref, labels_for_src,
            |src, dst, label_src| {
                let mut val = src.clone();
                val.extend_from_slice(label_src);
                vec![(dst.clone(), val)]
            },
        );
        join(
            edges_with_src_label, labels_for_dst,
            |dst, src_and_label, label_dst| {
                let mid = src_and_label.len() / 2;
                let src = &src_and_label[..mid];
                let label_src = &src_and_label[mid..];
                if label_src == label_dst.as_slice() {
                    vec![(src.to_vec(), dst.clone())]
                } else {
                    vec![]
                }
            },
        )
    }

    let_rec_from(
        vec![
            (
                Some(input(0)),
                trim(get_outer(1, 0), input(0)),
            ),
        ],
        get(0),
    )
}

// ---- Main ----

fn row(vals: &[u64]) -> Row { vals.to_vec() }

fn main() {
    let n: usize = std::env::args().nth(1).unwrap_or("10".into()).parse().unwrap();
    let edges_count = 2 * n;

    let mut rng = n as u64;
    let lcg = |r: &mut u64| { *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); *r };
    let mut edge_list = Vec::new();
    for _ in 0..edges_count {
        let src = (lcg(&mut rng) >> 32) % (n as u64);
        let dst = (lcg(&mut rng) >> 32) % (n as u64);
        edge_list.push((src, dst));
    }

    timely::execute_from_args(std::env::args().skip(2), move |worker| {
        let (mut input_handle, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (handle, edges) = scope.new_collection::<(Row, Row), Diff>();
            let mut probe = timely::dataflow::ProbeHandle::new();

            let program = scc_program();

            let output = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered = vec![edges.enter(inner)];
                let ctx = Context { inputs: &entered, scopes: vec![] };
                render(&program, inner, &ctx, 0).leave()
            });

            output
                .inspect(|x| println!("  scc: {:?}", x))
                .probe_with(&mut probe);

            (handle, probe)
        });

        for (src, dst) in &edge_list {
            input_handle.update((row(&[*src]), row(&[*dst])), 1);
        }
        input_handle.advance_to(1);
        input_handle.flush();
        while probe.less_than(&1u64) {
            worker.step();
        }
        println!("worker {}: done (n={})", worker.index(), n);
    }).unwrap();
}
