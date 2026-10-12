//! An arrangement imported into a child scope is the parent's trace, not a copy.
//!
//! Each program runs twice: as written, where children arrange their imports themselves, and
//! with the parent arranging them, so children read the parent's trace. The answers must agree
//! at every epoch while edges come and go, on both backends and at several workers. The loops
//! read the shared traces at iteration times, so this also checks that their compaction
//! requests are sound for the parent's arrangement.
use interactive::ir::Value;
use interactive::scope_ir::{Item, Node, Program, Ref, Scope, Source};
use interactive::server::{InputUpdate, RenderBackend, Server};
use interactive::{lower, parse};

fn program(src: &str) -> Program {
    let mut p = lower::lower_tree(parse::pipe::parse(src));
    p.optimize();
    p
}

/// The number of child-scope imports, anywhere in the tree, whose parent item is an arrangement.
fn arranged_imports(s: &Scope) -> usize {
    s.items.iter().map(|item| match item {
        Item::Op(_) => 0,
        Item::Sub(c) => {
            let here = c.imports.iter().filter(|imp| matches!(&imp.from,
                Source::Parent(Ref::Local(i)) if matches!(&s.items[*i], Item::Op(Node::Arrange(_) | Node::Reduce { .. }))
            )).count();
            here + arranged_imports(c)
        }
    }).sum()
}

/// Edge changes per epoch: a few insertions and a few deletions of earlier insertions.
fn churn(epochs: usize) -> Vec<Vec<InputUpdate>> {
    let mut state = 0x2545F4914F6CDD1Du64;
    let mut next = move |n: u64| { state ^= state << 13; state ^= state >> 7; state ^= state << 17; (state % n) as i64 };
    let mut live: Vec<(i64, i64)> = Vec::new();
    let edge = |(s, d): (i64, i64), diff| InputUpdate { key: Value::Tuple(vec![Value::Int(s), Value::Int(d)]), val: Value::unit(), diff };
    (0..epochs).map(|epoch| {
        let mut batch = Vec::new();
        for _ in 0..if epoch == 0 { 30 } else { 4 } {
            let e = (next(16), next(16));
            live.push(e);
            batch.push(edge(e, 1));
        }
        for _ in 0..if epoch == 0 { 0 } else { 3 } {
            let e = live.swap_remove(next(live.len() as u64) as usize);
            batch.push(edge(e, -1));
        }
        batch
    }).collect()
}

/// The export `result` after each epoch, read at worker 0.
fn run(backend: RenderBackend, workers: usize, program: &Program, epochs: &[Vec<InputUpdate>]) -> Vec<Vec<(Value, Value, i64)>> {
    let (program, epochs) = (program.clone(), epochs.to_vec());
    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let mut server = Server::with_backend(backend);
        server.install(worker, "p", &program).unwrap();
        let mut seen = Vec::new();
        for batch in epochs.iter() {
            let mine = if worker.index() == 0 { batch.clone() } else { Vec::new() };
            server.feed_batch("p", 0, mine).unwrap();
            server.tick(worker);
            let mut rows = server.snapshot(worker, "result").unwrap();
            rows.sort();
            seen.push(rows);
        }
        seen
    }).unwrap();
    guards.join().into_iter().next().unwrap().unwrap()
}

fn assert_sharing_agrees(written: &str, shared: &str) {
    let (written, shared) = (program(written), program(shared));
    assert_eq!(arranged_imports(&written.root), 0);
    assert!(arranged_imports(&shared.root) > 0, "the shared variant imports no arrangement");
    let epochs = churn(8);
    let want = run(RenderBackend::Vec, 1, &written, &epochs);
    assert!(want.iter().any(|rows| !rows.is_empty()));
    for backend in [RenderBackend::Vec, RenderBackend::Corgi] {
        for workers in [1, 3] {
            assert_eq!(run(backend, workers, &shared, &epochs), want, "{backend:?} at {workers} worker(s)");
        }
    }
}

const REACH: &str = r#"
    let edges = input 0 : ((int, int) ; ()) | key($0[0] ; $0[1]) EDGES;
    let roots = edges | key($0[0] ;) | distinct | filter($0[0] < 3);
    reach: {
        let proposals = reach | join(edges, ($2 ;));
        var reach = roots + proposals | distinct;
    }
    export "result" = reach::reach;
"#;

#[test]
fn reach() {
    assert_sharing_agrees(&REACH.replace(" EDGES", ""), &REACH.replace(" EDGES", " | arrange"));
}

#[test]
fn scc() {
    let src = interactive::load_program(&format!("{}/examples/programs/scc.ddp", env!("CARGO_MANIFEST_DIR")));
    let src = src.replace(r#"export "result" = outer::scc | map(;) | arrange | inspect(total);"#, r#"export "result" = outer::scc;"#);
    let shared = src
        .replace("| key($0[0] ; $0[1]);", "| key($0[0] ; $0[1]) | arrange;")
        .replace("let trans = edges | key($1 ; $0);", "let trans = edges | key($1 ; $0) | arrange;")
        .replace("let scc = edges + trim;", "let scc = edges + trim | arrange;");
    assert_ne!(src, shared);
    assert_sharing_agrees(&src, &shared);
}
