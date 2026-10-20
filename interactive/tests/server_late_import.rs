//! A program installed late reads the published traces it imports as of the time it starts
//! (`Server::import_as_of`), the traces' compaction frontier: every update's time advanced to it.
//! Before that frontier a compacted trace's batches can hold counts no time ever had (a retraction
//! advanced past the frontier, the insertion it retracts left behind), and an iterative scope
//! reading them there need not converge. Imports read as containers, as rows of arrangements, and
//! as arrangements read directly by a join, all from the frontier on. (Found by `difftest`.)

use std::sync::mpsc;
use std::time::Duration;

use interactive::server::Server;
use interactive::{lower, parse};

const PRODUCER: &str = r#"
    export "seeds" = input 0 : ((int, int) ; ());
    export "edges" = input 1 : ((int, int) ; ());
"#;

/// A row `(a, b)` of `x` meets an equal row of `edges` and steps to `(b % 5, b)`: `(5, 5)` steps
/// to `(0, 5)`, which steps to itself while `edges` holds `(0, 5)`.
const CONSUMER: &str = r#"
    let seeds = import "seeds";
    let edges = import "edges";
    s: {
        let step = x | join(edges, ((_, b), (), ()) -> (((b, 5) rem, b), ()));
        var x = (seeds + step) | distinct;
    }
    export "result" = s::x;
"#;

/// The feeds, per epoch, as `(input, rows)`: `(0, 5)` is added at epoch 2 and retracted at 14, and
/// enough else changes that the edges trace merges its batches unevenly.
const FEEDS: &[(u64, usize, &str)] = &[
    (0, 1, "0,0\n0,3 diff=2"),
    (1, 1, "-1,4 diff=2"),
    (2, 1, "1,2\n0,5"),
    (3, 1, "4,3\n3,0\n0,4"),
    (4, 1, "4,3 diff=2\n1,5"),
    (5, 0, "5,5"),
    (7, 1, "1,2\n1,5 diff=-1\n4,3 diff=-1"),
    (8, 1, "1,2 diff=-1\n4,3 diff=-1"),
    (10, 1, "5,3\n0,3 diff=-1"),
    (11, 1, "2,3\n4,4\n-1,-1\n1,2 diff=-1"),
    (14, 1, "5,5\n0,5 diff=-1"),
    (21, 0, "-1,2"),
];

/// The consumer's rows after the last epoch, installed before epoch `late`, with imports as
/// containers or as arrangements.
fn run(late: u64, arranged: bool) -> Vec<(interactive::ir::Value, interactive::ir::Value, i64)> {
    timely::execute_directly(move |worker| {
        let program = |src: &str| { let mut p = lower::lower_tree(parse::pipe::parse(src)); p.optimize(); p };
        let mut server = Server::new();
        server.set_arranged_imports(arranged);
        server.install(worker, "producer", &program(PRODUCER)).unwrap();
        for epoch in 0..22 {
            if epoch == late { server.install(worker, "consumer", &program(CONSUMER)).unwrap(); }
            for (_, input, rows) in FEEDS.iter().filter(|(e, _, _)| *e == epoch) {
                server.feed("producer", *input, rows, None).unwrap();
            }
            server.tick(worker);
        }
        server.snapshot(worker, "result").unwrap().into_rows()
    })
}

#[test]
fn late_importers_converge() {
    // Without the fix, the late cases never finish their first tick: fail rather than hang.
    let (send, recv) = mpsc::channel();
    std::thread::spawn(move || {
        let early = run(0, false);
        let late: Vec<_> = [(17, false), (21, false), (17, true), (21, true)].into_iter().map(|(at, arranged)| run(at, arranged)).collect();
        send.send((early, late)).unwrap();
    });
    let (early, late) = recv.recv_timeout(Duration::from_secs(60)).expect("a late importer did not converge");
    assert_eq!(early.len(), 3, "(5, 5), (0, 5) and (-1, 2): {early:?}");
    for rows in late {
        assert_eq!(rows, early);
    }
}

/// As above, with imports read as arrangements and operators reading them directly. `r` joins the
/// imported `pairs` with itself: read from before the compaction frontier, the join met the two
/// histories at their stored times and gave `r` rows there, and the loop then joined it with the
/// imported `weights` where that history holds counts no time had.
const ARRANGED_PRODUCER: &str = r#"
    export "pairs" = input 0 : ((int, int) ; ());
    export "weights" = input 5 : (int ; float);
"#;

const ARRANGED_CONSUMER: &str = r#"
    let pairs = import "pairs";
    let weights = import "weights";
    let r = pairs | join(pairs, ((a, b), (), _) -> ((b, (a, b) min) eq, 0)) | distinct;
    s: {
        let step = x | join(weights, (k, v, _) -> (k, v));
        var x = (r + step) | min;
    }
    export "result" = s::x;
"#;

const ARRANGED_FEEDS: &[(u64, usize, &str)] = &[
    (0, 0, "1,0 diff=1"),
    (0, 5, "-1 val=-0.0 diff=2\n5 val=-0.0 diff=1\n3 val=-1.5 diff=1\n4 val=-1.5 diff=1\n3 val=1.5 diff=2"),
    (2, 5, "1 val=1e300 diff=1\n0 val=-1.5 diff=1\n4 val=-0.0 diff=1\n3 val=4.0 diff=1"),
    (4, 5, "5 val=2.5 diff=1"),
    (5, 5, "3 val=1e300 diff=1\n-1 val=-1e-300 diff=1\n3 val=0.1 diff=1"),
    (6, 5, "1 val=4.0 diff=2\n0 val=-1e-300 diff=2\n-1 val=1.5 diff=1\n5 val=0.1 diff=1"),
    (7, 5, "1 val=-1e-300 diff=2\n4 val=0.0 diff=1\n2 val=0.0 diff=1\n0 val=0.0 diff=1\n1 val=4.0 diff=-1"),
    (9, 5, "4 val=2.5 diff=1\n5 val=2.0 diff=1\n4 val=0.0 diff=1"),
    (12, 5, "1 val=1e300 diff=1\n0 val=-1e-300 diff=-1"),
    (14, 5, "2 val=1.5 diff=1\n-1 val=1.5 diff=-1\n4 val=0.0 diff=-1"),
    (16, 5, "4 val=0.1 diff=1\n4 val=-1e-300 diff=1"),
    (18, 5, "2 val=0.1 diff=1\n3 val=-3.25 diff=2\n-1 val=0.0 diff=1\n3 val=0.0 diff=1\n0 val=1.5 diff=1"),
    (20, 5, "3 val=1.5 diff=1\n3 val=0.0 diff=1\n0 val=1.5 diff=1"),
    (22, 5, "2 val=0.1 diff=1\n2 val=-3.25 diff=2\n1 val=2.0 diff=1"),
    (23, 5, "5 val=-0.0 diff=2\n0 val=0.1 diff=1\n4 val=0.1 diff=1\n1 val=-1e-300 diff=-1\n5 val=0.1 diff=-1"),
    (25, 5, "1 val=2.0 diff=1"),
];

#[test]
fn late_arranged_importers_converge() {
    let (send, recv) = mpsc::channel();
    std::thread::spawn(move || {
        let rows = timely::execute_directly(move |worker| {
            let program = |src: &str| { let mut p = lower::lower_tree(parse::pipe::parse(src)); p.optimize(); p };
            let mut server = Server::new();
            server.set_arranged_imports(true);
            server.install(worker, "producer", &program(ARRANGED_PRODUCER)).unwrap();
            for epoch in 0..27 {
                if epoch == 25 { server.install(worker, "consumer", &program(ARRANGED_CONSUMER)).unwrap(); }
                for (_, input, rows) in ARRANGED_FEEDS.iter().filter(|(e, _, _)| *e == epoch) {
                    server.feed("producer", *input, rows, None).unwrap();
                }
                server.tick(worker);
            }
            server.snapshot(worker, "result").unwrap().into_rows()
        });
        send.send(rows).unwrap();
    });
    let rows = recv.recv_timeout(Duration::from_secs(30)).expect("the late importer did not converge");
    assert_eq!(rows.len(), 1, "key 1, at the least value 0: {rows:?}");
}
