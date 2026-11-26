//! `semijoin` and `antijoin`: the rows of `left` whose key is, or is not, a key of `right`. Each row of
//! `left` is kept or dropped once however many rows of `right` share its key, so the two always split
//! `left` exactly, where `left - (left | join(right, …))` subtracts a row once per matching row of `right`.

use interactive::server::host_time;
use timely::progress::Antichain;
use interactive::server::Server;
use interactive::{lower, parse};

const PROGRAM: &str = r#"
    let a = input 0 : ((int, int), ()) | map(((k, x), ()) -> (k, x));
    let b = input 1 : ((int, int), ()) | map(((k, y), ()) -> (k, y));

    output "semi" = a | semijoin(b);
    output "anti" = a | antijoin(b);
    output "split" = (a | semijoin(b)) + (a | antijoin(b));
    output "a" = a;

    -- The same filters written out, with the right side's keys made distinct first.
    let keys = b | map((k, _) -> (k, ())) | distinct;
    let matched = a | join(keys, (k, x, ()) -> (k, x));
    output "semi_ref" = matched;
    output "anti_ref" = a - matched;
    -- Without the distinct, a row of `a` is subtracted once per row of `b` with its key.
    output "anti_naive" = a - (a | join(b, (k, x, _) -> (k, x)));

    -- Inside an iterative scope: nodes reachable from `a`'s keys along `b`'s edges, but never
    -- through a node that is a key of `blocked`.
    let blocked = a | filter((k, _) -> (k, 3) eq);
    walk: {
        let step = reach | join(b, (_, (), dst) -> (dst, ())) | antijoin(blocked);
        set reach = (a | map((k, _) -> (k, ())) | antijoin(blocked)) + step | distinct;
    }
    output "reach" = walk::reach;
    walk_ref: {
        let open = (reach | join(b, (_, (), dst) -> (dst, ()))) + (a | map((k, _) -> (k, ())));
        let bad = blocked | map((k, _) -> (k, ())) | distinct;
        set reach = open - (open | join(bad, (k, (), ()) -> (k, ()))) | distinct;
    }
    output "reach_ref" = walk_ref::reach;
"#;

const TICKS: u64 = 24;
const OUTPUTS: [&str; 9] = ["semi", "anti", "split", "a", "semi_ref", "anti_ref", "reach", "reach_ref", "anti_naive"];

/// The rows fed at each tick, as `(input, key, value, diff)`: pseudo-random rows of small keys, so
/// that `b` often holds a key several times, each added and, a few ticks later, retracted.
fn feeds(tick: u64) -> Vec<(usize, i64, i64, i64)> {
    let row = |n: u64| {
        let h = n.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(17).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        ((h % 2) as usize, ((h >> 8) % 5) as i64, ((h >> 16) % 5) as i64)
    };
    let mut rows = Vec::new();
    for i in 0..5 {
        let (input, k, v) = row(5 * tick + i);
        rows.push((input, k, v, 1));
        if tick >= 4 {
            let (input, k, v) = row(5 * (tick - 4) + i);
            rows.push((input, k, v, -1));
        }
    }
    rows
}

/// Every output after each tick, in the order of `OUTPUTS`.
fn run(workers: usize) -> Vec<Vec<Vec<(String, String, i64)>>> {
    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let mut server = Server::new();
        let mut program = lower::lower_tree(parse::pipe::parse(PROGRAM));
        interactive::passes::optimize(&mut program);
        server.install(worker, "p", &program).unwrap();
        let mut seen = Vec::new();
        for tick in 0..TICKS {
            if worker.index() == 0 {
                for (input, k, v, d) in feeds(tick) {
                    server.feed("p", input, &format!("{k},{v} diff={d}"), tick).unwrap();
                }
            }
            for input in [0, 1] {
                server.seal("p", input, &Antichain::from_elem(host_time(tick + 1))).unwrap();
            }
            server.spin(worker);
            seen.push(OUTPUTS.iter().map(|name| server.snapshot(worker, &format!("p::{name}")).unwrap().rows()).collect());
        }
        seen
    })
    .unwrap();
    guards.join().into_iter().next().unwrap().unwrap()
}

#[test]
fn semijoin_and_antijoin_split_their_left_input() {
    let (mut duplicated, mut both) = (false, false);
    for workers in [1, 3] {
        for (tick, out) in run(workers).into_iter().enumerate() {
            let [semi, anti, split, a, semi_ref, anti_ref, reach, reach_ref, anti_naive] = &out[..] else { unreachable!() };
            let at = format!("tick {tick}, {workers} workers");
            both |= !semi.is_empty() && !anti.is_empty();
            assert_eq!(semi, semi_ref, "{at}: semijoin");
            assert_eq!(anti, anti_ref, "{at}: antijoin");
            assert_eq!(split, a, "{at}: the semijoin and antijoin together are the left input");
            assert!(anti.iter().chain(semi).all(|(_, _, d)| *d > 0), "{at}: no negative rows");
            assert_eq!(reach, reach_ref, "{at}: antijoin in an iterative scope");
            duplicated |= anti_naive != anti;
        }
    }
    assert!(both, "some tick keeps some rows and drops others");
    assert!(duplicated, "the right side holds a key twice at some tick, where the written-out antijoin goes wrong");
}

#[test]
fn semijoin_and_antijoin_expand_in_the_parser_and_share_their_input() {
    use interactive::{passes::Config, plan::{self, ddp::DdpStage}};
    let config = Config::parse("all").unwrap();
    // `left` is a pipeline, so the expansion writes it twice; once optimized, the plan is the one
    // written by hand with `left` named once.
    let sugar = "let b = input 1 : (int, int); \
        output \"s\" = input 0 : (int, int) | map((k, x) -> (k, (x, 1) add)) | semijoin(b); \
        output \"n\" = input 0 : (int, int) | map((k, x) -> (k, (x, 1) add)) | antijoin(b);";
    let by_hand = "let b = input 1 : (int, int); \
        let a = input 0 : (int, int) | map((k, x) -> (k, (x, 1) add)); \
        let matched = a | join(b | map((k, _) -> (k, ())) | distinct, (k, v, _) -> (k, v)); \
        output \"s\" = matched; \
        output \"n\" = a - matched;";
    let report = plan::report(sugar, &|_| None, &config).unwrap();
    let parsed = report.emit_ddp_at(DdpStage::Parsed).unwrap().text;
    assert!(!parsed.contains("semijoin") && !parsed.contains("antijoin") && parsed.contains("| join("), "{parsed}");
    let expected = plan::report(by_hand, &|_| None, &config).unwrap();
    assert!(report.compare_final(&expected).is_empty(), "{:?}", report.compare_final(&expected));
}
