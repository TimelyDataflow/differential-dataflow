//! A bitemporal query on a server whose root time is `(system time, event time)`, checked
//! against a brute-force oracle at every closed `(system, event)` point, including after late
//! corrections: an update about event time 1 that arrives at system time 1, after event time 2
//! was already seen at system time 0.
//!
//! The query is account balances (a `count` whose multiplicities are the amounts), each owner's
//! total (a join then a count), each owner's least-balance account (a join then a `min`), and
//! the accounts reachable over links from accounts in credit (a loop). The server is driven as
//! a client would drive it: feeds at `(s, e)` times, `tick` for system time, `advance` for event
//! time, and `snapshot_as_of` at a point. The oracle recomputes each export from the fed rows at
//! or before the point.

use std::collections::{BTreeMap, BTreeSet};

use interactive::server::{HostTime, Server};
use timely::order::PartialOrder;
use interactive::{lower, parse};

const PROGRAM: &str = include_str!("bitemporal.ddp");

const EXPORTS: [&str; 4] = ["balance", "owner_total", "owner_min", "reach"];

/// A root time `(system, event)`.
type Point = (u64, u64);

fn at((s, e): Point) -> HostTime {
    HostTime::new(smallvec::smallvec![s, e])
}

fn lane(time: &HostTime, lane: usize) -> u64 { time.get(lane).copied().unwrap_or(0) }

/// One client action.
#[derive(Clone, Debug)]
enum Step {
    /// `feed` input `input` with `rows` at `time`.
    Feed { input: usize, rows: Vec<(Vec<i64>, i64)>, time: Point },
    /// A feed below `now`, which the server must refuse.
    Refused { input: usize, rows: Vec<(Vec<i64>, i64)>, time: Point },
    /// `tick`: system time forward by one.
    Tick,
    /// `advance 1`: event time forward by `by`.
    AdvanceEvent(u64),
}

/// Rows as a feed writes them: `<key> [val=<v>] [diff=<d>]`, the key's fields comma-separated.
fn feed_text(input: usize, rows: &[(Vec<i64>, i64)]) -> String {
    rows.iter().map(|(fields, diff)| match input {
        // deposits: key (acct), diff the amount
        0 => format!("{} diff={diff}", fields[0]),
        // owners: key (acct), val owner
        1 => format!("{} val={} diff={diff}", fields[0], fields[1]),
        // links: key (a, b)
        _ => format!("{},{} diff={diff}", fields[0], fields[1]),
    }).collect::<Vec<_>>().join("\n")
}

/// The integers in a printed key or value, in order: `tuple(1, -3)` is `[1, -3]`, `()` is `[]`.
fn numbers(text: &str) -> Vec<i64> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for c in text.chars().chain(std::iter::once(' ')) {
        if c.is_ascii_digit() || (c == '-' && cur.is_empty()) {
            cur.push(c);
        } else if !cur.is_empty() {
            if let Ok(n) = cur.parse() { out.push(n); }
            cur.clear();
        }
    }
    out
}

type Rows = Vec<(Vec<i64>, Vec<i64>, i64)>;

fn sorted(mut rows: Rows) -> Rows {
    rows.sort();
    rows
}

/// Everything fed so far, with its time: the oracle's input.
#[derive(Default)]
struct Fed {
    /// `(input, fields, diff, time)`.
    rows: Vec<(usize, Vec<i64>, i64, Point)>,
}

impl Fed {
    /// Each export's rows at `point`: the accumulation of what was fed at or before it.
    fn expected(&self, point: Point) -> BTreeMap<&'static str, Rows> {
        let visible = |(s, e): Point| s <= point.0 && e <= point.1;
        let mut balance: BTreeMap<i64, i64> = BTreeMap::new();
        let mut owners: BTreeMap<(i64, i64), i64> = BTreeMap::new();
        let mut links: BTreeMap<(i64, i64), i64> = BTreeMap::new();
        for (input, fields, diff, time) in &self.rows {
            if !visible(*time) { continue; }
            match input {
                0 => *balance.entry(fields[0]).or_default() += diff,
                1 => *owners.entry((fields[0], fields[1])).or_default() += diff,
                _ => *links.entry((fields[0], fields[1])).or_default() += diff,
            }
        }
        balance.retain(|_, b| *b != 0);
        owners.retain(|_, m| *m != 0);
        links.retain(|_, m| *m != 0);

        let mut out = BTreeMap::new();
        out.insert("balance", balance.iter().map(|(a, b)| (vec![*a], vec![*b], 1)).collect());

        // Each deposit meets each owner row of its account; the count sums amount x multiplicity.
        let mut total: BTreeMap<i64, i64> = BTreeMap::new();
        let mut least: BTreeMap<i64, (i64, i64)> = BTreeMap::new();
        for ((acct, owner), mult) in &owners {
            if let Some(bal) = balance.get(acct) {
                *total.entry(*owner).or_default() += bal * mult;
                // `min` is over the values present with positive multiplicity.
                if *mult > 0 {
                    let candidate = (*bal, *acct);
                    least.entry(*owner).and_modify(|m| *m = (*m).min(candidate)).or_insert(candidate);
                }
            }
        }
        total.retain(|_, t| *t != 0);
        out.insert("owner_total", total.iter().map(|(o, t)| (vec![*o], vec![*t], 1)).collect());
        out.insert("owner_min", least.iter().map(|(o, (b, a))| (vec![*o], vec![*b, *a], 1)).collect());

        // Reachable over links (present with positive multiplicity) from accounts in credit.
        let mut reach: BTreeSet<i64> = balance.iter().filter(|(_, b)| **b > 0).map(|(a, _)| *a).collect();
        let mut frontier: Vec<i64> = reach.iter().copied().collect();
        while let Some(node) = frontier.pop() {
            for ((a, b), mult) in &links {
                if *a == node && *mult > 0 && reach.insert(*b) { frontier.push(*b); }
            }
        }
        out.insert("reach", reach.iter().map(|n| (vec![*n], vec![], 1)).collect());
        out
    }
}

/// Run `steps` on a server with root time `(system, event)` on `workers` workers, and after
/// every `tick` or `advance` check each export at every readable point of a small grid against
/// the oracle. Feeds are issued by worker 0 only, as the server binary does; every worker runs
/// the clock commands and the reads (a snapshot is a small dataflow that gathers to worker 0,
/// which every worker takes part in), and worker 0 compares. Returns the number of
/// `(point, export)` checks made on worker 0.
fn run(steps: Vec<Step>, workers: usize, shared: bool) -> usize {
    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let text = if shared { PROGRAM.replace("let owners = input 1 : ((int), (int));", "let owners = (input 1 : ((int), (int))) + (import \"seed_owners\");") } else { PROGRAM.to_string() };
        let program = lower::lower_tree(parse::pipe::parse(&text));
        let mut server = Server::new();
        if shared {
            let seed = lower::lower_tree(parse::pipe::parse(r#"export "seed_owners" = input 0 : ((int), (int));"#));
            server.install(worker, "seed", &seed).unwrap();
        }
        server.install_with_root(worker, "bank", &program, 2, at((0, 0))).unwrap();
        let w0 = worker.index() == 0;
        let mut fed = Fed::default();
        let mut checks = 0;
        let max_event = steps.iter().filter_map(|s| match s { Step::Feed { time, .. } => Some(time.1), _ => None }).max().unwrap_or(0);
        for step in &steps {
            match step {
                Step::Feed { input, rows, time } => {
                    if w0 {
                        if shared && *input == 1 && time.1 == 0 {
                            server.feed("seed", 0, &feed_text(*input, rows), Some(time.0)).unwrap();
                        } else {
                            server.feed_at("bank", *input, &feed_text(*input, rows), &at(*time)).unwrap();
                        }
                    }
                    for (fields, diff) in rows { fed.rows.push((*input, fields.clone(), *diff, *time)); }
                }
                Step::Refused { input, rows, time } => {
                    let result = server.feed_at("bank", *input, &feed_text(*input, rows), &at(*time));
                    assert!(result.is_err(), "a feed at {time:?} below now {:?} was accepted", server.now("bank").unwrap());
                }
                Step::Tick => {
                    if shared { server.tick(worker); }
                    server.tick_program(worker, "bank").unwrap();
                },
                Step::AdvanceEvent(by) => {
                    let now = server.now("bank").unwrap();
                    let next = at((lane(now, 0), lane(now, 1) + by));
                    if shared {
                        server.advance_input("seed", 0, &at((lane(now, 0) + 1, 0))).unwrap();
                    }
                    server.advance_to(worker, "bank", &next).unwrap();
                },
            }
            if !matches!(step, Step::Tick | Step::AdvanceEvent(_)) { continue; }
            // Every point of the grid that is closed and not compacted away, by the server's own
            // rule: the last closed epoch at every event time, and the open epoch below its event
            // time.
            let now = server.now("bank").unwrap().clone();
            let s = lane(&now, 0);
            let mut points: Vec<Point> = (0..=max_event + 1).chain([u64::MAX]).flat_map(|ev| [(s.saturating_sub(1), ev), (s, ev)]).collect();
            points.retain(|p| !now.less_equal(&at(*p))
                && server.trace_since("balance").unwrap().less_equal(&at(*p)));
            for point in points {
                let expected = fed.expected(point);
                for export in EXPORTS {
                    // Only the owner queries depend on the imported seed.
                    if shared && export.starts_with("owner_") && server.input_frontier("seed", 0).unwrap().less_equal(&at(point)) { continue; }
                    let got: Rows = server.snapshot_as_of(worker, export, &at(point)).unwrap().rows().into_iter()
                        .map(|(k, v, d)| (numbers(&k), numbers(&v), d)).collect();
                    if !w0 { continue; }
                    assert_eq!(sorted(got), sorted(expected[export].clone()), "export {export} at {point:?}, now {now:?}, {workers} workers");
                    checks += 1;
                }
            }
        }
        // A point compacted away is refused.
        let now = server.now("bank").unwrap().clone();
        if lane(&now, 0) >= 2 { assert!(server.snapshot_as_of(worker, "balance", &at((lane(&now, 0) - 2, 0))).is_err()); }
        if shared {
            let narrow = lower::lower_tree(parse::pipe::parse(r#"export "bad" = import "balance";"#));
            assert!(server.install(worker, "narrow", &narrow).unwrap_err().contains("width 2 into width 1"));
            assert!(server.trace_width("bad").is_none());
            assert_eq!(server.trace_width("seed_owners"), Some(1));
            assert_eq!(server.trace_width("balance"), Some(2));
        }
        checks
    }).unwrap();
    let counts: Vec<_> = guards.join().into_iter().map(Result::unwrap).collect();
    counts[0]
}

/// Deposits, owners and links at `(0, e)` for a few event times; at system time 1 a late
/// deposit at event time 1 and corrections at event time 2 and 3; then event time advanced to 2,
/// a feed below it refused, and feeds at and after it.
fn hand_scenario() -> Vec<Step> {
    let row = |fields: &[i64], diff: i64| (fields.to_vec(), diff);
    vec![
        Step::Feed { input: 1, rows: vec![row(&[1, 10], 1), row(&[2, 10], 1), row(&[3, 20], 1), row(&[4, 20], 1)], time: (0, 0) },
        Step::Feed { input: 2, rows: vec![row(&[1, 2], 1), row(&[2, 3], 1), row(&[3, 4], 1), row(&[4, 5], 1)], time: (0, 0) },
        Step::Feed { input: 0, rows: vec![row(&[1], 100), row(&[3], 70)], time: (0, 1) },
        Step::Feed { input: 0, rows: vec![row(&[2], 50)], time: (0, 2) },
        Step::Feed { input: 0, rows: vec![row(&[4], -20)], time: (0, 3) },
        Step::Tick,
        // Late: event time 1 is below the event time 3 already seen.
        Step::Feed { input: 0, rows: vec![row(&[2], 5)], time: (1, 1) },
        Step::Feed { input: 0, rows: vec![row(&[1], -30)], time: (1, 3) },
        // Account 3 changes owner from event time 2 on; the link 2 -> 3 is cut from event time 2 on.
        Step::Feed { input: 1, rows: vec![row(&[3, 20], -1), row(&[3, 10], 1)], time: (1, 2) },
        Step::Feed { input: 2, rows: vec![row(&[2, 3], -1)], time: (1, 2) },
        Step::Tick,
        Step::AdvanceEvent(2),
        Step::Refused { input: 0, rows: vec![row(&[1], 1)], time: (2, 1) },
        Step::Feed { input: 0, rows: vec![row(&[1], 1)], time: (2, 2) },
        Step::Feed { input: 0, rows: vec![row(&[4], 40)], time: (2, 5) },
        Step::Tick,
        // Account 2's balance cancels to zero from event time 2 at system time 3.
        Step::Feed { input: 0, rows: vec![row(&[2], -55)], time: (3, 2) },
        Step::Tick,
        Step::Tick,
    ]
}

/// Random feeds at random event times, late ones included, with ticks and event advances.
fn random_scenario(seed: u64, rounds: usize) -> Vec<Step> {
    let mut state = seed;
    let mut next = |n: u64| { state ^= state << 13; state ^= state >> 7; state ^= state << 17; state % n };
    let mut steps = Vec::new();
    let (mut s, mut e) = (0u64, 0u64);
    let mut owners: BTreeMap<i64, i64> = BTreeMap::new();
    let mut links: BTreeSet<(i64, i64)> = BTreeSet::new();
    for _ in 0..rounds {
        for _ in 0..1 + next(4) {
            let time = (s, e + next(4));
            match next(3) {
                0 => {
                    let rows = (0..1 + next(3)).map(|_| (vec![1 + next(6) as i64], (next(7) as i64) - 3)).filter(|(_, d)| *d != 0).collect();
                    steps.push(Step::Feed { input: 0, rows, time });
                }
                1 => {
                    let acct = 1 + next(6) as i64;
                    let owner = 10 + next(3) as i64;
                    let mut rows = Vec::new();
                    if let Some(old) = owners.insert(acct, owner) { rows.push((vec![acct, old], -1)); }
                    rows.push((vec![acct, owner], 1));
                    steps.push(Step::Feed { input: 1, rows, time });
                }
                _ => {
                    let link = (1 + next(6) as i64, 1 + next(6) as i64);
                    let diff = if links.remove(&link) { -1 } else { links.insert(link); 1 };
                    steps.push(Step::Feed { input: 2, rows: vec![(vec![link.0, link.1], diff)], time });
                }
            }
        }
        if next(4) == 0 { e += 1; steps.push(Step::AdvanceEvent(1)); } else { s += 1; steps.push(Step::Tick); }
    }
    steps
}

#[test]
fn bitemporal_balances_match_the_oracle() {
    for (workers, shared) in [(1, false), (2, false), (1, true), (2, true)] {
        let checks = run(hand_scenario(), workers, shared);
        eprintln!("hand scenario, {workers} workers, shared={shared}: {checks} (point, export) checks");
        assert!(checks > 0);
        let checks = run(random_scenario(0x5EED_2026_1028, 12), workers, shared);
        eprintln!("random scenario, {workers} workers, shared={shared}: {checks} (point, export) checks");
        assert!(checks > 0);
    }
}

// --- The demonstration -------------------------------------------------------------------------

/// Three accounts, six events, as a bank learns of them: each system time is a day of
/// bookkeeping, each event time the day a transaction is dated. The late fact is a deposit
/// dated day 1 that is booked on day 1 of bookkeeping, after a day-2 transaction was already
/// booked; the correction is a reversal, booked on day 2, of a deposit dated day 1.
fn demo_scenario() -> Vec<(&'static str, Vec<Step>)> {
    let row = |fields: &[i64], diff: i64| (fields.to_vec(), diff);
    vec![
        ("Day 0 of bookkeeping: Ann owns accounts 1 and 2, Bob owns 3; deposits dated day 1 (100 to account 1, 70 to account 3) and day 2 (50 to account 2).", vec![
            Step::Feed { input: 1, rows: vec![row(&[1, 10], 1), row(&[2, 10], 1), row(&[3, 20], 1)], time: (0, 0) },
            Step::Feed { input: 0, rows: vec![row(&[1], 100), row(&[3], 70)], time: (0, 1) },
            Step::Feed { input: 0, rows: vec![row(&[2], 50)], time: (0, 2) },
            Step::Tick,
        ]),
        ("Day 1: a late deposit of 5 to account 2, dated day 1 (day 2 was already booked); a withdrawal of 30 from account 1, dated day 3.", vec![
            Step::Feed { input: 0, rows: vec![row(&[2], 5)], time: (1, 1) },
            Step::Feed { input: 0, rows: vec![row(&[1], -30)], time: (1, 3) },
            Step::Tick,
        ]),
        ("Day 2: the 70 deposited to account 3 on day 1 was booked in error; it is reversed, dated day 1.", vec![
            Step::Feed { input: 0, rows: vec![row(&[3], -70)], time: (2, 1) },
            Step::Tick,
        ]),
        ("Day 3: nothing new is learned.", vec![Step::Tick]),
    ]
}

const DEMO_EVENTS: u64 = 3;
const DEMO_EXPORTS: [&str; 2] = ["balance", "owner_total"];

fn owner_name(owner: i64) -> String {
    match owner { 10 => "Ann".into(), 20 => "Bob".into(), other => format!("owner{other}") }
}

/// A cell: `a1:100 a2:50` for balances, `Ann:150 Bob:70` for owner totals, `-` when empty.
fn cell(export: &str, rows: &Rows) -> String {
    if rows.is_empty() { return "-".into(); }
    rows.iter().map(|(k, v, _)| match export {
        "balance" => format!("a{}:{}", k[0], v[0]),
        _ => format!("{}:{}", owner_name(k[0]), v[0]),
    }).collect::<Vec<_>>().join(" ")
}

/// Play the demo on one worker: after each day, print each export as a grid (rows: system
/// time, columns: event time, `*` where a cell differs from the one above it: what that day's
/// bookkeeping changed about history), check every cell against the oracle, and write the grid
/// data as JSON to `$DDIR_DEMO_JSON` if set.
///
/// `cargo test -p interactive --offline --test bitemporal bitemporal_demo -- --ignored --nocapture`
#[test]
#[ignore]
fn bitemporal_demo() {
    timely::execute_directly(|worker| {
        let program = lower::lower_tree(parse::pipe::parse(PROGRAM));
        let mut server = Server::new();
        server.install_with_root(worker, "bank", &program, 2, at((0, 0))).unwrap();
        let mut fed = Fed::default();
        // rows[system][event] per export, kept as read: a closed system time's row never changes.
        let mut grid: BTreeMap<&str, Vec<Vec<Rows>>> = DEMO_EXPORTS.iter().map(|e| (*e, Vec::new())).collect();
        let mut json_steps: Vec<String> = Vec::new();
        let events: Vec<Point> = (0..=DEMO_EVENTS).map(|e| (0, e)).chain([(0, u64::MAX)]).collect();
        let event_label = |e: u64| if e == u64::MAX { "all".to_string() } else { e.to_string() };
        for (label, steps) in demo_scenario() {
            let mut fed_json = Vec::new();
            for step in &steps {
                match step {
                    Step::Feed { input, rows, time } => {
                        server.feed_at("bank", *input, &feed_text(*input, rows), &at(*time)).unwrap();
                        for (fields, diff) in rows {
                            fed.rows.push((*input, fields.clone(), *diff, *time));
                            fed_json.push(format!("{{\"input\": {input}, \"fields\": {fields:?}, \"diff\": {diff}, \"system\": {}, \"event\": {}}}", time.0, time.1));
                        }
                    }
                    Step::Tick => server.tick_program(worker, "bank").unwrap(),
                    other => panic!("not in the demo: {other:?}"),
                }
            }
            // The day just closed is readable at every event time; read it, check it, keep it.
            let closed = lane(server.now("bank").unwrap(), 0) - 1;
            println!("\n== {label}\n   (now: bookkeeping day {} open)", lane(server.now("bank").unwrap(), 0));
            for export in DEMO_EXPORTS {
                let row: Vec<Rows> = events.iter().map(|&(_, e)| {
                    let got: Rows = server.snapshot_as_of(worker, export, &at((closed, e))).unwrap().rows().into_iter()
                        .map(|(k, v, d)| (numbers(&k), numbers(&v), d)).collect();
                    let got = sorted(got);
                    assert_eq!(got, sorted(fed.expected((closed, e))[export].clone()), "{export} at ({closed}, {e})");
                    got
                }).collect();
                grid.get_mut(export).unwrap().push(row);
                let rows = &grid[export];
                let width = rows.iter().flatten().map(|r| cell(export, r).len() + 1).max().unwrap_or(1).max(6);
                println!("\n   {export}: as of event day (columns), as known at the end of bookkeeping day (rows)");
                print!("   {:>8}", "");
                for &(_, e) in &events { print!(" {:>width$}", format!("day {}", event_label(e)), width = width); }
                println!();
                for (s, row) in rows.iter().enumerate() {
                    print!("   {:>8}", format!("day {s}"));
                    for (j, r) in row.iter().enumerate() {
                        let changed = s > 0 && rows[s - 1][j] != *r;
                        print!(" {:>width$}", format!("{}{}", if changed { "*" } else { "" }, cell(export, r)), width = width);
                    }
                    println!();
                }
            }
            let mut points = Vec::new();
            for s in 0..=closed as usize {
                for (j, &(_, e)) in events.iter().enumerate() {
                    let answers: Vec<String> = DEMO_EXPORTS.iter().map(|export| {
                        let rows: Vec<String> = grid[export][s][j].iter().map(|(k, v, d)| format!("{{\"key\": {k:?}, \"val\": {v:?}, \"diff\": {d}}}")).collect();
                        format!("\"{export}\": [{}]", rows.join(", "))
                    }).collect();
                    let changed = s > 0 && DEMO_EXPORTS.iter().any(|export| grid[export][s][j] != grid[export][s - 1][j]);
                    let event = if e == u64::MAX { "\"all\"".to_string() } else { e.to_string() };
                    points.push(format!("{{\"system\": {s}, \"event\": {event}, \"changed\": {changed}, \"answers\": {{{}}}}}", answers.join(", ")));
                }
            }
            json_steps.push(format!("{{\"label\": {label:?}, \"closed_system_time\": {closed}, \"fed\": [{}], \"points\": [{}]}}", fed_json.join(", "), points.join(", ")));
        }
        if let Ok(path) = std::env::var("DDIR_DEMO_JSON") {
            let exports: Vec<String> = DEMO_EXPORTS.iter().map(|e| format!("\"{e}\"")).collect();
            let text = format!("{{\"exports\": [{}], \"owners\": {{\"10\": \"Ann\", \"20\": \"Bob\"}}, \"steps\": [{}]}}\n", exports.join(", "), json_steps.join(",\n"));
            std::fs::write(&path, text).unwrap();
            println!("\nwrote {path}");
        }
    });
}

/// Independent inputs, output probes, trimmed zero points, nested time operators,
/// and preparation with projection/as-of filtering enabled all use the declared width.
#[test]
fn independent_inputs_and_time_shapes() {
    for workers in [1, 2] {
        let guards = timely::execute(timely::Config::process(workers), |worker| {
            let mut server = Server::new();
            server.set_passes(interactive::passes::Config::all());
            let program = lower::lower_tree(parse::pipe::parse(r#"
                let a = input 0 : (int, int);
                let b = input 1 : (int, int);
                s: {
                    let seen = a | delay((_, v) -> (0, 0, (256, v call bit_length) mul)) | lift_iter | lift
                        | map((k, ((v, iter), (s, e, i))) -> (k, (s, e, i, iter)));
                }
                export "left" = a;
                export "right" = b;
                export "seen" = s::seen;
                export "delta" = (a | lift | half_join(b, 0, (k, x, y) -> (k, (x, y))))
                               + (b | lift | half_join(a, 1, (k, y, x) -> (k, (x, y))));
            "#));
            server.install_with_root(worker, "split", &program, 2, at((0, 0))).unwrap();
            let reader = lower::lower_tree(parse::pipe::parse(r#"
                export "left_copy" = import "left";
                export "both" = (import "left") + (import "right");
            "#));
            server.install_with_root(worker, "reader", &reader, 2, at((0, 0))).unwrap();
            let relay = lower::lower_tree(parse::pipe::parse(r#"export "relay" = (import "both") + (import "left_copy");"#));
            server.install_with_root(worker, "relay", &relay, 2, at((0, 0))).unwrap();
            if worker.index() == 0 {
                server.feed_at("split", 0, "7 val=1", &at((0, 0))).unwrap();
                server.feed_at("split", 0, "8 val=1", &at((0, 5))).unwrap();
            }
            server.advance_input("split", 0, &at((1, 0))).unwrap();
            server.wait_for(worker, "left", &at((1, 0))).unwrap();
            let left = server.snapshot_as_of(worker, "left", &at((0, u64::MAX))).unwrap();
            assert_eq!(server.snapshot_as_of(worker, "left_copy", &at((0, u64::MAX))).unwrap().rows(), left.rows());
            let seen = server.snapshot_as_of(worker, "seen", &at((0, u64::MAX))).unwrap();
            let keyed = server.peek_as_of(worker, "left", Some("7"), &at((0, 0))).unwrap();
            if worker.index() == 0 {
                assert_eq!(left.len(), 2);
                assert_eq!(keyed.rows(), vec![("7".into(), "1".into(), 1)]);
                let rows: Rows = seen.rows().into_iter().map(|(k, v, d)| (numbers(&k), numbers(&v), d)).collect();
                assert_eq!(sorted(rows), vec![(vec![7], vec![0, 0, 256, 256], 1), (vec![8], vec![0, 5, 256, 256], 1)]);
            }
            // Input 1 and the exports using it remain open at the minimum.
            for export in ["right", "delta", "both"] {
                let error = server.snapshot_as_of(worker, export, &at((0, u64::MAX))).unwrap_err();
                let path = if export == "both" { "import \"right\" -> " } else { "" };
                assert_eq!(error, format!("trace {export:?} is not closed at [0,max]: {path}input 1 of program \"split\" is at write frontier {{[0,0]}}"));
            }
            assert_eq!(server.snapshot_as_of(worker, "left", &at((1, 0))).unwrap_err(),
                "trace \"left\" is not closed at [1,0]: input 0 of program \"split\" is at write frontier {[1,0]}");
            // Both inputs block, even though input 1 subsumes input 0 in the combined frontier.
            assert_eq!(server.snapshot_as_of(worker, "delta", &at((1, 0))).unwrap_err(),
                "trace \"delta\" is not closed at [1,0]:\n  - input 0 of program \"split\" is at write frontier {[1,0]}\n  - input 1 of program \"split\" is at write frontier {[0,0]}");
            let error = server.snapshot(worker, "left").unwrap_err();
            assert!(error.contains("width 2") && error.contains("snapshot_as_of") && error.contains("peek_as_of"), "{error}");
            assert_eq!(server.peek(worker, "left", None).unwrap_err(), error);
            assert_eq!(server.input_frontier("split", 1).unwrap(), &at((0, 0)));
            server.advance_input("split", 0, &at((5, 0))).unwrap();
            let error = server.advance_to(worker, "split", &at((3, 7))).unwrap_err();
            assert_eq!(error, "cannot advance input \"split\"/0 from frontier [5,0] to requested target [3,7]");
            assert_eq!(server.input_frontier("split", 1).unwrap(), &at((0, 0)), "bulk rejection is atomic");
            assert!(server.feed_at("split", 0, "9 val=1", &at((1, 5))).is_err());
            assert!(server.feed_at("split", 0, "9 val=1", &HostTime::new(smallvec::smallvec![2, 0, 1])).is_err());
            if worker.index() == 0 { server.feed_at("split", 1, "7 val=2", &at((0, 0))).unwrap(); }
            server.advance_input("split", 1, &at((0, 3))).unwrap();
            assert_eq!(server.snapshot_as_of(worker, "delta", &at((5, 3))).unwrap_err(),
                "trace \"delta\" is not closed at [5,3]:\n  - input 0 of program \"split\" is at write frontier {[5,0]}\n  - input 1 of program \"split\" is at write frontier {[0,3]}");
            assert_eq!(server.snapshot_as_of(worker, "both", &at((5, 3))).unwrap_err(),
                "trace \"both\" is not closed at [5,3]:\n  - import \"left\" -> input 0 of program \"split\" is at write frontier {[5,0]}\n  - import \"right\" -> input 1 of program \"split\" is at write frontier {[0,3]}");
            assert_eq!(server.snapshot_as_of(worker, "both", &at((5, 2))).unwrap_err(),
                "trace \"both\" is not closed at [5,2]: import \"left\" -> input 0 of program \"split\" is at write frontier {[5,0]}");
            // Preserve both import paths to a shared producer; omit the nonblocking right input.
            assert_eq!(server.snapshot_as_of(worker, "relay", &at((5, 2))).unwrap_err(),
                "trace \"relay\" is not closed at [5,2]:\n  - import \"both\" -> import \"left\" -> input 0 of program \"split\" is at write frontier {[5,0]}\n  - import \"left_copy\" -> import \"left\" -> input 0 of program \"split\" is at write frontier {[5,0]}");
            assert!(server.advance_input("split", 1, &at((0, 0))).unwrap_err().contains("cannot feed at [0,0]: input \"split\"/1 has width 2 and frontier [0,3]"));
            server.advance_input("split", 1, &at((2, 3))).unwrap();
            for export in ["left", "seen", "delta"] { server.wait_for(worker, export, &at((2, 0))).unwrap(); }
            let delta = server.snapshot_as_of(worker, "delta", &at((0, u64::MAX))).unwrap();
            if worker.index() == 0 {
                let rows: Rows = delta.rows().into_iter().map(|(k, v, d)| (numbers(&k), numbers(&v), d)).collect();
                assert_eq!(rows, vec![(vec![7], vec![1, 2, 0, 0], 1)]);
            }
            server.compact("left", &at((0, 3))).unwrap();
            assert_eq!(server.snapshot_as_of(worker, "left", &at((0, 2))).unwrap_err(), "point [0,2] is before trace \"left\"'s read floor [0,3]");
            assert!(server.compact("left", &at((0, 1))).is_err());
            let import = lower::lower_tree(parse::pipe::parse(r#"export "wide" = import "left" | lift;"#));
            assert!(server.install_with_root(worker, "wide", &import, 3, at((0, 0))).is_err());
            server.install_with_root(worker, "wide", &import, 3, at((0, 3))).unwrap();
            server.wait_for(worker, "wide", &at((2, 0))).unwrap();
            let wide = server.snapshot_as_of(worker, "wide", &HostTime::new(smallvec::smallvec![0, 5, 0])).unwrap();
            if worker.index() == 0 {
                let rows: Rows = wide.rows().into_iter().map(|(k, v, d)| (numbers(&k), numbers(&v), d)).collect();
                assert_eq!(sorted(rows), vec![(vec![7], vec![1, 0, 3, 0], 1), (vec![8], vec![1, 0, 5, 0], 1)]);
            }

            // A consumer advance cannot close points held open by a width-one import.
            let producer = lower::lower_tree(parse::pipe::parse(r#"export "lagging" = input 0 : (int, int);"#));
            server.install(worker, "producer", &producer).unwrap();
            for width in [1, 2] {
                let name = format!("consumer{width}");
                let consumer = lower::lower_tree(parse::pipe::parse(&format!("export \"{name}\" = import \"lagging\";")));
                server.install_with_root(worker, &name, &consumer, width, at((0, 0))).unwrap();
                server.advance_to(worker, &name, &at((3, 0))).unwrap();
                let point = if width == 1 { at((2, 0)) } else { at((2, u64::MAX)) };
                let error = server.snapshot_as_of(worker, &name, &point).unwrap_err();
                let point_text = if width == 1 { "[2]" } else { "[2,max]" };
                assert_eq!(error, format!("trace {name:?} is not closed at {point_text}: import \"lagging\" -> input 0 of program \"producer\" is at write frontier {{[0]}}"));
                assert_eq!(server.peek_as_of(worker, &name, None, &point).unwrap_err(), error);
                // Width one has no closed point yet; wider traces still require an explicit point.
                // Loading a published trace calls snapshot_local, with the same read checks.
                if width == 1 {
                    assert!(server.snapshot(worker, &name).unwrap().is_empty());
                    assert!(server.peek(worker, &name, None).unwrap().is_empty());
                    assert_eq!(server.load(worker, "split", 1, &name, 1).unwrap(), 0);
                } else {
                    let default_error = server.snapshot(worker, &name).unwrap_err();
                    assert_eq!(server.peek(worker, &name, None).unwrap_err(), default_error);
                    assert_eq!(server.load(worker, "split", 1, &name, 1).unwrap_err(), default_error);
                }
            }
        }).unwrap();
        for result in guards.join() { result.unwrap(); }
    }
}
