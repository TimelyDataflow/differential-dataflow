//! The ledger rules (`model/resolve.ddp`) and the ledger model (`model/ledger.ddp`), on one small history.
//!
//! Each round runs the rules on the indexes the rounds before it left, and checks every outcome.
//! After each round the model derives the same indexes from the manifest alone, and they must agree;
//! and every invariant of the model must hold of the manifest.

use std::collections::BTreeMap;

use interactive::scope_ir::Program;

/// A collection as rows of text, each with its multiplicity.
type Rows = BTreeMap<(String, String), i64>;

fn program(text: &str) -> Program {
    let mut program = interactive::lower::lower_tree(interactive::parse::pipe::parse(text));
    interactive::passes::optimize(&mut program);
    program
}

fn evaluate(program: &Program, inputs: &[Vec<String>], workers: usize) -> BTreeMap<String, Rows> {
    interactive::server::evaluate(timely::Config::process(workers), program, inputs).into_iter()
        .map(|(name, updates)| {
            let mut rows = Rows::new();
            for (key, val, diff) in updates.rows() { *rows.entry((key, val)).or_default() += diff; }
            (name, rows)
        }).collect()
}

fn lines(rows: &Rows) -> Vec<String> {
    rows.iter().map(|((key, val), diff)| format!("{key} val={val} diff={diff}")).collect()
}

fn absorb(into: &mut Rows, changes: &Rows) {
    for (row, diff) in changes { *into.entry(row.clone()).or_default() += diff; }
    into.retain(|_, diff| *diff != 0);
}

/// What a resolver keeps: the manifest, and the three indexes the rules read.
#[derive(Default)]
struct State { own: Rows, registered: Rows, follows: Rows, registrations: Rows, spans: Rows }

/// One round of the rules. Returns each request's outcome, by its key.
fn round(rules: &Program, state: &mut State, requests: &[(i64, String)], workers: usize) -> BTreeMap<i64, String> {
    let requests = requests.iter().map(|(key, actions)| format!("{key} val=tuple(list({actions}))")).collect();
    let out = evaluate(rules, &[lines(&state.own), requests, lines(&state.registered), lines(&state.follows)], workers);
    absorb(&mut state.own, &out["own_delta"]);
    absorb(&mut state.registered, &out["registered_add"]);
    absorb(&mut state.follows, &out["follows_add"]);
    absorb(&mut state.registrations, &out["registrations_add"]);
    absorb(&mut state.spans, &out["spans_add"]);
    out["results"].iter().map(|((key, val), diff)| {
        assert_eq!(*diff, 1, "one outcome for {key}");
        (key.trim_start_matches("tuple(").trim_end_matches(')').parse().unwrap(), val.clone())
    }).collect()
}

/// The model's account of the manifest: the indexes it derives must be the resolver's, and nothing may be wrong.
fn check(model: &Program, state: &State, workers: usize) {
    let out = evaluate(model, &[lines(&state.registrations), lines(&state.spans)], workers);
    assert_eq!(out["own"], state.own, "versions and own uppers");
    assert_eq!(out["registered"], state.registered, "widths and starts");
    assert_eq!(out["follows"], state.follows, "clocks followed");
    for invariant in ["malformed", "orphaned", "forked", "skipped", "early", "unclocked", "mismatched"] {
        assert!(out[invariant].is_empty(), "{invariant}: {:?}", out[invariant]);
    }
}

fn frontier(time: &[i64]) -> String {
    format!("list(list({}))", time.iter().map(i64::to_string).collect::<Vec<_>>().join(","))
}
fn create(ledger: i64, width: i64, start: &[i64], clock: Option<i64>) -> String {
    let clock = clock.map_or("inject(0,tuple())".to_string(), |clock| format!("inject(1,tuple(0,{clock}))"));
    format!("inject(0,tuple(tuple(0,{ledger}),list(7),{width},{},{clock}))", frontier(start))
}
fn append(ledger: i64, lower: &[i64], upper: &[i64], seqno: i64) -> String {
    format!("inject(1,tuple(tuple(0,{ledger}),{},{},inject(0,tuple()),{seqno}))", frontier(lower), frontier(upper))
}

const COMMITTED: &str = "tuple(inject(0,tuple()))";
fn rejected(index: i64, failure: &str) -> String { format!("tuple(inject(1,tuple({index},{failure})))") }
fn stale(version: i64, time: i64) -> String { format!("inject(0,tuple({version},list(list({time}))))") }
fn unknown(ledger: i64) -> String { format!("inject(2,tuple(0,{ledger}))") }
const INVALID: &str = "inject(3,tuple())";

#[test]
fn rounds_follow_the_model() {
    let rules = program(include_str!("../model/resolve.ddp"));
    let model = program(include_str!("../model/ledger.ddp"));
    for workers in [1, 2] {
        let mut state = State::default();

        // Round 1.
        let outcomes = round(&rules, &mut state, &[
            (10, create(8, 1, &[0], None)),
            (20, format!("{},{}", create(3, 1, &[0], None), append(3, &[0], &[1], 0))),
            // A clock must be registered before the round that follows it.
            (30, create(1, 1, &[0], Some(8))),
            // A start of the wrong width, and an upper of another width than its lower.
            (40, create(4, 2, &[0], None)),
            (50, append(5, &[0], &[1, 2], 0)),
            // Ledgers out of order.
            (60, format!("{},{}", append(3, &[1], &[2], 1), create(2, 1, &[0], None))),
            (70, append(3, &[1], &[2], 1)),
            // Stale, and told where the ledger stands as the round leaves it.
            (80, append(3, &[1], &[5], 1)),
            (90, append(77, &[0], &[1], 0)),
        ], workers);
        assert_eq!(outcomes, BTreeMap::from([
            (10, COMMITTED.into()), (20, COMMITTED.into()), (30, rejected(0, &unknown(8))),
            (40, rejected(0, INVALID)), (50, rejected(0, INVALID)), (60, rejected(1, INVALID)),
            (70, COMMITTED.into()), (80, rejected(0, &stale(2, 2))), (90, rejected(0, &unknown(77))),
        ]));
        check(&model, &state, workers);

        // Round 2: followers, and a clock's advance.
        let outcomes = round(&rules, &mut state, &[
            (10, format!("{},{}", create(1, 1, &[0], Some(8)), append(1, &[0], &[1], 0))),
            // A follower of another width than its clock.
            (20, create(6, 2, &[0, 0], Some(8))),
            (30, append(8, &[0], &[5], 0)),
            (40, create(9, 1, &[0], Some(8))),
        ], workers);
        assert_eq!(outcomes, BTreeMap::from([
            (10, COMMITTED.into()), (20, rejected(0, INVALID)), (30, COMMITTED.into()), (40, COMMITTED.into()),
        ]));
        check(&model, &state, workers);

        // Round 3: ledger 1's own upper is [1], its clock stands at [5], and advances to [7] in this round.
        let outcomes = round(&rules, &mut state, &[
            (10, append(1, &[1], &[2], 1)),
            // Clock 9 follows clock 8, so a ledger created under it begins at [5].
            (20, format!("{},{}", create(2, 1, &[0], Some(9)), append(2, &[0], &[1], 0))),
            (30, append(8, &[5], &[7], 1)),
            // The clock's advance lands after this append.
            (40, append(1, &[5], &[6], 1)),
            (50, format!("{},{}", create(2, 1, &[0], Some(9)), append(2, &[5], &[6], 0))),
            // Times of two coordinates, on a ledger of one.
            (60, append(3, &[2, 0], &[3, 0], 2)),
            // Stale against its own creation, which takes no effect.
            (70, format!("{},{}", create(12, 1, &[0], None), append(12, &[4], &[5], 0))),
        ], workers);
        assert_eq!(outcomes, BTreeMap::from([
            (10, rejected(0, &stale(2, 7))), (20, rejected(1, &stale(1, 7))), (30, COMMITTED.into()),
            (40, COMMITTED.into()), (50, COMMITTED.into()), (60, rejected(0, INVALID)),
            (70, rejected(1, &stale(0, 0))),
        ]));
        check(&model, &state, workers);

        // Where everything stands: (version, own upper).
        let stands = |ledger: i64, version: i64, time: i64| {
            ((format!("tuple(tuple(0,{ledger}))"), format!("tuple({version},list(list({time})))")), 1)
        };
        assert_eq!(state.own, Rows::from([stands(1, 2, 6), stands(2, 1, 6), stands(3, 2, 2), stands(8, 2, 7), stands(9, 0, 0)]));
    }
}

/// Two transactions that each append to one ledger and advance the other's clock both commit in one round,
/// though neither order of running them whole would commit both: an advance lands after its round's appends.
#[test]
fn a_clock_advance_lands_after_its_round() {
    let rules = program(include_str!("../model/resolve.ddp"));
    let model = program(include_str!("../model/ledger.ddp"));
    let mut state = State::default();
    let created = |outcomes: BTreeMap<i64, String>| assert!(outcomes.values().all(|outcome| outcome == COMMITTED));
    created(round(&rules, &mut state, &[(0, format!("{},{}", create(8, 1, &[0], None), create(9, 1, &[0], None)))], 2));
    created(round(&rules, &mut state, &[(0, format!("{},{}", create(1, 1, &[0], Some(9)), create(2, 1, &[0], Some(8))))], 2));
    let outcomes = round(&rules, &mut state, &[
        (10, format!("{},{}", append(1, &[0], &[1], 0), append(8, &[0], &[5], 0))),
        (20, format!("{},{}", append(2, &[0], &[1], 0), append(9, &[0], &[5], 0))),
    ], 2);
    assert_eq!(outcomes, BTreeMap::from([(10, COMMITTED.into()), (20, COMMITTED.into())]));
    check(&model, &state, 2);
    // The next round sees both advances: each follower now begins at [5].
    let outcomes = round(&rules, &mut state, &[(10, append(1, &[1], &[2], 1)), (20, append(2, &[5], &[6], 1))], 2);
    assert_eq!(outcomes, BTreeMap::from([(10, rejected(0, &stale(1, 5))), (20, COMMITTED.into())]));
    check(&model, &state, 2);
}
