//! DD's `identifiers` algorithm as a DDIR program: every record proposes `hash(round, record)`
//! as its id; among records proposing one id the least `(round, record)` wins and the losers
//! propose again at the next round. Written with `hash`, `min`, `negate`, and a `var`, over a
//! hash space small enough to force collisions, it still gives every record its own id — the
//! primitive an e-graph rule needs to mint a node for a new term inside the program.

use interactive::backend::vec;
use interactive::ir::Value;
use interactive::{lower, parse};

const PROGRAM_SRC: &str = "\
-- input 0: records (r) ;
let init = input 0 | key(0, $0[0] ; );                                   -- (round, record) ;
ids: {
    let all   = init + diff;                                              -- (round, record) ;
    let keyed = all | key(hash(HASHBOUND, $0[0], $0[1]) ; $0[0], $0[1]);         -- id ; (round, record)
    let win   = keyed | min;                                              -- id ; the least (round, record)
    let lose  = keyed + (win | negate);                                   -- id ; the rest
    -- a winner past round 0 has moved from its round-0 proposal; a loser moves on to the next round
    let moved = (win | filter($1[0] > 0) | map($1[0], $1[1] ; )) + (lose | map($1[0] + 1, $1[1] ; ));
    let orig  = moved | map(0, $0[1] ; ) | negate;                        -- less their round-0 proposals
    var diff  = moved + orig;
}
export \"ids\" = (init + ids::diff) | key($0[1] ; hash(HASHBOUND, $0[0], $0[1])) | arrange;   -- record ; id
export \"rounds\" = (init + ids::diff) | key($0[0] ; ) | count | arrange;               -- round ; how many settled there
";

fn int(v: &Value) -> i64 {
    match v {
        Value::Tuple(xs) => xs[0].as_int(),
        other => other.as_int(),
    }
}

fn run(bound: i64, n: i64) -> std::collections::BTreeMap<String, Vec<((Value, Value), i64)>> {
    let src = PROGRAM_SRC.replace("HASHBOUND", &bound.to_string());
    let tree = lower::lower_tree(parse::pipe::parse(&src));
    let records: Vec<(Value, Value)> = (1..=n).map(|n| (Value::Tuple(vec![Value::Int(n)]), Value::unit())).collect();
    vec::evaluate(&tree, &[records])
}

#[test]
fn identifiers_without_collisions() {
    let out = run(1 << 40, 8);
    let ids: Vec<(i64, i64)> = out["ids"].iter().filter(|(_, d)| *d > 0).map(|((k, v), _)| (int(k), int(v))).collect();
    println!("ids: {ids:?}");
    assert_eq!(ids.len(), 8);
}

#[test]
fn identifiers_are_unique_despite_collisions() {
    let out = run(64, 32);
    let ids: Vec<(i64, i64)> = out["ids"]
        .iter()
        .filter(|(_, d)| *d > 0)
        .map(|((k, v), _)| (int(k), int(v)))
        .collect();
    assert_eq!(ids.len(), 32, "every record has an id: {ids:?}");
    let distinct: std::collections::BTreeSet<i64> = ids.iter().map(|(_, id)| *id).collect();
    assert_eq!(distinct.len(), 32, "ids are unique: {ids:?}");
    let rounds: Vec<(i64, i64)> = out["rounds"].iter().filter(|(_, d)| *d > 0).map(|((k, v), _)| (int(k), int(v))).collect();
    println!("records per round: {rounds:?}");
    assert!(rounds.iter().any(|(round, _)| *round > 0), "the hash space was wide enough that nothing collided; narrow it");
}
