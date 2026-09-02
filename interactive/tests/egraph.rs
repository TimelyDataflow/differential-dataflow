//! The e-graph program against a reference: a small Rust equality-saturation engine with the
//! same rules (congruence, commutativity, constant folding, x+0, x*1, x*0, double negation) run
//! to a fixpoint on the same random DAGs. The partition of the INPUT nodes must agree exactly,
//! every class cost must agree, and no minted name may collide.

use std::collections::{BTreeMap, HashMap};

use interactive::backend::vec;
use interactive::ir::Value;
use interactive::{lower, parse};

fn tup(fields: &[i64]) -> Value {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}

/// The same generator as `tests/corgi_backend.rs`.
fn egraph_rows(n: u64, seed: u64) -> Vec<(Value, Value)> {
    let mut s = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut next = move || { s ^= s << 13; s ^= s >> 7; s ^= s << 17; s };
    (0..n)
        .map(|id| {
            let (op, x, y) = if id < 2 {
                ((next() % 2) as i64, (next() % 3) as i64, 0)
            } else {
                let op = match next() % 8 { 0 => 0, 1 => 1, 2 | 3 => 2, 4 | 5 => 3, _ => 4 };
                let (x, y) = if op < 2 { ((next() % 3) as i64, 0) } else { ((next() % id) as i64, (next() % id) as i64) };
                (op, x, y)
            };
            (tup(&[id as i64, op, x, y]), Value::unit())
        })
        .collect()
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
enum Term { Num(u64), Var(u64), Add(u64, u64), Mul(u64, u64), Neg(u64) }

/// A reference e-graph: nodes by id, a union-find over ids, and rebuild + rules to a fixpoint.
struct Ref {
    nodes: BTreeMap<u64, Term>,
    parent: HashMap<u64, u64>,
    next_id: u64,
}

impl Ref {
    fn find(&mut self, x: u64) -> u64 {
        let p = *self.parent.get(&x).unwrap_or(&x);
        if p == x { return x; }
        let r = self.find(p);
        self.parent.insert(x, r);
        r
    }
    /// union by smaller id, so leaders are comparable with the program's min-leader.
    fn union(&mut self, a: u64, b: u64) -> bool {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb { return false; }
        let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
        self.parent.insert(hi, lo);
        true
    }
    fn canon(&mut self, t: Term) -> Term {
        match t {
            Term::Add(a, b) => Term::Add(self.find(a), self.find(b)),
            Term::Mul(a, b) => Term::Mul(self.find(a), self.find(b)),
            Term::Neg(a) => Term::Neg(self.find(a)),
            leaf => leaf,
        }
    }
    /// the node for a canonical term, minted if absent.
    fn mint(&mut self, t: Term) -> u64 {
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        for id in ids {
            if self.canon(self.nodes[&id]) == t { return id; }
        }
        let id = self.next_id;
        self.next_id += 1;
        self.nodes.insert(id, t);
        id
    }
    fn const_of(&mut self, class: u64) -> Option<u64> {
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        let mut best = None;
        for id in ids {
            if let Term::Num(n) = self.nodes[&id] {
                if self.find(id) == class { best = Some(best.map_or(n, |b: u64| b.min(n))); }
            }
        }
        best
    }
    fn saturate(&mut self) {
        loop {
            let mut changed = false;
            // congruence: equal canonical signatures merge
            let mut by_sig: HashMap<Term, u64> = HashMap::new();
            let ids: Vec<u64> = self.nodes.keys().copied().collect();
            for &id in &ids {
                let sig = self.canon(self.nodes[&id]);
                match by_sig.get(&sig) {
                    Some(&other) => changed |= self.union(id, other),
                    None => { by_sig.insert(sig, id); }
                }
            }
            // rules
            for &id in &ids {
                match self.canon(self.nodes[&id]) {
                    Term::Add(a, b) => {
                        let m = self.mint(Term::Add(b, a));
                        changed |= self.union(id, m);
                        if let (Some(x), Some(y)) = (self.const_of(a), self.const_of(b)) {
                            let m = self.mint(Term::Num(x.wrapping_add(y)));
                            changed |= self.union(id, m);
                        }
                        if self.const_of(b) == Some(0) { changed |= self.union(id, a); }
                    }
                    Term::Mul(a, b) => {
                        let m = self.mint(Term::Mul(b, a));
                        changed |= self.union(id, m);
                        if let (Some(x), Some(y)) = (self.const_of(a), self.const_of(b)) {
                            let m = self.mint(Term::Num(x.wrapping_mul(y)));
                            changed |= self.union(id, m);
                        }
                        if self.const_of(b) == Some(1) { changed |= self.union(id, a); }
                        if self.const_of(b) == Some(0) { changed |= self.union(id, b); }
                    }
                    Term::Neg(a) => {
                        for &other in &ids {
                            if let Term::Neg(c) = self.canon(self.nodes[&other]) {
                                if self.find(other) == a { changed |= self.union(id, c); }
                            }
                        }
                    }
                    _ => {}
                }
            }
            if !changed { break; }
        }
    }
    /// cost per class: min over its nodes of 1 + the children's class costs, to a fixpoint.
    fn costs(&mut self) -> HashMap<u64, u64> {
        let mut cost: HashMap<u64, u64> = HashMap::new();
        loop {
            let mut changed = false;
            let ids: Vec<u64> = self.nodes.keys().copied().collect();
            for id in ids {
                let c = match self.canon(self.nodes[&id]) {
                    Term::Num(_) | Term::Var(_) => Some(1),
                    Term::Add(a, b) | Term::Mul(a, b) => cost.get(&a).and_then(|x| cost.get(&b).map(|y| 1 + x + y)),
                    Term::Neg(a) => cost.get(&a).map(|x| 1 + x),
                };
                if let Some(c) = c {
                    let class = self.find(id);
                    let cur = cost.get(&class).copied();
                    if cur.map_or(true, |k| c < k) {
                        cost.insert(class, c);
                        changed = true;
                    }
                }
            }
            if !changed { break; }
        }
        cost
    }
}

fn ints(v: &Value) -> Vec<i64> {
    match v {
        Value::Tuple(xs) => xs.iter().map(|x| x.as_int()).collect(),
        Value::Int(n) => vec![*n],
        other => panic!("expected ints, got {other:?}"),
    }
}

#[test]
fn egraph_math_matches_reference() {
    let path = format!("{}/tests/programs/egraph_math.ddp", env!("CARGO_MANIFEST_DIR"));
    let mut tree = lower::lower_tree(parse::pipe::parse(&interactive::load_program(&path)));
    tree.optimize();
    for seed in 1..=6u64 {
        let n = 40;
        let rows = egraph_rows(n, seed);
        let out = vec::evaluate(&tree, &[rows.clone()]);

        // the reference over the same DAG
        let mut r = Ref { nodes: BTreeMap::new(), parent: HashMap::new(), next_id: 1 << 40 };
        for (k, _) in &rows {
            let f = ints(k);
            let (id, op, x, y) = (f[0] as u64, f[1], f[2] as u64, f[3] as u64);
            let t = match op { 0 => Term::Num(x), 1 => Term::Var(x), 2 => Term::Add(x, y), 3 => Term::Mul(x, y), _ => Term::Neg(x) };
            r.nodes.insert(id, t);
        }
        r.saturate();
        let ref_cost = r.costs();

        // partition of the input nodes: same leader in the program iff same class in the reference
        let classes: HashMap<u64, u64> = out["classes"]
            .iter()
            .filter(|(_, d)| *d > 0)
            .map(|((k, v), _)| (ints(k)[0] as u64, ints(v)[0] as u64))
            .collect();
        for i in 0..n {
            assert!(classes.contains_key(&i), "seed {seed}: input node {i} has no class");
            for j in 0..n {
                let same_prog = classes[&i] == classes[&j];
                let same_ref = r.find(i) == r.find(j);
                assert_eq!(same_prog, same_ref, "seed {seed}: nodes {i} and {j} — program says same={same_prog}, reference says same={same_ref}");
            }
        }
        // costs of the input nodes' classes agree
        let costs: HashMap<u64, u64> = out["cost"].iter().filter(|(_, d)| *d > 0).map(|((k, v), _)| (ints(k)[0] as u64, ints(v)[0] as u64)).collect();
        for i in 0..n {
            let leader = classes[&i];
            let want = ref_cost[&r.find(i)];
            assert_eq!(costs.get(&leader).copied(), Some(want), "seed {seed}: cost of node {i}'s class");
        }
        // no minted name collided
        assert!(out["collisions"].iter().all(|(_, d)| *d <= 0), "seed {seed}: a minted name collided");
    }
}
