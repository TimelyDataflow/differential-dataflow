//! Property test: a three-atom worst-case-optimal delta join, against the same oracle.
//!
//! Where `delta_join_property` checks two atoms joined by `half_join`, this checks three atoms
//! joined by the `count` / `propose` / `validate` trio — the machinery that sizes each atom,
//! routes each prefix to the atom offering fewest extensions, and semijoins against the rest.
//!
//! The query is a triangle over three *independent* relations, `Q(a,b,c) :- R(a,b), S(b,c),
//! T(a,c)`, evaluated as three delta rules, one seeded by each relation. Independent relations
//! rather than one edge set in three orientations means a wrong cut cannot accidentally cancel
//! against itself.
//!
//! Times are `Product<usize, usize>`, so cuts admit updates incomparable to the request and the
//! carried join time is load-bearing: an output tuple exists at the join of all three
//! contributing times, which is not any one of them. Under the previous accumulate-and-emit-at-
//! `initial` behavior this test could not pass.

use std::sync::{Arc, Mutex};

use timely::order::Product;
use timely::progress::Antichain;
use timely::dataflow::operators::vec::UnorderedInput;
use timely::dataflow::operators::vec::unordered_input::UnorderedHandle;

use differential_dataflow::AsCollection;
use differential_dataflow::lattice::Lattice;
use differential_dogs3::{CollectionIndex, ProposeExtensionMethod};
use differential_dogs3::operators::Cut;

type Time = Product<usize, usize>;
/// `((src, dst), time, diff)` — one update of one relation.
type Edge = ((u32, u32), Time, isize);
/// `((a, b, c), time, diff)` — one update of the triangle.
type Triangle = ((u32, u32, u32), Time, isize);

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 { self.next() % n }
}

/// A small domain, so triangles actually close, and a small time grid, so times tie and sit
/// incomparable often.
fn generate(rng: &mut Rng, count: usize) -> Vec<Edge> {
    (0..count)
        .map(|_| {
            let src = rng.below(3) as u32;
            let dst = rng.below(3) as u32;
            let outer = rng.below(3) as usize;
            let inner = rng.below(3) as usize;
            let diff = if rng.below(4) == 0 { -1 } else { 1 };
            ((src, dst), Product::new(outer, inner), diff)
        })
        .collect()
}

fn consolidate(mut updates: Vec<Triangle>) -> Vec<Triangle> {
    updates.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
    let mut out: Vec<Triangle> = Vec::new();
    for (data, time, diff) in updates {
        match out.last_mut() {
            Some(last) if last.0 == data && last.1 == time => last.2 += diff,
            _ => out.push((data, time, diff)),
        }
    }
    out.retain(|(_, _, diff)| *diff != 0);
    out
}

/// Brute force: every triple of updates that closes a triangle, once, at the join of its three
/// times, with the product of its three diffs.
fn oracle(r: &[Edge], s: &[Edge], t: &[Edge]) -> Vec<Triangle> {
    let mut out = Vec::new();
    for ((a, b), tr, dr) in r {
        for ((b2, c), ts, ds) in s {
            if b2 != b { continue; }
            for ((a2, c2), tt, dt) in t {
                if a2 == a && c2 == c {
                    out.push(((*a, *b, *c), tr.join(ts).join(tt), dr * ds * dt));
                }
            }
        }
    }
    consolidate(out)
}

/// `Cut::Before` is strict, so the compaction bound must step strictly back.
fn step_back(time: &Time, antichain: &mut Antichain<Time>) {
    antichain.insert(Product::new(time.outer.saturating_sub(1), time.inner.saturating_sub(1)));
}

/// The three-rule worst-case-optimal triangle join.
fn wcoj_triangle(workers: usize, r: Vec<Edge>, s: Vec<Edge>, t: Vec<Edge>) -> Vec<Triangle> {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);

    timely::execute(timely::Config::process(workers), move |worker| {
        let is_source = worker.index() == 0;
        let (sink, r, s, t) = (Arc::clone(&sink), r.clone(), s.clone(), t.clone());

        let (mut ir, mut is, mut it, cr, cs, ct) = worker.dataflow::<usize, _, _>(|scope| {
            scope.scoped("Delta", |inner| {
                let ((in_r, cap_r), data_r): ((UnorderedHandle<Time, Edge>, _), _) = inner.new_unordered_input();
                let ((in_s, cap_s), data_s): ((UnorderedHandle<Time, Edge>, _), _) = inner.new_unordered_input();
                let ((in_t, cap_t), data_t): ((UnorderedHandle<Time, Edge>, _), _) = inner.new_unordered_input();

                let rel_r = data_r.as_collection();
                let rel_s = data_s.as_collection();
                let rel_t = data_t.as_collection();

                // Six indices: each relation keyed on either of its two columns.
                let r_by_a = CollectionIndex::index(rel_r.clone());
                let r_by_b = CollectionIndex::index(rel_r.clone().map(|(a, b)| (b, a)));
                let s_by_b = CollectionIndex::index(rel_s.clone());
                let s_by_c = CollectionIndex::index(rel_s.clone().map(|(b, c)| (c, b)));
                let t_by_a = CollectionIndex::index(rel_t.clone());
                let t_by_c = CollectionIndex::index(rel_t.clone().map(|(a, c)| (c, a)));

                // Atom positions: R = 0, S = 1, T = 2. Every cut is derived from the pair.
                //   rule 0 seeds R, binds (a,b), extends by c using S (on b) and T (on a)
                //   rule 1 seeds S, binds (b,c), extends by a using R (on b) and T (on c)
                //   rule 2 seeds T, binds (a,c), extends by b using R (on a) and S (on c)
                let rule0 = rel_r.clone()
                    .extend(&mut [
                        &mut s_by_b.extend_using(|&(_a, b): &(u32, u32)| b, Cut::for_positions(0, 1), step_back),
                        &mut t_by_a.extend_using(|&(a, _b): &(u32, u32)| a, Cut::for_positions(0, 2), step_back),
                    ])
                    .map(|((a, b), c)| (a, b, c));

                let rule1 = rel_s.clone()
                    .extend(&mut [
                        &mut r_by_b.extend_using(|&(b, _c): &(u32, u32)| b, Cut::for_positions(1, 0), step_back),
                        &mut t_by_c.extend_using(|&(_b, c): &(u32, u32)| c, Cut::for_positions(1, 2), step_back),
                    ])
                    .map(|((b, c), a)| (a, b, c));

                let rule2 = rel_t.clone()
                    .extend(&mut [
                        &mut r_by_a.extend_using(|&(a, _c): &(u32, u32)| a, Cut::for_positions(2, 0), step_back),
                        &mut s_by_c.extend_using(|&(_a, c): &(u32, u32)| c, Cut::for_positions(2, 1), step_back),
                    ])
                    .map(|((a, c), b)| (a, b, c));

                rule0.concat(rule1).concat(rule2)
                    .inspect(move |x: &Triangle| sink.lock().unwrap().push(*x));

                (in_r, in_s, in_t, cap_r, cap_s, cap_t)
            })
        });

        if is_source {
            for (handle, cap, updates) in [(&mut ir, &cr, &r), (&mut is, &cs, &s), (&mut it, &ct, &t)] {
                let mut activated = handle.activate();
                let mut session = activated.session(cap);
                for update in updates.iter() { session.give(*update); }
            }
        }
    })
    .unwrap();

    let captured = Arc::try_unwrap(captured).unwrap().into_inner().unwrap();
    consolidate(captured)
}

#[test]
fn wcoj_triangle_matches_brute_force() {
    for workers in [1, 4] {
        let mut rng = Rng(0xc0ff_ee00_1234_5678);
        for instance in 0..25 {
            let r = generate(&mut rng, 5);
            let s = generate(&mut rng, 5);
            let t = generate(&mut rng, 5);

            let expected = oracle(&r, &s, &t);
            let actual = wcoj_triangle(workers, r.clone(), s.clone(), t.clone());

            assert_eq!(
                actual, expected,
                "instance {instance} at {workers} worker(s)\n  R: {r:?}\n  S: {s:?}\n  T: {t:?}",
            );
        }
    }
}

