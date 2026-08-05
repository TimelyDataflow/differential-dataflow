//! Regression test for the `lookup_map` off-by-one (`key_con.index(1)` -> `index(0)`).
//!
//! `lookup_map` stages the probe key into a capacity-1 `KeyContainer`, pushes one
//! element (valid index 0), and (before the fix) reads it back at index 1 — out of
//! bounds, panicking on *every* probe. This test drives a triangle prefix-extension
//! join (`extend` -> `propose`/`count`/`validate` -> `lookup_map`) over a single
//! in-memory triangle: before the fix it panics; after the fix it finds (0, 1, 2).

use std::sync::{Arc, Mutex};

use timely::dataflow::operators::probe::Handle;
use timely::progress::Antichain;
use differential_dataflow::input::Input;

use differential_dogs3::{CollectionIndex, ProposeExtensionMethod};
use differential_dogs3::operators::Cut;

/// `Cut::Before` is strict, and logical compaction destroys strictness, so the bound must sit
/// strictly below every time still held.
fn step_back(time: &usize, antichain: &mut Antichain<usize>) {
    antichain.insert(time.saturating_sub(1));
}

#[test]
fn lookup_map_triangle_wcoj_finds_triangle() {
    let found: Arc<Mutex<Vec<(u32, u32, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let found_outer = found.clone();

    timely::execute(timely::Config::thread(), move |worker| {
        let found_outer = found_outer.clone();
        let mut probe = Handle::new();

        let mut input = worker.dataflow::<usize, _, _>(|scope| {
            let (input, edges) = scope.new_collection::<(u32, u32), isize>();

            // Q(a,b,c) := E1(a,b), E2(b,c), E3(a,c), via the dogsdogsdogs WCOJ `extend`
            // path, which routes through propose/count/validate -> lookup_map.
            //
            // Atom positions are E1 = 0, E2 = 1, E3 = 2, and every cut is derived from the
            // pair of positions rather than encoded in the timestamp. Two indices suffice —
            // one per orientation — where the alt/neu encoding needed four.
            let forward = CollectionIndex::index(edges.clone());
            let reverse = CollectionIndex::index(edges.clone().map(|(x, y)| (y, x)));

            //   dQ/dE1 := dE1(a,b), E2(b,c), E3(a,c);  bind (a,b), extend by c
            let changes1 = edges.clone()
                .extend(&mut [
                    &mut forward.extend_using(|(_a, b): &(u32, u32)| *b, Cut::for_positions(0, 1), step_back),
                    &mut forward.extend_using(|(a, _b): &(u32, u32)| *a, Cut::for_positions(0, 2), step_back),
                ])
                .map(|((a, b), c)| (a, b, c));

            //   dQ/dE2 := dE2(b,c), E1(a,b), E3(a,c);  bind (b,c), extend by a
            let changes2 = edges.clone()
                .extend(&mut [
                    &mut reverse.extend_using(|(b, _c): &(u32, u32)| *b, Cut::for_positions(1, 0), step_back),
                    &mut reverse.extend_using(|(_b, c): &(u32, u32)| *c, Cut::for_positions(1, 2), step_back),
                ])
                .map(|((b, c), a)| (a, b, c));

            //   dQ/dE3 := dE3(a,c), E1(a,b), E2(b,c);  bind (a,c), extend by b
            let changes3 = edges
                .extend(&mut [
                    &mut forward.extend_using(|(a, _c): &(u32, u32)| *a, Cut::for_positions(2, 0), step_back),
                    &mut reverse.extend_using(|(_a, c): &(u32, u32)| *c, Cut::for_positions(2, 1), step_back),
                ])
                .map(|((a, c), b)| (a, b, c));

            let triangles = changes1.concat(changes2).concat(changes3);

            triangles
                .inspect_batch(move |_t, xs| {
                    let mut v = found_outer.lock().unwrap();
                    // Only collect records present with positive multiplicity, so a
                    // retraction (negative diff) or a net-zero update can't make the
                    // test pass and weaken the regression signal.
                    for (data, _t, r) in xs {
                        if *r > 0 {
                            v.push(*data);
                        }
                    }
                })
                .probe_with(&mut probe);

            input
        });

        // A single triangle: directed edges 0->1, 1->2, 0->2.
        input.advance_to(0);
        input.insert((0, 1));
        input.insert((1, 2));
        input.insert((0, 2));
        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) {
            worker.step();
        }
    })
    .unwrap();

    let mut got = found.lock().unwrap().clone();
    got.sort();
    got.dedup();
    assert_eq!(
        got,
        vec![(0, 1, 2)],
        "expected exactly the triangle (0,1,2); got {:?}",
        got
    );
}
