//! Regression test for the `lookup_map` off-by-one (`key_con.index(1)` -> `index(0)`).
//!
//! `lookup_map` stages the probe key into a capacity-1 `KeyContainer`, pushes one
//! element (valid index 0), and (before the fix) reads it back at index 1 — out of
//! bounds, panicking on *every* probe. This test drives a triangle worst-case-optimal
//! join (`extend` -> `propose`/`count`/`validate` -> `lookup_map`) over a single
//! in-memory triangle: before the fix it panics; after the fix it finds (0, 1, 2).

use std::sync::{Arc, Mutex};

use timely::dataflow::operators::probe::Handle;
use differential_dataflow::input::Input;

use differential_dogs3::{CollectionIndex, altneu::AltNeu, ProposeExtensionMethod};

#[test]
fn lookup_map_triangle_wcoj_finds_triangle() {
    let found: Arc<Mutex<Vec<(u32, u32, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let found_outer = found.clone();

    timely::execute(timely::Config::thread(), move |worker| {
        let found_outer = found_outer.clone();
        let mut probe = Handle::new();

        let mut input = worker.dataflow::<usize, _, _>(|scope| {
            let (input, edges) = scope.new_collection::<(u32, u32), isize>();

            let forward = edges.clone();
            let reverse = edges.map(|(x, y)| (y, x));

            // Q(a,b,c) := E1(a,b), E2(b,c), E3(a,c), via the dogsdogsdogs WCOJ `extend`
            // path, which routes through propose/count/validate -> lookup_map.
            let triangles = scope.scoped::<AltNeu<usize>, _, _>("Triangles", |inner| {
                let forward = forward.enter(inner);
                let reverse = reverse.enter(inner);

                let alt_forward = CollectionIndex::index(forward.clone());
                let alt_reverse = CollectionIndex::index(reverse.clone());
                let neu_forward = CollectionIndex::index(forward.clone().delay(|t| AltNeu::neu(t.time.clone())));
                let neu_reverse = CollectionIndex::index(reverse.clone().delay(|t| AltNeu::neu(t.time.clone())));

                //   dQ/dE1 := dE1(a,b), E2(b,c), E3(a,c)
                let changes1 = forward.clone()
                    .extend(&mut [
                        &mut neu_forward.extend_using(|(_a, b)| *b),
                        &mut neu_forward.extend_using(|(a, _b)| *a),
                    ])
                    .map(|((a, b), c)| (a, b, c));

                //   dQ/dE2 := dE2(b,c), E1(a,b), E3(a,c)
                let changes2 = forward.clone()
                    .extend(&mut [
                        &mut alt_reverse.extend_using(|(b, _c)| *b),
                        &mut neu_reverse.extend_using(|(_b, c)| *c),
                    ])
                    .map(|((b, c), a)| (a, b, c));

                //   dQ/dE3 := dE3(a,c), E1(a,b), E2(b,c)
                let changes3 = forward
                    .extend(&mut [
                        &mut alt_forward.extend_using(|(a, _c)| *a),
                        &mut alt_reverse.extend_using(|(_a, c)| *c),
                    ])
                    .map(|((a, c), b)| (a, b, c));

                changes1.concat(changes2).concat(changes3).leave(scope)
            });

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
