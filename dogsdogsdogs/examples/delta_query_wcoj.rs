use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::vec::Map;
use differential_dataflow::AsCollection;
use differential_dataflow::input::Input;
use graph_map::GraphMMap;

use differential_dogs3::{CollectionIndex, altneu::AltNeu};
use differential_dogs3::{ProposeExtensionMethod};

fn main() {

    // snag a filename to use for the input graph.
    let filename = std::env::args().nth(1).unwrap();
    let batching = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
    let inspect = std::env::args().any(|x| x == "inspect");

    timely::execute_from_args(std::env::args().skip(2), move |worker| {

        let timer = std::time::Instant::now();
        let graph = GraphMMap::new(&filename);

        let peers = worker.peers();
        let index = worker.index();

        let mut probe = Handle::new();

        let mut input = worker.dataflow::<usize,_,_>(|scope| {

            let (edges_input, edges) = scope.new_collection();

            let forward = edges.clone();
            let reverse = edges.map(|(x,y)| (y,x));

            // Q(a,b,c) :=  E1(a,b),  E2(b,c),  E3(a,c)
            let triangles = scope.scoped::<AltNeu<usize>,_,_>("DeltaQuery (Triangles)", |inner| {

                // Each relation we'll need.
                let forward = forward.enter(inner);
                let reverse = reverse.enter(inner);

                // Hold compaction back one base-time step, so that the alt/neu distinction
                // survives, and compare in the total order on `AltNeu` (lexicographic on
                // `(time, neu)`). Delta streams enter at `alt`, so an `alt` arrangement is
                // seen at the delta's own time and a `neu` one only strictly after it: the
                // tag is what makes the comparison strict or not.
                let frontier_func = |time: &AltNeu<usize>, antichain: &mut timely::progress::Antichain<AltNeu<usize>>| {
                    antichain.insert(AltNeu::alt(time.time.saturating_sub(1)));
                };
                let comparison = |t1: &AltNeu<usize>, t2: &AltNeu<usize>| t1 <= t2;

                // Without using wrappers yet, maintain an "old" and a "new" copy of edges.
                let alt_forward = CollectionIndex::index(forward.clone(), frontier_func, comparison);
                let alt_reverse = CollectionIndex::index(reverse.clone(), frontier_func, comparison);
                let neu_forward = CollectionIndex::index(forward.clone().delay(|time| AltNeu::neu(time.time.clone())), frontier_func, comparison);
                let neu_reverse = CollectionIndex::index(reverse.clone().delay(|time| AltNeu::neu(time.time.clone())), frontier_func, comparison);

                // Stash each delta's own time as its payload, to be advanced by the times of
                // the records it matches, and delayed to once we leave the delta region.
                let deltas = forward.inner.map(|(d, t, r)| ((d, t.clone()), t, r)).as_collection();

                // For each relation, we form a delta query driven by changes to that relation.
                //
                // The sequence of joined relations are such that we only introduce relations
                // which share some bound attributes with the current stream of deltas.
                // Each joined relation is delayed { alt -> neu } if its position in the
                // sequence is greater than the delta stream.
                // Each joined relation is directed { forward, reverse } by whether the
                // bound variable occurs in the first or second position.

                //   dQ/dE1 := dE1(a,b), E2(b,c), E3(a,c)
                let changes1 =
                deltas
                    .clone()
                    .extend(&mut [
                        &mut neu_forward.extend_using(|(_a,b)| *b),
                        &mut neu_forward.extend_using(|(a,_b)| *a),
                    ])
                    .map(|(((a,b),c), payload)| ((a,b,c), payload));

                //   dQ/dE2 := dE2(b,c), E1(a,b), E3(a,c)
                let changes2 =
                deltas
                    .clone()
                    .extend(&mut [
                        &mut alt_reverse.extend_using(|(b,_c)| *b),
                        &mut neu_reverse.extend_using(|(_b,c)| *c),
                    ])
                    .map(|(((b,c),a), payload)| ((a,b,c), payload));

                //   dQ/dE3 := dE3(a,c), E1(a,b), E2(b,c)
                let changes3 = deltas
                    .extend(&mut [
                        &mut alt_forward.extend_using(|(a,_c)| *a),
                        &mut alt_reverse.extend_using(|(_a,c)| *c),
                    ])
                    .map(|(((a,c),b), payload)| ((a,b,c), payload));

                // Delay updates to the payload time worked out while extending.
                changes1.concat(changes2).concat(changes3)
                    .inner.map(|((d, payload), _time, r)| (d, payload, r)).as_collection()
                    .leave(scope)
            });

            triangles
                .filter(move |_| inspect)
                .inspect(|x| println!("\tTriangle: {:?}", x))
                .probe_with(&mut probe);

            edges_input
        });

        let mut index = index;
        while index < graph.nodes() {
            input.advance_to(index);
            for &edge in graph.edges(index) {
                input.insert((index as u32, edge));
            }
            index += peers;
            input.advance_to(index);
            input.flush();
            if (index / peers) % batching == 0 {
                while probe.less_than(input.time()) {
                    worker.step();
                }
                println!("{:?}\tRound {} complete", timer.elapsed(), index);
            }
        }

    }).unwrap();
}
