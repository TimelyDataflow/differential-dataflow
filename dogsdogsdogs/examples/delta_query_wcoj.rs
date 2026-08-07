use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::vec::Map;
use differential_dataflow::AsCollection;
use differential_dataflow::input::Input;
use graph_map::GraphMMap;

use differential_dogs3::CollectionIndex;
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

            // Hold compaction back one step, so that we do not lose the distinction between
            // "strictly before" and "at the same time".
            let frontier_func = |time: &usize, antichain: &mut timely::progress::Antichain<usize>| {
                antichain.insert(time.saturating_sub(1));
            };

            // One index per orientation. The "old" and "new" copies these replace differed
            // only in whether a lookup could see updates at the delta's own time, which each
            // `extend_using` below now states for itself.
            let index_forward = CollectionIndex::index(forward.clone(), frontier_func);
            let index_reverse = CollectionIndex::index(reverse.clone(), frontier_func);

            // Stash each delta's own time as its payload, to be advanced by the times of the
            // records it matches, and delayed to once we are done extending.
            let deltas = forward.inner.map(|(d, t, r)| ((d, t.clone()), t, r)).as_collection();

            // For each relation, we form a delta query driven by changes to that relation.
            //
            // The sequence of joined relations are such that we only introduce relations
            // which share some bound attributes with the current stream of deltas.
            // Each lookup is strict exactly when it reaches a relation later in the sequence
            // than the delta stream, which is what stops a pair of updates matching twice.
            // Each joined relation is directed { forward, reverse } by whether the
            // bound variable occurs in the first or second position.

            //   dQ/dE1 := dE1(a,b), E2(b,c), E3(a,c)
            let changes1 =
            deltas
                .clone()
                .extend(&mut [
                    &mut index_forward.extend_using(|(_a,b)| *b, true),
                    &mut index_forward.extend_using(|(a,_b)| *a, true),
                ])
                .map(|(((a,b),c), payload)| ((a,b,c), payload));

            //   dQ/dE2 := dE2(b,c), E1(a,b), E3(a,c)
            let changes2 =
            deltas
                .clone()
                .extend(&mut [
                    &mut index_reverse.extend_using(|(b,_c)| *b, false),
                    &mut index_reverse.extend_using(|(_b,c)| *c, true),
                ])
                .map(|(((b,c),a), payload)| ((a,b,c), payload));

            //   dQ/dE3 := dE3(a,c), E1(a,b), E2(b,c)
            let changes3 = deltas
                .extend(&mut [
                    &mut index_forward.extend_using(|(a,_c)| *a, false),
                    &mut index_reverse.extend_using(|(_a,c)| *c, false),
                ])
                .map(|(((a,c),b), payload)| ((a,b,c), payload));

            // Delay updates to the payload time worked out while extending.
            let triangles = changes1.concat(changes2).concat(changes3)
                .inner.map(|((d, payload), _time, r)| (d, payload, r)).as_collection();

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
