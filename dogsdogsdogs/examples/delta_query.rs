use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::vec::Map;
use differential_dataflow::AsCollection;
use differential_dataflow::input::Input;
use graph_map::GraphMMap;


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

            // Graph oriented both ways, indexed by key.
            let forward_key = edges.clone().arrange_by_key();
            let reverse_key = edges.clone().map(|(x,y)| (y,x))
                                   .arrange_by_key();

            // Graph oriented both ways, indexed by (key, val).
            let forward_self = edges.clone().arrange_by_self();
            let reverse_self = edges.clone().map(|(x,y)| (y,x))
                                    .arrange_by_self();

            // // Graph oriented both ways, counts of distinct vals for each key.
            // // Not required without worst-case-optimal join strategy.
            // let forward_count = edges.map(|(x,y)| x).arrange_by_self();
            // let reverse_count = edges.map(|(x,y)| y).arrange_by_self();

            // Q(a,b,c) :=  E1(a,b),  E2(b,c),  E3(a,c)
            //
            // For each relation, we form a delta query driven by changes to that relation.
            //
            // The sequence of joined relations are such that we only introduce relations
            // which share some bound attributes with the current stream of deltas.
            // Each joined relation is directed { forward, reverse } by whether the
            // bound variable occurs in the first or second position.
            //
            // Each lookup is strict exactly when it reaches a relation later in the sequence
            // than the delta stream, which is what stops a pair of updates matching twice.

            let key1 = |x: &(u32, u32)| x.0;
            let key2 = |x: &(u32, u32)| x.1;

            use differential_dogs3::operators::propose;
            use differential_dogs3::operators::validate;

            // Hold compaction back one step, so that we do not lose the distinction between
            // "strictly before" and "at the same time".
            let frontier_func = |time: &usize, antichain: &mut timely::progress::Antichain<usize>| {
                antichain.insert(time.saturating_sub(1));
            };

            // Stash each delta's own time as its payload, to be advanced by the times of the
            // records it matches, and delayed to once we are done extending.
            let deltas = edges.clone().inner.map(|(d, t, r)| ((d, t.clone()), t, r)).as_collection();

            //   dQ/dE1 := dE1(a,b), E2(b,c), E3(a,c)
            let changes1 = propose(deltas.clone(), forward_key.clone(), key2.clone(), frontier_func, true);
            let changes1 = validate(changes1, forward_self.clone(), key1.clone(), frontier_func, true);
            let changes1 = changes1.map(|(((a,b),c), payload)| ((a,b,c), payload));

            //   dQ/dE2 := dE2(b,c), E1(a,b), E3(a,c)
            let changes2 = propose(deltas.clone(), reverse_key.clone(), key1.clone(), frontier_func, false);
            let changes2 = validate(changes2, reverse_self.clone(), key2.clone(), frontier_func, true);
            let changes2 = changes2.map(|(((b,c),a), payload)| ((a,b,c), payload));

            //   dQ/dE3 := dE3(a,c), E1(a,b), E2(b,c)
            let changes3 = propose(deltas, forward_key.clone(), key1.clone(), frontier_func, false);
            let changes3 = validate(changes3, reverse_self.clone(), key2.clone(), frontier_func, false);
            let changes3 = changes3.map(|(((a,c),b), payload)| ((a,b,c), payload));

            // Delay updates to the payload time worked out while extending.
            let triangles_prev = changes1.concat(changes2).concat(changes3)
                .inner.map(|((d, payload), _time, r)| (d, payload, r)).as_collection();

            // The same query as a conventional three-way join, which shares no machinery with
            // the delta fragments above and so is an independent answer rather than a second
            // opinion from the same method.
            let triangles_next =
            edges
                .map(|(x,y)| (y,x))
                .join_core(forward_key, |b,a,c| Some(((*a, *c), *b)))
                .join_core(forward_self, |(a,c), b, &()| Some((*a,*b,*c)));

            // Test that the concatenated delta fragments equal the conventional join.
            triangles_prev.clone().assert_eq(triangles_next);

            triangles_prev
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
