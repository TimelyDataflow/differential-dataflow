use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::vec::Map;
use differential_dataflow::AsCollection;
use differential_dataflow::input::Input;
use graph_map::GraphMMap;

use timely::progress::Antichain;

use differential_dogs3::operators::{propose, validate, Cut};

/// `Cut::Before` is strict, and logical compaction destroys strictness, so the bound must sit
/// strictly below every time still held. Sound for the lax cuts here too, just conservative.
fn step_back(time: &usize, antichain: &mut Antichain<usize>) {
    antichain.insert(time.saturating_sub(1));
}

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
            // One delta query per relation, driven by changes to that relation. Each rule
            // proposes from one atom and validates against the other; which side of a tie
            // each is read at follows from the atoms' positions (E1 = 0, E2 = 1, E3 = 2).
            //
            // These are the raw `propose` / `validate` operators rather than `extend`, so the
            // delta region is bracketed by hand: each update carries its own time as the
            // initial join time while its dataflow timestamp stays the order time the cuts
            // compare against, and the carried time becomes the update's own time on the way
            // out. `extend` does both for you.
            let key1 = |x: &(u32, u32)| x.0;
            let key2 = |x: &(u32, u32)| x.1;

            let seeded = edges.inner.map(|(d, t, r)| ((d, t.clone()), t, r)).as_collection();

            //   dQ/dE1 := dE1(a,b), E2(b,c), E3(a,c)
            let changes1 = propose(seeded.clone(), forward_key.clone(), Cut::for_positions(0, 1), step_back, key2);
            let changes1 = validate(changes1, forward_self.clone(), Cut::for_positions(0, 2), step_back, key1);
            let changes1 = changes1
                .inner.map(|((data, carried), _order, r)| (data, carried, r)).as_collection()
                .map(|((a, b), c)| (a, b, c));

            //   dQ/dE2 := dE2(b,c), E1(a,b), E3(a,c)
            let changes2 = propose(seeded.clone(), reverse_key.clone(), Cut::for_positions(1, 0), step_back, key1);
            let changes2 = validate(changes2, reverse_self.clone(), Cut::for_positions(1, 2), step_back, key2);
            let changes2 = changes2
                .inner.map(|((data, carried), _order, r)| (data, carried, r)).as_collection()
                .map(|((b, c), a)| (a, b, c));

            //   dQ/dE3 := dE3(a,c), E1(a,b), E2(b,c)
            let changes3 = propose(seeded, forward_key, Cut::for_positions(2, 0), step_back, key1);
            let changes3 = validate(changes3, reverse_self, Cut::for_positions(2, 1), step_back, key2);
            let changes3 = changes3
                .inner.map(|((data, carried), _order, r)| (data, carried, r)).as_collection()
                .map(|((a, c), b)| (a, b, c));

            let triangles_prev = changes1.concat(changes2).concat(changes3);

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
