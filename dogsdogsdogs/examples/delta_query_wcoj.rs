use timely::dataflow::operators::probe::Handle;
use differential_dataflow::input::Input;
use graph_map::GraphMMap;

use timely::progress::Antichain;

use differential_dogs3::operators::Cut;
use differential_dogs3::CollectionIndex;
use differential_dogs3::ProposeExtensionMethod;

/// `Cut::Before` is strict, and logical compaction destroys strictness, so the bound must sit
/// strictly below every time still held.
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

            // Q(a,b,c) :=  E1(a,b),  E2(b,c),  E3(a,c)
            //
            // One delta query per relation, driven by changes to that relation. Relations are
            // sequenced so each introduces a variable sharing a bound attribute with the
            // running prefix, and directed { forward, reverse } by whether the bound variable
            // occurs first or second.
            //
            // Which side of a tie each atom reads at is derived from the atoms' positions
            // (E1 = 0, E2 = 1, E3 = 2), so two indices suffice. Encoding the same distinction
            // in the timestamp needed an "old" and a "new" copy of each, and four indices.
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
