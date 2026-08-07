use timely::dataflow::operators::{ToStream, vec::{Map, Partition, count::Accumulate}, Inspect, Probe};
use timely::dataflow::operators::probe::Handle;
use differential_dataflow::{Collection, AsCollection};
use differential_dataflow::input::Input;
use graph_map::GraphMMap;

use differential_dogs3::{CollectionIndex, PrefixExtender};

fn main() {

    // snag a filename to use for the input graph.
    let filename = std::env::args().nth(1).unwrap();
    let batching = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();

    timely::execute_from_args(std::env::args().skip(2), move |worker| {

        // let timer = std::time::Instant::now();

        let peers = worker.peers();
        let index = worker.index();

        // // What you might do if you used GraphMMap:
        let graph = GraphMMap::new(&filename);
        let nodes = graph.nodes();
        let edges = (0..nodes).filter(move |node| node % peers == index)
                              .flat_map(|node| graph.edges(node).iter().cloned().map(move |dst| (node as u32, dst)))
                              .map(|(src, dst)| ((src, dst), Default::default(), 1))
                              .collect::<Vec<_>>();

        let edges2 = edges.clone();

        println!("loaded {} nodes, {} edges", nodes, edges.len());

        let mut probe = Handle::new();

        // The index and its readers must share a dataflow: the extenders hold scope-bound
        // arrangements rather than exported traces.
        let mut edges = worker.dataflow::<usize,_,_>(|scope| {

            // Hold compaction back one step, and let a prefix see arranged updates at its own
            // time and earlier. The index is static here, so every prefix sees all of it.
            let frontier_func = |time: &usize, antichain: &mut timely::progress::Antichain<usize>| {
                antichain.insert(time.saturating_sub(1));
            };

            let index = CollectionIndex::index(Collection::new(edges.to_stream(scope)), frontier_func);

            let mut index_xz = index.extend_using(|&(ref x, ref _y)| *x, false);
            let mut index_yz = index.extend_using(|&(ref _x, ref y)| *y, false);

            let (edges_input, edges) = scope.new_collection();

            // Stash each prefix's own time as its payload, to be advanced by the times of the
            // records it matches, and delayed to once we are done extending.
            let prefixes = edges.inner.map(|(p, t, r): ((u32, u32), usize, isize)| ((p, t.clone()), t, r)).as_collection();

            // determine stream of (prefix, count, index) indicating relation with fewest extensions.
            let counts  = prefixes.map(|(p, payload)| ((p, usize::MAX, usize::MAX), payload));
            let counts0 = index_xz.count(counts,  0);
            let counts1 = index_yz.count(counts0, 1);

            // partition by index.
            let parts = counts1.inner.partition(2, |(((p, _c, i), payload),t,d)| (i as u64,((p, payload),t,d)));

            // propose extensions using relation based on index.
            let propose0 = index_xz.propose(parts[0].clone().as_collection());
            let propose1 = index_yz.propose(parts[1].clone().as_collection());

            // validate proposals with the other index.
            let validate0 = index_yz.validate(propose0);
            let validate1 = index_xz.validate(propose1);

            validate0
                .concat(validate1)
                // Delay updates to the payload time worked out while extending.
                .inner.map(|((extended, payload), _time, r)| (extended, payload, r))
                .count()
                .inspect(move |x| println!("{:?}", x))
                // .inspect(move |x| println!("{:?}:\t{:?}", timer.elapsed(), x))
                .probe_with(&mut probe);

            edges_input
        });

        let mut index = 0;
        while index < edges2.len() {
            let limit = std::cmp::min(batching, edges2.len() - index);
            for offset in 0 .. limit {
                edges.insert(edges2[index + offset].0);
                edges.advance_to(index + offset + 1);
            }
            index += limit;
            edges.flush();
            while probe.less_than(edges.time()) {
                worker.step();
            }
        }

    }).unwrap();
}
