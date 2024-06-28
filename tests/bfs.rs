use std::fmt::Debug;
use rand::{Rng, SeedableRng, StdRng};

use std::sync::{Arc, Mutex};

use timely::{Config, PartialOrder};
use timely::container::flatcontainer::{MirrorRegion, Push, Region, RegionPreference, ReserveItems};

use timely::dataflow::*;
use timely::dataflow::operators::Capture;
use timely::dataflow::operators::capture::Extract;
use timely::order::{FlatProductRegion, Product};

use differential_dataflow::input::Input;
use differential_dataflow::Collection;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::trace::implementations::ord_neu::{FlatKeySpineDefault, FlatValSpineDefault};

type Node = usize;
type Edge = (Node, Node);

#[test] fn bfs_10_20_1000() { test_sizes(10, 20, 1000, 3); }
#[test] fn bfs_100_200_10() { test_sizes(100, 200, 10, 3); }
#[test] fn bfs_100_2000_1() { test_sizes(100, 2000, 1, 3); }

fn test_sizes(nodes: usize, edges: usize, rounds: usize, threads: usize) {

    let root_list = vec![(1, 0, 1)];
    let mut edge_list = Vec::new();

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
    let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

    for _ in 0 .. edges {
        edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 0, 1));
    }

    for round in 1 .. rounds {
        edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), round, 1));
        edge_list.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)), round,-1));
    }

    let mut results = [
        bfs_sequential(root_list.clone(), edge_list.clone()),
        bfs_differential(root_list.clone(), edge_list.clone(), Config::process(threads)),
        bfs_differential_flat(root_list.clone(), edge_list.clone(), Config::process(threads)),
    ];

    for results in results.iter_mut() {
        results.sort();
        results.sort_by(|x,y| x.1.cmp(&y.1));
    }

    let results1 = &results[0];
    for other in results.iter().skip(1) {
        if results1 != other {
            println!("RESULTS INEQUAL!!!");
            for x in results1 {
                if !other.contains(x) {
                    println!("  in seq, not diff: {:?}", x);
                }
            }
            for x in other {
                if !results1.contains(x) {
                    println!("  in diff, not seq: {:?}", x);
                }
            }
        }
        assert_eq!(results1, other);
    }
}


fn bfs_sequential(
    root_list: Vec<(usize, usize, isize)>,
    edge_list: Vec<((usize, usize), usize, isize)>)
-> Vec<((usize, usize), usize, isize)> {

    let mut nodes = 0;
    for &(root, _, _) in &root_list {
        nodes = ::std::cmp::max(nodes, root + 1);
    }
    for &((src,dst), _, _) in &edge_list {
        nodes = ::std::cmp::max(nodes, src + 1);
        nodes = ::std::cmp::max(nodes, dst + 1);
    }

    let mut rounds = 0;
    for &(_, time, _) in &root_list { rounds = ::std::cmp::max(rounds, time + 1); }
    for &(_, time, _) in &edge_list { rounds = ::std::cmp::max(rounds, time + 1); }

    let mut counts = vec![0; nodes];
    let mut results = Vec::new();

    for round in 0 .. rounds {

        let mut roots = ::std::collections::HashMap::new();
        for &(root, time, diff) in &root_list {
            if time <= round { *roots.entry(root).or_insert(0) += diff; }
        }

        let mut edges = ::std::collections::HashMap::new();
        for &((src, dst), time, diff) in &edge_list {
            if time <= round { *edges.entry((src, dst)).or_insert(0) += diff; }
        }

        let mut dists = vec![usize::max_value(); nodes];
        for (&key, &val) in roots.iter() {
            if val > 0 { dists[key] = 0; }
        }

        let mut changes = true;
        while changes {
            changes = false;
            for (&(src, dst), &cnt) in edges.iter() {
                if cnt > 0 {
                    if dists[src] != usize::max_value() && dists[dst] > dists[src] + 1 {
                        dists[dst] = dists[src] + 1;
                        changes = true;
                    }
                }
            }
        }

        let mut new_counts = vec![0; nodes];
        for &value in dists.iter() {
            if value != usize::max_value() {
                new_counts[value] += 1;
            }
        }

        for index in 0 .. nodes {
            if new_counts[index] != counts[index] {
                if new_counts[index] != 0 { results.push(((index, new_counts[index]), round, 1)); }
                if counts[index] != 0 { results.push(((index, counts[index]), round,-1)); }
                counts[index] = new_counts[index];
            }
        }
    }

    results
}

fn bfs_differential(
    roots_list: Vec<(usize, usize, isize)>,
    edges_list: Vec<((usize, usize), usize, isize)>,
    config: Config,
)
-> Vec<((usize, usize), usize, isize)>
{

    let (send, recv) = ::std::sync::mpsc::channel();
    let send = Arc::new(Mutex::new(send));

    timely::execute(config, move |worker| {

        let mut roots_list = roots_list.clone();
        let mut edges_list = edges_list.clone();

        // define BFS dataflow; return handles to roots and edges inputs
        let (mut roots, mut edges) = worker.dataflow(|scope| {

            let send = send.lock().unwrap().clone();

            let (root_input, roots) = scope.new_collection();
            let (edge_input, edges) = scope.new_collection();

            bfs(&edges, &roots).map(|(_, dist)| dist)
                               .count()
                               .map(|(x,y)| (x, y as usize))
                               .inner
                               .capture_into(send);

            (root_input, edge_input)
        });

        // sort by decreasing insertion time.
        roots_list.sort_by(|x,y| y.1.cmp(&x.1));
        edges_list.sort_by(|x,y| y.1.cmp(&x.1));

        let mut round = 0;
        while roots_list.len() > 0 || edges_list.len() > 0 {

            while roots_list.last().map(|x| x.1) == Some(round) {
                let (node, _time, diff) = roots_list.pop().unwrap();
                roots.update(node, diff);
            }
            while edges_list.last().map(|x| x.1) == Some(round) {
                let ((src, dst), _time, diff) = edges_list.pop().unwrap();
                edges.update((src, dst), diff);
            }

            round += 1;
            roots.advance_to(round);
            edges.advance_to(round);
        }

    }).unwrap();

    recv.extract()
        .into_iter()
        .flat_map(|(_, list)| list.into_iter().map(|((dst,cnt),time,diff)| ((dst,cnt), time, diff)))
        .collect()
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn bfs<G: Scope>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, (Node, usize)>
where G::Timestamp: Lattice+Ord {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, 0));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());

        inner.join_map(&edges, |_k,l,d| (*d, l+1))
             .concat(&nodes)
             .reduce(|_, s, t| t.push((*s[0].0, 1)))
     })
}

fn bfs_differential_flat(
    roots_list: Vec<(usize, usize, isize)>,
    edges_list: Vec<((usize, usize), usize, isize)>,
    config: Config,
) -> Vec<((usize, usize), usize, isize)> {
    let (send, recv) = std::sync::mpsc::channel();
    let send = Arc::new(Mutex::new(send));

    timely::execute(config, move |worker| {
        let mut roots_list = roots_list.clone();
        let mut edges_list = edges_list.clone();

        // define BFS dataflow; return handles to roots and edges inputs
        let (mut roots, mut edges) = worker.dataflow(|scope| {
            let send = send.lock().unwrap().clone();

            let (root_input, roots) = scope.new_collection();
            let (edge_input, edges) = scope.new_collection();

            let c = bfs_flat(&edges, &roots).map(|(_, dist)| (dist, ()));
            let arranged = c.arrange::<FlatKeySpineDefault<usize, usize, isize, Vec<((usize, ()), _, _)>>>();
            type T2 = FlatValSpineDefault<usize, isize, usize, isize, Vec<((usize, isize), usize, isize)>>;
            let reduced = arranged.reduce_abelian::<_, _, _, T2>("Count", |_k, s, t| {
                t.push((s[0].1.clone(), isize::from(1i8)))
            });
            reduced
                .as_collection(|k, c| (k, c as usize))
                .inner
                .capture_into(send);

            (root_input, edge_input)
        });

        // sort by decreasing insertion time.
        roots_list.sort_by(|x, y| y.1.cmp(&x.1));
        edges_list.sort_by(|x, y| y.1.cmp(&x.1));

        let mut round = 0;
        while roots_list.len() > 0 || edges_list.len() > 0 {
            while roots_list.last().map(|x| x.1) == Some(round) {
                let (node, _time, diff) = roots_list.pop().unwrap();
                roots.update(node, diff);
            }
            while edges_list.last().map(|x| x.1) == Some(round) {
                let ((src, dst), _time, diff) = edges_list.pop().unwrap();
                edges.update((src, dst), diff);
            }

            round += 1;
            roots.advance_to(round);
            edges.advance_to(round);
        }
    })
        .unwrap();

    recv.extract()
        .into_iter()
        .flat_map(|(_, list)| {
            list.into_iter()
                .map(|((dst, cnt), time, diff)| ((dst, cnt), time, diff))
        })
        .collect()
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn bfs_flat<G: Scope>(
    edges: &Collection<G, Edge>,
    roots: &Collection<G, Node>,
) -> Collection<G, (Node, usize)>
where
    G::Timestamp: Lattice + Ord + RegionPreference,
    for<'a> G::Timestamp: PartialOrder<<<G::Timestamp as RegionPreference>::Region as Region>::ReadItem<'a>>,
    <G::Timestamp as RegionPreference>::Region: Region<Owned=G::Timestamp> + Push<G::Timestamp>,
    for<'a> <Product<G::Timestamp, u64> as RegionPreference>::Region: Region<Owned=Product<G::Timestamp, u64>> + Push<<<Product<G::Timestamp, u64> as RegionPreference>::Region as Region>::ReadItem<'a>>,
    <G::Timestamp as RegionPreference>::Region: Clone + Ord,
    for<'a> FlatProductRegion<<G::Timestamp as RegionPreference>::Region, MirrorRegion<u64>>: Push<&'a Product<G::Timestamp, u64>>,
    for<'a> <FlatProductRegion<<G::Timestamp as RegionPreference>::Region, MirrorRegion<u64>> as Region>::ReadItem<'a>: Copy + Ord + Debug,
    Product<G::Timestamp, u64>: for<'a> PartialOrder<<<Product<G::Timestamp, u64> as RegionPreference>::Region as Region>::ReadItem<'a>>,
    for<'a> <<Product<G::Timestamp, u64> as RegionPreference>::Region as Region>::ReadItem<'a>: PartialOrder<Product<G::Timestamp, u64>>,
    for<'a> <Product<G::Timestamp, u64> as RegionPreference>::Region: ReserveItems<<<Product<G::Timestamp, u64> as RegionPreference>::Region as Region>::ReadItem<'a>>,
{
    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, 0));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {
        let edges = edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());

        type Spine<K, V, T, R = isize> = FlatValSpineDefault<K, V, T, R, Vec<((K, V), T, R)>>;
        let arranged1 = inner.arrange::<Spine<Node, Node, Product<G::Timestamp, _>>>();
        let arranged2 = edges.arrange::<Spine<Node, Node, Product<G::Timestamp, _>>>();
        arranged1
            .join_core(&arranged2, move |_k, l, d| Some((d, l + 1)))
            .concat(&nodes)
            .reduce(|_, s, t| t.push((*s[0].0, 1)))
    })
}
