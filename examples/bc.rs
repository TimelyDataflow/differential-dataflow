extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;
extern crate docopt;

use docopt::Docopt;

use std::thread;

use std::fs::File;
use std::io::{BufRead, BufReader};

use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::{Communicator, ProcessCommunicator, ThreadCommunicator};
use timely::networking::initialize_networking;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::collection_trace::LeastUpperBound;

use differential_dataflow::operators::*;

static USAGE: &'static str = "
Usage: bc [options] [<arguments>...]

Options:
    -w <arg>, --workers <arg>    number of workers per process [default: 1]
    -p <arg>, --processid <arg>  identity of this process      [default: 0]
    -n <arg>, --processes <arg>  number of processes involved  [default: 1]
    -h <arg>, --hosts <arg>      file containing list of host:port for workers
";

fn main () {
    let args = Docopt::new(USAGE).and_then(|dopt| dopt.parse()).unwrap_or_else(|e| e.exit());

    let workers: u64 = if let Ok(threads) = args.get_str("-w").parse() { threads }
                       else { panic!("invalid setting for --workers: {}", args.get_str("-t")) };
    let process_id: u64 = if let Ok(proc_id) = args.get_str("-p").parse() { proc_id }
                          else { panic!("invalid setting for --processid: {}", args.get_str("-p")) };
    let processes: u64 = if let Ok(processes) = args.get_str("-n").parse() { processes }
                         else { panic!("invalid setting for --processes: {}", args.get_str("-n")) };

    println!("Starting bc dataflow with");
    println!("\tworkers:\t{}", workers);
    println!("\tprocesses:\t{}", processes);
    println!("\tprocessid:\t{}", process_id);

    // vector holding communicators to use; one per local worker.
    if processes > 1 {
        println!("Initializing BinaryCommunicator");

        let hosts = args.get_str("-h");
        let addresses: Vec<_> = if hosts != "" {
            let reader = BufReader::new(File::open(hosts).unwrap());
            reader.lines().take(processes as usize).map(|x| x.unwrap()).collect()
        }
        else {
            (0..processes).map(|index| format!("localhost:{}", 2101 + index).to_string()).collect()
        };

        if addresses.len() != processes as usize { panic!("only {} hosts for -p: {}", addresses.len(), processes); }

        let communicators = initialize_networking(addresses, process_id, workers).ok().expect("error initializing networking");

        bc_spawn(communicators);
    }
    else if workers > 1 { bc_spawn(ProcessCommunicator::new_vector(workers)); }
    else { bc_spawn(vec![ThreadCommunicator]); };
}

fn bc_spawn<C: Communicator+Send>(communicators: Vec<C>) {

    let mut guards = Vec::new();
    for communicator in communicators.into_iter() {
        guards.push(thread::Builder::new().name(format!("worker thread {}", communicator.index()))
                                          .spawn(move || thread_main(communicator))
                                          .unwrap());
    }

    for guard in guards { guard.join().unwrap(); }
}

fn thread_main<C: Communicator>(comm: C) {

    let index = comm.index();
    let start = time::precise_time_s();
    let start2 = time::precise_time_s();

    // define a new computational scope, in which to run BFS
    let mut computation = GraphRoot::new(comm);

    // define BFS dataflow; return handles to roots and edges inputs
    let (mut roots, mut graph) = computation.subcomputation(|builder| {

        let (edge_input, graph) = builder.new_input();
        let (node_input, roots) = builder.new_input();

        let edges = graph.map(|((x,y),w)| ((y,x), w)).concat(&graph);

        let dists = bc(&edges, &roots);    // determine distances to each graph node

        dists.consolidate(|x| x.0, |x| x.0)
             .inspect_batch(move |t,b| {
             println!("epoch: {:?}, length: {}, processing: {}", t, b.len(), (time::precise_time_s() - start2) - (t.inner as f64));
            //  for elem in b.iter() {
            //      println!("\t{:?}", elem);
            //  }
        });

        // dists.map(|((_,s),w)| (s,w))        // keep only the distances, not node ids
        //      .consolidate(|x| *x, |x| *x)   // aggregate into one record per distance
        //      .inspect_batch(move |t, x| {   // print up something neat for each update
        //          println!("observed at {:?}:", t);
        //          println!("elapsed: {}s", time::precise_time_s() - (start + t.inner as f64));
        //          for y in x {
        //              println!("\t{:?}", y);
        //          }
        //      });

        (node_input, edge_input)
    });

    let nodes = 100_000u32; // the u32 helps type inference understand what nodes are
    let edges = 200_000;

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
    let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

    println!("performing BFS on {} nodes, {} edges:", nodes, edges);

    // trickle edges in to dataflow
    let mut left = edges;
    while left > 0 {
        let next = std::cmp::min(left, 1000);
        graph.send_at(0, (0..next).map(|_| ((rng1.gen_range(0, nodes),
                                             rng1.gen_range(0, nodes)), 1)));
        computation.step();
        left -= next;
    }

    // start the root set out with roots 0, 1, and 2
    roots.advance_to(0);
    computation.step();
    computation.step();
    computation.step();
    println!("loaded; elapsed: {}s", time::precise_time_s() - start);


        if index == 0 { roots.send_at(0, (0..10).map(|x| (x,1))); }
    roots.advance_to(1);
    roots.close();

    // repeatedly change edges
    if index == 0 {

    let mut round = 0 as u32;

    while computation.step() {
        // once each full second ticks, change an edge
        if time::precise_time_s() - start >= round as f64 {
            // add edges using prior rng; remove edges using fresh rng with the same seed
            let changes = vec![((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1),
                               ((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1)];
            graph.send_at(round, changes.into_iter());
            graph.advance_to(round + 1);
            round += 1;
        }
    }
}

    graph.close();                  // seal the source of edges
    while computation.step() { }    // wind down the computation
}

// returns pairs (n, (r, b, s)) indicating node n can be reached from root r by b in s steps.
// one pair for each shortest path (so, this number can get quite large, but it is in binary)
fn bc<G: GraphBuilder>(edges: &Stream<G, ((u32, u32), i32)>,
                       roots: &Stream<G, (u32 ,i32)>)
                            -> Stream<G, ((u32, u32, u32, u32), i32)>
where G::Timestamp: LeastUpperBound {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|(x,w)| ((x, x, x, 0), w));

    let dists = nodes.iterate(u32::max_value(), |x| x.0, |x| x.0, |dists| {

        let edges = dists.builder().enter(&edges);
        let nodes = dists.builder().enter(&nodes);

        dists.join_u(&edges, |(n,r,_,s)| (n, (r,s)), |e| e, |&n, &(r,s), &d| (d, r, n, s+1))
             .concat(&nodes)
             .group_by(|(n,r,b,s)| ((n,r),(s,b)),
                       |&(n,r,_,_)| (n + r) as u64,
                       |&(n,r)| (n + r) as u64,
                       |&(n,r), &(b,s)| (n,r,b,s),
                       |&(_n,_r), mut s, t| {
                 // keep only shortest paths
                 let ref_s: &(u32, u32) = s.peek().unwrap().0;
                 let min_s = ref_s.0;
                 t.extend(s.take_while(|x| (x.0).0 == min_s).map(|(&(s,b),w)| ((b,s), w)));
             })
             .inspect_batch(|t,b| {
                 println!("iteration: {:?}, length: {}", t, b.len());
                //  for elem in b.iter() {
                //      println!("\t{:?}", elem);
                //  }
             })
     });

     dists
}
