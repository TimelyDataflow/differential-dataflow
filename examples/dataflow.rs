extern crate rand;
// extern crate time;
extern crate timely;
extern crate differential_dataflow;

use std::hash::Hash;
use std::time::Instant;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

type Node = usize;
type Edge = (Node, Node);

fn main() {

    let users: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let topics: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();
    let inspect: bool = std::env::args().nth(4).unwrap() == "inspect";

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args().skip(5), move |computation| {

        let timer = Instant::now();

        // define BFS dataflow; return handles to roots and edges inputs
        let (mut tweets, mut queries, probe) = computation.scoped(|scope| {

            // entries corresponding to (@username, @mention, #topic), but a (u32, u32, u32) instead.
            let (tweet_input, tweets) = scope.new_input(); 
            let tweets = Collection::new(tweets);

            // determine connected components based on mentions.
            let labels = connected_components(&tweets.map(|(u,m,_)| (u,m)));

            // produce pairs (label, topic) for each topic.
            let label_topics = tweets.map(|(u,_,t)| (u,t))
                                     .join_map(&labels, |_,&t,&l| (l,t));

            // group by (l,t) and emit a count for each.
            let counts = label_topics.map(|x| (x,()))
                                     .group(|_,s,t| t.push((s[0].1, 1)));

            let k = 5;
            // retain the k largest counts. negate first to exploit ordering.
            let topk = counts.map(|((l,t), c)| (l, (-c, t)))
                             .group(move |_,s,t| {
                                 t.extend(s[..k].iter().map(|&((_,t),_)| (t,1)));
                             });

            // entries corresponding to a @username, but a u32 instead.
            let (query_input, queries) = scope.new_input(); 
            let queries = Collection::new(queries);

            let label_query = queries.map(|q| (q,()))
                                     .join_map(&labels, |q,_,&l| (l,q.clone()));
 
            let mut query_topics = label_query.join_map(&topk, |k,x,&y| (k.clone(), x.clone(), y));

            if !inspect {
                query_topics = query_topics.filter(|_| false);
            }

            let probe = query_topics.consolidate_by(|&(_,q,_)| q)
                                    .inspect(|&((l,q,t),w)| println!("\t(query: {},\tlabel: {},\ttopic:{}\t(weight: {})", q, l, t, w))
                                    .probe();

            (tweet_input, query_input, probe.0)
        });

        let tweet_seed: &[_] = &[0, 1, 2, computation.index()];
        let mut tweet_rng1: StdRng = SeedableRng::from_seed(tweet_seed);    // rng for edge additions
        let mut tweet_rng2: StdRng = SeedableRng::from_seed(tweet_seed);    // rng for edge deletions

        let query_seed: &[_] = &[1, 2, 3, computation.index()];
        let mut query_rng1: StdRng = SeedableRng::from_seed(query_seed);    // rng for edge additions
        let mut query_rng2: StdRng = SeedableRng::from_seed(query_seed);    // rng for edge deletions

        println!("performing AppealingDataflow with {} users, {} topics:", users, topics);

        for _ in 0 .. users/computation.peers() {
            tweets.send(((tweet_rng1.gen_range(0, users), 
                          tweet_rng1.gen_range(0, users),
                          tweet_rng1.gen_range(0, topics)),1));
        } 

        if computation.index() == 0 {
            queries.send((query_rng1.gen_range(0, users),1));
        }

        println!("loaded; elapsed: {:?}", timer.elapsed());

        tweets.advance_to(1);
        queries.advance_to(1);
        computation.step_while(|| probe.lt(queries.time()));

        println!("stable; elapsed: {:?}", timer.elapsed());

        if batch > 0 {
            let mut changes = Vec::new();
            for wave in 0.. {
                let mut my_batch = batch / computation.peers();
                if computation.index() < (batch % computation.peers()) { 
                    my_batch += 1; 
                }

                for _ in 0..my_batch {
                    changes.push(((tweet_rng1.gen_range(0, users), 
                                   tweet_rng1.gen_range(0, users),
                                   tweet_rng1.gen_range(0, topics)), 1));
                    changes.push(((tweet_rng2.gen_range(0, users), 
                                   tweet_rng2.gen_range(0, users),
                                   tweet_rng2.gen_range(0, topics)),-1));
                }


                let start = ::std::time::Instant::now();
                let round = *tweets.epoch();
                for change in changes.drain(..) {
                    tweets.send(change);
                }
                if computation.index() == 0 {
                    queries.send((query_rng1.gen_range(0, users), 1));
                    queries.send((query_rng2.gen_range(0, users),-1));
                }

                tweets.advance_to(round + 1);
                queries.advance_to(round + 1);
                computation.step_while(|| probe.lt(queries.time()));

                if computation.index() == 0 {
                    println!("wave {}: avg {:?}", wave, start.elapsed() / (batch as u32));
                }
            }
        }
    }).unwrap();
}

fn connected_components<G: Scope>(edges: &Collection<G, Edge>) -> Collection<G, (Node, Node)>
    where G::Timestamp: Lattice+Hash+Ord {

    // each edge (x,y) means that we need at least a label for the min of x and y.
    let nodes = edges.map_in_place(|pair| {
                        let min = std::cmp::min(pair.0, pair.1);
                        *pair = (min, min);
                     })
                     .consolidate_by(|x| x.0);

    // each edge should exist in both directions.
    let edges = edges.map_in_place(|x| ::std::mem::swap(&mut x.0, &mut x.1))
                     .concat(&edges);

    // don't actually use these labels, just grab the type
    nodes.filter(|_| false)
         .iterate(|inner| {
             let edges = edges.enter(&inner.scope());
             let nodes = nodes.enter_at(&inner.scope(), |r| 256 * (64 - (r.0).0.leading_zeros() as u64));

            inner.join_map(&edges, |_k,l,d| (*d,*l))
                 .concat(&nodes)
                 .group(|_, s, t| { t.push((s[0].0, 1)); } )
         })
}
