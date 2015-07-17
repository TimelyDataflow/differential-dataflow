extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::Communicator;

use differential_dataflow::operators::*;

struct Variable<G, T: Hash> {
    pub stream: Stream<G, T>,
    feedback: FeedbackHelper<ObserverHelper<FeedbackObserver<G::Timestamp, D>>>,
    current:  Stream<G, T>,
}

impl<G, T: Hash> Variable<G, T> {
    pub fn new<G: GraphBuilder<T>>(graph: G) -> Variable<G, T> {
        let (feedback, cycle) = subgraph.loop_variable(Product::new(G::Timestamp::max(), iterations), Local(T::Summary::one()));
        Variable {
            stream: cycle.clone(),
            feedback: feedback,
            current: cycle.clone(),
        }
    }
    pub fn add_rule(&mut self, other: &Stream<G, T>) {
        self.current = self.current.concat(other);
    }
}

impl<G, T: Hash> Drop for Variable<G, T> {
    fn drop(&mut self) {
        self.current.group_by(|x| (x, ()), |x,_| *x, |x,_,t| t.push((x,1)))
                    .consolidate()
                    .connect_loop(self.feedback);
    }
}

fn main() {
    timely::initialize(std::env::args(), |communicator| {

        let start = time::precise_time_s();
        let mut computation = GraphRoot::new(communicator);
        let input = computation.subcomputation(|subgraph| {         // sequence of facts scope

            let (input, facts) = builder.new_input();               // a source of base facts
            let fixed_point = subgraph.subcomputation(|cycle| {     // iterative derivation scope

                let constant = cycle.enter(facts);                  // constant facts collection
                let mut var = Variable::new(cycle);                 // variable facts collection

                var.add_rule(&constant);                            // union in constant facts
                var.add_rule(&var.stream.join(&constant), ... );    // add new joined facts

                var.stream.leave()
            })

            fixed_point.inspect_batch(|t,b| println!("{:?}: {} records", t, b.len()));

            input
        })


        let nodes = 1_000u32; // the u32 helps type inference understand what nodes are

        let seed: &[_] = &[1, 2, 3, computation.index() as usize];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions

        input.send_at(0, (0..100).map(|_| ((rng1.gen_range(0, nodes),
                                            rng1.gen_range(0, nodes)), 1)));

        // repeatedly change edges
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions
        let mut round = 0 as u32;
        while computation.step() {
            // once each full second ticks, change an edge
            if time::precise_time_s() - start >= round as f64 {
                // add edges using prior rng; remove edges using fresh rng with the same seed
                let changes = vec![((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1),
                                   ((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1)];
                input.send_at(round, changes.into_iter());
                input.advance_to(round + 1);
                round += 1;
            }
        }

        input.close();                  // seal the source of edges
        while computation.step() { }    // wind down the computation
    });
}
