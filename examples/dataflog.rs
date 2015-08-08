extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use std::hash::{Hash, SipHasher, Hasher};
use std::fmt::Debug;
use std::io::{BufReader, BufRead};
use std::fs::File;

use timely::Data;
use timely::dataflow::*;
use timely::dataflow::scopes::Child;
use timely::dataflow::operators::*;
use timely::dataflow::operators::feedback::{Observer, Helper};
use timely::dataflow::channels::pushers::Counter;
use timely::progress::nested::product::Product;
use timely::progress::nested::Summary::Local;
use timely::progress::timestamp::RootTimestamp;

use differential_dataflow::operators::*;
use differential_dataflow::collection_trace::LeastUpperBound;

fn hash<T: Hash>(x: &T) -> u64 {
    let mut h = SipHasher::new();
    x.hash(&mut h);
    h.finish()
}

/// A collection defined by multiple mutually recursive rules.
pub struct Variable<G: Scope, D: Ord+Default+Clone+Data+Debug+Hash>
where G::Timestamp: LeastUpperBound {
    feedback: Option<Helper<Product<G::Timestamp, u64>,(D, i32),Counter<Product<G::Timestamp, u64>,
                                         (D, i32),Observer<Product<G::Timestamp, u64>,(D, i32)>>>>,
    current:  Stream<Child<G, u64>, (D,i32)>,
}

impl<G: Scope, D: Ord+Default+Clone+Data+Debug+Hash> Variable<G, D> where G::Timestamp: LeastUpperBound {
    /// Creates a new `Variable` and a `Stream` representing its output, from a supplied `source` stream.
    pub fn from(source: &Stream<Child<G, u64>, (D,i32)>) -> (Variable<G, D>, Stream<Child<G,u64>, (D, i32)>) {
        let (feedback, cycle) = source.scope().loop_variable(Product::new(G::Timestamp::max(), u64::max_value()), Local(1));
        let mut result = Variable { feedback: Some(feedback), current: cycle.clone() };
        let stream = cycle.clone();
        result.add(source);
        (result, stream)
    }
    /// Adds a new source of data to the `Variable`.
    pub fn add(&mut self, source: &Stream<Child<G, u64>, (D,i32)>) {
        self.current = self.current.concat(source);
    }
}

impl<G: Scope, D: Ord+Default+Debug+Hash+Clone+Data> Drop for Variable<G, D> where G::Timestamp: LeastUpperBound {
    fn drop(&mut self) {
        if let Some(feedback) = self.feedback.take() {
            self.current.group_by(|x| (x, ()), hash, hash, |x,_| x.clone(), |x,_,t| t.push((x.clone(),1)))
                        .connect_loop(feedback);
        }
    }
}

fn main() {

    timely::execute_from_args(std::env::args(), |root| {

        let start = time::precise_time_s();
        let (mut p, mut q, mut u, mut p_query, mut q_query, probe) = root.scoped::<u64, _, _>(move |outer| {

            // inputs for p, q, and u base facts.
            let (p_input, p) = outer.new_input();
            let (q_input, q) = outer.new_input();
            let (u_input, u) = outer.new_input();

            // determine which rules fire with what variable settings.
            let (ir1, ir2, ir3) = outer.scoped::<u64, _, _>(|inner| {

                let u = inner.enter(&u);
                let (mut p_rules, p) = Variable::from(&inner.enter(&p));
                let (mut q_rules, q) = Variable::from(&inner.enter(&q));

                // P(x,z) := P(x,y), P(y,z)
                let ir1 = p.join_u(&p, |(x,y)| (y,x), |(y,z)| (y,z), |&y, &x, &z| (x,y,z));
                p_rules.add(&ir1.map(|((x,_,z),w)| ((x,z),w)));

                // Q(x,r,z) := P(x,y), Q(y,r,z)
                let ir2 = p.join_u(&q, |(x,y)| (y,x), |(y,r,z)| (y,(r,z)), |&y, &x, &(r,z)| (r,x,y,z));
                q_rules.add(&ir2.map(|((r,x,_,z),w)| ((x,r,z),w)));

                // P(x,z) := P(y,w), Q(x,r,y), U(w,r,z)
                let ir3 = p.join_u(&q, |(y,w)| (y,w), |(x,r,y)| (y,(x,r)), |&y, &w, &(x,r)| (r,w,x,y))
                           .join(&u, |(r,w,x,y)| ((r,w), (y,x)), |(w,r,z)| ((r,w),z), hash, |&(r,w), &(y,x), &z| (r,w,x,y,z));
                p_rules.add(&ir3.map(|((_,_,x,_,z),w)| ((x,z),w)));

                // extract the results and return
                (ir1.leave(), ir2.leave(), ir3.leave())
            });

            // inputs through which to demand explanations.
            let (p_query_input, p_query) = outer.new_input();
            let (q_query_input, q_query) = outer.new_input();

            // now we determine which p and q need explaining, and which rules produce each of them
            let (p_base, q_base, ir1_need, ir2_need, ir3_need) = outer.scoped::<u64, _, _>(|inner| {

                // the p and q tuples we want explained.
                let (mut p_rules, p_query) = Variable::from(&inner.enter(&p_query));
                let (mut q_rules, q_query) = Variable::from(&inner.enter(&q_query));

                // semi-join each ir using the tuple it would produce, against the tuples we want explained.
                let ir1_need = inner.enter(&ir1).semijoin(&p_query, |(x,y,z)| ((x,z), y), hash, |&(x,z),&y| (x,y,z));
                let ir2_need = inner.enter(&ir2).semijoin(&q_query, |(r,x,y,z)| ((x,r,z), y), hash, |&(x,r,z),&y| (r,x,y,z));
                let ir3_need = inner.enter(&ir3).semijoin(&p_query, |(r,w,x,y,z)| ((x,z), (r,w,y)), hash, |&(x,z),&(r,w,y)| (r,w,x,y,z));

                // need p from: 2x ir1, 1x ir2, 1x ir3.
                p_rules.add(&ir1_need.map(|((x,y,_),w2)| ((x,y),w2)));
                p_rules.add(&ir1_need.map(|((_,y,z),w2)| ((y,z),w2)));
                p_rules.add(&ir2_need.map(|((_,x,y,_),w2)| ((x,y),w2)));
                p_rules.add(&ir3_need.map(|((_,w,_,y,_),w2)| ((y,w),w2)));

                // need q from: 1x ir2, 1x ir3.
                q_rules.add(&ir2_need.map(|((r,_,y,z),w2)| ((y,r,z),w2)));
                q_rules.add(&ir3_need.map(|((r,_,x,y,_),w2)| ((x,r,y),w2)));

                (p_query.leave(), q_query.leave(), ir1_need.leave(), ir2_need.leave(), ir3_need.leave())
            });

            let (probe, _) = p_base.consolidate(hash).probe();

            p_base.consolidate(hash).inspect(|&(x,_w)| println!("Required P{:?}", x));
            q_base.consolidate(hash).inspect(|&(x,_w)| println!("Required Q{:?}", x));

            ir1_need.consolidate(hash).inspect(|&(x,_w)| println!("Required IR1 {:?}", x));
            ir2_need.consolidate(hash).inspect(|&(x,_w)| println!("Required IR2 {:?}", x));
            ir3_need.consolidate(hash).inspect(|&(x,_w)| println!("Required IR3 {:?}", x));

            (p_input, q_input, u_input, p_query_input, q_query_input, probe)
        });

        if root.index() == 0 {
            let p_file = BufReader::new(File::open("/Users/mcsherry/Desktop/p.txt").unwrap());
            for readline in p_file.lines() {
                let line = readline.ok().expect("read error");
                let elts: Vec<&str> = line[..].split(",").collect();
                let src: u32 = elts[0].parse().ok().expect("malformed src");
                let dst: u32 = elts[1].parse().ok().expect("malformed dst");
                p.send(((src, dst), 1));
            }

            let q_file = BufReader::new(File::open("/Users/mcsherry/Desktop/q.txt").unwrap());
            for readline in q_file.lines() {
                let line = readline.ok().expect("read error");
                let elts: Vec<&str> = line[..].split(",").collect();
                let src: u32 = elts[0].parse().ok().expect("malformed src");
                let dst: u32 = elts[1].parse().ok().expect("malformed dst");
                let aeo: u32 = elts[2].parse().ok().expect("malformed dst");
                q.send(((src, dst, aeo), 1));
            }

            let u_file = BufReader::new(File::open("/Users/mcsherry/Desktop/u.txt").unwrap());
            for readline in u_file.lines() {
                let line = readline.ok().expect("read error");
                let elts: Vec<&str> = line[..].split(",").collect();
                let src: u32 = elts[0].parse().ok().expect("malformed src");
                let dst: u32 = elts[1].parse().ok().expect("malformed dst");
                let aeo: u32 = elts[2].parse().ok().expect("malformed dst");
                u.send(((src, dst, aeo), 1));
            }
        }

        println!("loading:\t{}", time::precise_time_s() - start);

        p.close();
        q.close();
        u.close();

        p_query.advance_to(1);
        q_query.advance_to(1);

        while probe.lt(&RootTimestamp::new(1)) {
            root.step();
        }

        println!("derivation:\t{}", time::precise_time_s() - start);
        let timer = time::precise_time_s();

        p_query.send(((36465u32,10135u32), 1));
        p_query.advance_to(2);
        q_query.advance_to(2);

        while probe.lt(&RootTimestamp::new(2)) {
            root.step();
        }

        println!("query:\t{}", time::precise_time_s() - timer);

        p_query.close();
        q_query.close();

        while root.step() { }    // wind down the computation

    });
}
