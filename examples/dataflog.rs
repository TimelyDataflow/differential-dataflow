extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

extern crate abomonation;

use abomonation::Abomonation;

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
            self.current.group_by(|x| (x, ()),
                            |x| { let mut h = SipHasher::new(); x.hash(&mut h); h.finish() },
                            |x| { let mut h = SipHasher::new(); x.hash(&mut h); h.finish() },
                            |x,_| x.clone(), |x,_,t| t.push((x.clone(),1)))
                        .connect_loop(feedback);
        }
    }
}

type P = (u32, u32);
type Q = (u32, u32, u32);
type U = (u32, u32, u32);

#[derive(Copy, Clone, Ord, Eq, PartialOrd, PartialEq, Debug, Hash)]
enum ProvenanceP {
    EDB(P),
    IR1(P, P),
    IR3(P, Q, U)
}

impl ProvenanceP {
    fn to_p(&self) -> P {
        match *self {
            ProvenanceP::EDB(p) => p,
            ProvenanceP::IR1((x,_),(_,z)) => (x,z),
            ProvenanceP::IR3((_,_),(x,_,_),(_,_,z)) => (x,z),
        }
    }
}

impl Abomonation for ProvenanceP { }

#[derive(Copy, Clone, Ord, Eq, PartialOrd, PartialEq, Debug, Hash)]
enum ProvenanceQ {
    EDB(Q),
    IR2(P, Q),
}

impl ProvenanceQ {
    fn to_q(&self) -> Q {
        match *self {
            ProvenanceQ::EDB(q) => q,
            ProvenanceQ::IR2((x,_),(_,r,z)) => (x,r,z),
        }
    }
}

fn test<D: Data>() {}

impl Abomonation for ProvenanceQ { }

fn main() {

    test::<ProvenanceP>();
    test::<ProvenanceQ>();

    timely::execute_from_args(std::env::args(), |root| {

        let start = time::precise_time_s();
        let (mut p, mut q, mut u, mut p_query, mut q_query, probe) = root.scoped::<u64, _, _>(|outer| {

            let (p_input, p) = outer.new_input();
            let (q_input, q) = outer.new_input();
            let (u_input, u) = outer.new_input();

            let ((p, p_prov), (q, q_prov)) = outer.scoped::<u64, _, _>(|inner| {

                let mut p_prov = inner.enter(&p).map(|(p,w)| (ProvenanceP::EDB(p),w));
                let mut q_prov = inner.enter(&q).map(|(q,w)| (ProvenanceQ::EDB(q),w));

                let u = inner.enter(&u);
                let (mut p_rules, p) = Variable::from(&inner.enter(&p));
                let (mut q_rules, q) = Variable::from(&inner.enter(&q));

                // add one rule for P:
                // P(x,z) := P(x,y), P(y,z)
                let ir1 = p.join_u(&p, |(x,y)| (y,x), |(y,z)| (y,z), |&y, &x, &z| ProvenanceP::IR1((x,y),(y,z)));

                // add two rules for Q:
                // Q(x,r,z) := P(x,y), Q(y,r,z)
                // Q(x,r,z) := P(y,w), Q(x,r,y), U(w,r,z)
                let ir2 = p.join_u(&q, |(x,y)| (y,x), |(y,r,z)| (y,(r,z)), |&y, &x, &(r,z)| ProvenanceQ::IR2((x,y),(y,r,z)));

                let ir3 = p.join_u(&q, |(y,w)| (y,w), |(x,r,y)| (y,(x,r)), |&y, &w, &(x,r)| ((y,w),(x,r,y)))
                           .join(&u, |((y,w),(x,r,_))| ((r,w), (y,x)), |(w,r,z)| ((r,w),z),
                                    |&((_,w),(_,r,_))| { let mut h = SipHasher::new(); (r,w).hash(&mut h); h.finish() },
                                    |&(w,r,_)| { let mut h = SipHasher::new(); (r,w).hash(&mut h); h.finish() },
                                    |&(r,w)|    { let mut h = SipHasher::new(); (r,w).hash(&mut h); h.finish() },
                                    |&(r,w), &(y,x), &z| ProvenanceP::IR3((y,w), (x,r,y), (w,r,z)));

                // let ir2 = p.join_u(&q, |p| (p.1,p), |q| (q.0,q), |_, &p, &q| ProvenanceQ::IR2(p,q));
                // let ir3 = p.join_u(&q, |p| (p.0,p), |q| (q.2,q), |_, &p, &q)| (p,q)
                //            .join(&u, |(p,q)| ((q.0,q.1), (p,q)), |u| ((u.1,u.2),u),
                //                  |&(p,q)| { let mut h = SipHasher::new(); (q.0,q.1).hash(&mut h); h.finish() },
                //                  |&u| { let mut h = SipHasher::new(); (u.1,u.2).hash(&mut h); h.finish() },
                //                  |&k|    { let mut h = SipHasher::new(); k.hash(&mut h); h.finish() },
                //                  |_, &(p,q), &u| ProvenanceQ::IR3(p, q, u);

                p_rules.add(&ir1.map(|(x,w)| (x.to_p(),w)));
                p_rules.add(&ir3.map(|(x,w)| (x.to_p(),w)));
                p_prov = p_prov.concat(&ir1).concat(&ir3);

                q_rules.add(&ir2.map(|(x,w)| (x.to_q(),w)));
                q_prov = q_prov.concat(&ir2);

                // extract the results and return
                ((p.leave(), p_prov.leave()), (q.leave(), q_prov.leave()))
            });

            // p.consolidate(|x| { let mut h = SipHasher::new(); x.hash(&mut h); h.finish() })
            //  .inspect(|&((x,y),_)| println!("P({}, {})", x, y));
            // q.consolidate(|x| { let mut h = SipHasher::new(); x.hash(&mut h); h.finish() })
            //  .inspect(|&((x,r,z),_)| println!("Q({}, {}, {})", x, r, z));

            let (p_query_input, p_query) = outer.new_input();
            let (q_query_input, q_query) = outer.new_input();

            // now we
            let (p_base, q_base, u_base) = outer.scoped::<u64, _, _>(|inner| {

                let (mut p_rules, p_query) = Variable::from(&inner.enter(&p_query));
                let (mut q_rules, q_query) = Variable::from(&inner.enter(&q_query));

                let p_prov = inner.enter(&p_prov);
                let q_prov = inner.enter(&q_prov);

                // everything in p_query needs to be explained using p_prov
                let p_needs = p_query.join(&p_prov, |p| (p,()), |x| (x.to_p(), x),
                                            |x| { let mut h = SipHasher::new(); x.hash(&mut h); h.finish() },
                                            |x| { let mut h = SipHasher::new(); x.to_p().hash(&mut h); h.finish() },
                                            |x| { let mut h = SipHasher::new(); x.hash(&mut h); h.finish() },
                                            |_,_,x| *x);

                p_rules.add(&p_needs.filter_map2(|(x,w)| if let ProvenanceP::IR1(p,_) = x { Some((p,w)) } else { None }));
                p_rules.add(&p_needs.filter_map2(|(x,w)| if let ProvenanceP::IR1(_,p) = x { Some((p,w)) } else { None }));

                // everything in q_query needs to be explained using p_prov
                let q_needs = q_query.join(&q_prov, |q| (q,()), |x| (x.to_q(), x),
                                            |x| { let mut h = SipHasher::new(); x.hash(&mut h); h.finish() },
                                            |x| { let mut h = SipHasher::new(); x.to_q().hash(&mut h); h.finish() },
                                            |x| { let mut h = SipHasher::new(); x.hash(&mut h); h.finish() },
                                            |_,_,x| *x);
                p_rules.add(&q_needs.filter_map2(|(x,w)| if let ProvenanceQ::IR2(p,_) = x { Some((p,w)) } else { None }));
                q_rules.add(&q_needs.filter_map2(|(x,w)| if let ProvenanceQ::IR2(_,q) = x { Some((q,w)) } else { None }));
                p_rules.add(&p_needs.filter_map2(|(x,w)| if let ProvenanceP::IR3(p,_,_) = x { Some((p,w)) } else { None }));
                q_rules.add(&p_needs.filter_map2(|(x,w)| if let ProvenanceP::IR3(_,q,_) = x { Some((q,w)) } else { None }));

                // the tuples we *need* are instances of EDB, or a u instance for IR3.
                // let p_base = p_needs.filter_map2(|(x,w)| if let ProvenanceP::EDB(p) = x { Some((p,w)) } else { None });
                // let q_base = q_needs.filter_map2(|(x,w)| if let ProvenanceQ::EDB(q) = x { Some((q,w)) } else { None });
                let u_base = p_needs.filter_map2(|(x,w)| if let ProvenanceP::IR3(_,_,u) = x { Some((u,w)) } else { None });

                (p_needs.leave(), q_needs.leave(), u_base.leave())
            });

            let (probe, _) = p_base.consolidate(|x| { let mut h = SipHasher::new(); x.hash(&mut h); h.finish() })
                  .inspect(|&(x,w)| println!("Required: {:?} -> {:?}\t{}", x, x.to_p(), w))
                  .probe();
            q_base.consolidate(|x| { let mut h = SipHasher::new(); x.hash(&mut h); h.finish() })
                  .inspect(|&(x,w)| println!("Required: {:?} -> {:?}\t{}", x, x.to_q(), w));
            u_base.consolidate(|x| { let mut h = SipHasher::new(); x.hash(&mut h); h.finish() })
                  .inspect(|&((x,y,z),w)| println!("Required: U({}, {}, {})\t{}", x, y, z, w));

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

        // p.send(((0u64,1u64), 1));
        // p.send(((1u64,2u64), 1));
        // q.send(((3u64,4u64,0u64), 1));
        // u.send(((2u64,4u64,6u64), 1));

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

        // p_query.send(((0,2), 1));
        p_query.send(((36465u32,10135u32), 1));

        p_query.close();
        q_query.close();

        while root.step() { }    // wind down the computation

        println!("query:\t{}", time::precise_time_s() - timer);
    });
}
