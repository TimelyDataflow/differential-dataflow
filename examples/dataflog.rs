#[allow(unused_variables)]
extern crate fnv;
extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use std::collections::HashMap;
use std::io::{BufReader, BufRead};
use std::fs::File;

use timely::dataflow::*;
use timely::dataflow::scopes::Child;
use timely::dataflow::operators::*;
use timely::dataflow::operators::feedback::Handle;
use timely::progress::timestamp::RootTimestamp;

use differential_dataflow::Data;
use differential_dataflow::operators::*;
use differential_dataflow::collection::LeastUpperBound;
use differential_dataflow::operators::join::JoinBy;

/// A collection defined by multiple mutually recursive rules.
pub struct Variable<G: Scope, D: Default+Data>
where G::Timestamp: LeastUpperBound {
    feedback: Option<Handle<G::Timestamp, u64,(D, i32)>>,
    current:  Stream<Child<G, u64>, (D,i32)>,
}

impl<G: Scope, D: Default+Data> Variable<G, D> where G::Timestamp: LeastUpperBound {
    /// Creates a new `Variable` and a `Stream` representing its output, from a supplied `source` stream.
    pub fn from(source: &Stream<Child<G, u64>, (D,i32)>) -> (Variable<G, D>, Stream<Child<G,u64>, (D, i32)>) {
        let (feedback, cycle) = source.scope().loop_variable(u64::max_value(), 1);
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

impl<G: Scope, D: Default+Data> Drop for Variable<G, D> where G::Timestamp: LeastUpperBound {
    fn drop(&mut self) {
        if let Some(feedback) = self.feedback.take() {
            self.current.threshold(|x| x.hashed(), |_| HashMap::new(), |_, w| if w > 0 { 1 } else { 0 })
                        .connect_loop(feedback);
        }
    }
}

macro_rules! rule {
    ($name1: ident ($($var1:ident),*) := $name2: ident ($($var2:ident),*) $name3: ident ($($var3:ident),*) : ($($var4:ident),*) = ($($var5:ident),*)) => {{
        let result =
            $name2.0.join_by(
                &$name3.0,
                |($( $var2, )*)| (($( $var4, )*), ( $($var2, )*)),
                |($( $var3, )*)| (($( $var5, )*), ( $($var3, )*)),
                |x| x.hashed(),
                |_, &($( $var2, )*), &($( $var3, )*)| (($( $var2, )*), ($( $var3, )*)));
        $name1.1.add(&result.map(|((($( $var2, )*), ($( $var3, )*)), __w)| (($( $var1, )*), __w)));

        let temp = result.semijoin_by(
            &$name1.2,
            |(($( $var2, )*), ($( $var3, )*))| (($( $var1, )*), (($( $var2, )*), ($( $var3, )*))),
            |x| x.hashed(),
            |_, &(($( $var2, )*), ($( $var3, )*))| (($( $var2, )*), ($( $var3, )*)));
        $name2.3.add(&temp.map(|(( ($( $var2, )*) ,_),__w)| (($( $var2, )*),__w)));
        $name3.3.add(&temp.map(|(( _, ($( $var3, )*)),__w)| (($( $var3, )*),__w)));

        temp
    }};
}

macro_rules! rule_u {
    ($name1: ident ($($var1:ident),*) := $name2: ident ($($var2:ident),*) $name3: ident ($($var3:ident),*) : $var4:ident = $var5:ident) => {{
        let result =
            $name2.0.join_by_u(
                &$name3.0,
                |($( $var2, )*)| ($var4, ( $($var2, )*)),
                |($( $var3, )*)| ($var5, ( $($var3, )*)),
                |_, &($( $var2, )*), &($( $var3, )*)| (($( $var2, )*), ($( $var3, )*)));
        $name1.1.add(&result.map(|((($( $var2, )*), ($( $var3, )*)), __w)| (($( $var1, )*), __w)));

        let temp = result.semijoin_by(
            &$name1.2,
            |(($( $var2, )*), ($( $var3, )*))| (($( $var1, )*), (($( $var2, )*), ($( $var3, )*))),
            |x| x.hashed(),
            |_, &(($( $var2, )*), ($( $var3, )*))| (($( $var2, )*), ($( $var3, )*)));
        $name2.3.add(&temp.map(|(( ($( $var2, )*) ,_),__w)| (($( $var2, )*),__w)));
        $name3.3.add(&temp.map(|(( _, ($( $var3, )*)),__w)| (($( $var3, )*),__w)));

        temp
    }};
}

macro_rules! variable {
    ($name0: ident : $name1: ident, $name2: ident) => {{
        let temp1 = Variable::from(&$name0.enter(&$name1));
        let temp2 = Variable::from(&$name0.enter(&$name2));
        (temp1.1, temp1.0, temp2.1, temp2.0)
    }}
}


fn main() {

    timely::execute_from_args(std::env::args(), |root| {

        let start = time::precise_time_s();
        let (mut p, mut q, mut u, mut p_query, mut q_query, probe) = root.scoped::<u64, _, _>(move |outer| {

            // inputs for p, q, and u base facts.
            let (p_input, p) = outer.new_input();
            let (q_input, q) = outer.new_input();
            let (u_input, u) = outer.new_input();

            // inputs through which to demand explanations.
            let (p_query_input, p_query) = outer.new_input();
            let (q_query_input, q_query) = outer.new_input();

            let p_temp = p.filter(|_| false);
            let q_temp = q.filter(|_| false);

            let (p_del, q_del, ir1, ir2, ir3) = outer.scoped::<u64,_,_>(|middle| {

                let mut p_del = variable!(middle : p_temp, p_temp);
                let mut q_del = variable!(middle : q_temp, q_temp);

                let p_query = middle.enter(&p_query);
                let q_query = middle.enter(&q_query);

                let p_edb = middle.enter(&p).concat(&p_del.0.map(|(x,w)| (x,-w))).consolidate();
                let q_edb = middle.enter(&q).concat(&q_del.0.map(|(x,w)| (x,-w))).consolidate();
                let u = middle.enter(&u);

                // determine which rules fire with what variable settings.
                let (p_der, q_der, p_bad, q_bad, ir1, ir2, ir3) = middle.scoped::<u64, _, _>(|inner| {

                    let (_unused, u) = Variable::from(&inner.enter(&u));

                    let mut p = variable!(inner : p_edb, p_query);
                    let mut q = variable!(inner : q_edb, q_query);

                    let ir1 = rule_u!(p(x,z)   := p(x,_y1) p(_y2,z)   : _y1 = _y2);
                    let ir2 = rule_u!(q(x,r,z) := p(x,_y1) q(_y2,r,z) : _y1 = _y2);

                    // P(x,z) := P(y,w), Q(x,r,y), U(w,r,z)
                    let ir3 = p.0.join_by_u(&q.0, |(y,w)| (y,w), |(x,r,y)| (y,(x,r)), |&y, &w, &(x,r)| (r,w,x,y))
                                 .join_by(&u, |(r,w,x,y)| ((r,w), (y,x)), |(w,r,z)| ((r,w),z), |x| x.hashed(), |&(r,w), &(y,x), &z| (r,w,x,y,z));
                    p.1.add(&ir3.map(|((_,_,x,_,z),w)| ((x,z),w)));
                    let ir3_need = ir3.semijoin_by(&p.2, |(r,w,x,y,z)| ((x,z), (r,w,y)), |x| x.hashed(), |&(x,z),&(r,w,y)| (r,w,x,y,z));
                    p.3.add(&ir3_need.map(|((_,w,_,y,_),w2)| ((y,w),w2)));
                    q.3.add(&ir3_need.map(|((r,_,x,y,_),w2)| ((x,r,y),w2)));

                    // extract the results and return
                    (p.0.leave(), q.0.leave(), p.2.leave(), q.2.leave(), ir1.leave(), ir2.leave(), ir3_need.leave())
                });

                p_der.inspect(|x| println!("P{:?}", x));
                q_der.inspect(|x| println!("Q{:?}", x));

                // p_bad and q_bad are p and q tuples involved in the derivation.
                // we should remove some of them from p, q by adding them to p_del, q_del.
                let p_bad = p_bad.map(|(x,w)| ((x,()),w))
                             .join(&p_edb.map(|(x,w)| ((x,()),w)))
                             .map(|((x,(),()),w)| (((),x),w));
                            //  .group(|_,s,t| t.push(s.next().map(|(&x,w)|(x,w)).unwrap()))
                            //  .map(|(((),x),w)| (x,w));

                let q_bad = q_bad.map(|(x,w)| ((x,()),w))
                             .join(&q_edb.map(|(x,w)| ((x,()),w)))
                             .map(|((x,(),()),w)| (((),x),w));
                            //  .group(|_,s,t| t.push(s.next().map(|(&x,w)|(x,w)).unwrap()))
                            //  .map(|(((),x),w)| (x,w));

                p_bad.inspect(|x| println!("PBAD{:?}", x));
                q_bad.inspect(|x| println!("QBAD{:?}", x));

                let p_bad_new: Stream<_, ((u32,u32), i32)> = p_bad.cogroup_by_inner(&q_bad, |k| k.hashed(), |_,&x| x, |_| HashMap::new(), |key, input1, input2, output| {
                     output.push(input1.next().map(|(&x,w)|(x,w)).unwrap());
                });

                let q_bad_new: Stream<_, ((u32,u32,u32), i32)> = p_bad.cogroup_by_inner(&q_bad, |k| k.hashed(), |_,&x| x, |_| HashMap::new(), |key, input1, input2, output| {
                    if input1.next() == None {
                        input2.next().map(|(&x,w)| output.push((x,w)));
                    }
                });

                p_del.1.add(&p_bad_new);
                q_del.1.add(&q_bad_new);

                (p_del.0.leave(), q_del.0.leave(), ir1.leave(), ir2.leave(), ir3.leave())
            });

            let (probe, _) = p_del.consolidate().probe();

            p_del.consolidate().inspect(|&(x,_w)| println!("Deleted P{:?}", x));
            q_del.consolidate().inspect(|&(x,_w)| println!("Deleted Q{:?}", x));

            ir1.consolidate().inspect(|&(x,_w)| println!("Required IR1 {:?}", x));
            ir2.consolidate().inspect(|&(x,_w)| println!("Required IR2 {:?}", x));
            ir3.consolidate().inspect(|&(x,_w)| println!("Required IR3 {:?}", x));

            (p_input, q_input, u_input, p_query_input, q_query_input, probe)
        });

        if root.index() == 0 {
            let p_file = BufReader::new(File::open("/Users/mcsherry/Desktop/p.txt").unwrap());
            for readline in p_file.lines() {
                let line = readline.ok().expect("read error");
                let elts: Vec<&str> = line[..].split(",").collect();
                let src: u32 = elts[0].parse().ok().expect("malformed src");
                let dst: u32 = elts[1].parse().ok().expect("malformed dst");
                if src != dst {
                    p.send(((src, dst), 1));
                }
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
        root.step();

        println!("derivation:\t{}", time::precise_time_s() - start);
        let timer = time::precise_time_s();

        // p_query.send(((1u32,3u32), 1));
        q_query.send(((1u32,4u32,5u32), 1));
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
