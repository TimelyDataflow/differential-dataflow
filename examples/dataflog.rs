extern crate timely;
extern crate differential_dataflow;

use timely::dataflow::*;
use timely::dataflow::scopes::Child;
use timely::dataflow::operators::*;
use timely::dataflow::operators::feedback::Handle;

use differential_dataflow::{Data, Collection, AsCollection};
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

/// A collection defined by multiple mutually recursive rules.
///
/// A `Variable` names a collection that may be used in mutually recursive rules. This implementation
/// is like the `Variable` defined in `iterate.rs` optimized for Datalog rules: it supports repeated
/// addition of collections, and a final `distinct` operator applied before connecting the definition.
pub struct Variable<'a, G: Scope, D: Default+Data>
where G::Timestamp: Lattice+Ord {
    feedback: Option<Handle<G::Timestamp, u64,(D, isize)>>,
    current: Collection<Child<'a, G, u64>, D>,
    cycle: Collection<Child<'a, G, u64>, D>,
}

impl<'a, G: Scope, D: Default+Data> Variable<'a, G, D> where G::Timestamp: Lattice+Ord {
    /// Creates a new `Variable` from a supplied `source` stream.
    pub fn from(source: &Collection<Child<'a, G, u64>, D>) -> Variable<'a, G, D> {
        let (feedback, cycle) = source.inner.scope().loop_variable(u64::max_value(), 1);
        let cycle = Collection::new(cycle);
        let mut result = Variable { feedback: Some(feedback), current: cycle.clone(), cycle: cycle };
        result.add(source);
        result
    }
    /// Adds a new source of data to the `Variable`.
    pub fn add(&mut self, source: &Collection<Child<'a, G, u64>, D>) {
        self.current = self.current.concat(source);
    }
}

impl<'a, G: Scope, D: Default+Data> ::std::ops::Deref for Variable<'a, G, D> where G::Timestamp: Lattice+Ord {
    type Target = Collection<Child<'a, G, u64>, D>;
    fn deref(&self) -> &Self::Target {
        &self.cycle
    }
}

impl<'a, G: Scope, D: Default+Data> Drop for Variable<'a, G, D> where G::Timestamp: Lattice+Ord {
    fn drop(&mut self) {
        if let Some(feedback) = self.feedback.take() {
            self.current.distinct()
                        .inner
                        .connect_loop(feedback);
        }
    }
}

fn main() {

    // start up timely computation
    timely::execute_from_args(std::env::args(), |root| {

        // construct streaming scope
        root.scoped::<u64,_,_>(move |outer| {

            // inputs for base facts; currently not used because no data on hand.
            let (_cin, c) = outer.new_input::<((u32,u32,u32),isize)>(); let c = c.as_collection();
            let (_pin, p) = outer.new_input::<((u32,u32),isize)>(); let p = p.as_collection();
            let (_qin, q) = outer.new_input::<((u32,u32,u32),isize)>(); let q = q.as_collection();
            let (_rin, r) = outer.new_input::<((u32,u32,u32),isize)>(); let r = r.as_collection();
            let (_sin, s) = outer.new_input::<((u32,u32),isize)>(); let s = s.as_collection();
            let (_uin, u) = outer.new_input::<((u32,u32,u32),isize)>(); let u = u.as_collection();

            // construct iterative derivation scope
            let (_p, _q) = outer.scoped::<u64,_,_>(|inner| {

                // create new variables
                let mut p = Variable::from(&p.enter(inner));
                let mut q = Variable::from(&q.enter(inner));

                // unchanging variables needn't be `mut`.
                let c = Variable::from(&c.enter(inner));
                let r = Variable::from(&r.enter(inner));
                let s = Variable::from(&s.enter(inner));
                let u = Variable::from(&u.enter(inner));

                // IR1: p(x,z) := p(x,y), p(y,z)
                let ir1 = p.map(|(x,y)| (y,x))
                           .join_map(&p, |_y,&x,&z| (x,z));
                p.add(&ir1);

                // IR2: q(x,r,z) := p(x,y), q(y,r,z)
                let ir2 = p.map(|(x,y)| (y,x))
                           .join_map(&q.map(|(y,r,z)| (y,(r,z))), |_y,&x,&(r,z)| (x,r,z));
                q.add(&ir2);

                // IR3: p(x,z) := p(y,w), u(w,r,z), q(x,r,y)
                let ir3 = p.map(|(y,w)| (w,y))
                           .join_map(&u.map(|(w,r,z)| (w,(r,z))), |_w,&y,&(r,z)| ((y,r),z))
                           .join_map(&q.map(|(x,r,y)| ((y,r),x)), |_yr,&z,&x| (x,z));
                p.add(&ir3);

                // IR4: p(x,z) := c(y,w,z), p(x,w), p(x,y)
                let ir4 = c.map(|(y,w,z)| (w,(y,z)))
                           .join_map(&p.map(|(x,w)| (w,x)), |_w,&(y,z),&x| ((x,y),z))
                           .semijoin(&p)
                           .map(|((x,_y),z)| (x,z));
                p.add(&ir4);

                // IR5: q(x,q,z) := q(x,r,z), s(r,q)
                let ir5 = q.map(|(x,r,z)| (r,(x,z)))
                           .join_map(&s, |_r,&(x,z),&q| (x,q,z));
                q.add(&ir5);

                // IR6: q(x,e,o) := q(x,y,z), r(y,u,e), q(z,u,o)
                let ir6 = q.map(|(x,y,z)| (y,(x,z)))
                           .join_map(&r.map(|(y,u,e)| (y,(u,e))), |_y,&(x,z),&(u,e)| ((z,u),(x,e)))
                           .join_map(&q.map(|(z,u,o)| ((z,u),o)), |_zu,&(x,e),&o| (x,e,o));
                q.add(&ir6);

                // return the derived p and q
                (p.leave(), q.leave())
            });

        });

    }).unwrap();
}
