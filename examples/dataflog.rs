extern crate timely;
extern crate differential_dataflow;

use timely::progress::nested::product::Product;
use timely::dataflow::*;
use timely::dataflow::scopes::child::Iterative as  Child;
use timely::dataflow::operators::*;
use timely::dataflow::operators::feedback::Handle;

use differential_dataflow::input::Input;
use differential_dataflow::{Data, Collection, Hashable};
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

/// A collection defined by multiple mutually recursive rules.
///
/// A `Variable` names a collection that may be used in mutually recursive rules. This implementation
/// is like the `Variable` defined in `iterate.rs` optimized for Datalog rules: it supports repeated
/// addition of collections, and a final `distinct` operator applied before connecting the definition.
pub struct Variable<'a, G: Scope, D: Default+Data+Hashable>
where G::Timestamp: Lattice+Ord {
    feedback: Option<Handle<Product<G::Timestamp, u64>, (D, Product<G::Timestamp, u64>, isize)>>,
    current: Collection<Child<'a, G, u64>, D>,
    cycle: Collection<Child<'a, G, u64>, D>,
}

impl<'a, G: Scope, D: Default+Data+Hashable> Variable<'a, G, D> where G::Timestamp: Lattice+Ord {
    /// Creates a new `Variable` from a supplied `source` stream.
    pub fn from(source: &Collection<Child<'a, G, u64>, D>) -> Variable<'a, G, D> {
        let (feedback, cycle) = source.inner.scope().loop_variable(1);
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

impl<'a, G: Scope, D: Default+Data+Hashable> ::std::ops::Deref for Variable<'a, G, D> where G::Timestamp: Lattice+Ord {
    type Target = Collection<Child<'a, G, u64>, D>;
    fn deref(&self) -> &Self::Target {
        &self.cycle
    }
}

impl<'a, G: Scope, D: Default+Data+Hashable> Drop for Variable<'a, G, D> where G::Timestamp: Lattice+Ord {
    fn drop(&mut self) {
        if let Some(feedback) = self.feedback.take() {
            self.current.distinct()
                        .inner
                        .map(|(x,t,d)| (x, Product::new(t.outer, t.inner+1), d))
                        .connect_loop(feedback);
        }
    }
}

fn main() {

    // start up timely computation
    timely::execute_from_args(std::env::args(), |worker| {

        // construct streaming scope
        worker.dataflow::<u64,_,_>(move |outer| {

            // inputs for base facts; currently not used because no data on hand.
            let (_cin, c) = outer.new_collection::<(u32,u32,u32),isize>();
            let (_pin, p) = outer.new_collection::<(u32,u32),isize>();
            let (_qin, q) = outer.new_collection::<(u32,u32,u32),isize>();
            let (_rin, r) = outer.new_collection::<(u32,u32,u32),isize>();
            let (_sin, s) = outer.new_collection::<(u32,u32),isize>();
            let (_uin, u) = outer.new_collection::<(u32,u32,u32),isize>();

            // construct iterative derivation scope
            let (_p, _q) = outer.iterative::<u64,_,_>(|inner| {

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
