#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;

extern crate rand;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use timely::dataflow::*;

use differential_dataflow::input::Input;
use differential_dataflow::operators::*;

// Types whose definitions I don't actually know.
#[derive(Ord,PartialOrd,Eq,PartialEq,Clone,Copy,Abomonation,Debug,Hash)]
struct Region(usize);
#[derive(Ord,PartialOrd,Eq,PartialEq,Clone,Copy,Abomonation,Debug,Hash)]
struct Borrow(usize);
#[derive(Ord,PartialOrd,Eq,PartialEq,Clone,Copy,Abomonation,Debug,Hash)]
struct Point(usize);
// apparently unused?
#[derive(Ord,PartialOrd,Eq,PartialEq,Clone,Copy,Abomonation,Debug,Hash)]
enum EdgeKind { Inter, Intra }

fn main() {

    timely::execute_from_args(std::env::args().skip(1), move |worker| {

        let mut probe = ProbeHandle::new();

        let mut inputs = worker.dataflow::<(),_,_>(|scope| {

            // inputs to the computation
            let (input_1, borrow_region) = scope.new_collection::<(Region,Borrow,Point),isize>();
            let (input_2, next_statement) = scope.new_collection::<(Point,Point),isize>();
            let (input_3, goto) = scope.new_collection::<(Point,Point),isize>();
            let (input_4, rloets) = scope.new_collection::<(Region,Point),isize>();
            let (input_5, killed) = scope.new_collection::<(Borrow,Point),isize>();
            let (input_6, out_lives) = scope.new_collection::<(Region,Region,Point),isize>();

            // `cfg_edge` contains pairs (P, Q)
            let cfg_edge =
            next_statement
                .concat(&goto)
                .distinct()
                .probe_with(&mut probe);

            // `region_live_at` contains pairs (R, P)
            let region_live_at_1 = rloets.clone();
            let region_live_at_2 = goto.map(|(p,q)| (q,p))
                                       .join_map(&rloets.map(|(r,q)| (q,r)), |_q,&p,&r| (r,p));
            let region_live_at =
            region_live_at_1
                .concat(&region_live_at_2)
                .distinct()
                .probe_with(&mut probe);

            // `points_to` contains triples (R, B, P)
            let points_to =
            borrow_region
                .iterate(|points_to| {

                    // rule 1: base case.
                    let mut result = borrow_region.enter(&points_to.scope());

                    // rule 2: result + whatever from this rule.
                    result =
                    points_to
                        .map(|(r2,b,p)| ((b,p),r2))
                        .antijoin(&killed.enter(&points_to.scope()))
                        .map(|((b,p),r2)| ((r2,p),b))
                        .join(&out_lives.enter(&points_to.scope()).map(|(r2,r1,p)| ((r2,p),r1)))
                        .map(|((_r2,p),b,r1)| (p,(b,r1)))
                        .join(&next_statement.enter(&points_to.scope()))
                        .map(|(_p,(b,r1),q)| (r1,b,q))
                        .concat(&result);

                    // rule 3: result + whatever from this rule.
                    result =
                    points_to
                        .map(|(r1,b,p)| ((b,p),r1))
                        .antijoin(&killed.enter(&points_to.scope()))
                        .map(|((b,p),r1)| (p,(b,r1)))
                        .join(&next_statement.enter(&points_to.scope()))
                        .map(|(p,(b,r1),q)| ((r1,q),(b,p)))
                        .semijoin(&region_live_at.enter(&points_to.scope()))
                        .map(|((r1,q),(b,_p))| (r1,b,q))
                        .concat(&result);

                    result.distinct()
                })
                .probe_with(&mut probe);

            let borrow_live_at =
            points_to
                .map(|(r,b,p)| ((r,p),b))
                .semijoin(&region_live_at)
                .probe_with(&mut probe);

            (input_1, input_2, input_3, input_4, input_5, input_6)
        });


    }).unwrap();
}
