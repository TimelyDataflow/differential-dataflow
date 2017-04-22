use timely::dataflow::*;
// use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

// use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use ::Collections;

// -- $ID$
// -- TPC-H/TPC-R Order Priority Checking Query (Q4)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     o_orderpriority,
//     count(*) as order_count
// from
//     orders
// where
//     o_orderdate >= date ':1'
//     and o_orderdate < date ':1' + interval '3' month
//     and exists (
//         select
//             *
//         from
//             lineitem
//         where
//             l_orderkey = o_orderkey
//             and l_commitdate < l_receiptdate
//     )
// group by
//     o_orderpriority
// order by
//     o_orderpriority;
// :n -1

pub fn query<G: Scope>(collections: &mut Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: Lattice+Ord {

    let lineitems = 
    collections
        .lineitems()
        .flat_map(|l| if l.commit_date < l.receipt_date { Some(l.order_key).into_iter() } else { None.into_iter() })
        .distinct();

    collections
        .orders()
        .flat_map(|o| 
            if o.order_date >= ::types::create_date(1993, 7, 1) && o.order_date < ::types::create_date(1993, 10, 1) {
                Some((o.order_key, o.order_priority)).into_iter()
            }
            else { None.into_iter() }
        )
        .semijoin(&lineitems)
        .map(|o| o.1)
        .count()
        .probe()
        .0
}