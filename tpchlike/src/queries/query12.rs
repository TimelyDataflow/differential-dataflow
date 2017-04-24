use timely::dataflow::*;
// use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

// use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use ::Collections;
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R Shipping Modes and Order Priority Query (Q12)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     l_shipmode,
//     sum(case
//         when o_orderpriority = '1-URGENT'
//             or o_orderpriority = '2-HIGH'
//             then 1
//         else 0
//     end) as high_line_count,
//     sum(case
//         when o_orderpriority <> '1-URGENT'
//             and o_orderpriority <> '2-HIGH'
//             then 1
//         else 0
//     end) as low_line_count
// from
//     orders,
//     lineitem
// where
//     o_orderkey = l_orderkey
//     and l_shipmode in (':1', ':2')
//     and l_commitdate < l_receiptdate
//     and l_shipdate < l_commitdate
//     and l_receiptdate >= date ':3'
//     and l_receiptdate < date ':3' + interval '1' year
// group by
//     l_shipmode
// order by
//     l_shipmode;
// :n -1

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: Lattice+Ord {

    let orders = 
    collections
        .orders()
        .map(|o| (o.order_key, starts_with(&o.order_priority, b"1-URGENT") || starts_with(&o.order_priority, b"2-HIGH")));

    collections
        .lineitems()
        .flat_map(|l| 
            if (starts_with(&l.ship_mode, b"MAIL") || starts_with(&l.ship_mode, b"SHIP")) && 
                l.commit_date < l.receipt_date && l.ship_date < l.commit_date && 
                create_date(1994,1,1) <= l.receipt_date && l.receipt_date < create_date(1995,1,1) {

                Some((l.order_key, l.ship_mode)).into_iter()

            }
            else { None.into_iter() }
        )
        .join(&orders)
        .count()
        .probe()
        .0
}