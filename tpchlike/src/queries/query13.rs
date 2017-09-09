use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use ::Collections;

// -- $ID$
// -- TPC-H/TPC-R Customer Distribution Query (Q13)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     c_count,
//     count(*) as custdist
// from
//     (
//         select
//             c_custkey,
//             count(o_orderkey)
//         from
//             customer left outer join orders on
//                 c_custkey = o_custkey
//                 and o_comment not like '%:1%:2%'
//         group by
//             c_custkey
//     ) as c_orders (c_custkey, c_count)
// group by
//     c_count
// order by
//     custdist desc,
//     c_count desc;
// :n -1

fn substring2(source: &[u8], query1: &[u8], query2: &[u8]) -> bool {
    if let Some(pos) = (0 .. (source.len() - query1.len())).position(|i| &source[i..][..query1.len()] == query1) {
        (pos .. query2.len()).any(|i| &source[i..][..query2.len()] == query2)
    }
    else { false }
}

pub fn query<G: Scope>(collections: &mut Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: Lattice+TotalOrder+Ord {

    let orders =
    collections
        .orders()
        .flat_map(|o| if !substring2(&o.comment.as_bytes(), b"special", b"requests") { Some(o.cust_key) } else { None } );

    collections
        .customers()
        .map(|c| c.cust_key)
        .concat(&orders)
        .count_total_u()
        .map(|(_cust_key, count)| (count-1) as usize)
        .count_total_u()
        .probe()
}