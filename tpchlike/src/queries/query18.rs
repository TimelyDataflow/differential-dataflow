use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::difference::DiffPair;

use ::Collections;
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R Large Volume Customer Query (Q18)
// -- Function Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     c_name,
//     c_custkey,
//     o_orderkey,
//     o_orderdate,
//     o_totalprice,
//     sum(l_quantity)
// from
//     customer,
//     orders,
//     lineitem
// where
//     o_orderkey in (
//         select
//             l_orderkey
//         from
//             lineitem
//         group by
//             l_orderkey having
//                 sum(l_quantity) > :1
//     )
//     and c_custkey = o_custkey
//     and o_orderkey = l_orderkey
// group by
//     c_name,
//     c_custkey,
//     o_orderkey,
//     o_orderdate,
//     o_totalprice
// order by
//     o_totalprice desc,
//     o_orderdate;
// :n 100


fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

fn substring(source: &[u8], query: &[u8]) -> bool {
    (0 .. (source.len() - query.len())).any(|offset| 
        (0 .. query.len()).all(|i| source[i + offset] == query[i])
    )
}

pub fn query<G: Scope>(collections: &mut Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: Lattice+Ord {

    collections
        .lineitems()
        .inner
        .map(|(l, t, d)| (l.order_key, t, (l.quantity as isize) * d))
        .as_collection()
        .count()
        .filter(|x| x.1 > 300)
        .join(&collections.orders().map(|o| (o.order_key, (o.cust_key, o.order_date, o.total_price))))
        .map(|(okey, quantity, (custkey, date, price))| (custkey, (okey, date, price, quantity)))
        .join(&collections.customers().map(|c| (c.cust_key, c.name.to_string())))
        .probe()
        .0
}