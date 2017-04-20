use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use ::Collections;

// -- $ID$
// -- TPC-H/TPC-R Shipping Priority Query (Q3)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     l_orderkey,
//     sum(l_extendedprice * (1 - l_discount)) as revenue,
//     o_orderdate,
//     o_shippriority
// from
//     customer,
//     orders,
//     lineitem
// where
//     c_mktsegment = ':1'
//     and c_custkey = o_custkey
//     and l_orderkey = o_orderkey
//     and o_orderdate < date ':2'
//     and l_shipdate > date ':2'
// group by
//     l_orderkey,
//     o_orderdate,
//     o_shippriority
// order by
//     revenue desc,
//     o_orderdate;
// :n 10


fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

fn substring(source: &[u8], query: &[u8]) -> bool {
    (0 .. (source.len() - query.len())).any(|offset| 
        (0 .. query.len()).all(|i| source[i + offset] == query[i])
    )
}

pub fn query<G: Scope>(collections: &Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: Lattice+Ord {

    let customers =
    collections
        .customers
        .filter(|c| starts_with(&c.mktsegment[..], b"BUILDING"))
        .map(|c| c.cust_key);

    let lineitems = 
    collections
        .lineitems
        .filter(|l| l.ship_date > ::types::create_date(1995, 3, 15))
        .map(|l| (l.order_key, l.extended_price * (100 - l.discount) / 100));

    let orders = 
    collections
        .orders
        .filter(|o| o.order_date < ::types::create_date(1995, 3, 15))
        .map(|o| (o.cust_key, (o.order_key, o.order_date, o.ship_priority)));

    orders
        .semijoin(&customers)
        .map(|(_, (order_key, order_date, ship_priority))| (order_key, (order_date, ship_priority)))
        .join(&lineitems)
        .inner
        .map(|((order_key, (date, ship), price), time, diff)| ((order_key, date, ship), time, price * diff as i64))
        .as_collection()
        .count()
        .probe()
        .0
}