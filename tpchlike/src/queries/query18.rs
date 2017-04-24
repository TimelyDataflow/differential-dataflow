use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::{ArrangeBySelf, ArrangeByKey};
use differential_dataflow::operators::join::JoinArranged;
use differential_dataflow::operators::group::GroupArranged;
use differential_dataflow::trace::Trace;
use differential_dataflow::trace::implementations::ord::OrdValSpine as DefaultValTrace;
use differential_dataflow::lattice::Lattice;
use ::Collections;

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

pub fn query<G: Scope>(collections: &mut Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: Lattice+Ord {

    collections
        .lineitems()
        .inner
        .map(|(l, t, d)| (l.order_key, t, (l.quantity as isize) * d))
        .as_collection()
        // .count()
        // .filter(|x| x.1 > 300)
        .arrange_by_self()
        .group_arranged(|_k,_s,t| t.push((_s[0].1, 1)), DefaultValTrace::new())
        .join_arranged(&collections.orders().map(|o| (o.order_key, (o.cust_key, o.order_date, o.total_price))).arrange_by_key_hashed(), |k,v1,v2| (k.clone(), v1.clone(), v2.clone()))
        .filter(|x| x.1 > 300)
        .map(|(okey, quantity, (custkey, date, price))| (custkey, (okey, date, price, quantity)))
        .join(&collections.customers().map(|c| (c.cust_key, c.name.to_string())))
        .probe()
        .0
}