use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::operators::group::GroupArranged;
use differential_dataflow::trace::Trace;
use differential_dataflow::trace::implementations::ord::OrdKeySpine as DefaultKeyTrace;
use differential_dataflow::trace::implementations::ord::OrdValSpine as DefaultValTrace;
use differential_dataflow::lattice::TotalOrder;
use differential_dataflow::hashable::UnsignedWrapper;

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
where G::Timestamp: TotalOrder+Ord {

    println!("TODO: Q18 could use filter trace wrapper (eval vs filter in `join_core`)");
    println!("TODO: Q18 uses `group_arranged` to get arrangement, but could use count_total");

    let orders =
    collections
        .orders()
        .map(|o| (UnsignedWrapper::from(o.order_key), (o.cust_key, o.order_date, o.total_price)))
        .arrange(DefaultValTrace::new());

    collections
        .lineitems()
        .inner
        .map(|(l, t, d)| ((UnsignedWrapper::from(l.order_key), ()), t, (l.quantity as isize) * d))
        .as_collection()
        .arrange(DefaultKeyTrace::new())
        .group_arranged(|_k,s,t| t.push((s[0].1, 1)), DefaultValTrace::new())
        .join_core(&orders, |&o_key, &quant, &(cust_key, date, price)| 
            if quant > 300 { 
                Some((cust_key, (o_key, date, price, quant)))
            }
            else { None }
        )
        .join_u(&collections.customers().map(|c| (c.cust_key, c.name.to_string())))
        .probe()
}