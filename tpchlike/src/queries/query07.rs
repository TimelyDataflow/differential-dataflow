use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use ::Collections;
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R Volume Shipping Query (Q7)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     supp_nation,
//     cust_nation,
//     l_year,
//     sum(volume) as revenue
// from
//     (
//         select
//             n1.n_name as supp_nation,
//             n2.n_name as cust_nation,
//             extract(year from l_shipdate) as l_year,
//             l_extendedprice * (1 - l_discount) as volume
//         from
//             supplier,
//             lineitem,
//             orders,
//             customer,
//             nation n1,
//             nation n2
//         where
//             s_suppkey = l_suppkey
//             and o_orderkey = l_orderkey
//             and c_custkey = o_custkey
//             and s_nationkey = n1.n_nationkey
//             and c_nationkey = n2.n_nationkey
//             and (
//                 (n1.n_name = ':1' and n2.n_name = ':2')
//                 or (n1.n_name = ':2' and n2.n_name = ':1')
//             )
//             and l_shipdate between date '1995-01-01' and date '1996-12-31'
//     ) as shipping
// group by
//     supp_nation,
//     cust_nation,
//     l_year
// order by
//     supp_nation,
//     cust_nation,
//     l_year;
// :n -1

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: Lattice+Ord {

    let nations =
    collections
        .nations()
        .filter(|n| starts_with(&n.name, b"FRANCE") || starts_with(&n.name, b"GERMANY"))
        .map(|n| (n.nation_key, n.name));

    let customers = 
    collections
        .customers()
        .map(|c| (c.nation_key, c.cust_key))
        .join_u(&nations)
        .map(|(_nation_key, cust_key, name)| (cust_key, name));

    let orders = 
    collections
        .orders()
        .map(|o| (o.cust_key, o.order_key))
        .join_u(&customers)
        .map(|(_cust_key, order_key, name)| (order_key, name));

    let suppliers = 
    collections
        .suppliers()
        .map(|s| (s.nation_key, s.supp_key))
        .join_u(&nations)
        .map(|(_nation_key, supp_key, name)| (supp_key, name));

    collections
        .lineitems()
        .inner
        .flat_map(|(l, t, d)| 
            if create_date(1995, 1, 1) <= l.ship_date && l.ship_date <= create_date(1996, 12, 31) {
                Some(((l.supp_key, (l.order_key, l.ship_date)), t, (l.extended_price * (100 - l.discount)) as isize / 100 * d)).into_iter()
            }
            else { None.into_iter() }
        )
        .as_collection()
        .join_u(&suppliers)
        .map(|(_supp_key, (order_key, ship_date), name_s)| (order_key, (ship_date, name_s)))
        .join_u(&orders)
        .map(|(_order_key, (ship_date, name_s), name_c)| (name_s, name_c, ship_date >> 16))
        .filter(|x| x.0 != x.1)
        .count()
        .probe()
        .0
}