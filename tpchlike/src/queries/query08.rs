use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::difference::DiffPair;
use differential_dataflow::lattice::Lattice;

use ::Collections;
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R National Market Share Query (Q8)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     o_year,
//     sum(case
//         when nation = ':1' then volume
//         else 0
//     end) / sum(volume) as mkt_share
// from
//     (
//         select
//             extract(year from o_orderdate) as o_year,
//             l_extendedprice * (1 - l_discount) as volume,
//             n2.n_name as nation
//         from
//             part,
//             supplier,
//             lineitem,
//             orders,
//             customer,
//             nation n1,
//             nation n2,
//             region
//         where
//             p_partkey = l_partkey
//             and s_suppkey = l_suppkey
//             and l_orderkey = o_orderkey
//             and o_custkey = c_custkey
//             and c_nationkey = n1.n_nationkey
//             and n1.n_regionkey = r_regionkey
//             and r_name = ':2'
//             and s_nationkey = n2.n_nationkey
//             and o_orderdate between date '1995-01-01' and date '1996-12-31'
//             and p_type = ':3'
//     ) as all_nations
// group by
//     o_year
// order by
//     o_year;
// :n -1

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: Lattice+TotalOrder+Ord {

    let regions = collections.regions().filter(|r| starts_with(&r.name, b"AMERICA")).map(|r| r.region_key);
    let nations1 = collections.nations().map(|n| (n.region_key, n.nation_key)).semijoin_u(&regions).map(|x| x.1);
    let customers = collections.customers().map(|c| (c.nation_key, c.cust_key)).semijoin_u(&nations1).map(|x| x.1);
    let orders = 
    collections
        .orders()
        .flat_map(|o|
            if create_date(1995,1,1) <= o.order_date && o.order_date <= create_date(1996, 12, 31) {
                Some((o.cust_key, (o.order_key, o.order_date >> 16)))
            }
            else { None }
        )
        .semijoin_u(&customers)
        .map(|x| x.1);

    let nations2 = collections.nations.map(|n| (n.nation_key, starts_with(&n.name, b"BRAZIL")));
    let suppliers = 
    collections
        .suppliers()
        .map(|s| (s.nation_key, s.supp_key))
        .join_u(&nations2)
        .map(|(_, supp_key, is_name)| (supp_key, is_name));

    let parts = collections.parts.filter(|p| p.typ.as_str() == "ECONOMY ANODIZED STEEL").map(|p| p.part_key);

    collections
        .lineitems()
        .explode(|l| Some(((l.part_key, (l.supp_key, l.order_key)), ((l.extended_price * (100 - l.discount)) as isize / 100))))
        .semijoin_u(&parts)
        .map(|(_part_key, (supp_key, order_key))| (order_key, supp_key))
        .join_u(&orders)
        .map(|(_order_key, supp_key, order_date)| (supp_key, order_date))
        .join_u(&suppliers)
        .explode(|(_, order_date, is_name)| Some((order_date, DiffPair::new(if is_name { 1 } else { 0 }, 1))))
        .count_total_u()
        .probe()
}