use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use ::Collections;
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R Top Supplier Query (Q15)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// create view revenue:s (supplier_no, total_revenue) as
//     select
//         l_suppkey,
//         sum(l_extendedprice * (1 - l_discount))
//     from
//         lineitem
//     where
//         l_shipdate >= date ':1'
//         and l_shipdate < date ':1' + interval '3' month
//     group by
//         l_suppkey;
//
// :o
// select
//     s_suppkey,
//     s_name,
//     s_address,
//     s_phone,
//     total_revenue
// from
//     supplier,
//     revenue:s
// where
//     s_suppkey = supplier_no
//     and total_revenue = (
//         select
//             max(total_revenue)
//         from
//             revenue:s
//     )
// order by
//     s_suppkey;
//
// drop view revenue:s;
// :n -1

pub fn query<G: Scope>(collections: &mut Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: Lattice+Ord {

    println!("TODO: query 15 takes a global aggregate with key 0u8, instead of ().");

    // revenue by supplier
    let revenue = 
        collections
            .lineitems()
            .inner
            .flat_map(|(item, time, diff)| 
                if create_date(1996, 1, 1) <= item.ship_date && item.ship_date < create_date(1996,4,1) {
                    Some((item.supp_key, time, (item.extended_price * (100 - item.discount) / 100) as isize * diff))
                }
                else { None }
            )
            .as_collection();

    // suppliers with maximum revenue
    let top_suppliers =
        revenue
            // do a hierarchical min, to improve update perf.
            .map(|key| ((key % 1000) as u16, key))
            .group_u(|_k, s, t| {
                let max = s.iter().map(|x| x.1).max().unwrap();
                t.extend(s.iter().filter(|x| x.1 == max));
            })
            .map(|(_,key)| ((key % 100) as u8, key))
            .group_u(|_k, s, t| {
                let max = s.iter().map(|x| x.1).max().unwrap();
                t.extend(s.iter().filter(|x| x.1 == max));
            })
            .map(|(_,key)| ((key % 10) as u8, key))
            .group_u(|_k, s, t| {
                let max = s.iter().map(|x| x.1).max().unwrap();
                t.extend(s.iter().filter(|x| x.1 == max));
            })
            .map(|(_,key)| (0u8, key))
            .group_u(|_k, s, t| {
                let max = s.iter().map(|x| x.1).max().unwrap();
                t.extend(s.iter().filter(|x| x.1 == max));
            })
            .map(|(_, key)| key)
            .count_u();

    collections
        .suppliers()
        .map(|s| (s.supp_key, (s.name, s.address.to_string(), s.phone)))
        .join_u(&top_suppliers)
        .probe()
}