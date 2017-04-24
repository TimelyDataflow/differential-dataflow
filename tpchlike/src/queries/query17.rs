use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use ::Collections;

// -- $ID$
// -- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//   sum(l_extendedprice) / 7.0 as avg_yearly
// from
//   lineitem,
//   part
// where
//   p_partkey = l_partkey
//   and p_brand = ':1'
//   and p_container = ':2'
//   and l_quantity < (
//     select
//       0.2 * avg(l_quantity)
//     from
//       lineitem
//     where
//       l_partkey = p_partkey
//   );
// :n -1

pub fn query<G: Scope>(collections: &mut Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: Lattice+Ord {

    println!("TODO: query 17 gets a global count with key 0u8, rather than ().");

    let parts = 
    collections
        .parts()   // We fluff out search strings to have the right lengths. \\
        .filter(|x| &x.brand == b"Brand#2300" && &x.container == b"MED BOX000")
        .map(|x| x.part_key);

    collections
        .lineitems()
        .map(|x| (x.part_key, (x.quantity, x.extended_price)))
        .semijoin_u(&parts)
        .group_u(|_k, s, t| {

            // determine the total and count of quantity.
            let total: i64 = s.iter().map(|x| (x.0).0).sum();
            let count: i64 = s.iter().map(|x| x.1 as i64).sum();

            // threshold we are asked to use.
            let threshold = (total / count) / 7;

            // produce as output those tuples with below-threshold quantity.
            t.extend(s.iter().filter(|&&((quantity,_),_)| quantity < threshold)
                             .map(|&((_,price),count)| (price, count)));
        })
        .inner
        .map(|((_part, price), time, count)| (0u8, time, price * count as i64))
        .as_collection()
        .count_u()
        .probe()
        .0
}