use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::TotalOrder;

use ::Collections;

// -- $ID$
// -- TPC-H/TPC-R Important Stock Identification Query (Q11)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     ps_partkey,
//     sum(ps_supplycost * ps_availqty) as value
// from
//     partsupp,
//     supplier,
//     nation
// where
//     ps_suppkey = s_suppkey
//     and s_nationkey = n_nationkey
//     and n_name = ':1'
// group by
//     ps_partkey having
//         sum(ps_supplycost * ps_availqty) > (
//             select
//                 sum(ps_supplycost * ps_availqty) * :2
//             from
//                 partsupp,
//                 supplier,
//                 nation
//             where
//                 ps_suppkey = s_suppkey
//                 and s_nationkey = n_nationkey
//                 and n_name = ':1'
//         )
// order by
//     value desc;
// :n -1

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: TotalOrder+Ord {

    println!("TODO: Q11 does a global aggregation with 0u8 as a key rather than ().");

    let nations =
    collections
        .nations()
        .filter(|n| starts_with(&n.name, b"GERMANY"))
        .map(|n| n.nation_key);

    let suppliers =
    collections
        .suppliers()
        .map(|s| (s.nation_key, s.supp_key))
        .semijoin_u(&nations)
        .map(|s| s.1);

    collections
        .partsupps()
        .explode(|x| Some(((x.supp_key, x.part_key), (x.supplycost as isize) * (x.availqty as isize))))
        .semijoin_u(&suppliers)
        .map(|(_, part_key)| (0u8, part_key))
        .group_u(|_part_key, s, t| {
            let threshold: isize = s.iter().map(|x| x.1 as isize).sum::<isize>() / 10000;
            t.extend(s.iter().filter(|x| x.1 > threshold).map(|&(&a,b)| (a, b)));
        })
        .map(|(_, part_key)| part_key)
        .count_total_u()
        .probe()
}