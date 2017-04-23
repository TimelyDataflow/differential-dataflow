use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use ::Collections;
use ::types::create_date;

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
where G::Timestamp: Lattice+Ord {

    let nations =
    collections
        .nations()
        .filter(|n| starts_with(&n.name, b"GERMANY"))
        .map(|n| n.nation_key);

    let suppliers =
    collections
        .suppliers()
        .map(|s| (s.nation_key, s.supp_key))
        .semijoin(&nations)
        .map(|s| s.1);

    collections
        .partsupps()
        .inner
        .map(|(x,t,d)| ((x.supp_key, x.part_key), t, ((x.supplycost as isize) * (x.availqty as isize) * d)))
        .as_collection()
        .semijoin(&suppliers)
        .map(|(_, part_key)| ((), part_key))
        .group(|_part_key, s, t| {
            let threshold: isize = s.iter().map(|x| x.1 as isize).sum::<isize>() / 10000;
            t.extend(s.iter().filter(|x| x.1 > threshold));
        })
        .count()
        .probe()
        .0
}