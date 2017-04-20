use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use ::Collections;

// -- $ID$
// -- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     s_acctbal,
//     s_name,
//     n_name,
//     p_partkey,
//     p_mfgr,
//     s_address,
//     s_phone,
//     s_comment
// from
//     part,
//     supplier,
//     partsupp,
//     nation,
//     region
// where
//     p_partkey = ps_partkey
//     and s_suppkey = ps_suppkey
//     and p_size = :1
//     and p_type like '%:2'
//     and s_nationkey = n_nationkey
//     and n_regionkey = r_regionkey
//     and r_name = ':3'
//     and ps_supplycost = (
//         select
//             min(ps_supplycost)
//         from
//             partsupp,
//             supplier,
//             nation,
//             region
//         where
//             p_partkey = ps_partkey
//             and s_suppkey = ps_suppkey
//             and s_nationkey = n_nationkey
//             and n_regionkey = r_regionkey
//             and r_name = ':3'
//     )
// order by
//     s_acctbal desc,
//     n_name,
//     s_name,
//     p_partkey;
// :n 100

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

    let regions = 
    collections
        .regions
        .filter(|x| starts_with(&x.name[..], b"EUROPE"))
        .map(|x| x.region_key);

    let nations = 
    collections
        .nations
        .map(|x| (x.region_key, (x.nation_key, x.name)))
        .semijoin(&regions)
        .map(|(regio_key, (nation_key, name))| (nation_key, name));

    let suppliers = 
    collections
        .suppliers
        .map(|x| (x.nation_key, (x.acctbal, x.name, x.address, x.phone, x.comment, x.supp_key)))
        .semijoin(&nations.map(|x| x.0))
        .map(|(nat, (acc, nam, add, phn, com, key))| (key, (nat, acc, nam, add, phn, com)));

    let parts = 
    collections
        .parts
        .filter(|x| substring(&x.typ[..], b"BRASS") && x.size == 15)
        .map(|x| (x.part_key, x.mfgr));

    let partsupps = 
    collections
        .partsupps
        .map(|x| (x.supp_key, (x.part_key, x.supplycost)))
        .semijoin(&suppliers.map(|x| x.0))
        .map(|(supp, (part, supply_cost))| (part, (supply_cost, supp)))
        .semijoin(&parts.map(|x| x.0))
        .group(|_x, s, t| {
            let minimum = (s[0].0).0;
            t.extend(s.iter().take_while(|x| (x.0).0 == minimum));
        });

    partsupps
        .join(&parts)
        .map(|(part_key, (cost, supp), mfgr)| (supp, (cost, part_key, mfgr)))
        .join(&suppliers)
        .map(|(supp, (cost, part, mfgr), (nat, acc, nam, add, phn, com))| (nat, (cost, part, mfgr, acc, nam, add, phn, com)))
        .join(&nations)
        .probe()
        .0
}