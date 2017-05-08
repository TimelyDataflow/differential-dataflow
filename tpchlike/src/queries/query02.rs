use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::TotalOrder;

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

pub fn query<G: Scope>(collections: &mut Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: TotalOrder+Ord {

    let regions = 
    collections
        .regions()
        .flat_map(|x| if starts_with(&x.name[..], b"EUROPE") { Some(x.region_key) } else { None });

    let nations = 
    collections
        .nations()
        .map(|x| (x.region_key, (x.nation_key, x.name)))
        .semijoin_u(&regions)
        .map(|(_region_key, (nation_key, name))| (nation_key, name));

    let suppliers = 
    collections
        .suppliers()
        .map(|x| (x.nation_key, (x.acctbal, x.name, x.address.to_string(), x.phone, x.comment.to_string(), x.supp_key)))
        .semijoin_u(&nations.map(|x| x.0))
        .map(|(nat, (acc, nam, add, phn, com, key))| (key, (nat, acc, nam, add, phn, com)));

    let parts = 
    collections
        .parts()
        .flat_map(|x| if substring(x.typ.as_str().as_bytes(), b"BRASS") && x.size == 15 { Some((x.part_key, x.mfgr)) } else { None });

    let partsupps = 
    collections
        .partsupps()
        .map(|x| (x.supp_key, (x.part_key, x.supplycost)))
        .semijoin_u(&suppliers.map(|x| x.0))
        .map(|(supp, (part, supply_cost))| (part, (supply_cost, supp)))
        .semijoin_u(&parts.map(|x| x.0))
        .group_u(|_x, s, t| {
            let minimum = (s[0].0).0;
            t.extend(s.iter().take_while(|x| (x.0).0 == minimum));
        });

    partsupps
        .join_u(&parts)
        .map(|(part_key, (cost, supp), mfgr)| (supp, (cost, part_key, mfgr)))
        .join_u(&suppliers)
        .map(|(_supp, (cost, part, mfgr), (nat, acc, nam, add, phn, com))| (nat, (cost, part, mfgr, acc, nam, add, phn, com)))
        .join_u(&nations)
        .probe()
}