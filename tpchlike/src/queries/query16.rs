use timely::dataflow::*;
// use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

// use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use ::Collections;

// -- $ID$
// -- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     p_brand,
//     p_type,
//     p_size,
//     count(distinct ps_suppkey) as supplier_cnt
// from
//     partsupp,
//     part
// where
//     p_partkey = ps_partkey
//     and p_brand <> ':1'
//     and p_type not like ':2%'
//     and p_size in (:3, :4, :5, :6, :7, :8, :9, :10)
//     and ps_suppkey not in (
//         select
//             s_suppkey
//         from
//             supplier
//         where
//             s_comment like '%Customer%Complaints%'
//     )
// group by
//     p_brand,
//     p_type,
//     p_size
// order by
//     supplier_cnt desc,
//     p_brand,
//     p_type,
//     p_size;
// :n -1

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

fn substring2(source: &[u8], query1: &[u8], query2: &[u8]) -> bool {
    if let Some(pos) = (0 .. (source.len() - query1.len())).position(|i| &source[i..][..query1.len()] == query1) {
        (pos .. query2.len()).any(|i| &source[i..][..query2.len()] == query2)
    }
    else { false }
}

pub fn query<G: Scope>(collections: &mut Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: Lattice+Ord {

    println!("TODO: query 16 could use a count_u if it joins after to re-collect its attributes");

    let suppliers =
    collections
        .suppliers()
        .flat_map(|s| 
            if substring2(s.comment.as_bytes(), b"Customer", b"Complaints") {
                Some((s.supp_key)).into_iter()
            }
            else { None.into_iter() }
        );


    let parts = collections
        .partsupps()
        .map(|ps| (ps.supp_key, ps.part_key))
        .antijoin_u(&suppliers)
        .map(|(_supp_key, part_key)| part_key);

    collections
        .parts()
        .flat_map(|p| 
            if !starts_with(&p.brand, b"Brand45") && !starts_with(&p.typ.as_bytes(), b"MEDIUM POLISHED") && [49, 14, 23, 45, 19, 3, 36, 9].contains(&p.size) {
                Some((p.part_key, (p.brand, p.typ.to_string(), p.size))).into_iter()
            }
            else { None.into_iter() }

        )
        .semijoin_u(&parts)
        .count()
        .probe()
        .0
}