use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::difference::DiffPair;

use ::Collections;

// -- $ID$
// -- TPC-H/TPC-R Global Sales Opportunity Query (Q22)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     cntrycode,
//     count(*) as numcust,
//     sum(c_acctbal) as totacctbal
// from
//     (
//         select
//             substring(c_phone from 1 for 2) as cntrycode,
//             c_acctbal
//         from
//             customer
//         where
//             substring(c_phone from 1 for 2) in
//                 (':1', ':2', ':3', ':4', ':5', ':6', ':7')
//             and c_acctbal > (
//                 select
//                     avg(c_acctbal)
//                 from
//                     customer
//                 where
//                     c_acctbal > 0.00
//                     and substring(c_phone from 1 for 2) in
//                         (':1', ':2', ':3', ':4', ':5', ':6', ':7')
//             )
//             and not exists (
//                 select
//                     *
//                 from
//                     orders
//                 where
//                     o_custkey = c_custkey
//             )
//     ) as custsale
// group by
//     cntrycode
// order by
//     cntrycode;
// :n -1

pub fn query<G: Scope>(collections: &mut Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: Lattice+Ord {

    let customers = 
    collections
        .customers()
        .flat_map(|c| {
            let cc: [u8;2] = [c.phone[0], c.phone[1]];
            if (&cc == b"13" || &cc == b"31" || &cc == b"23" || &cc == b"29" || &cc == b"30" || &cc == b"18" || &cc == b"17") && c.acctbal > 0 {
                Some((((cc[1] as u16) << 8) + cc[0] as u16, c.acctbal, c.cust_key))
            }
            else { None }
        });

    let averages = 
    customers
        .inner
        .map(|((cc, acctbal, _), t, d)| (cc, t, DiffPair::new(acctbal as isize * d, d)))
        .as_collection()
        .count()
        .map(|(cc, pair)| (cc, pair.element1 / pair.element2));

    customers
        .map(|(cc, acct, key)| (key, (cc, acct)))
        .antijoin_u(&collections.orders().map(|o| o.cust_key).distinct_u())
        .map(|(_, (cc, acct))| (cc, acct))
        .join_u(&averages)
        .filter(|&(_cc, acct, avg)| acct as isize > avg)
        .inner
        .map(|((cc, acct, _aggs), t, d)| (cc, t, DiffPair::new(acct as isize * d, d)))
        .as_collection()
        .count_u()
        .probe()
}