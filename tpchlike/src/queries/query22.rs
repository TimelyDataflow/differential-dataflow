use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::difference::DiffPair;
use differential_dataflow::operators::group::GroupArranged;
use differential_dataflow::operators::ThresholdTotal;
use differential_dataflow::lattice::Lattice;

use differential_dataflow::trace::Trace;
use differential_dataflow::trace::implementations::ord::OrdValSpine as DefaultValTrace;

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
where G::Timestamp: Lattice+TotalOrder+Ord {

    println!("TODO: Q22 uses a `group` for counting to get an arrangement; could use `count_total`");

    let customers = 
    collections
        .customers()
        .flat_map(|c| {
            if c.acctbal > 0 {
                match &[c.phone[0], c.phone[1]] {
                    b"13" | b"31" | b"23" | b"29" | b"30" | b"18" | b"17" => {
                        Some((((c.phone[1] as u16) << 8) + c.phone[0] as u16, c.acctbal, c.cust_key))
                    },
                    _ => None,
                }
            }
            else { None }
        });

    let averages = 
    customers
        .explode(|(cc, acctbal, _)| Some(((cc, ()), DiffPair::new(acctbal as isize, 1))))
        .group_arranged(|_k,s,t| t.push((s[0].1, 1)), DefaultValTrace::new());

    customers
        .map(|(cc, acct, key)| (key, (cc, acct)))
        .antijoin(&collections.orders().map(|o| o.cust_key).distinct_total())
        .map(|(_, (cc, acct))| (cc, acct as isize))
        .join_core(&averages, |&cc, &acct, &pair| {
            let acct : isize = acct;
            let pair : DiffPair<isize, isize> = pair;
            if acct > (pair.element1 / pair.element2) { Some((cc, acct)) } else { None }
        })
        .explode(|(cc, acct)| Some((cc, DiffPair::new(acct as isize, 1))))
        .count_total()
        .probe()
}