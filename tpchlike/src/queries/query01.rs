use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::difference::DiffPair;

use ::Collections;

// -- $ID$
// -- TPC-H/TPC-R Pricing Summary Report Query (Q1)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     l_returnflag,
//     l_linestatus,
//     sum(l_quantity) as sum_qty,
//     sum(l_extendedprice) as sum_base_price,
//     sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
//     sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
//     avg(l_quantity) as avg_qty,
//     avg(l_extendedprice) as avg_price,
//     avg(l_discount) as avg_disc,
//     count(*) as count_order
// from
//     lineitem
// where
//     l_shipdate <= date '1998-12-01' - interval ':1' day (3)
// group by
//     l_returnflag,
//     l_linestatus
// order by
//     l_returnflag,
//     l_linestatus;
// :n -1

pub fn query<G: Scope>(collections: &Collections<G>) -> ProbeHandle<G::Timestamp> where G::Timestamp: Lattice+Ord {

    collections.lineitems
               .filter(|x| x.ship_date < ::types::create_date(1998,9,1))
               .inner
               .map(|(item, time, diff)| 
                ((item.return_flag, item.line_status), time, 
                    DiffPair::new(item.quantity, 
                    DiffPair::new(item.extended_price,
                    DiffPair::new(item.extended_price * (100 - item.discount) / 100,
                    DiffPair::new(item.extended_price * (100 - item.discount) * (100 + item.tax) / 10000,
                    DiffPair::new(item.discount, diff)))))))
               .as_collection()
               .count()
               .probe()
               .0
}