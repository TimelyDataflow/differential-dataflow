extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use std::rc::Rc;
use std::cell::RefCell;
use std::time::Instant;

use std::fs::File;
use std::io::{BufRead, BufReader};

use timely::dataflow::*;
use timely::dataflow::operators::*;

use timely::dataflow::channels::pact::Pipeline;
use timely::progress::timestamp::RootTimestamp;

use differential_dataflow::Collection;
use differential_dataflow::operators::group::GroupUnsigned;
use differential_dataflow::operators::join::JoinUnsigned;

fn main() {

    let parts_pattern = std::env::args().nth(2).unwrap();
    let items_pattern = std::env::args().nth(3).unwrap();
    let delimiter = std::env::args().nth(4).unwrap();

    timely::execute_from_args(std::env::args().skip(4), move |computation| {

        let comm_index = computation.index();
        let comm_peers = computation.peers();

        let timer = Instant::now();

        let epoch = Rc::new(RefCell::new(0u64));
        let clone = epoch.clone();

        // form the TPCH17-like query
        let (mut parts, mut items) = computation.scoped(|builder| {

            let (part_input, parts) = builder.new_input::<((u32, String, String), i32)>();
            let (item_input, items) = builder.new_input::<((u32, u32, u64), i32)>();

            let parts = Collection::new(parts);
            let items = Collection::new(items);

            // filter parts by brand and container
            let parts = parts.filter(|x| x.1 == "Brand#23" && x.2 == "MED BOX")
                             .map(|(key, _, _)| key);

            // restrict lineitems to those of the relevant part
            let items = items.map(|x| (x.0, (x.1, x.2)))
                             .semijoin_u(&parts)
                             .map(|(k,v)| (k, v.0, v.1));

            // compute the average quantities
            let average = items.map(|(x,y,_)| (x,y)).group_u(|_,s,t| {
                let mut sum = 0;
                let mut cnt = 0;
                for (&val,wgt) in s {
                    cnt += wgt;
                    sum += val;
                }
                t.push((sum / cnt as u32, 1));
            });

            // join items against their averages, filter by quantity, remove filter coordinate
            items.map(|x| (x.0, (x.1, x.2)))
                 .join_map_u(&average, |k, x, f| (*k, x.0, x.1, *f))
                 .filter(|&(_, q, _, avg)| q < avg / 5)
                 .map(|(key, _, price, _)| (key, price))
                 .inner
                 .unary_notify::<u32, _, _>(Pipeline, "Subscribe", vec![RootTimestamp::new(0)], move |i,_,n| {
                     while let Some(_) = i.next() { }
                     for (time,_) in n.next() {
                         *clone.borrow_mut() = time.inner + 1;
                         if n.frontier(0).len() > 0 {
                             n.notify_at(time.delayed(&RootTimestamp::new(*clone.borrow())));
                         }
                     }
                 })
                 ;

            (part_input, item_input)
        });

        // read the parts input file
        if let Ok(parts_file) = File::open(format!("{}-{}-{}", parts_pattern, comm_index, comm_peers)) {
            let parts_reader = BufReader::new(parts_file);
            for (index, line) in parts_reader.lines().enumerate() {
                if index % comm_peers == comm_index {
                    let text = line.ok().expect("read error");
                    let mut fields = text.split(&delimiter);
                    let part_id = fields.next().unwrap().parse::<u32>().unwrap();
                    fields.next();
                    fields.next();
                    let brand = fields.next().unwrap().to_owned();
                    fields.next();
                    fields.next();
                    let container = fields.next().unwrap().to_owned();
                    parts.send(((part_id, brand, container), 1));
                }
            }

            computation.step();
        }
        else { println!("worker {}: did not find input {}-{}-{}", computation.index(), parts_pattern, comm_index, comm_peers); }

        // read the lineitems input file
        let mut items_buffer = Vec::new();
        if let Ok(items_file) = File::open(format!("{}-{}-{}", items_pattern, comm_index, comm_peers)) {
            let items_reader =  BufReader::new(items_file);
            for (index, line) in items_reader.lines().enumerate() {
                if index % comm_peers == comm_index {
                    let text = line.ok().expect("read error");
                    let mut fields = text.split(&delimiter);
                    fields.next();
                    let item_id = fields.next().unwrap().parse::<u32>().unwrap();
                    fields.next();
                    fields.next();
                    let quantity = fields.next().unwrap().parse::<u32>().unwrap();
                    let extended_price = fields.next().unwrap().parse::<f64>().unwrap() as u64;
                    items_buffer.push(((item_id, quantity, extended_price), 1i32));
                }
            }
        }
        else { println!("worker {}: did not find input {}-{}-{}", comm_index, items_pattern, comm_index, comm_peers); }

        println!("data loaded at {:?}", timer.elapsed());
        let timer = ::std::time::Instant::now();

        parts.close();
        // let item_count = items_buffer.len();

        for (index, item) in items_buffer.drain(..).enumerate() {
            items.send(item);
            items.advance_to(index as u64 + 1);
            while *epoch.borrow() <= index as u64 {
                computation.step();
            }
        }

        // items.send_at(0, items_buffer.drain_temp());

        items.close();

        while computation.step() { }
        computation.step(); // shut down

        println!("computation finished at {:?}", timer.elapsed());
        // println!("rate: {}", (item_count as f64) / timer.elapsed());
    }).unwrap();
}
