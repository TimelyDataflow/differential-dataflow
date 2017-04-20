extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use std::time::Instant;

use std::fs::File;
use std::io::{BufRead, BufReader};

use timely::dataflow::*;
use timely::dataflow::operators::*;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::input::InputSession;

fn main() {

    timely::execute_from_args(std::env::args().skip(4), |computation| {

        let items_pattern = std::env::args().nth(1).unwrap();
        let delimiter = std::env::args().nth(2).unwrap();
        let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();

        let comm_index = computation.index();
        let comm_peers = computation.peers();

        let timer = Instant::now();

        // form the TPCH15-like query
        let (mut items, probe) = computation.scoped::<usize,_,_>(move |builder| {

            let (item_input, items) = builder.new_input::<((u32, u32, u32), _, _)>();

            // restrict lineitems to those of the relevant part
            let items = items.map(|((sup_no, price, disc), t, d)| (sup_no, t, d * (price * (100 - disc) / 100) as isize))
                             // would put a .filter here if we care about dates
                             .as_collection();

            let probe = items.count_u()
                             .map(|(sup_no, count)| (0u8, (count as usize, sup_no)))
                             .group_u(|_k,s,t| {
                                let min = (s[0].0).0;
                                t.extend(s.iter().take_while(|x| (x.0).0 == min).map(|x| ((x.0).1, 1)));
                             })
                             .probe();

            (item_input, probe.0)
        });

        // read the lineitems input file
        let mut items_buffer = Vec::new();
        let items_file = File::open(items_pattern).expect("didn't find items file");
        let items_reader =  BufReader::new(items_file);
        for (index, line) in items_reader.lines().enumerate() {
            if index % comm_peers == comm_index {
                let text = line.ok().expect("read error");
                let mut fields = text.split(&delimiter);
                fields.next();  // order key
                fields.next();  // part key
                let sup_no = fields.next().unwrap().parse::<u32>().unwrap();
                fields.next();  // line number
                fields.next();  // quantity
                let extended_price = fields.next().unwrap().parse::<f64>().unwrap() as u32;
                let discount = (fields.next().unwrap().parse::<f32>().unwrap() * 100.0) as u32;
                items_buffer.push(((sup_no, extended_price, discount), index));
            }
        }

        println!("data loaded at {:?}", timer.elapsed());
        let timer = ::std::time::Instant::now();
        let item_count = items_buffer.len();

        // create an input session and start feeding items.
        let mut session = InputSession::from(&mut items);
        for (counter, (record, index)) in items_buffer.drain(..).enumerate() {
            if session.time().inner < index { session.advance_to(index); }
            session.insert(record);
            if counter % batch == batch - 1 {
                session.flush();
                computation.step_while(|| probe.lt(session.time()));
                let elapsed = timer.elapsed();
                let nanos = 1000000000 * elapsed.as_secs() + elapsed.subsec_nanos() as u64;
                let rate = (index as f64) / (nanos as f64 / 1000000000.0);
                // println!("{:?}:\tprocessed {:?} elements", timer.elapsed(), counter);
                println!("rate: {:?}", rate);
            }
        }

        // finish session and drain computation.
        session.flush();
        computation.step_while(|| probe.lt(session.time()));

        println!("computation finished at {:?}", timer.elapsed());
        let elapsed = timer.elapsed();
        let nanos = (elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64) as f64;
        println!("throughput:  {:?} elt/s", item_count as f64 / (nanos / 1000000000.0));
        println!("avg latency: {:?} ns", nanos / ((item_count / batch) as f64));
    }).unwrap();
}

pub struct Part {
    part_key: usize,
    name: [u8; 55],
    mfgr: [u8; 25],
    brand: [u8; 10],
    type: [u8; 25],
    size: i32,
    container: [u8; 10],
    retail_price: i64,
    comment: [u8; 23],
}

pub struct Supplier {
    supp_key: usize,
    name: [u8; 25],
    address: [u8; 40],
    nation_key: usize,
    phone: [u8; 15],
    acctbal: i64,
    comment: [u8; 101],
}

pub struct PartSupp {
    part_key: usize,
    supp_key: usize,
    availqty: i32,
    supplycost: i64,
    comment: [u8; 199],
}

pub struct Customer {
    cust_key: usize,
    name: [u8; 25],
    address: [u8; 40],
    nation_key: usize,
    phone: [u8; 15],
    acctbal: i64,
    mktsegment: [u8; 10],
    comment: [u8; 117],
}

pub struct Orders {
    order_key: usize,
    cust_key: usize,
    order_status: [u8; 1],
    total_price: i64,
    order_date: DateTime,
    order_priority: [u8; 15],
    clerk: [u8; 15],
    ship_priority: i32,
    comment: [u8; 79],
}

pub struct LineItem {
    order_key: usize,
    part_key: usize,
    supp_key: usize,
    line_number: i32,
    quantity: i64,
    extended_price: i64,
    discount: i64,
    tax: i64,
    return_flag: [u8; 1],
    line_status: [u8; 1],
    ship_date: DateTime,
    commit_date: DateTime,
    receipt_date: DateTime,
    ship_instruct: [u8; 25],
    ship_mode: [u8; 10],
    comment: [u8; 44],
}

pub struct Nation {
    nation_key: usize,
    name: [u8; 25],
    region_key: usize,
    comment: [u8; 152],
}

pub struct Region {
    region_key: usize,
    name: [u8; 25],
    comment: [u8; 152],
}