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

fn main() {

    timely::execute_from_args(std::env::args().skip(4), |computation| {

        let parts_pattern = std::env::args().nth(1).unwrap();
        let items_pattern = std::env::args().nth(2).unwrap();
        let delimiter = std::env::args().nth(3).unwrap();
        let batch: usize = std::env::args().nth(4).unwrap().parse().unwrap();

        let comm_index = computation.index();
        let comm_peers = computation.peers();

        let timer = Instant::now();

        // form the TPCH17-like query
        let (mut parts, mut items, probe) = computation.scoped::<usize,_,_>(move |builder| {

            let (part_input, parts) = builder.new_input::<((u32, String, String), _, _)>();
            let (item_input, items) = builder.new_input::<((u32, u32, u64), _, _)>();

            // filter parts by brand and container
            let parts = parts.as_collection()
                             .filter(|x| x.1 == "Brand#23" && x.2 == "MED BOX")
                             .map(|(key, _, _)| key);

            // restrict lineitems to those of the relevant part
            let items = items.as_collection()
                             .map(|x| (x.0, (x.1, x.2)))
                             .semijoin(&parts)
                             .map(|(k,v)| (k, v.0, v.1));

            // group by item id, keep way below-average quantities.
            let probe = items.map(|x| (x.0, (x.1, x.2)))
                             .group(|_, s, t| {

                                 let sum: u32 = s.iter().map(|x| (x.0).0).sum();
                                 let cnt = s.len() as u32;
                                 let avg = sum / cnt as u32;

                                 for &((q, price), _) in s {
                                    if q < avg / 5 {
                                        t.push((price, 1));
                                    }
                                 }

                             })
                             .probe();

            (part_input, item_input, probe.0)
        });

        // read the parts input file
        let parts_file = File::open(parts_pattern).expect("didn't find parts file");
        if comm_index == 0 {
            let &time = parts.time();
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
                    parts.send(((part_id, brand, container), time, 1));
                }
            }
        }

        computation.step();

        // read the lineitems input file
        let mut items_buffer = Vec::new();
        let items_file = File::open(items_pattern).expect("didn't find items file");
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
                items_buffer.push(((item_id, quantity, extended_price), index));
            }
        }

        println!("data loaded at {:?}", timer.elapsed());
        let timer = ::std::time::Instant::now();

        parts.close();

        let item_count = items_buffer.len();

        {
        let mut counter = 0;
        let mut session = ::differential_dataflow::input::InputSession::from(&mut items);
        for ((item_id, quantity, extended_price), index) in items_buffer.drain(..) {
            if session.time().inner < index { session.advance_to(index); }
            session.insert((item_id, quantity, extended_price));
            if counter % batch == batch - 1 {
                session.flush();
                computation.step_while(|| probe.lt(session.time()));
            }
            counter += 1;
        }
        }

        items.close();

        while computation.step() { }
        computation.step(); // shut down

        println!("computation finished at {:?}", timer.elapsed());
        let elapsed = timer.elapsed();
        let nanos = (elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64) as f64;
        println!("rate: {:?}", item_count as f64 / (nanos / 1000000000.0));
    }).unwrap();
}
