#[macro_use]
extern crate abomonation;
extern crate timely;
extern crate differential_dataflow;
extern crate arrayvec;

use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::time::Instant;

use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;

use differential_dataflow::{Collection, AsCollection};

mod types;
mod queries;

use types::*;

fn main() {

    timely::execute_from_args(std::env::args().skip(4), |worker| {

        let index = worker.index();
        let peers = worker.peers();

        let prefix = ::std::env::args().nth(1).unwrap();;
        let logical_batch = ::std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
        let physical_batch = ::std::env::args().nth(3).unwrap().parse::<usize>().unwrap();
        let query: usize = ::std::env::args().nth(4).unwrap().parse().unwrap();

        let (mut inputs, probes, used) = worker.scoped::<usize,_,_>(move |scope| {

            // create new inputs to use in workers!
            let (cust_in, cust) = scope.new_input();
            let (line_in, line) = scope.new_input();
            let (nats_in, nats) = scope.new_input();
            let (ords_in, ords) = scope.new_input();
            let (part_in, part) = scope.new_input();
            let (psup_in, psup) = scope.new_input();
            let (regs_in, regs) = scope.new_input();
            let (supp_in, supp) = scope.new_input();

            let mut collections = Collections::new(
                cust.as_collection(),
                line.as_collection(),
                nats.as_collection(),
                ords.as_collection(),
                part.as_collection(),
                psup.as_collection(),
                regs.as_collection(),
                supp.as_collection(),
            );  

            let mut probes = Vec::new();

            match query {
                // 1 => probes.push(queries::query01::query(&mut collections)),
                // 2 => probes.push(queries::query02::query(&mut collections)),
                // 3 => probes.push(queries::query03::query(&mut collections)),
                // 4 => probes.push(queries::query04::query(&mut collections)),
                // 5 => probes.push(queries::query05::query(&mut collections)),
                // 6 => probes.push(queries::query06::query(&mut collections)),
                // 7 => probes.push(queries::query07::query(&mut collections)),
                // 8 => probes.push(queries::query08::query(&mut collections)),
                // 9 => probes.push(queries::query09::query(&mut collections)),
                // 10 => probes.push(queries::query10::query(&mut collections)),
                // 11 => probes.push(queries::query11::query(&mut collections)),
                // 12 => probes.push(queries::query12::query(&mut collections)),
                // 14 => probes.push(queries::query14::query(&mut collections)),
                // 15 => probes.push(queries::query15::query(&mut collections)),
                // 17 => probes.push(queries::query17::query(&mut collections)),
                // 18 => probes.push(queries::query18::query(&mut collections)),
                // 19 => probes.push(queries::query19::query(&mut collections)),
                // 20 => probes.push(queries::query20::query(&mut collections)),
                // 21 => probes.push(queries::query21::query(&mut collections)),
                22 => probes.push(queries::query22::query(&mut collections)),
                _ => panic!("query: {:?} unimplemented", query),
            }

            // return the various input handles, and the list of probes.
            ((cust_in, line_in, nats_in, ords_in, part_in, psup_in, regs_in, supp_in), probes, collections.used)
        });

        // customer.tbl lineitem.tbl    nation.tbl  orders.tbl  part.tbl    partsupp.tbl    region.tbl  supplier.tbl
        let mut customers = if used[0] { load::<Customer>(prefix.as_str(), "customer.tbl", index, peers, logical_batch, physical_batch, 0) } else { Vec::new() };
        let mut lineitems = if used[1] { load::<LineItem>(prefix.as_str(), "lineitem.tbl", index, peers, logical_batch, physical_batch, 1) } else { Vec::new() };
        let mut nations = if used[2] { load::<Nation>(prefix.as_str(), "nation.tbl", index, peers, logical_batch, physical_batch, 2) } else { Vec::new() };
        let mut orders = if used[3] { load::<Order>(prefix.as_str(), "orders.tbl", index, peers, logical_batch, physical_batch, 3) } else { Vec::new() };
        let mut parts = if used[4] { load::<Part>(prefix.as_str(), "part.tbl", index, peers, logical_batch, physical_batch, 4) } else { Vec::new() };
        let mut partsupps = if used[5] { load::<PartSupp>(prefix.as_str(), "partsupp.tbl", index, peers, logical_batch, physical_batch, 5) } else { Vec::new() };
        let mut regions = if used[6] { load::<Region>(prefix.as_str(), "region.tbl", index, peers, logical_batch, physical_batch, 6) } else { Vec::new() };
        let mut suppliers = if used[7] { load::<Supplier>(prefix.as_str(), "supplier.tbl", index, peers, logical_batch, physical_batch, 7 ) } else { Vec::new() };

        let mut tuples = 0usize;
        tuples += customers.iter().map(|x| x.len()).sum();
        tuples += lineitems.iter().map(|x| x.len()).sum();
        tuples += nations.iter().map(|x| x.len()).sum();
        tuples += orders.iter().map(|x| x.len()).sum();
        tuples += parts.iter().map(|x| x.len()).sum();
        tuples += partsupps.iter().map(|x| x.len()).sum();
        tuples += regions.iter().map(|x| x.len()).sum();
        tuples += suppliers.iter().map(|x| x.len()).sum();

        let timer = Instant::now();
        let mut round = 0;
        while customers.len() > 0 || lineitems.len() > 0 || nations.len() > 0 || orders.len() > 0 || parts.len() > 0 || partsupps.len() > 0 || regions.len() > 0 || suppliers.len() > 0 {

            // introduce physical batch of data for each input with remaining data.
            if let Some(mut data) = customers.pop() { inputs.0.send_batch(&mut data); }
            if let Some(mut data) = lineitems.pop() { inputs.1.send_batch(&mut data); }
            if let Some(mut data) = nations.pop() { inputs.2.send_batch(&mut data); }
            if let Some(mut data) = orders.pop() { inputs.3.send_batch(&mut data); }
            if let Some(mut data) = parts.pop() { inputs.4.send_batch(&mut data); }
            if let Some(mut data) = partsupps.pop() { inputs.5.send_batch(&mut data); }
            if let Some(mut data) = regions.pop() { inputs.6.send_batch(&mut data); }
            if let Some(mut data) = suppliers.pop() { inputs.7.send_batch(&mut data); }

            // catch all inputs up to the same (next) round.
            let next_round = 8 * (round + 1) * physical_batch;
            inputs.0.advance_to(next_round);
            inputs.1.advance_to(next_round);
            inputs.2.advance_to(next_round);
            inputs.3.advance_to(next_round);
            inputs.4.advance_to(next_round);
            inputs.5.advance_to(next_round);
            inputs.6.advance_to(next_round);
            inputs.7.advance_to(next_round);

            let time = inputs.0.time(); 
            worker.step_while(|| probes.iter().all(|p| p.lt(&time)));
            round += 1;
        }

        let elapsed = timer.elapsed();
        let nanos = elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64;
        println!("elapsed: {:?}, tuples: {:?}, rate: {:?}", timer.elapsed(), tuples, (tuples as f64) / (nanos as f64 / 1000000000.0));

    }).unwrap();
}

pub struct Collections<G: Scope> {
    customers: Collection<G, Customer, isize>,
    lineitems: Collection<G, LineItem, isize>,
    nations: Collection<G, Nation, isize>,
    orders: Collection<G, Order, isize>,
    parts: Collection<G, Part, isize>,
    partsupps: Collection<G, PartSupp, isize>,
    regions: Collection<G, Region, isize>,
    suppliers: Collection<G, Supplier, isize>,
    used: [bool; 8],
}

impl<G: Scope> Collections<G> {
    fn new(
        customers: Collection<G, Customer, isize>,
        lineitems: Collection<G, LineItem, isize>,
        nations: Collection<G, Nation, isize>,
        orders: Collection<G, Order, isize>,
        parts: Collection<G, Part, isize>,
        partsupps: Collection<G, PartSupp, isize>,
        regions: Collection<G, Region, isize>,
        suppliers: Collection<G, Supplier, isize>,
    ) -> Self {

        Collections {
            customers: customers,
            lineitems: lineitems,
            nations: nations,
            orders: orders,
            parts: parts,
            partsupps: partsupps,
            regions: regions,
            suppliers: suppliers,
            used: [false; 8]
        }
    }

    fn customers(&mut self) -> &Collection<G, Customer, isize> { self.used[0] = true; &self.customers }
    fn lineitems(&mut self) -> &Collection<G, LineItem, isize> { self.used[1] = true; &self.lineitems }
    fn nations(&mut self) -> &Collection<G, Nation, isize> { self.used[2] = true; &self.nations }
    fn orders(&mut self) -> &Collection<G, Order, isize> { self.used[3] = true; &self.orders }
    fn parts(&mut self) -> &Collection<G, Part, isize> { self.used[4] = true; &self.parts }
    fn partsupps(&mut self) -> &Collection<G, PartSupp, isize> { self.used[5] = true; &self.partsupps }
    fn regions(&mut self) -> &Collection<G, Region, isize> { self.used[6] = true; &self.regions }
    fn suppliers(&mut self) -> &Collection<G, Supplier, isize> { self.used[7] = true; &self.suppliers }
}

// Returns a sequence of physical batches of ready-to-go timestamped data. Not clear that `input` can exploit the pre-arrangement yet.
fn load<T>(prefix: &str, name: &str, index: usize, peers: usize, logical_batch: usize, physical_batch: usize, off: usize) 
    -> Vec<Vec<(T, Product<RootTimestamp, usize>, isize)>> 
where T: for<'a> From<&'a str> {

    let mut result = Vec::new();
    let mut buffer = Vec::new();

    let path = format!("{}{}", prefix, name);

    let items_file = File::open(&path).expect("didn't find items file");
    let mut items_reader =  BufReader::new(items_file);
    let mut count = 0;

    let mut line = String::new();

    while items_reader.read_line(&mut line).unwrap() > 0 { 

        if count % peers == index {

            let logical = (8 * count / logical_batch) + off;
            let physical = logical / physical_batch;
            let round = physical / 8;

            while result.len() < round {
                result.push(buffer);
                buffer = Vec::with_capacity(2 + logical_batch * physical_batch / peers);
            }

            let item = T::from(line.as_str());
            buffer.push((item, RootTimestamp::new(logical), 1));
        }

        if count % 10000 == 0 { 
            print!("\rreading records from {:?}: {:?}", name, count); 
            ::std::io::stdout().flush().ok().expect("Could not flush stdout"); 
        }

        count += 1;

        line.clear();
    }
    println!("\rreading records from {:?}: {:?}", name, count); 
    
    if buffer.len() > 0 {
        result.push(buffer);
    }

    result.reverse();
    result
}
