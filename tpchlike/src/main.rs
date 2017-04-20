#[macro_use]
extern crate abomonation;
extern crate timely;
extern crate differential_dataflow;

use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::time::Instant;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use differential_dataflow::{Collection, AsCollection};
use differential_dataflow::input::InputSession;

mod types;
mod queries;

use types::*;

fn main() {

    timely::execute_from_args(std::env::args().skip(4), |computation| {

        let index = computation.index();
        let peers = computation.peers();

        let prefix = ::std::env::args().nth(1).unwrap();;
        let logical_batch = ::std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
        let physical_batch = ::std::env::args().nth(3).unwrap().parse::<usize>().unwrap();

        // customer.tbl lineitem.tbl    nation.tbl  orders.tbl  part.tbl    partsupp.tbl    region.tbl  supplier.tbl
        let mut customers = load::<Customer>(prefix.as_str(), "customer.tbl", index, peers, logical_batch).into_iter().flat_map(|x| x.into_iter()).peekable();
        let mut lineitems = load::<LineItem>(prefix.as_str(), "lineitem.tbl", index, peers, logical_batch).into_iter().flat_map(|x| x.into_iter()).peekable();
        let mut nations = load::<Nation>(prefix.as_str(), "nation.tbl", index, peers, logical_batch).into_iter().flat_map(|x| x.into_iter()).peekable();
        let mut orders = load::<Order>(prefix.as_str(), "orders.tbl", index, peers, logical_batch).into_iter().flat_map(|x| x.into_iter()).peekable();
        let mut parts = load::<Part>(prefix.as_str(), "part.tbl", index, peers, logical_batch).into_iter().flat_map(|x| x.into_iter()).peekable();
        let mut partsupps = load::<PartSupp>(prefix.as_str(), "partsupp.tbl", index, peers, logical_batch).into_iter().flat_map(|x| x.into_iter()).peekable();
        let mut regions = load::<Region>(prefix.as_str(), "region.tbl", index, peers, logical_batch).into_iter().flat_map(|x| x.into_iter()).peekable();
        let mut suppliers = load::<Supplier>(prefix.as_str(), "supplier.tbl", index, peers, logical_batch).into_iter().flat_map(|x| x.into_iter()).peekable();

        let (mut inputs, probes) = computation.scoped::<usize,_,_>(move |builder| {

            // create new inputs to use in computations!
            let (cust_in, cust) = builder.new_input();
            let (line_in, line) = builder.new_input();
            let (nats_in, nats) = builder.new_input();
            let (ords_in, ords) = builder.new_input();
            let (part_in, part) = builder.new_input();
            let (psup_in, psup) = builder.new_input();
            let (regs_in, regs) = builder.new_input();
            let (supp_in, supp) = builder.new_input();

            let collections = Collections::new(
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

            // probes.push(queries::query01::query(&collections));
            // probes.push(queries::query02::query(&collections));
            probes.push(queries::query06::query(&collections));
            // probes.push(queries::query15::query(&collections));
            // probes.push(queries::query17::query(&collections));

            // return the various input handles, and the list of probes.
            ((cust_in, line_in, nats_in, ords_in, part_in, psup_in, regs_in, supp_in), probes)
        });

        // Create input sessions for use in simulating the input.
        let mut customers_input = InputSession::from(&mut inputs.0);
        let mut lineitems_input = InputSession::from(&mut inputs.1);
        let mut nations_input = InputSession::from(&mut inputs.2);
        let mut orders_input = InputSession::from(&mut inputs.3);
        let mut parts_input = InputSession::from(&mut inputs.4);
        let mut partsupps_input = InputSession::from(&mut inputs.5);
        let mut regions_input = InputSession::from(&mut inputs.6);
        let mut suppliers_input = InputSession::from(&mut inputs.7);

        let timer = Instant::now();

        for round in 0 .. (7_000_000 / physical_batch) + 2 {

            let physical_round = round * physical_batch;

            while customers.peek().map(|x| x.0 < physical_round) == Some(true) {
                let (count, data) = customers.next().unwrap();
                customers_input.advance_to(8 * count + 0);
                customers_input.insert(data); 
            }

            while lineitems.peek().map(|x| x.0 < physical_round) == Some(true) {
                let (count, data) = lineitems.next().unwrap();
                lineitems_input.advance_to(8 * count + 1);
                lineitems_input.insert(data); 
            }

            while nations.peek().map(|x| x.0 < physical_round) == Some(true) {
                let (count, data) = nations.next().unwrap();
                nations_input.advance_to(8 * count + 2);
                nations_input.insert(data); 
            }

            while orders.peek().map(|x| x.0 < physical_round) == Some(true) {
                let (count, data) = orders.next().unwrap();
                orders_input.advance_to(8 * count + 3);
                orders_input.insert(data); 
            }

            while parts.peek().map(|x| x.0 < physical_round) == Some(true) {
                let (count, data) = parts.next().unwrap();
                parts_input.advance_to(8 * count + 4);
                parts_input.insert(data); 
            }

            while partsupps.peek().map(|x| x.0 < physical_round) == Some(true) {
                let (count, data) = partsupps.next().unwrap();
                partsupps_input.advance_to(8 * count + 5);
                partsupps_input.insert(data); 
            }

            while regions.peek().map(|x| x.0 < physical_round) == Some(true) {
                let (count, data) = regions.next().unwrap();
                regions_input.advance_to(8 * count + 6);
                regions_input.insert(data); 
            }

            while suppliers.peek().map(|x| x.0 < physical_round) == Some(true) {
                let (count, data) = suppliers.next().unwrap();
                suppliers_input.advance_to(8 * count + 7);
                suppliers_input.insert(data); 
            }

            // catch all inputs up to the same (next) round.
            customers_input.advance_to(8 * physical_round);
            lineitems_input.advance_to(8 * physical_round);
            nations_input.advance_to(8 * physical_round);
            orders_input.advance_to(8 * physical_round);
            parts_input.advance_to(8 * physical_round);
            partsupps_input.advance_to(8 * physical_round);
            regions_input.advance_to(8 * physical_round);
            suppliers_input.advance_to(8 * physical_round);

            // flush each of the sessions.
            customers_input.flush();
            lineitems_input.flush();
            nations_input.flush();
            orders_input.flush();
            parts_input.flush();
            partsupps_input.flush();
            regions_input.flush();
            suppliers_input.flush();

            let time = customers_input.time(); 
            computation.step_while(|| probes.iter().all(|p| p.lt(&time)));
        }

        println!("elapsed: {:?}", timer.elapsed());

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
        }
    }
}

fn load<T>(prefix: &str, name: &str, index: usize, peers: usize, batch: usize) -> Vec<Vec<(usize, T)>> 
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

            let item = T::from(line.as_str());

            if buffer.len() == buffer.capacity() {
                if buffer.len() > 0 {
                    result.push(buffer);
                }
                buffer = Vec::with_capacity(1 << 10);
            }

            buffer.push((count / batch, item));
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

    result
}
