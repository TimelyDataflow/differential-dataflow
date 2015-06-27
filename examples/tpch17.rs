extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

extern crate docopt;
use docopt::Docopt;

use std::rc::Rc;
use std::cell::RefCell;

use std::thread;
use std::fs::File;
use std::io::{BufRead, BufReader};

use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::*;
use timely::networking::initialize_networking;
use timely::networking::initialize_networking_from_file;

use timely::drain::DrainExt;

use timely::progress::timestamp::RootTimestamp;
use timely::communication::pact::Pipeline;

use differential_dataflow::operators::*;

static USAGE: &'static str = "
Usage: tpch17 [options] [<arguments>...] <parts> <items> <delim>

Options:
    -w <arg>, --workers <arg>    number of workers per process [default: 1]
    -p <arg>, --processid <arg>  identity of this process      [default: 0]
    -n <arg>, --processes <arg>  number of processes involved  [default: 1]
    -h <arg>, --hosts <arg>      list of host:port for workers
";

fn main() {

    let args = Docopt::new(USAGE).and_then(|dopt| dopt.parse()).unwrap_or_else(|e| e.exit());

    let workers: u64 = if let Ok(threads) = args.get_str("-w").parse() { threads }
                       else { panic!("invalid setting for --workers: {}", args.get_str("-t")) };
    let process_id: u64 = if let Ok(proc_id) = args.get_str("-p").parse() { proc_id }
                          else { panic!("invalid setting for --processid: {}", args.get_str("-p")) };
    let processes: u64 = if let Ok(processes) = args.get_str("-n").parse() { processes }
                         else { panic!("invalid setting for --processes: {}", args.get_str("-n")) };

    println!("Starting timely with");
    println!("\tworkers:\t{}", workers);
    println!("\tprocesses:\t{}", processes);
    println!("\tprocessid:\t{}", process_id);

    let parts_file = args.get_str("<parts>").to_owned();
    let items_file = args.get_str("<items>").to_owned();
    let mut delimiter = args.get_str("<delim>").to_owned();
    if delimiter.len() == 0 { delimiter = " ".to_owned(); }

    println!("delimiter: {}", delimiter);

    // vector holding communicators to use; one per local worker.
    if processes > 1 {
        println!("Initializing BinaryCommunicator");

        let hosts = args.get_str("-h");
        let communicators = if hosts != "" {
            initialize_networking_from_file(hosts, process_id, workers).ok().expect("error initializing networking")
        }
        else {
            let addresses = (0..processes).map(|index| format!("localhost:{}", 2101 + index).to_string()).collect();
            initialize_networking(addresses, process_id, workers).ok().expect("error initializing networking")
        };

        start_main(communicators, parts_file, items_file, delimiter);
    }
    else if workers > 1 {
        println!("Initializing ProcessCommunicator");
        start_main(ProcessCommunicator::new_vector(workers), parts_file, items_file, delimiter);
    }
    else {
        println!("Initializing ThreadCommunicator");
        start_main(vec![ThreadCommunicator], parts_file, items_file, delimiter);
    };
}

fn start_main<C: Communicator+Send>(communicators: Vec<C>, parts_pattern: String, items_pattern: String, delimiter: String) {
    let mut guards = Vec::new();
    for communicator in communicators.into_iter() {
        let parts = parts_pattern.clone();
        let items = items_pattern.clone();
        let delim = delimiter.clone();
        guards.push(thread::Builder::new().name(format!("worker thread {}", communicator.index()))
                                          .spawn(move || test_dataflow(communicator, parts, items, delim))
                                          .unwrap());
    }

    for guard in guards { guard.join().unwrap(); }
}

fn test_dataflow<C: Communicator>(communicator: C, parts_pattern: String, items_pattern: String, delimiter: String) {

    let comm_index = communicator.index();
    let comm_peers = communicator.peers();

    let mut start = time::precise_time_s();
    let mut computation = GraphRoot::new(communicator);

    let epoch = Rc::new(RefCell::new(0u64));
    let clone = epoch.clone();

    // form the TPCH17-like query
    let (mut parts, mut items) = computation.subcomputation(|builder| {

        let (part_input, parts) = builder.new_input::<((u32, String, String), i32)>();
        let (item_input, items) = builder.new_input::<((u32, u32, u64), i32)>();

        // filter parts by brand and container
        let parts = parts.filter(|x| (x.0).1 == "Brand#23" && (x.0).2 == "MED BOX")
                         .map(|((key, _, _), wgt)| (key, wgt));

        // restrict lineitems to those of the relevant part
        let items = items.join_u(&parts, |x| (x.0, (x.1,x.2)), |y| (y,()), |k,x,_| (*k,x.0,x.1));

        // compute the average quantities
        let average = items.group_by_u(|(x,y,_)| (x,y), |k,v| (*k,*v), |_,s,t| {
            let mut sum = 0;
            let mut cnt = 0;
            for (&val,wgt) in s {
                cnt += wgt;
                sum += val;
            }
            t.push((sum / cnt as u32, 1));
        });

        // join items against their averages, filter by quantity, remove filter coordinate
        items.join_u(&average, |x| (x.0, (x.1, x.2)), |y| y, |k, x, f| (*k, x.0, x.1, *f))
             .filter(|&((_, q, _, avg),_)| q < avg / 5)
             .map(|((key,_,price,_), wgt)| ((key,price), wgt))
             .unary_notify::<u32, _, _>(Pipeline, format!("Subscribe"), vec![RootTimestamp::new(0)], move |i,_,n| {
                 while let Some(_) = i.pull() { }
                 for (time,_) in n.next() {
                     *clone.borrow_mut() = time.inner + 1;
                     if n.frontier(0).len() > 0 {
                         n.notify_at(&RootTimestamp::new(*clone.borrow()));
                     }
                 }
             })
             ;

        (part_input, item_input)
    });

    // read the parts input file
    if let Ok(parts_file) = File::open(format!("{}-{}-{}", parts_pattern, comm_index, comm_peers)) {

        let mut parts_buffer = Vec::new();
        let parts_reader = BufReader::new(parts_file);

        for (index, line) in parts_reader.lines().enumerate() {
            if index as u64 % comm_peers == comm_index {
                let text = line.ok().expect("read error");
                let mut fields = text.split(&delimiter);
                let part_id = fields.next().unwrap().parse::<u32>().unwrap();
                fields.next();
                fields.next();
                let brand = fields.next().unwrap().to_owned();
                fields.next();
                fields.next();
                let container = fields.next().unwrap().to_owned();
                parts_buffer.push(((part_id, brand, container), 1));
            }
        }

        parts.send_at(0u64, parts_buffer.drain_temp());
        computation.step();
    }
    else { println!("worker {}: did not find input {}-{}-{}", computation.index(), parts_pattern, comm_index, comm_peers); }

    // read the lineitems input file
    let mut items_buffer = Vec::new();
    if let Ok(items_file) = File::open(format!("{}-{}-{}", items_pattern, comm_index, comm_peers)) {
        let items_reader =  BufReader::new(items_file);
        for (index, line) in items_reader.lines().enumerate() {
            if index as u64 % comm_peers == comm_index {
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

    println!("data loaded at {}", time::precise_time_s() - start);
    start = time::precise_time_s();

    parts.close();
    let item_count = items_buffer.len();

    let mut buffer = vec![];
    for (index, item) in items_buffer.drain_temp().enumerate() {
        buffer.push(item);
        items.send_at(index as u64, buffer.drain_temp());
        items.advance_to(index as u64 + 1);
        while *epoch.borrow() <= index as u64 {
            computation.step();
        }
    }

    // items.send_at(0, items_buffer.drain_temp());

    items.close();

    while computation.step() { }
    computation.step(); // shut down

    println!("computation finished at {}", time::precise_time_s() - start);
    println!("rate: {}", (item_count as f64) / (time::precise_time_s() - start));
}
