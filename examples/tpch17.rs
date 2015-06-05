extern crate rand;
extern crate time;
extern crate columnar;
extern crate timely;
extern crate differential_dataflow;

extern crate docopt;
use docopt::Docopt;

use std::thread;
use std::fs::File;
use std::io::{BufRead, BufReader};

use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::*;
use timely::networking::initialize_networking;
use timely::networking::initialize_networking_from_file;

use timely::drain::DrainExt;

// use differential_dataflow::collection_trace::lookup::UnsignedInt;
// use differential_dataflow::collection_trace::LeastUpperBound;

use differential_dataflow::operators::*;

// The typical differential dataflow vertex receives updates of the form (key, time, value, update),
// where the data are logically partitioned by key, and are then subject to various aggregations by time,
// accumulating for each value the update integers. The resulting multiset is the subjected to computation.

// The implementation I am currently most comfortable with is *conservative* in the sense that it will defer updates
// until it has received all updates for a time, at which point it commits these updates permanently. This is done
// to avoid issues with running logic on partially formed data, but should also simplify our data management story.
// Rather than requiring random access to diffs, we can store them as flat arrays (possibly sorted) and integrate
// them using merge techniques. Updating cached accumulations seems maybe harder w/o hashmaps, but we'll see...

static USAGE: &'static str = "
Usage: tpch17 [options] [<arguments>...]

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

        start_main(communicators);
    }
    else if workers > 1 {
        println!("Initializing ProcessCommunicator");
        start_main(ProcessCommunicator::new_vector(workers));
    }
    else {
        println!("Initializing ThreadCommunicator");
        start_main(vec![ThreadCommunicator]);
    };
}

fn start_main<C: Communicator+Send>(communicators: Vec<C>) {
    // let communicators = ProcessCommunicator::new_vector(1);
    let mut guards = Vec::new();
    for communicator in communicators.into_iter() {
        guards.push(thread::Builder::new().name(format!("worker thread {}", communicator.index()))
                                          .spawn(move || test_dataflow(communicator))
                                          .unwrap());
    }

    for guard in guards { guard.join().unwrap(); }
}

fn test_dataflow<C: Communicator>(communicator: C) {

    let start = time::precise_time_s();
    let mut computation = GraphRoot::new(communicator);

    let (mut parts, mut items) = computation.subcomputation(|builder| {

        let (part_input, parts) = builder.new_input::<((u32, String, String), i32)>();
        let (item_input, items) = builder.new_input::<((u32, u32, u64), i32)>();

        // compute the average quantities
        let average = items.group_by_u(|(x,y,_)| (x,y), |k,v| (*k,*v), |_,s,t| {
            let mut sum = 0;
            let mut cnt = 0;
            for &(val,wgt) in s.iter() {
                cnt += wgt;
                sum += val;
            }
            t.push((sum / cnt as u32, 1));
        });

        // filter parts by brand and container
        let parts = parts.filter(|x| (x.0).1 == "Brand#33" && (x.0).2 == "WRAP PACK")
                         .map(|((key, _, _), wgt)| (key, wgt));

        // join items against their averages, filter by quantity, remove filter coordinate
        let items = items.join_u(&average, |x| (x.0, (x.1, x.2)), |y| y, |k, x, f| (*k, x.0, x.1, *f))
                         .filter(|&((_, q, _, avg),_)| q < avg / 5)
                         .map(|((key,_,price,_), wgt)| ((key,price), wgt));

        // semi-join against the part keys we retained. think of a better way to produce sum ...
        parts.join_u(&items, |k| (k,()), |(k,p)| (k,p), |_,_,p| *p)
             .inspect_batch(|_t,x| println!("results: {:?}", x.len()));

        (part_input, item_input)
    });

    let mut parts_buffer = Vec::new();
    let parts_reader =  BufReader::new(File::open(format!("/Users/mcsherry/Desktop/tpch-sf-10/part-{}.tbl", computation.index())).unwrap());
    for line in parts_reader.lines() {
        let text = line.ok().expect("read error");
        let mut fields = text.split("|");

        let part_id = fields.next().unwrap().parse::<u32>().unwrap();
        fields.next();
        fields.next();
        let brand = fields.next().unwrap().to_owned();
        fields.next();
        fields.next();
        let container = fields.next().unwrap().to_owned();

        parts_buffer.push(((part_id, brand, container), 1));
        if parts_buffer.len() == 1024 {
            parts.send_at(0u64, parts_buffer.drain_temp());
            computation.step();

        }
    }

    parts.send_at(0u64, parts_buffer.drain_temp());
    computation.step();

    let mut items_buffer = Vec::new();
    let items_reader =  BufReader::new(File::open(format!("/Users/mcsherry/Desktop/tpch-sf-10/lineitem-{}.tbl", computation.index())).unwrap());
    for line in items_reader.lines() {
        let text = line.ok().expect("read error");
        let mut fields = text.split("|");

        fields.next();
        let item_id = fields.next().unwrap().parse::<u32>().unwrap();
        fields.next();
        fields.next();
        let quantity = fields.next().unwrap().parse::<u32>().unwrap();
        let extended_price = fields.next().unwrap().parse::<f64>().unwrap() as u64;

        items_buffer.push(((item_id, quantity, extended_price), 1));
        if items_buffer.len() == 1024 {
            items.send_at(0u64, items_buffer.drain_temp());
            computation.step();
        }
    }

    items.send_at(0u64, items_buffer.drain_temp());
    computation.step();

    println!("data loaded at {}", time::precise_time_s() - start);

    parts.close();
    items.close();

    while computation.step() { std::thread::yield_now(); }
    computation.step(); // shut down

    println!("computation finished at {}", time::precise_time_s() - start);
}
