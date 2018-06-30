extern crate timely;
extern crate differential_dataflow;

use std::net::TcpListener;
use timely::dataflow::operators::{Map, capture::{EventReader, Replay}};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely::logging::{TimelySetup, TimelyEvent};
use timely::logging::TimelyEvent::{Operates, Channels, Messages};

use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::Join;

fn main() {
    timely::execute_from_args(std::env::args().skip(2), |worker| {

        let source_peers = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();

        // create replayers from disjoint partition of source worker identifiers.
        let replayers =
        (0 .. source_peers)
            .filter(|i| i % worker.peers() == worker.index())
            .map(|i| TcpListener::bind(format!("127.0.0.1:{}", 8000 + i)).unwrap())
            .collect::<Vec<_>>()
            .into_iter()
            .map(|l| l.incoming().next().unwrap().unwrap())
            .map(|r| EventReader::<Product<RootTimestamp, u64>,(u64, TimelySetup, TimelyEvent),_>::new(r))
            .collect::<Vec<_>>();

        worker.dataflow(|scope| {
            let stream =
            replayers
                .replay_into(scope);

            let operates =
            stream
                .flat_map(|(t,_,x)| if let Operates(event) = x { Some((event, RootTimestamp::new(t), 1 as isize)) } else { None })
                .as_collection();

            let channels =
            stream
                .flat_map(|(t,_,x)| if let Channels(event) = x { Some((event, RootTimestamp::new(t), 1 as isize)) } else { None })
                .as_collection();

            let messages =
            stream
                .flat_map(|(t,_,x)| if let Messages(event) = x { Some((event, RootTimestamp::new(t), 1)) } else { None })
                .map(|(event, _time, _)| (event.channel, RootTimestamp::new(u64::max_value()), event.length as isize))
                .as_collection();

            let operates = operates.map(|event| (event.addr, event.name)).inspect(|x| println!("Operates: {:?}", x.0));
            let channels = channels.map(|event| (event.id, (event.scope_addr, event.source, event.target))).inspect(|x| println!("Channels: {:?}", x.0));

            let channels =
            channels
                .map(|(id, (addr, src, tgt))| {
                    let mut src_addr = addr.clone();
                    src_addr.push(src.0);
                    let mut tgt_addr = addr.clone();
                    tgt_addr.push(tgt.0);
                    (id, ((src_addr, src.1), (tgt_addr, tgt.1)))
                })
                .map(|(id, ((src_id, src_port), target))| (src_id, (id, src_port, target)))
                .join_map(&operates, |source_id, &(id, src_port, ref target), src_name| (target.0.clone(), (id, src_port, target.1, src_name.clone())))
                .join_map(&operates, |target_id, &(id, src_port, tgt_port, ref src_name), tgt_name| (id, (src_name.clone(), tgt_name.clone())));

            channels
                .semijoin(&messages)
                .consolidate()
                .inspect(|x| println!("messages:\t({:?} -> {:?}):\t{:?}", ((x.0).1).0, ((x.0).1).1, x.2));
        })
    }).unwrap(); // asserts error-free execution
}
