extern crate timely;
extern crate differential_dataflow;

use std::net::TcpListener;
use timely::dataflow::operators::{Inspect, Map, capture::{EventReader, Replay}};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely::logging::{TimelySetup, TimelyEvent, StartStop, ScheduleEvent};
use timely::logging::TimelyEvent::{Operates, Channels, Messages, Schedule};
use timely::dataflow::operators::aggregation::StateMachine;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;

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

            let stream = replayers.replay_into(scope);

            let operates =
                stream
                    .flat_map(|(t,_,x)| if let Operates(event) = x { Some((event, RootTimestamp::new(t), 1 as isize)) } else { None })
                    .as_collection()
                    .inspect(|x| println!("Operates: {:?}", x.0));

            let operates_by_addr =
                operates
                    .map(|event| (event.addr, event.name));

            let operates_by_index =
                operates
                    .map(|event| (event.id, event.name));

            let channels =
                stream
                    .flat_map(|(t,_,x)| if let Channels(event) = x { Some((event, RootTimestamp::new(t), 1 as isize)) } else { None })
                    .as_collection()
                    .map(|event| (event.id, (event.scope_addr, event.source, event.target)))
                    .inspect(|x| println!("Channels: {:?}", x.0));

            // let schedule =
            //     stream
            //         .flat_map(|(t,_,x)| if let Schedule(event) = x { Some((t, event)) } else { None })
            //         .unary_frontier(Exchange::new(|x: &(u64, ScheduleEvent)| x.1.id as u64), "ScheduleFlattener", |cap, info| {

            //             let mut state = Vec::new();

            //             move |input, output| {

            //                 input.for_each(|cap, data| {

            //                     let mut session = output.session(&cap);

            //                     data.sort_by(|x,y| x.0.cmp(&y.0));
            //                     for (time, event) in data.drain(..) {

            //                         while state.len() <= event.id { state.push(None); }

            //                         match event.start_stop {
            //                             StartStop::Start => {
            //                                 assert!(state[event.id].is_none());
            //                                 state[event.id] = Some(time);
            //                             },
            //                             StartStop::Stop { activity: _ } => {
            //                                 assert!(state[event.id].is_some());
            //                                 let start = state[event.id].take().unwrap();
            //                                 let stop = time;
            //                                 session.give((event.id, RootTimestamp::new(u64::max_value()), (stop - start) as isize));
            //                             },
            //                         }
            //                     }
            //                 });

            //             }
            //         })
            //         .as_collection()
            //         .consolidate();

            // operates_by_index
            //     .semijoin(&schedule)
            //     .inspect(|x| println!("Schedule: {:?} for {:?}ns", x.0, x.2));

            let messages =
                stream
                    .flat_map(|(t,_,x)| if let Messages(event) = x { Some((event, RootTimestamp::new(t), 1)) } else { None })
                    .map(|(event, _time, _)| (event.channel, _time, event.length as isize))
                    .as_collection();

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
                    .join_map(&operates_by_addr, |source_id, &(id, src_port, ref target), src_name| (target.0.clone(), (id, src_port, target.1, src_name.clone())))
                    .join_map(&operates_by_addr, |target_id, &(id, src_port, tgt_port, ref src_name), tgt_name| (id, (src_name.clone(), tgt_name.clone())));

            channels
                .semijoin(&messages)
                .consolidate()
                .inspect(|x| println!("messages:\t({:?} -> {:?}):\t{:?}", ((x.0).1).0, ((x.0).1).1, x.2));
        })
    }).unwrap(); // asserts error-free execution
}
