extern crate timely;
extern crate differential_dataflow;
extern crate actix;
extern crate actix_web;
extern crate futures;

extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate serde_json;

use std::sync::{Arc, Mutex};

use std::net::TcpListener;
use timely::dataflow::operators::{Map, capture::{EventReader, Replay}};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely::logging::{TimelySetup, TimelyEvent};
use timely::logging::TimelyEvent::{Operates, Channels, Messages};

use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::Join;

use actix::*;
use actix_web::*;
use std::path::PathBuf;
use actix_web::{App, HttpRequest, Result, http::Method, fs::NamedFile};

use futures::future::Future;

use serde::Serialize;

#[derive(Serialize)]
enum LoggingUpdate {
    ChannelMessages {
        from: String,
        to: String,
        count: isize
    },
}

impl actix::Message for LoggingUpdate {
    type Result = ();
}

struct Ws {
    addr: Arc<Mutex<Vec<Addr<Syn, Ws>>>>,
}

impl Actor for Ws {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let addr: Addr<Syn, Ws> = ctx.address();
        self.addr.lock().unwrap().push(addr);
    }
}

fn index(req: HttpRequest) -> Result<NamedFile> {
    Ok(NamedFile::open("examples/index.html")?)
}

impl Handler<LoggingUpdate> for Ws {
    type Result = ();

    fn handle(&mut self, msg: LoggingUpdate, ctx: &mut Self::Context) {
        ctx.text(serde_json::to_string(&msg).unwrap());
    }
}

impl StreamHandler<ws::Message, ws::ProtocolError> for Ws {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        match msg {
            ws::Message::Ping(msg) => ctx.pong(&msg),
            _ => (),
        }
    }
}

fn main() {
    let endpoint = Arc::new(Mutex::new(Vec::new()));
    let endpoint_actix = endpoint.clone();
    ::std::mem::drop(::std::thread::spawn(move || {
        server::new(move || {
            let endpoint_actix = endpoint_actix.clone();
            App::new()
                .resource("/ws/", move |r| {
                    let endpoint_actix = endpoint_actix.clone();
                    r.f(move |req| ws::start(req, Ws { addr: endpoint_actix.clone() }))
                })
                .resource("/", |r| r.method(Method::GET).f(index))
        }).bind("0.0.0.0:9000").unwrap().run();
    }));

    timely::execute_from_args(std::env::args(), move |worker| {
        let endpoint = endpoint.clone();

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
                .inspect(move |x| {
                    for chan in endpoint.lock().unwrap().iter_mut() {
                        chan.send(LoggingUpdate::ChannelMessages {
                            from: ((x.0).1).0.clone(),
                            to: ((x.0).1).1.clone(),
                            count: x.2,
                        }).wait().unwrap();
                    }
                });
                // .inspect(|x| println!("messages:\t({:?} -> {:?}):\t{:?}", ((x.0).1).0, ((x.0).1).1, x.2));
        })
    }).unwrap(); // asserts error-free execution
}
