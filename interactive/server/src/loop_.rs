//! Multi-worker live control loop. Worker 0 admits one FIFO command stream;
//! a single-source Timely broadcast delivers that order to every worker.
//! No external clock or distributed sequencing protocol participates in
//! command ordering.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};

use differential_dataflow::operators::arrange::ShutdownButton;
use interactive::server::{Command as ServerCommand, OuterTime, Server};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::vec::{Broadcast, Input as VecInput};
use timely::dataflow::operators::{CapabilitySet, Exchange, Inspect, Probe};
use timely::worker::Worker;

use crate::cmd::{ConnectionId, PreparedCommand, Request};
use crate::ControlEvent;

struct Tail {
    dataflow_id: usize,
    _shutdown: ShutdownButton<CapabilitySet<OuterTime>>,
    trace: String,
    probe: ProbeHandle<OuterTime>,
}

type TailKey = (ConnectionId, String);

/// A control record that is safe to send through Timely. Response senders stay
/// in worker 0's `responses` map and are recovered with `token` after replay.
/// Parse failures are records too: replaying them preserves each session's
/// position in the order while allowing worker 0 to return the error in place.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
enum Work {
    Request {
        token: u64,
        reqid: String,
        command: Result<PreparedCommand, String>,
        connection_id: ConnectionId,
    },
    SessionEnded(ConnectionId),
    Shutdown,
}

pub fn run_worker(
    worker: &mut Worker,
    events: Option<Receiver<ControlEvent>>,
) {
    // Logging a park wakes the diagnostics dataflow, whose scheduling logs can
    // in turn wake it again. Keep idle servers genuinely idle unless an
    // operator explicitly requests diagnostics.
    let diagnostics_enabled = std::env::var("DDIR_DIAGNOSTICS").as_deref() == Ok("1");
    let (_diagnostics_traces, _diagnostics_server) = if diagnostics_enabled {
        let diagnostics_port = std::env::var("DDIR_DIAG_PORT")
            .ok()
            .and_then(|port| port.parse().ok())
            .unwrap_or(51371);
        let diagnostics::logging::LoggingState { traces, sink } =
            diagnostics::logging::register(worker, false);
        let server = if worker.index() == 0 {
            Some(diagnostics::server::Server::start(diagnostics_port, sink))
        } else {
            drop(sink);
            None
        };
        (Some(traces), server)
    } else {
        (None, None)
    };

    let work_queue = Rc::new(RefCell::new(VecDeque::new()));
    let queue_out = work_queue.clone();
    let input = worker.dataflow::<u64, _, _>(|scope| {
        let (input, stream) = scope.new_input::<Work>();
        stream
            .broadcast()
            .sink(Pipeline, "QueueControl", move |(input, _frontier)| {
                input.for_each(|_time, data| {
                    queue_out.borrow_mut().extend(data.drain(..));
                });
            });
        input
    });
    // Worker 0 is the only source. A single FIFO source already supplies a
    // total order, so a wall-clock sequencer would add machinery, not meaning.
    let mut work_input = (worker.index() == 0).then_some(input);

    let mut server = Server::new();
    let mut tails: HashMap<TailKey, Tail> = HashMap::new();
    let mut responses: HashMap<u64, Sender<String>> = HashMap::new();
    let mut next_token = 0u64;
    let mut intake_closed = false;
    let mut shutdown = false;

    while !shutdown {
        let next_work = { work_queue.borrow_mut().pop_front() };
        if let Some(work) = next_work {
            match work {
                Work::Request {
                    token,
                    reqid,
                    command,
                    connection_id,
                } => {
                    let response = if worker.index() == 0 {
                        responses.remove(&token)
                    } else {
                        None
                    };
                    dispatch(
                        command,
                        connection_id,
                        &reqid,
                        response,
                        &mut server,
                        &mut tails,
                        worker,
                        &mut shutdown,
                    );
                }
                Work::SessionEnded(connection) => {
                    stop_connection(connection, &mut tails, worker);
                }
                Work::Shutdown => {
                    shutdown = true;
                }
            }
            continue;
        }

        let mut admitted = false;
        if let Some(events) = events.as_ref() {
            // Bound each intake turn so a continuously busy network cannot
            // starve the Timely scheduler that distributes the admitted work.
            for _ in 0..1024 {
                match events.try_recv() {
                    Ok(ControlEvent::Request(request)) => {
                        let Request {
                            reqid,
                            kind,
                            resp,
                            connection_id,
                        } = request;
                        let token = next_token;
                        next_token = next_token
                            .checked_add(1)
                            .expect("control request token overflow");
                        responses.insert(token, resp);
                        work_input
                            .as_mut()
                            .expect("worker 0 owns command input")
                            .send(Work::Request {
                                token,
                                reqid,
                                command: kind,
                                connection_id,
                            });
                        admitted = true;
                    }
                    Ok(ControlEvent::SessionEnded(connection)) => {
                        work_input
                            .as_mut()
                            .expect("worker 0 owns command input")
                            .send(Work::SessionEnded(connection));
                        admitted = true;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        if !intake_closed {
                            work_input
                                .as_mut()
                                .expect("worker 0 owns command input")
                                .send(Work::Shutdown);
                            intake_closed = true;
                            admitted = true;
                        }
                        break;
                    }
                }
            }

            if admitted {
                work_input
                    .as_mut()
                    .expect("worker 0 owns command input")
                    .flush();
                // Move the newly admitted batch into the distributed stream.
                worker.step();
                continue;
            }
        }

        worker.step_or_park(None);
    }

    drop(work_input);
    for (_, tail) in tails.drain() {
        worker.drop_dataflow(tail.dataflow_id);
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch(
    command: Result<PreparedCommand, String>,
    connection_id: ConnectionId,
    reqid: &str,
    response: Option<Sender<String>>,
    server: &mut Server,
    tails: &mut HashMap<TailKey, Tail>,
    worker: &mut Worker,
    shutdown: &mut bool,
) {
    let result = match command {
        Err(error) => Err(error),
        Ok(PreparedCommand::Server(command)) => match command {
            ServerCommand::Install { name, program } => server
                .install(worker, &name, &program)
                .map(|()| format!("installed {:?}", name)),
            ServerCommand::Drop { name } => {
                if tails.values().any(|tail| {
                    server
                        .program_info()
                        .iter()
                        .find(|program| program.name == name)
                        .is_some_and(|program| program.exports.contains(&tail.trace))
                }) {
                    Err(format!(
                        "cannot drop {:?}: a tail is reading one of its exports",
                        name
                    ))
                } else {
                    // Every worker replays this drop in command order, but
                    // does not physically rendezvous. The typed process
                    // allocator safely orphans any in-flight channel data;
                    // re-check this assumption for zero-copy allocators.
                    server
                        .drop_program(worker, &name)
                        .map(|()| format!("dropped {:?}", name))
                }
            }
            ServerCommand::Feed {
                prog,
                input,
                key,
                val,
                time,
                diff,
            } => {
                // An external row must enter the distributed collection once.
                // Its dataflow exchanges will place it on the appropriate peer.
                let result = if worker.index() == 0 {
                    server.feed(&prog, input, key, val, time, diff)
                } else {
                    Ok(())
                };
                result.map(|()| format!("fed {:?} input {} at t={}", prog, input, server.epoch()))
            }
            ServerCommand::Bind { trace, prog, input } => server
                .bind(worker, &trace, &prog, input)
                .map(|()| format!("bound {:?} -> {:?} input {}", trace, prog, input)),
            ServerCommand::Unbind { trace, prog, input } => server
                .unbind(worker, &trace, &prog, input)
                .map(|()| format!("unbound {:?} -> {:?} input {}", trace, prog, input)),
            ServerCommand::List => {
                if let Some(response) = response.as_ref() {
                    for program in server.program_info() {
                        send(
                            response,
                            reqid,
                            "data",
                            format!(
                                "program name={:?} origin={} inputs={:?} imports={:?} exports={:?}",
                                program.name,
                                program.origin,
                                program.inputs,
                                program.imports,
                                program.exports
                            ),
                        );
                    }
                    for (name, importers) in server.trace_info() {
                        send(
                            response,
                            reqid,
                            "data",
                            format!("trace name={:?} importers={}", name, importers),
                        );
                    }
                    for (source, target, input) in server.binding_info() {
                        send(
                            response,
                            reqid,
                            "data",
                            format!(
                                "binding source={:?} target={:?} input={}",
                                source, target, input
                            ),
                        );
                    }
                }
                Ok(format!("t={}", server.epoch()))
            }
            ServerCommand::Peek { trace, key } => match server.snapshot(worker, &trace) {
                Ok(rows) => {
                    if let Some(response) = response.as_ref() {
                        for (row_key, val, diff) in rows {
                            if key.as_ref().map_or(true, |key| key == &row_key) {
                                send(
                                    response,
                                    reqid,
                                    "data",
                                    format!("diff={} key={:?} val={:?}", diff, row_key, val),
                                );
                            }
                        }
                    }
                    Ok(format!("t={}", server.epoch()))
                }
                Err(error) => Err(error),
            },
            ServerCommand::Tick { n } => {
                for _ in 0..n {
                    tick(server, tails, worker);
                }
                Ok(format!("t={}", server.epoch()))
            }
            ServerCommand::Exit => {
                *shutdown = connection_id == 0;
                Ok("bye".into())
            }
        },
        Ok(PreparedCommand::Tail { name }) => start_tail(
            connection_id,
            reqid,
            &name,
            response.clone(),
            server,
            tails,
            worker,
        )
        .map(|()| format!("tailing {:?} from t={}", name, server.epoch())),
        Ok(PreparedCommand::Stop { tail_reqid }) => {
            let key = (connection_id, tail_reqid.clone());
            match tails.remove(&key) {
                Some(tail) => {
                    worker.drop_dataflow(tail.dataflow_id);
                    if let Some(response) = response.as_ref() {
                        send(response, &tail_reqid, "end", String::new());
                    }
                    Ok(format!("stopped {}", tail_reqid))
                }
                None => Err(format!("no tail {:?} in this session", tail_reqid)),
            }
        }
    };

    if let Some(response) = response.as_ref() {
        match result {
            Ok(body) => send(response, reqid, "ok", body),
            Err(body) => send(response, reqid, "err", body),
        }
    }
}

fn start_tail(
    connection: ConnectionId,
    reqid: &str,
    name: &str,
    response: Option<Sender<String>>,
    server: &Server,
    tails: &mut HashMap<TailKey, Tail>,
    worker: &mut Worker,
) -> Result<(), String> {
    let key = (connection, reqid.to_string());
    if tails.contains_key(&key) {
        return Err(format!("tail reqid {:?} is already active", reqid));
    }
    let mut trace = server
        .trace(name)
        .ok_or_else(|| format!("no trace {:?}", name))?;
    let dataflow_id = worker.next_dataflow_index();
    let tag = reqid.to_string();
    let mut probe = ProbeHandle::new();
    let shutdown = worker.dataflow::<OuterTime, _, _>(|scope| {
        let (arranged, shutdown) = trace.import_core(scope.clone(), "TailImport");
        arranged
            .as_collection(|key, val| (key.clone(), val.clone()))
            .inner
            .exchange(|_| 0u64)
            .inspect(move |((key, val), time, diff)| {
                if let Some(response) = response.as_ref() {
                    send(
                        response,
                        &tag,
                        "data",
                        format!("time={} diff={} key={:?} val={:?}", time, diff, key, val),
                    );
                }
            })
            .probe_with(&mut probe);
        shutdown
    });

    // A tail's acknowledgement is its initial-replay boundary. Drive the new
    // import through the current closed epoch before returning. The response
    // channel is FIFO, so every replayed `data` message precedes dispatch's
    // terminal `ok` and later commands cannot mistake replay for fresh data.
    let epoch = server.epoch();
    while probe.less_than(&epoch) {
        worker.step();
    }
    tails.insert(
        key,
        Tail {
            dataflow_id,
            _shutdown: shutdown,
            trace: name.to_string(),
            probe,
        },
    );
    Ok(())
}

fn tick(server: &mut Server, tails: &mut HashMap<TailKey, Tail>, worker: &mut Worker) {
    server.tick(worker);
    let epoch = server.epoch();
    while tails.values().any(|tail| tail.probe.less_than(&epoch)) {
        worker.step();
    }
}

fn stop_connection(
    connection: ConnectionId,
    tails: &mut HashMap<TailKey, Tail>,
    worker: &mut Worker,
) {
    let keys: Vec<_> = tails
        .keys()
        .filter(|(id, _)| *id == connection)
        .cloned()
        .collect();
    for key in keys {
        if let Some(tail) = tails.remove(&key) {
            worker.drop_dataflow(tail.dataflow_id);
        }
    }
}

fn send(sender: &Sender<String>, reqid: &str, kind: &str, body: String) {
    let suffix = if body.is_empty() {
        String::new()
    } else {
        format!(" {}", body)
    };
    let _ = sender.send(format!("{} {}{}\n", reqid, kind, suffix));
}
