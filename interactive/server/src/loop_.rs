//! Multi-worker live control loop. Worker 0 admits one FIFO command stream;
//! a single-source Timely exchange delivers one ordered record to every worker.
//! No external clock or distributed sequencing protocol participates in
//! command ordering.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::time::Instant;

use differential_dataflow::operators::arrange::ShutdownButton;
use interactive::runtime::container::Updates;
use interactive::runtime::exchange::GatherPact;
use interactive::runtime::feed::render_row;
use interactive::runtime::frame;
use interactive::server::{host_time, Command as ServerCommand, HostContainer, HostTime, OuterTime, Server};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::vec::{Input as VecInput, Map};
use timely::dataflow::operators::{CapabilitySet, Exchange, Probe};
use timely::worker::Worker;

use crate::cmd::{ConnectionId, Out, PreparedCommand, ReadFormat, Request};
use crate::ControlEvent;

struct Tail {
    dataflow_id: usize,
    _shutdown: ShutdownButton<CapabilitySet<HostTime>>,
    trace: String,
    probe: ProbeHandle<HostTime>,
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
        /// Original logical row count retained when a batch is sharded.
        logical_count: Option<usize>,
    },
    SessionEnded(ConnectionId),
    Shutdown,
}

impl Work {
    /// Produce exactly one ordered control record per worker. Large input
    /// batches are partitioned before exchange; small commands are cloned.
    fn distribute(self, peers: usize) -> Vec<(u64, Work)> {
        match self {
            Work::Request {
                token,
                reqid,
                command:
                    Ok(PreparedCommand::Server(ServerCommand::FeedColumns {
                        prog,
                        input,
                        columns,
                    })),
                connection_id,
                logical_count: _,
            } => {
                let logical_count = columns.len();
                (0..peers)
                    .map(|target| {
                        let rows: Vec<usize> = (target..logical_count).step_by(peers).collect();
                        (
                            target as u64,
                            Work::Request {
                                token,
                                reqid: reqid.clone(),
                                command: Ok(PreparedCommand::Server(ServerCommand::FeedColumns {
                                    prog: prog.clone(),
                                    input,
                                    columns: columns.select(&rows),
                                })),
                                connection_id,
                                logical_count: Some(logical_count),
                            },
                        )
                    })
                    .collect()
            }
            work => (0..peers)
                .map(|target| (target as u64, work.clone()))
                .collect(),
        }
    }
}

/// The diagnostics views, installed as program `diag` when diagnostics are on.
const DIAG_PROGRAM: &str = include_str!("../diag.ddp");

pub fn run_worker(
    worker: &mut Worker,
    events: Option<Receiver<ControlEvent>>,
) {
    // With DDIR_DIAGNOSTICS=1 diagnostics own the worker's loggers, registered before any dataflow
    // is built so that every operator is seen. Their events are only folded in at `tick`, so an
    // idle server stays idle.
    let diagnostics = (std::env::var("DDIR_DIAGNOSTICS").as_deref() == Ok("1"))
        .then(|| interactive::diag::register(worker));

    let work_queue = Rc::new(RefCell::new(VecDeque::new()));
    let queue_out = work_queue.clone();
    let peers = worker.peers();
    let input = worker.dataflow::<u64, _, _>(|scope| {
        let (input, stream) = scope.new_input::<Work>();
        stream
            .flat_map(move |work| work.distribute(peers))
            .exchange(|(target, _work)| *target)
            .map(|(_target, work)| work)
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
    // The diagnostics program: views over the `diag.*` sources, published as `diag.*` traces.
    if let Some(diagnostics) = &diagnostics {
        server.set_diagnostics(Rc::clone(diagnostics));
        let mut program = interactive::lower::lower_tree(interactive::parse::pipe::parse(DIAG_PROGRAM));
        program.optimize();
        server.install(worker, "diag", &program).expect("the diagnostics program installs");
    }
    // Published traces reach importers as arrangements; DDIR_ARRANGED_IMPORTS=0 sends containers
    // instead (each importer re-arranges what it joins).
    server.set_arranged_imports(std::env::var("DDIR_ARRANGED_IMPORTS").map_or(true, |v| v != "0"));
    let mut tails: HashMap<TailKey, Tail> = HashMap::new();
    let mut responses: HashMap<u64, Sender<Out>> = HashMap::new();
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
                    logical_count,
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
                        logical_count,
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
                                logical_count: None,
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
    response: Option<Sender<Out>>,
    server: &mut Server,
    tails: &mut HashMap<TailKey, Tail>,
    worker: &mut Worker,
    shutdown: &mut bool,
    logical_count: Option<usize>,
) {
    let result = match command {
        Err(error) => Err(error),
        Ok(PreparedCommand::Server(command)) => match command {
            ServerCommand::Install { name, program } => server
                .install(worker, &name, &program)
                .map(|()| format!("installed {:?} ({} ops)", name, program.op_count())),
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
                columns,
                time,
            } => {
                // An external row must enter the distributed collection once.
                // Its dataflow exchanges will place it on the appropriate peer.
                let result = if worker.index() == 0 {
                    server.feed_columns(&prog, input, columns, time)
                } else {
                    Ok(())
                };
                result.map(|()| format!("fed {:?} input {} at t={}", prog, input, server.epoch()))
            }
            ServerCommand::FeedColumns {
                prog,
                input,
                columns,
            } => server.feed_columns(&prog, input, columns, None).map(|()| {
                format!(
                    "fed {} rows to {:?} input {} at t={}",
                    logical_count.expect("distributed batch retains its logical row count"),
                    prog,
                    input,
                    server.epoch()
                )
            }),
            // Collective: every worker sources its own shard.
            ServerCommand::Load { prog, input, source, diff } => {
                let local = server.loads_from_trace(&source);
                server.load(worker, &prog, input, &source, diff).map(|rows| {
                    if local {
                        format!("fed trace {source:?} with diff={diff} into {prog:?} input {input} ({rows} rows on worker 0)")
                    } else {
                        format!("loaded {rows} rows into {prog:?} input {input}")
                    }
                })
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
            ServerCommand::Tick { n } => {
                let timer = Instant::now();
                for _ in 0..n {
                    tick(server, tails, worker);
                }
                Ok(format!("t={} elapsed={:.2?}", server.epoch(), timer.elapsed()))
            }
            ServerCommand::Exit => {
                *shutdown = connection_id == 0;
                Ok("bye".into())
            }
        },
        Ok(PreparedCommand::Plan { name, program }) => {
            // Only the responding worker needs the report. It reads the catalog in command
            // order, including successful loads and drops from this or other sessions.
            if let Some(response) = response.as_ref() {
                interactive::plan::report(&program, &|trace| server.trace_shape(trace)).map(|report| {
                    for line in report.print().lines() { send(response, reqid, "data", line.to_string()); }
                    let incomplete = !report.unresolved.is_empty() || report.stages.iter().any(|s| !s.problems.is_empty());
                    format!("planned {:?} ({})", name, if incomplete { "incomplete; see report" } else { "4 stages" })
                })
            } else { Ok(String::new()) }
        }
        Ok(PreparedCommand::Peek { name, key, format }) => server.peek(worker, &name, key.as_deref()).map(|updates| {
            if let Some(response) = response.as_ref() {
                match format {
                    ReadFormat::Corgi => {
                        let (k, v) = server.trace_shape(&name).expect("a peeked trace has a shape");
                        send_frame(response, reqid, "schema", format!("trace={name} key={k} val={v}").into_bytes(), None);
                        send_frame(response, reqid, "data", frame::encode(&updates, &[]), None);
                    }
                    ReadFormat::Columns if !updates.is_empty() => {
                        send(response, reqid, "data", format!("keys={:?} vals={:?} diffs={:?}", updates.keys, updates.vals, updates.diffs));
                    }
                    ReadFormat::Rows => {
                        for row in 0..updates.len() {
                            send(response, reqid, "data", render_row(&updates, row, None));
                        }
                    }
                    ReadFormat::Columns => {}
                }
            }
            format!("t={}", server.epoch())
        }),
        Ok(PreparedCommand::Tail { name, format }) => start_tail(
            connection_id,
            reqid,
            &name,
            format,
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

/// A binary tail's bound on frame bytes queued for its client and not yet written. Past it the
/// subscription sends `err` and `end` and stops sending, rather than holding back the dataflow or
/// growing server memory.
const TAIL_PENDING_BYTES: usize = 256 << 20;

/// Subscribe the session to `name`'s changes. Every worker reads its share of the trace and sends
/// it whole to worker 0, which writes one text line per row (or a column dump), or with `as corgi`
/// as a `data` frame after one `schema` frame, and then a `progress` frame whenever the
/// subscription's frontier advances: every change before that time has been sent.
#[allow(clippy::too_many_arguments)]
fn start_tail(
    connection: ConnectionId,
    reqid: &str,
    name: &str,
    format: ReadFormat,
    response: Option<Sender<Out>>,
    server: &Server,
    tails: &mut HashMap<TailKey, Tail>,
    worker: &mut Worker,
) -> Result<(), String> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use timely::container::CapacityContainerBuilder;

    let key = (connection, reqid.to_string());
    if tails.contains_key(&key) {
        return Err(format!("tail reqid {:?} is already active", reqid));
    }
    let mut trace = server
        .published(name)
        .ok_or_else(|| format!("no trace {:?}", name))?;
    if let (Some(response), ReadFormat::Corgi) = (response.as_ref(), format) {
        let (k, v) = server.trace_shape(name).expect("a published trace has a shape");
        send_frame(response, reqid, "schema", format!("trace={name} key={k} val={v}").into_bytes(), None);
    }
    let dataflow_id = worker.next_dataflow_index();
    let tag = reqid.to_string();
    let mut probe = ProbeHandle::new();
    let shutdown = interactive::server::host_dataflow(worker, |_, scope| {
        let (containers, shutdown) = trace.import_containers(scope, "TailImport", server.import_as_of());
        let pending = Arc::new(AtomicUsize::new(0));
        let (mut upper, mut dropped) = (None::<OuterTime>, false);
        containers
            .inner
            .unary_frontier::<CapacityContainerBuilder<Vec<()>>, _, _, _>(GatherPact, "Tail", move |capability, _info| {
                drop(capability);
                move |(input, frontier), _output| {
                    let Some(response) = response.as_ref() else {
                        input.for_each(|_, _| {});
                        return;
                    };
                    let mut out = |kind: &str, bytes: Vec<u8>| {
                        if dropped { return }
                        if pending.load(Ordering::Relaxed) > TAIL_PENDING_BYTES {
                            dropped = true;
                            send(response, &tag, "err", "client too slow: subscription ended; send stop".into());
                            send(response, &tag, "end", String::new());
                            return;
                        }
                        pending.fetch_add(bytes.len(), Ordering::Relaxed);
                        send_frame(response, &tag, kind, bytes, Some(Arc::clone(&pending)));
                    };
                    input.for_each(|_time, data| {
                        let HostContainer { keys, vals, times, diffs } = std::mem::take(data);
                        let updates = Updates { keys, vals, diffs };
                        // A published trace's times are `(epoch, [])`: its rows go out at their epochs.
                        let epochs = &times.raw_lanes()[..times.raw_lanes().len().min(1)];
                        match format {
                            ReadFormat::Corgi => out("data", frame::encode(&updates, epochs)),
                            ReadFormat::Columns => {
                                let epochs: Vec<OuterTime> = (0..times.len()).map(|i| times.get(i).outer).collect();
                                send(response, &tag, "data", format!("times={:?} diffs={:?} keys={:?} vals={:?}", epochs, updates.diffs, updates.keys, updates.vals));
                            }
                            ReadFormat::Rows => {
                                for row in 0..updates.len() {
                                    send(response, &tag, "data", render_row(&updates, row, Some(times.get(row).outer)));
                                }
                            }
                        }
                    });
                    if format == ReadFormat::Corgi {
                        if let Some(t) = frontier.frontier().iter().map(|t| t.outer).min() {
                            if upper != Some(t) {
                                upper = Some(t);
                                out("progress", t.to_le_bytes().to_vec());
                            }
                        }
                    }
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
    while probe.less_than(&host_time(epoch)) {
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
    while tails.values().any(|tail| tail.probe.less_than(&host_time(epoch))) {
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

fn send(sender: &Sender<Out>, reqid: &str, kind: &str, body: String) {
    let suffix = if body.is_empty() {
        String::new()
    } else {
        format!(" {}", body)
    };
    let _ = sender.send(Out::Line(format!("{} {}{}\n", reqid, kind, suffix)));
}

/// A binary frame of `kind`: its header line, then `bytes`.
fn send_frame(sender: &Sender<Out>, reqid: &str, kind: &str, bytes: Vec<u8>, pending: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>) {
    let header = format!("{} frame {} {}\n", reqid, kind, bytes.len());
    let _ = sender.send(Out::Frame { header, bytes, pending });
}
