//! Multi-worker live control loop. Worker 0 admits one FIFO command stream;
//! a single-source Timely exchange delivers one ordered record to every worker.
//! No external clock or distributed sequencing protocol participates in
//! command ordering.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};

use differential_dataflow::operators::arrange::ShutdownButton;
use interactive::runtime::container::Updates;
use interactive::runtime::exchange::GatherPact;
use interactive::runtime::feed::render_row;
use interactive::runtime::frame;
use interactive::server::{Command as ServerCommand, FeedTimes, HostContainer, HostTime, OuterTime, Server};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::vec::Map;
use timely::dataflow::operators::core::UnorderedInput;
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

fn wire_time(point: &HostTime) -> String {
    if point.len() <= 1 { point.first().copied().unwrap_or(0).to_string() }
    else { serde_json::to_string(&point[..]).expect("u64 coordinates serialize") }
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
    /// One worker's part of a group: each command as `Request` would carry it, run back to back
    /// with one answer.
    Group {
        token: u64,
        reqid: String,
        connection_id: ConnectionId,
        members: Vec<(ServerCommand, Option<usize>)>,
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
                token, reqid, connection_id,
                command: Ok(PreparedCommand::Server(ServerCommand::FeedBatch { prog, input, columns, times })),
                ..
            } => {
                (0..peers).map(|target| {
                    let rows: Vec<_> = (target..columns.len()).step_by(peers).collect();
                    let times = match &times {
                        FeedTimes::Shared(point) => FeedTimes::Shared(point.clone()),
                        FeedTimes::PerRow { lanes, least, width } => FeedTimes::PerRow {
                            lanes: lanes.iter().map(|lane| rows.iter().map(|&i| lane[i]).collect()).collect(),
                            least: least.clone(), width: *width,
                        },
                    };
                    let command = ServerCommand::FeedBatch {
                        prog: prog.clone(), input: input.clone(), columns: columns.select(&rows), times,
                    };
                    (target as u64, Work::Request {
                        token, reqid: reqid.clone(), command: Ok(PreparedCommand::Server(command)),
                        connection_id, logical_count: Some(columns.len()),
                    })
                }).collect()
            }
            // Each worker receives its whole part of a group as one record, so that no worker
            // steps its dataflows between the group's commands.
            Work::Request { token, reqid, connection_id, command: Ok(PreparedCommand::Group(members)), .. } => {
                let mut parts: Vec<Vec<_>> = (0..peers).map(|_| Vec::with_capacity(members.len())).collect();
                for command in members {
                    let member = Work::Request {
                        token, reqid: reqid.clone(), command: Ok(PreparedCommand::Server(command)),
                        connection_id, logical_count: None,
                    };
                    for (target, part) in member.distribute(peers) {
                        let Work::Request { command: Ok(PreparedCommand::Server(shard)), logical_count, .. } = part else {
                            unreachable!("a command distributes as commands");
                        };
                        parts[target as usize].push((shard, logical_count));
                    }
                }
                parts.into_iter().enumerate().map(|(target, part)| {
                    (target as u64, Work::Group { token, reqid: reqid.clone(), connection_id, members: part })
                }).collect()
            }
            work => (0..peers)
                .map(|target| (target as u64, work.clone()))
                .collect(),
        }
    }
}

/// The diagnostics views, installed as program `diag` when diagnostics are on.
const DIAG_PROGRAM: &str = include_str!("../diag.ddp");

/// The passes install runs, from `DDIR_PASSES` (default all except projection): comma-separated words applied in
/// order from the default, as `passes::Config::parse` reads them, so `none` or `all,-projection`.
pub fn passes_from_env() -> Result<interactive::passes::Config, String> {
    std::env::var("DDIR_PASSES").map_or(Ok(interactive::passes::Config::default()), |text| interactive::passes::Config::parse(&text))
}

pub fn run_worker(
    worker: &mut Worker,
    events: Option<Receiver<ControlEvent>>,
) {
    // With DDIR_DIAGNOSTICS=1 diagnostics own the worker's loggers, registered before any dataflow
    // is built so that every operator is seen. Their events are only folded in at `diag`, so an
    // idle server stays idle.
    let diagnostics = (std::env::var("DDIR_DIAGNOSTICS").as_deref() == Ok("1"))
        .then(|| interactive::diag::register(worker));

    let work_queue = Rc::new(RefCell::new(VecDeque::new()));
    let queue_out = work_queue.clone();
    let peers = worker.peers();
    let input = worker.dataflow::<HostTime, _, _>(|scope| {
        let (input, stream) = scope.new_unordered_input::<timely::container::CapacityContainerBuilder<Vec<Work>>>();
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
    // The passes install runs: DDIR_PASSES, as `passes::Config::parse` reads it (default all except projection).
    // `main` has already checked that it parses.
    server.set_passes(passes_from_env().expect("DDIR_PASSES parses"));
    // The diagnostics program: views over the `diag.*` sources, published as `diag.*` traces.
    if let Some(diagnostics) = &diagnostics {
        server.set_diagnostics(Rc::clone(diagnostics));
        let program = interactive::lower::lower_tree(interactive::parse::pipe::parse(DIAG_PROGRAM));
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
    let mut admitted = Vec::new();

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
                Work::Group { token, reqid, connection_id, members } => {
                    let response = if worker.index() == 0 {
                        responses.remove(&token)
                    } else {
                        None
                    };
                    let result = execute_group(members, connection_id, &reqid, &mut server, &mut tails, worker, &mut shutdown);
                    answer(response.as_ref(), &reqid, result);
                }
                Work::SessionEnded(connection) => {
                    stop_connection(connection, &mut tails, worker);
                    // Stdin is connection 0: its end stops the server, as `exit` on stdin does.
                    if connection == 0 { shutdown = true; }
                }
                Work::Shutdown => {
                    shutdown = true;
                }
            }
            continue;
        }

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
                        admitted.push(Work::Request {
                            token,
                            reqid,
                            command: kind,
                            connection_id,
                            logical_count: None,
                        });
                    }
                    Ok(ControlEvent::SessionEnded(connection)) => {
                        admitted.push(Work::SessionEnded(connection));
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        if !intake_closed {
                            admitted.push(Work::Shutdown);
                            intake_closed = true;
                        }
                        break;
                    }
                }
            }

            if !admitted.is_empty() {
                let (input, capability) = work_input.as_mut().expect("worker 0 owns command input");
                input.activate().session(capability).give_container(&mut admitted);
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

/// Run one command, and answer it on worker 0.
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
    let result = execute(command, connection_id, reqid, response.clone(), server, tails, worker, shutdown, logical_count);
    answer(response.as_ref(), reqid, result);
}

/// Run a group's commands back to back: one answer, which names the first that failed. Every
/// command runs, as it would on its own: some refusals are decided on worker 0 alone (a one-row
/// feed's time), and stopping there would leave the workers at different commands.
fn execute_group(
    members: Vec<(ServerCommand, Option<usize>)>,
    connection_id: ConnectionId,
    reqid: &str,
    server: &mut Server,
    tails: &mut HashMap<TailKey, Tail>,
    worker: &mut Worker,
    shutdown: &mut bool,
) -> Result<String, String> {
    let count = members.len();
    let mut failures = Vec::new();
    for (position, (command, logical_count)) in members.into_iter().enumerate() {
        let word = match &command {
            ServerCommand::Feed { .. } | ServerCommand::FeedBatch { .. } | ServerCommand::Load { .. } => "feed",
            ServerCommand::Seal { .. } => "seal",
            ServerCommand::Trim { .. } => "trim",
            ServerCommand::Spin => "spin",
            _ => "command",
        };
        if let Err(error) = execute(Ok(PreparedCommand::Server(command)), connection_id, reqid, None, server, tails, worker, shutdown, logical_count) {
            failures.push(format!("command {} ({word}): {error}", position + 1));
        }
    }
    match &failures[..] {
        [] => Ok(format!("ran {count} commands")),
        [first, ..] => Err(format!("{} of {count} commands failed, the others ran; {first}", failures.len())),
    }
}

/// Answer a command, on the worker holding its response channel.
fn answer(response: Option<&Sender<Out>>, reqid: &str, result: Result<String, String>) {
    if let Some(response) = response {
        match result {
            Ok(body) => send(response, reqid, "ok", body),
            Err(body) => send(response, reqid, "err", body),
        }
    }
}

/// Run one command. Only the worker holding `response` sends its data.
#[allow(clippy::too_many_arguments)]
fn execute(
    command: Result<PreparedCommand, String>,
    connection_id: ConnectionId,
    reqid: &str,
    response: Option<Sender<Out>>,
    server: &mut Server,
    tails: &mut HashMap<TailKey, Tail>,
    worker: &mut Worker,
    shutdown: &mut bool,
    logical_count: Option<usize>,
) -> Result<String, String> {
    match command {
        Err(error) => Err(error),
        Ok(PreparedCommand::Server(command)) => match command {
            ServerCommand::Install { name, program, width } => server.install_with_width(worker, &name, &program, width)
                .map(|()| format!("installed {name:?} ({} ops)", program.op_count())),
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
            ServerCommand::Feed { prog, input, columns, time }
            | ServerCommand::FeedBatch { prog, input, columns, times: FeedTimes::Shared(time) } => {
                server.input_position(&prog, &input).and_then(|input| {
                    let point = match time {
                        Some(time) => time,
                        None => server.input_time(&prog, input)?.clone(),
                    };
                    // Batches arrive sharded. A single row enters the collection once.
                    if logical_count.is_some() || worker.index() == 0 {
                        server.feed_columns_at(&prog, input, columns, &point)?;
                    }
                    let time = wire_time(&point);
                    match logical_count {
                        Some(rows) => Ok(format!("fed {rows} rows to {prog:?} input {input} at t={time}")),
                        None => Ok(format!("fed {prog:?} input {input} at t={time}")),
                    }
                })
            }
            ServerCommand::FeedBatch { prog, input, columns, times: FeedTimes::PerRow { lanes, least, width } } => server.input_position(&prog, &input).and_then(|input| {
                server.validate_batch(&prog, input, &least, width)?;
                let times = interactive::runtime::col_times::ColTimes::from_raw_lanes(lanes, columns.len());
                let interactive::runtime::container::Updates { keys, vals, diffs } = columns;
                server.feed_container(&prog, input, HostContainer { keys, vals, times, diffs })?;
                Ok(format!("fed {} rows to {prog:?} input {input} at their times",
                    logical_count.expect("a timed batch retains its logical row count")))
            }),
            // Collective: every worker moves its own handle. Nothing is waited for.
            ServerCommand::Seal { prog, input, frontier } => server.input_position(&prog, &input).and_then(|input| {
                server.seal(&prog, input, &frontier)?;
                let text = match frontier.elements() {
                    [point] => wire_time(point),
                    points => serde_json::to_string(&points.iter().map(|p| &p[..]).collect::<Vec<_>>())
                        .expect("u64 coordinates serialize"),
                };
                Ok(format!("sealed {prog:?} input {input} to {text}"))
            }),
            ServerCommand::Trim { trace, frontier } => server.trim(worker, &trace, frontier.as_ref()).map(|()| format!("trimmed {trace:?}")),
            // Collective: outputs and tails must reach or pass their commanded targets.
            ServerCommand::Spin => {
                server.spin(worker);
                settle_tails(server, tails, worker);
                Ok("spun".to_string())
            }
            // Collective: every worker sources its own shard.
            ServerCommand::Load { prog, input, source, diff, time } => server.input_position(&prog, &input).and_then(|input| {
                let local = server.loads_from_trace(&source);
                if local {
                    // A seal may have returned before every shard reached the snapshot cut.
                    server.wait_for_frontier(worker, &source, &server.target(&source)?)?;
                }
                let time = match time {
                    Some(point) => point,
                    None => server.input_time(&prog, input)?.clone(),
                };
                server.load_at(worker, &prog, input, &source, diff, &time).map(|rows| {
                    if local {
                        format!("fed trace {source:?} with diff={diff} into {prog:?} input {input} ({rows} rows on worker 0)")
                    } else {
                        format!("loaded {rows} rows into {prog:?} input {input}")
                    }
                })
            }),
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
                }
                Ok(String::new())
            }
            ServerCommand::SampleDiagnostics => server.sample_diagnostics(worker).map(|t| format!("diagnostics t={t}")),
            ServerCommand::Exit => {
                *shutdown = connection_id == 0;
                Ok("bye".into())
            }
        },
        Ok(PreparedCommand::Plan { name, program, timings }) => {
            // Only the responding worker needs the report. It reads the catalog in command
            // order, including successful loads and drops from this or other sessions.
            if let Some(response) = response.as_ref() {
                interactive::plan::report_with_read_floor(
                    &program, &|trace| server.source_shape(trace), &server.passes(),
                    |program| server.install_read_floor(program).first().copied().unwrap_or(0),
                ).and_then(|report| {
                    let printed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        report.print_with(interactive::plan::PrintOptions { timings, ..Default::default() })
                    })).map_err(|_| "plan printing panicked".to_string())?;
                    for line in printed.text.lines() { send(response, reqid, "data", line.to_string()); }
                    let incomplete = !report.unresolved.is_empty() || report.stages.iter().any(|s| !s.problems.is_empty());
                    let status = if incomplete { "incomplete; see report".into() } else { format!("{} stages", report.stages.len()) };
                    Ok(format!("planned {:?} ({status})", name))
                })
            } else { Ok(String::new()) }
        }
        Ok(PreparedCommand::Peek { name, key, as_of, format }) => (|| {
            let time = match as_of {
                Some(time) => time,
                None => server.latest_readable(&name)?.ok_or_else(|| format!("trace {name:?} has no closed time in its retained history"))?,
            };
            let updates = server.peek_as_of(worker, &name, key.as_deref(), &time)?;
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
            Ok(format!("as-of={}", wire_time(&time)))
        })(),
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
        .map(|()| format!("tailing {name:?}")),
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
        Ok(PreparedCommand::Group(_)) => unreachable!("a group reaches the workers as `Work::Group`"),
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
    if server.trace_width(name).is_some_and(|w| w > 1) {
        return Err("tail requires scalar time; use peek as-of with a point for this trace".into());
    }
    let as_of = server.trace_since(name).ok_or("trace history released")?.clone();
    if let (Some(response), ReadFormat::Corgi) = (response.as_ref(), format) {
        let (k, v) = server.trace_shape(name).expect("a published trace has a shape");
        send_frame(response, reqid, "schema", format!("trace={name} key={k} val={v}").into_bytes(), None);
    }
    let dataflow_id = worker.next_dataflow_index();
    let tag = reqid.to_string();
    let mut probe = ProbeHandle::new();
    let shutdown = interactive::server::host_dataflow(worker, |scope| {
        let (containers, shutdown) = trace.import_containers(scope, "TailImport", as_of);
        let pending = Arc::new(AtomicUsize::new(0));
        let (mut last_frontier, mut dropped) = (None, false);
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
                        // Each change carries the scalar time at which it took effect.
                        // Times that are all zero are stored as no lane; the frame still says them.
                        let zeros;
                        let epochs = match times.raw_lanes() {
                            [] => { zeros = [vec![0; times.len()]]; &zeros[..] }
                            lanes => &lanes[..1],
                        };
                        match format {
                            ReadFormat::Corgi => out("data", frame::encode(&updates, epochs)),
                            ReadFormat::Columns => {
                                let epochs: Vec<OuterTime> = (0..times.len()).map(|i| times.get(i).first().copied().unwrap_or(0)).collect();
                                send(response, &tag, "data", format!("times={:?} diffs={:?} keys={:?} vals={:?}", epochs, updates.diffs, updates.keys, updates.vals));
                            }
                            ReadFormat::Rows => {
                                for row in 0..updates.len() {
                                    send(response, &tag, "data", render_row(&updates, row, Some(times.get(row).first().copied().unwrap_or(0))));
                                }
                            }
                        }
                    });
                    if format == ReadFormat::Corgi {
                        let upper = frontier.frontier().iter().map(|t| t.first().copied().unwrap_or(0)).min();
                        if last_frontier != Some(upper) {
                            last_frontier = Some(upper);
                            out("progress", upper.map(|t| t.to_le_bytes().to_vec()).unwrap_or_default());
                        }
                    }
                }
            })
            .probe_with(&mut probe);
        shutdown
    });

    // A tail's acknowledgement is its initial-replay boundary. Drive the new
    // import through the commanded write frontier before returning. The response
    // channel is FIFO, so every replayed `data` message precedes dispatch's
    // terminal `ok` and later commands cannot mistake replay for fresh data.
    let target = server.target(name)?;
    while behind(&probe, &target) {
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

/// Whether a reader's `probe` has yet to reach `target`, the frontier of the trace it reads.
fn behind(probe: &ProbeHandle<HostTime>, target: &timely::progress::Antichain<HostTime>) -> bool {
    probe.with_frontier(|frontier| frontier.iter().any(|time| !target.less_equal(time)))
}

/// Run until every tail has sent the changes that the commands so far have made to its trace:
/// those before the meet of its input frontiers.
/// `spin` waits for this delivery. A seal only moves its input.
fn settle_tails(server: &Server, tails: &HashMap<TailKey, Tail>, worker: &mut Worker) {
    let targets: Vec<_> = tails.values().map(|tail| (tail, server.target(&tail.trace).expect("a tailed trace is published"))).collect();
    while targets.iter().any(|(tail, target)| behind(&tail.probe, target)) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::{prepare, Catalog, Cmd};
    use interactive::runtime::feed::FeedDecoder;
    use interactive::server::{host_time, InputId};

    #[test]
    fn timed_batch_distributes_rows_and_replicates_its_bounds() {
        let program = interactive::parse::pipe::parse(r#"output "x" = input 0 : (int, ());"#);
        let shape = interactive::lower::lower_tree(program).root.imports[0].shape.clone().unwrap();
        let mut decoder = FeedDecoder::new(&shape.0, &shape.1);
        for key in ["0", "1", "2", "3"] { decoder.push(key, None, 1).unwrap(); }
        let times = vec![vec![8, 0, 4, 8], vec![0, 7, 7, 1]];
        let command = Cmd::FeedTimed {
            prog: "p".into(), input: InputId::Position(0), columns: decoder.finish(), times: times.clone(),
        };
        let work = Work::Request {
            token: 0, reqid: "f".into(), connection_id: 0, logical_count: None,
            command: prepare(command, &Catalog::default()),
        };
        for (target, work) in work.distribute(3) {
            let Work::Request { logical_count: Some(4), command: Ok(PreparedCommand::Server(
                ServerCommand::FeedBatch { columns, times: FeedTimes::PerRow { lanes, least, width }, .. }
            )), .. } = work else { panic!("expected a shard of the four-row batch") };
            let rows: Vec<_> = (target as usize..4).step_by(3).collect();
            assert_eq!(columns.len(), rows.len());
            for (i, (key, _, _)) in columns.rows().iter().enumerate() {
                assert_eq!(key, &rows[i].to_string());
                assert_eq!((lanes[0][i], lanes[1][i]), (times[0][rows[i]], times[1][rows[i]]));
            }
            assert_eq!(width, 2);
            assert_eq!(least, [HostTime::new([0, 7].into_iter().collect()), host_time(8)]);
        }
    }
}
