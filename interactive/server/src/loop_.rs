//! Single-worker live control loop. Network sessions parse commands off-worker;
//! this thread alone owns timely and the DDIR registry.

use std::any::{type_name_of_val, Any};
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::time::{Duration, Instant};

use differential_dataflow::operators::arrange::ShutdownButton;
use interactive::scope_ir::{Program, Source};
use interactive::server::{OuterTime, Server};
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::CapabilitySet;
use timely::worker::Worker;

use crate::cmd::{Cmd, ConnectionId, DataflowRef, Request};

struct Tail {
    dataflow_id: usize,
    _shutdown: ShutdownButton<CapabilitySet<OuterTime>>,
    trace: String,
    probe: ProbeHandle<OuterTime>,
}

type TailKey = (ConnectionId, String);

pub fn run_worker(
    worker: &mut Worker,
    requests: Receiver<Request>,
    session_ends: Receiver<ConnectionId>,
) {
    let diagnostics_port = std::env::var("DDIR_DIAG_PORT")
        .ok()
        .and_then(|port| port.parse().ok())
        .unwrap_or(51371);
    let diagnostics = diagnostics::logging::register(worker, false);
    let _diagnostics_server =
        diagnostics::server::Server::start(diagnostics_port, diagnostics.sink);
    let mut server = Server::new();
    let mut tails: HashMap<TailKey, Tail> = HashMap::new();
    let tick_ms = std::env::var("DDIR_TICK_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(250u64);
    let interval = Duration::from_millis(tick_ms);
    let mut last_tick = Instant::now();
    let mut shutdown = false;

    while !shutdown {
        match requests.try_recv() {
            Ok(request) => dispatch(request, &mut server, &mut tails, worker, &mut shutdown),
            Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => {
                // Session-end notifications use a separate channel. Only
                // consume them after all already-queued commands, so a final
                // `stop` followed by `exit` cannot race its own cleanup.
                while let Ok(connection) = session_ends.try_recv() {
                    stop_connection(connection, &mut tails, worker);
                }
                if tick_ms > 0 && !tails.is_empty() && last_tick.elapsed() >= interval {
                    tick(&mut server, &mut tails, worker);
                    last_tick = Instant::now();
                } else {
                    worker.step();
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        }
    }
    for (_, tail) in tails.drain() {
        worker.drop_dataflow(tail.dataflow_id);
    }
}

fn dispatch(
    request: Request,
    server: &mut Server,
    tails: &mut HashMap<TailKey, Tail>,
    worker: &mut Worker,
    shutdown: &mut bool,
) {
    let Request {
        reqid,
        kind,
        resp,
        connection_id,
    } = request;
    let result = match kind {
        Err(e) => Err(e),
        Ok(Cmd::Load {
            id_hint,
            bindings,
            program,
            explain,
        }) => {
            if explain {
                Err("load --explain is reserved; explanation is not implemented here yet".into())
            } else {
                load(&id_hint, &bindings, &program, server, worker)
                    .map(|()| format!("installed {:?}", id_hint))
            }
        }
        Ok(Cmd::Drop { target }) => match name_ref(target) {
            Err(e) => Err(e),
            Ok(name) => {
                if tails.values().any(|tail| {
                    server
                        .program_info()
                        .iter()
                        .find(|p| p.name == name)
                        .is_some_and(|p| p.exports.contains(&tail.trace))
                }) {
                    Err(format!(
                        "cannot drop {:?}: a tail is reading one of its exports",
                        name
                    ))
                } else {
                    server
                        .drop_program(worker, &name)
                        .map(|()| format!("dropped {:?}", name))
                }
            }
        },
        Ok(Cmd::Feed {
            prog,
            input,
            key,
            val,
            time,
            diff,
        }) => server
            .feed(&prog, input, key, val, time, diff)
            .map(|()| format!("fed {:?} input {} at t={}", prog, input, server.epoch())),
        Ok(Cmd::List) => {
            for program in server.program_info() {
                send(
                    &resp,
                    &reqid,
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
                    &resp,
                    &reqid,
                    "data",
                    format!("trace name={:?} importers={}", name, importers),
                );
            }
            Ok(format!("t={}", server.epoch()))
        }
        Ok(Cmd::Peek { name }) => match server.snapshot(worker, &name) {
            Ok(rows) => {
                for (key, val, diff) in rows {
                    send(
                        &resp,
                        &reqid,
                        "data",
                        format!("diff={} key={:?} val={:?}", diff, key, val),
                    );
                }
                Ok(format!("t={}", server.epoch()))
            }
            Err(e) => Err(e),
        },
        Ok(Cmd::Tail { name }) => start_tail(
            connection_id,
            &reqid,
            &name,
            resp.clone(),
            server,
            tails,
            worker,
        )
        .map(|()| format!("tailing {:?} from t={}", name, server.epoch())),
        Ok(Cmd::Stop { tail_reqid }) => {
            let key = (connection_id, tail_reqid.clone());
            match tails.remove(&key) {
                Some(tail) => {
                    worker.drop_dataflow(tail.dataflow_id);
                    send(&resp, &tail_reqid, "end", String::new());
                    Ok(format!("stopped {}", tail_reqid))
                }
                None => Err(format!("no tail {:?} in this session", tail_reqid)),
            }
        }
        Ok(Cmd::Tick { n }) => {
            for _ in 0..n {
                tick(server, tails, worker);
            }
            Ok(format!("t={}", server.epoch()))
        }
        Ok(Cmd::Query { .. }) => Err(
            "query is reserved for --explain dataflows and is not implemented"
                .into(),
        ),
        Ok(Cmd::Exit) => {
            *shutdown = connection_id == 0;
            Ok("bye".into())
        }
    };
    match result {
        Ok(body) => send(&resp, &reqid, "ok", body),
        Err(body) => send(&resp, &reqid, "err", body),
    }
}

fn load(
    name: &str,
    bindings: &std::collections::BTreeMap<String, String>,
    source: &str,
    server: &mut Server,
    worker: &mut Worker,
) -> Result<(), String> {
    let mut program = catch_unwind(AssertUnwindSafe(|| {
        let statements = interactive::parse::pipe::parse(source);
        interactive::lower::lower_tree(statements)
    }))
    .map_err(panic_message)?;
    apply_bindings(&mut program, bindings)?;
    program.optimize();
    server.install(worker, name, &program)
}

fn apply_bindings(
    program: &mut Program,
    bindings: &std::collections::BTreeMap<String, String>,
) -> Result<(), String> {
    for (local, binding) in bindings {
        let import = program
            .root
            .imports
            .iter_mut()
            .find(|import| import.name == *local)
            .ok_or_else(|| format!("binding names no import {:?}", local))?;
        import.from = Source::Trace(random_binding(binding)?);
    }
    Ok(())
}

/// Translate the call spelling of a binding (`random(...)`) into its
/// content-addressed source name.
fn random_binding(binding: &str) -> Result<String, String> {
    let Some(body) = binding
        .strip_prefix("random(")
        .and_then(|s| s.strip_suffix(')'))
    else {
        return Ok(binding.to_string());
    };
    let mut values: HashMap<&str, &str> = HashMap::new();
    for field in body.split(',') {
        let (key, value) = field
            .trim()
            .split_once('=')
            .ok_or_else(|| format!("malformed random field {:?}", field))?;
        values.insert(key.trim(), value.trim());
    }
    let nodes = values.remove("range").ok_or("random requires range")?;
    let edges = values.remove("count").ok_or("random requires count")?;
    let arity = values.remove("arity").unwrap_or("2");
    let seed = values.remove("seed").unwrap_or("0");
    let churn = values.remove("churn").unwrap_or("0");
    if !values.is_empty() {
        return Err(format!("unknown random fields: {:?}", values.keys()));
    }
    Ok(format!(
        "random:nodes={},edges={},arity={},seed={},churn={}",
        nodes, edges, arity, seed, churn
    ))
}

fn start_tail(
    connection: ConnectionId,
    reqid: &str,
    name: &str,
    response: Sender<String>,
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
            .as_collection(|k, v| (k.clone(), v.clone()))
            .inspect(move |((key, val), time, diff)| {
                send(
                    &response,
                    &tag,
                    "data",
                    format!("time={} diff={} key={:?} val={:?}", time, diff, key, val),
                );
            })
            .probe_with(&mut probe);
        shutdown
    });
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

fn name_ref(target: DataflowRef) -> Result<String, String> {
    match target {
        DataflowRef::Name(name) => Ok(name),
        DataflowRef::Id(id) => Err(format!(
            "numeric dataflow id {} is no longer exposed; use its name",
            id
        )),
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

fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        format!("DDIR parser panicked ({})", type_name_of_val(&panic))
    }
}
