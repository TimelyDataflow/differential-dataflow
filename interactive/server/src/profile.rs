//! Per-operator scheduling time, kept by the server from Timely's logs.
//!
//! With `DDIR_PROFILE=1`, every worker records each operator's name and
//! address as it is built and the time spent in each of its schedulings. The
//! record outlives the operator: a program installed for one epoch and then
//! dropped still adds up, and successive installs under one name add together.
//! `profile` reports each operator's own time (a scope's time less its
//! children's), summed over workers and grouped by program, address within the
//! program, and operator name. `profile reset` forgets the times.
//!
//! This replaces Timely's `"timely"` logger, as the diagnostics dataflow does,
//! so the two are exclusive.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use interactive::backend::LabelEventBuilder;
use timely::logging::{StartStop, TimelyEvent, TimelyEventBuilder};
use timely::worker::Worker;

/// What one worker has recorded.
#[derive(Default)]
struct WorkerProfile {
    /// Operator id -> its name and address (the first element is its dataflow).
    operators: HashMap<usize, (String, Vec<usize>)>,
    /// Operator id -> total time scheduled.
    elapsed: HashMap<usize, Duration>,
    /// Dataflow index -> the program that built it.
    programs: HashMap<usize, String>,
    /// Operator id -> the program statement it was rendered from, when DDIR logged one.
    labels: HashMap<usize, String>,
}

fn workers() -> &'static Mutex<Vec<WorkerProfile>> {
    static WORKERS: OnceLock<Mutex<Vec<WorkerProfile>>> = OnceLock::new();
    WORKERS.get_or_init(|| Mutex::new(Vec::new()))
}

/// Whether `DDIR_PROFILE=1`.
pub fn enabled() -> bool {
    std::env::var("DDIR_PROFILE").as_deref() == Ok("1")
}

/// Install this worker's logger. Call before the worker builds any dataflow.
pub fn register(worker: &Worker) {
    let index = worker.index();
    {
        let mut workers = workers().lock().unwrap();
        if workers.len() < worker.peers() {
            workers.resize_with(worker.peers(), WorkerProfile::default);
        }
    }
    let mut starts: HashMap<usize, Duration> = HashMap::new();
    let mut registry = worker.log_register().expect("timely logging is available");
    registry.insert::<LabelEventBuilder, _>("ddir/labels", move |_time, data| {
        let Some(events) = data else { return };
        let mut workers = workers().lock().unwrap();
        let mine = &mut workers[index];
        for (_, event) in events.iter() {
            for id in event.first..event.end {
                mine.labels.entry(id).or_insert_with(|| event.label.clone());
            }
        }
    });
    registry.insert::<TimelyEventBuilder, _>("timely", move |_time, data| {
        let Some(events) = data else { return };
        let mut workers = workers().lock().unwrap();
        let mine = &mut workers[index];
        for (time, event) in events.iter() {
            match event {
                TimelyEvent::Operates(e) => {
                    mine.operators.insert(e.id, (e.name.clone(), e.addr.clone()));
                }
                TimelyEvent::Schedule(e) => match e.start_stop {
                    StartStop::Start => {
                        starts.insert(e.id, *time);
                    }
                    StartStop::Stop => {
                        if let Some(start) = starts.remove(&e.id) {
                            *mine.elapsed.entry(e.id).or_default() += time.saturating_sub(start);
                        }
                    }
                },
                _ => {}
            }
        }
    });
}

/// Record that this worker's dataflow `dataflow` belongs to program `name`.
pub fn note_program(worker_index: usize, dataflow: usize, name: &str) {
    if let Some(mine) = workers().lock().unwrap().get_mut(worker_index) {
        mine.programs.entry(dataflow).or_insert_with(|| name.to_string());
    }
}

/// Forget every recorded time; operator names stay.
pub fn reset() {
    for worker in workers().lock().unwrap().iter_mut() {
        worker.elapsed.clear();
    }
}

/// One row of a report: an operator's own time, summed over workers and installs.
pub struct Row {
    pub program: String,
    /// The program statement the operator was rendered from, or empty.
    pub label: String,
    /// The operator's address within its dataflow.
    pub addr: Vec<usize>,
    pub name: String,
    pub own: Duration,
    /// How many operators (workers times installs) the row sums.
    pub instances: usize,
}

/// Every operator's own time, largest first, and the total.
pub fn report() -> (Vec<Row>, Duration) {
    let workers = workers().lock().unwrap();
    let mut rows: HashMap<(String, Vec<usize>, String, String), (Duration, usize)> = HashMap::new();
    for worker in workers.iter() {
        // A scope's time includes its children's; take theirs out so each row is its own.
        let by_addr: HashMap<&[usize], usize> =
            worker.operators.iter().map(|(id, (_, addr))| (addr.as_slice(), *id)).collect();
        let mut own: HashMap<usize, Duration> = worker.elapsed.clone();
        for (id, (_, addr)) in worker.operators.iter() {
            if addr.len() < 2 {
                continue;
            }
            let child = worker.elapsed.get(id).copied().unwrap_or_default();
            if let Some(parent) = by_addr.get(&addr[..addr.len() - 1]) {
                if let Some(time) = own.get_mut(parent) {
                    *time = time.saturating_sub(child);
                }
            }
        }
        for (id, time) in own {
            let Some((name, addr)) = worker.operators.get(&id) else { continue };
            let program = worker
                .programs
                .get(&addr[0])
                .cloned()
                .unwrap_or_else(|| "(server)".to_string());
            let label = worker.labels.get(&id).cloned().unwrap_or_default();
            let entry = rows.entry((program, addr[1..].to_vec(), name.clone(), label)).or_default();
            entry.0 += time;
            entry.1 += 1;
        }
    }
    let total = rows.values().map(|(time, _)| *time).sum();
    let mut rows: Vec<Row> = rows
        .into_iter()
        .map(|((program, addr, name, label), (own, instances))| Row { program, label, addr, name, own, instances })
        .collect();
    rows.sort_by_key(|row| std::cmp::Reverse(row.own));
    (rows, total)
}
