//! Arrangement sizes, for the `sizes` command.
//!
//! With `DDIR_SIZES=1`, every worker registers a timely logger (operator names and addresses)
//! and a differential logger (batch, merge, drop, share and batcher events) that keep running
//! totals per operator in one process-wide table. `sizes` reports the table, summed across
//! workers: for each arrangement, the batches and updates its trace holds, how many handles
//! share it, and what its merge batcher holds. Updates are `(data, time, diff)` triples, so an
//! arrangement whose updates grow while its data does not is retaining history.
//!
//! Each worker also measures its published traces after every tick: their batches, updates and
//! heap bytes, counting each shared buffer once (see `Server::trace_footprints`). Bytes are not
//! available from the logging events, so private arrangements report updates but not bytes.
//!
//! The totals are per process: with several processes, each reports its own workers. Events
//! reach the table when a worker's loggers flush, so a report can trail the most recent work.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use differential_dataflow::logging::{DifferentialEvent, DifferentialEventBuilder};
use timely::logging::{TimelyEvent, TimelyEventBuilder};
use interactive::server::TraceFootprint;
use timely::worker::Worker;

/// Running totals for one operator on one worker.
#[derive(Clone, Default)]
pub struct Totals {
    pub batches: i64,
    pub updates: i64,
    pub shares: i64,
    pub batcher_updates: i64,
    pub batcher_bytes: i64,
}

impl Totals {
    fn add(&mut self, other: &Totals) {
        self.batches += other.batches;
        self.updates += other.updates;
        self.shares += other.shares;
        self.batcher_updates += other.batcher_updates;
        self.batcher_bytes += other.batcher_bytes;
    }
}

/// One operator's name, address, and totals, summed across workers.
pub struct Row {
    pub addr: Vec<usize>,
    pub name: String,
    pub totals: Totals,
}

#[derive(Default)]
struct Table {
    /// `(worker, operator id)` -> name and address, while the operator is live.
    names: BTreeMap<(usize, usize), (String, Vec<usize>)>,
    /// `(worker, operator id)` -> totals.
    totals: BTreeMap<(usize, usize), Totals>,
    /// worker -> its published traces' footprints, as of its most recent tick.
    traces: BTreeMap<usize, Vec<(String, TraceFootprint)>>,
}

static TABLE: OnceLock<Mutex<Table>> = OnceLock::new();

fn table() -> &'static Mutex<Table> {
    TABLE.get_or_init(Default::default)
}

/// Whether `DDIR_SIZES=1` asked for sizes.
pub fn enabled() -> bool {
    std::env::var("DDIR_SIZES").as_deref() == Ok("1")
}

/// Register this worker's loggers. Call before building dataflows whose arrangements to count.
pub fn register(worker: &mut Worker) {
    let index = worker.index();
    let mut registry = worker.log_register().expect("logging is initialized");
    registry.insert::<TimelyEventBuilder, _>("timely", move |_time: &Duration, data: &mut Option<Vec<(Duration, TimelyEvent)>>| {
        let Some(data) = data else { return };
        let mut events = data.iter().filter(|(_, event)| matches!(event, TimelyEvent::Operates(_) | TimelyEvent::Shutdown(_))).peekable();
        if events.peek().is_none() {
            return;
        }
        let mut table = table().lock().unwrap();
        for (_, event) in events {
            match event {
                TimelyEvent::Operates(e) => {
                    table.names.insert((index, e.id), (e.name.clone(), e.addr.clone()));
                }
                TimelyEvent::Shutdown(e) => {
                    table.names.remove(&(index, e.id));
                    table.totals.remove(&(index, e.id));
                }
                _ => {}
            }
        }
    });
    registry.insert::<DifferentialEventBuilder, _>("differential/arrange", move |_time: &Duration, data: &mut Option<Vec<(Duration, DifferentialEvent)>>| {
        let Some(data) = data else { return };
        if data.is_empty() {
            return;
        }
        let mut table = table().lock().unwrap();
        for (_, event) in data.iter() {
            // Batch, merge and drop events balance; see `DifferentialEvent`.
            let (operator, delta) = match event {
                DifferentialEvent::Batch(e) => (e.operator, Totals { batches: 1, updates: e.length as i64, ..Default::default() }),
                DifferentialEvent::Merge(e) => match e.complete {
                    Some(complete) => (e.operator, Totals { batches: -1, updates: complete as i64 - (e.length1 + e.length2) as i64, ..Default::default() }),
                    None => continue,
                },
                DifferentialEvent::Drop(e) => (e.operator, Totals { batches: -1, updates: -(e.length as i64), ..Default::default() }),
                DifferentialEvent::TraceShare(e) => (e.operator, Totals { shares: e.diff as i64, ..Default::default() }),
                DifferentialEvent::Batcher(e) => (e.operator, Totals { batcher_updates: e.records_diff as i64, batcher_bytes: e.size_diff as i64, ..Default::default() }),
                DifferentialEvent::MergeShortfall(_) => continue,
            };
            table.totals.entry((index, operator)).or_default().add(&delta);
        }
    });
}

/// Every live operator with a non-zero total, summed across workers by address, in address order.
pub fn report() -> Vec<Row> {
    let table = table().lock().unwrap();
    let mut rows: BTreeMap<Vec<usize>, Row> = BTreeMap::new();
    for (key, totals) in table.totals.iter() {
        let Some((name, addr)) = table.names.get(key) else { continue };
        rows.entry(addr.clone())
            .or_insert_with(|| Row { addr: addr.clone(), name: name.clone(), totals: Totals::default() })
            .totals
            .add(totals);
    }
    rows.into_values()
        .filter(|row| {
            let t = &row.totals;
            t.batches != 0 || t.updates != 0 || t.batcher_updates != 0 || t.batcher_bytes != 0
        })
        .collect()
}

/// Record this worker's published-trace footprints (call after each tick).
pub fn record_traces(worker: usize, traces: Vec<(String, TraceFootprint)>) {
    table().lock().unwrap().traces.insert(worker, traces);
}

/// Published traces' footprints, summed across workers, in name order.
pub fn traces() -> Vec<(String, TraceFootprint)> {
    let table = table().lock().unwrap();
    let mut sums: BTreeMap<String, TraceFootprint> = BTreeMap::new();
    for rows in table.traces.values() {
        for (name, f) in rows {
            let sum = sums.entry(name.clone()).or_default();
            sum.batches += f.batches;
            sum.updates += f.updates;
            sum.bytes += f.bytes;
        }
    }
    sums.into_iter().collect()
}
