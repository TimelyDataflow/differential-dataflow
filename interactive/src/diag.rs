//! Diagnostics: the server's own logging, as `diag.*` collections a program can import.
//!
//! With diagnostics on, each worker keeps timely's `"timely"` events, DD's
//! `"differential/arrange"` events, and the statement labels rendering logs to `"ddir/labels"`.
//! At each `tick` the server turns the events since the last one into rows of these sources, at
//! the epoch being closed. Each source's count carries its magnitude:
//!
//! - `diag.operator`: `((worker, op), (program, statement, name, address))`, +1 when the operator
//!   is created and −1 when it shuts down or its dataflow is dropped.
//! - `diag.time`: `((worker, op), ())`, nanoseconds the operator was scheduled, net of the
//!   operators nested in it, while it lives.
//! - `diag.time_retired`: `((worker, program, statement, name), ())`, the time of operators that
//!   have shut down. When an operator shuts down its total moves here from `diag.time`, so a
//!   program installed and dropped every epoch still adds up, while the state stays bounded by the
//!   distinct `(program, statement, name)` triples.
//! - `diag.records`: `((worker, op), ())`, the updates the operator's arrangement holds, from
//!   DD's batch, merge and drop events. Retracted, if anything is left, when the operator shuts
//!   down.
//!
//! Strings (program, statement and operator names) are lists of their bytes. An operator outside
//! any installed program (the server's own control dataflow, a peek) belongs to `(server)`, and
//! the diagnostics program and sources to `diag`.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::Duration;

use corgi::{Shape, Value as CValue};
use differential_dataflow::logging::{DifferentialEvent, DifferentialEventBuilder};
use timely::logging::{StartStop, TimelyEvent, TimelyEventBuilder};
use timely::worker::Worker;

use crate::backend::LabelEventBuilder;
use crate::corgi::container::Updates;
use crate::ir::Diff;

/// The diagnostics sources, by the name a program imports.
pub const SOURCES: [&str; 4] = ["diag.operator", "diag.time", "diag.time_retired", "diag.records"];

/// Whether `name` is a diagnostics source.
pub fn is_source(name: &str) -> bool {
    SOURCES.contains(&name)
}

fn text() -> Shape {
    Shape::List(Box::new(Shape::Int))
}

/// The shape of diagnostics source `name`'s rows.
pub fn shape(name: &str) -> Option<(Shape, Shape)> {
    let worker_op = Shape::Prod(vec![Shape::Int, Shape::Int]);
    match name {
        "diag.operator" => Some((worker_op, Shape::Prod(vec![text(), text(), text(), Shape::List(Box::new(Shape::Int))]))),
        "diag.time" | "diag.records" => Some((worker_op, Shape::Unit)),
        "diag.time_retired" => Some((Shape::Prod(vec![Shape::Int, text(), text(), text()]), Shape::Unit)),
        _ => None,
    }
}

/// The program an installed name counts toward: the diagnostics program and its sources are
/// `diag`.
fn program_of(name: &str) -> String {
    if name == "diag" || name.starts_with("diag.") { "diag".to_string() } else { name.to_string() }
}

/// What the feeder knows about one live operator.
struct Op {
    program: String,
    statement: String,
    name: String,
    addr: Vec<usize>,
    /// Nanoseconds scheduled so far, net of nested operators: what `diag.time` holds for it.
    time: i64,
    /// Updates its arrangement holds: what `diag.records` holds for it.
    records: i64,
}

/// One worker's diagnostics: events not yet processed, the live operators, and the changes not
/// yet fed to the installed sources.
#[derive(Default)]
pub struct Diagnostics {
    worker: usize,
    timely: Vec<(Duration, TimelyEvent)>,
    differential: Vec<DifferentialEvent>,
    /// Operator id -> the statement that built it, from `"ddir/labels"`, until its operator's
    /// creation is processed; the first label wins, which is the most specific.
    labels: HashMap<usize, String>,
    /// Dataflow index -> the installed name it runs, as the server noted it.
    dataflows: HashMap<usize, String>,
    /// Operator id -> when its current scheduling started.
    starts: HashMap<usize, Duration>,
    ops: HashMap<usize, Op>,
    /// Operators that logged a shutdown since the last processing.
    shutdown: HashSet<usize>,
    /// `(program, statement, name)` -> nanoseconds of operators that have shut down.
    retired: HashMap<(String, String, String), i64>,
    /// Changes since the last feed, per source.
    pending: Pending,
    /// How often a dropped operator still held records when it shut down, which DD's drop
    /// events should have retracted.
    pub unbalanced_records: usize,
}

#[derive(Default)]
struct Pending {
    operator: Vec<(usize, Diff)>,
    /// Descriptions of operators retracted from `diag.operator` this round, which are gone from
    /// `ops` by the time the rows are built.
    gone: HashMap<usize, (String, String, String, Vec<usize>)>,
    time: HashMap<usize, i64>,
    retired: HashMap<(String, String, String), i64>,
    records: HashMap<usize, i64>,
}

/// Register this worker's loggers, which feed a fresh [`Diagnostics`]. Call before the worker
/// builds any dataflow it should see.
pub fn register(worker: &mut Worker) -> Rc<RefCell<Diagnostics>> {
    let diag = Rc::new(RefCell::new(Diagnostics { worker: worker.index(), ..Default::default() }));
    let mut registry = worker.log_register().expect("timely logging is available");
    let timely_sink = Rc::clone(&diag);
    registry.insert::<TimelyEventBuilder, _>("timely", move |_time, data| {
        if let Some(events) = data {
            let mut state = timely_sink.borrow_mut();
            for (time, event) in events.drain(..) {
                match event {
                    TimelyEvent::Operates(_) | TimelyEvent::Schedule(_) | TimelyEvent::Shutdown(_) => state.timely.push((time, event)),
                    _ => {}
                }
            }
        }
    });
    let arrange_sink = Rc::clone(&diag);
    registry.insert::<DifferentialEventBuilder, _>("differential/arrange", move |_time, data| {
        if let Some(events) = data {
            let mut state = arrange_sink.borrow_mut();
            for (_, event) in events.drain(..) {
                match event {
                    DifferentialEvent::Batch(_) | DifferentialEvent::Merge(_) | DifferentialEvent::Drop(_) => state.differential.push(event),
                    _ => {}
                }
            }
        }
    });
    let label_sink = Rc::clone(&diag);
    registry.insert::<LabelEventBuilder, _>("ddir/labels", move |_time, data| {
        if let Some(events) = data {
            let mut state = label_sink.borrow_mut();
            for (_, event) in events.drain(..) {
                for id in event.first..event.end {
                    state.labels.entry(id).or_insert_with(|| event.label.clone());
                }
            }
        }
    });
    diag
}

/// Flush this worker's loggers, so that every event so far has reached its `Diagnostics`.
pub fn flush(worker: &Worker) {
    if let Some(mut registry) = worker.log_register() {
        registry.flush();
    }
}

impl Diagnostics {
    /// Record that dataflow `index` runs the installed `name`.
    pub fn note_dataflow(&mut self, index: usize, name: &str) {
        self.dataflows.insert(index, program_of(name));
    }

    /// Fold the events received so far into the live operators and the pending changes.
    /// `installed` are the worker's live dataflows: an operator of any other has been dropped.
    pub fn process(&mut self, installed: &HashSet<usize>) {
        let timely = std::mem::take(&mut self.timely);
        let mut elapsed: HashMap<usize, i64> = HashMap::new();
        let mut created = Vec::new();
        for (time, event) in timely {
            match event {
                TimelyEvent::Operates(e) => {
                    let program = e.addr.first().and_then(|d| self.dataflows.get(d)).cloned().unwrap_or_else(|| "(server)".to_string());
                    created.push(e.id);
                    self.ops.insert(e.id, Op { program, statement: String::new(), name: e.name, addr: e.addr, time: 0, records: 0 });
                }
                TimelyEvent::Schedule(e) => match e.start_stop {
                    StartStop::Start => { self.starts.insert(e.id, time); }
                    StartStop::Stop => {
                        if let Some(start) = self.starts.remove(&e.id) {
                            *elapsed.entry(e.id).or_default() += time.saturating_sub(start).as_nanos() as i64;
                        }
                    }
                },
                TimelyEvent::Shutdown(e) => { self.shutdown.insert(e.id); }
                _ => {}
            }
        }
        // Labels arrive while a program renders, and reach us with its operators. A label range
        // also covers channel identifiers, which name no operator, so none is kept past here.
        for &id in &created {
            if let (Some(op), Some(label)) = (self.ops.get_mut(&id), self.labels.get(&id)) {
                op.statement = label.clone();
            }
        }
        self.labels.clear();
        // A scope's scheduling includes its children's: each operator's own time is its own less
        // the time of the operators directly inside it, all within this window.
        let by_addr: HashMap<&[usize], usize> = self.ops.iter().map(|(id, op)| (op.addr.as_slice(), *id)).collect();
        let mut own = elapsed.clone();
        for (id, nanos) in &elapsed {
            if let Some(op) = self.ops.get(id) {
                if op.addr.len() >= 2 {
                    if let Some(parent) = by_addr.get(&op.addr[..op.addr.len() - 1]) {
                        if let Some(time) = own.get_mut(parent) { *time -= nanos; }
                    }
                }
            }
        }
        drop(by_addr);
        for (id, nanos) in own {
            if let Some(op) = self.ops.get_mut(&id) {
                op.time += nanos;
                *self.pending.time.entry(id).or_default() += nanos;
            }
        }
        for event in std::mem::take(&mut self.differential) {
            let (op, diff) = match event {
                DifferentialEvent::Batch(e) => (e.operator, e.length as i64),
                DifferentialEvent::Merge(e) => match e.complete {
                    Some(complete) => (e.operator, complete as i64 - (e.length1 + e.length2) as i64),
                    None => continue,
                },
                DifferentialEvent::Drop(e) => (e.operator, -(e.length as i64)),
                _ => continue,
            };
            if diff == 0 { continue; }
            if let Some(entry) = self.ops.get_mut(&op) {
                entry.records += diff;
                *self.pending.records.entry(op).or_default() += diff;
            }
        }
        for &id in &created {
            if self.ops.contains_key(&id) {
                self.pending.operator.push((id, 1));
            }
        }
        // Retire the operators that shut down, or whose dataflow is gone: their rows keyed by
        // the operator go, and their time moves to `diag.time_retired`.
        let shutdown = std::mem::take(&mut self.shutdown);
        let dead: Vec<usize> = self.ops.iter()
            .filter(|(id, op)| shutdown.contains(id) || op.addr.first().is_some_and(|d| !installed.contains(d)))
            .map(|(id, _)| *id)
            .collect();
        for id in dead {
            let op = self.ops.remove(&id).expect("listed above");
            self.starts.remove(&id);
            if op.time != 0 {
                *self.pending.time.entry(id).or_default() -= op.time;
                let key = (op.program.clone(), op.statement.clone(), op.name.clone());
                *self.retired.entry(key.clone()).or_default() += op.time;
                *self.pending.retired.entry(key).or_default() += op.time;
            }
            if op.records != 0 {
                self.unbalanced_records += 1;
                *self.pending.records.entry(id).or_default() -= op.records;
            }
            self.pending.operator.push((id, -1));
            self.pending.gone.insert(id, (op.program, op.statement, op.name, op.addr));
        }
        self.dataflows.retain(|index, _| installed.contains(index));
    }

    /// The changes to source `name` since the last call, which are then forgotten for it.
    pub fn take_pending(&mut self, name: &str) -> Option<Updates> {
        let worker = self.worker as i64;
        match name {
            "diag.operator" => {
                let rows = std::mem::take(&mut self.pending.operator);
                let gone = std::mem::take(&mut self.pending.gone);
                let described: Vec<(usize, (String, String, String, Vec<usize>), Diff)> = rows.into_iter()
                    .filter_map(|(id, diff)| {
                        let desc = match self.ops.get(&id) {
                            Some(op) if diff > 0 => (op.program.clone(), op.statement.clone(), op.name.clone(), op.addr.clone()),
                            _ => gone.get(&id)?.clone(),
                        };
                        Some((id, desc, diff))
                    })
                    .collect();
                operator_rows(worker, described)
            }
            "diag.time" => keyed_rows(worker, std::mem::take(&mut self.pending.time)),
            "diag.records" => keyed_rows(worker, std::mem::take(&mut self.pending.records)),
            "diag.time_retired" => retired_rows(worker, std::mem::take(&mut self.pending.retired)),
            _ => None,
        }
    }

    /// Source `name`'s whole contents now, for a source installed after diagnostics began.
    pub fn snapshot(&self, name: &str) -> Option<Updates> {
        let worker = self.worker as i64;
        match name {
            "diag.operator" => operator_rows(worker, self.ops.iter()
                .map(|(id, op)| (*id, (op.program.clone(), op.statement.clone(), op.name.clone(), op.addr.clone()), 1))
                .collect()),
            "diag.time" => keyed_rows(worker, self.ops.iter().map(|(id, op)| (*id, op.time)).collect()),
            "diag.records" => keyed_rows(worker, self.ops.iter().map(|(id, op)| (*id, op.records)).collect()),
            "diag.time_retired" => retired_rows(worker, self.retired.clone()),
            _ => None,
        }
    }

    /// Forget the pending changes of every source: called before a source is installed and seeded
    /// from [`Self::snapshot`], once the installed sources have taken theirs.
    pub fn clear_pending(&mut self) {
        self.pending = Pending::default();
    }

    /// How many live operators the feeder tracks, and how many retired triples: its own state.
    pub fn size(&self) -> (usize, usize) {
        (self.ops.len(), self.retired.len())
    }

    /// The updates each program's live arrangements hold on this worker, as the feeder counts
    /// them: what `diag.records` holds, by program, including the diagnostics program's own.
    pub fn records_by_program(&self) -> HashMap<String, i64> {
        let mut out = HashMap::new();
        for op in self.ops.values() {
            if op.records != 0 { *out.entry(op.program.clone()).or_default() += op.records; }
        }
        out
    }
}

/// A column of strings, each as a list of its bytes.
fn text_column<'a>(strings: impl Iterator<Item = &'a str>) -> CValue {
    let (mut ends, mut bytes) = (Vec::new(), Vec::new());
    for s in strings {
        bytes.extend_from_slice(s.as_bytes());
        ends.push(bytes.len());
    }
    CValue::List(ends.into(), Box::new(CValue::u8(bytes)))
}

/// A column of lists of integers.
fn int_lists_column<'a>(lists: impl Iterator<Item = &'a [usize]>) -> CValue {
    let (mut ends, mut values) = (Vec::new(), Vec::new());
    for list in lists {
        values.extend(list.iter().map(|&x| x as i64));
        ends.push(values.len());
    }
    CValue::List(ends.into(), Box::new(CValue::int(values)))
}

fn worker_op_keys(worker: i64, ids: &[usize]) -> CValue {
    CValue::Prod(vec![CValue::int(ids.iter().map(|_| worker)), CValue::int(ids.iter().map(|&id| id as i64))])
}

fn operator_rows(worker: i64, rows: Vec<(usize, (String, String, String, Vec<usize>), Diff)>) -> Option<Updates> {
    if rows.is_empty() { return None; }
    let ids: Vec<usize> = rows.iter().map(|(id, _, _)| *id).collect();
    let vals = CValue::Prod(vec![
        text_column(rows.iter().map(|(_, d, _)| d.0.as_str())),
        text_column(rows.iter().map(|(_, d, _)| d.1.as_str())),
        text_column(rows.iter().map(|(_, d, _)| d.2.as_str())),
        int_lists_column(rows.iter().map(|(_, d, _)| d.3.as_slice())),
    ]);
    Some(Updates { keys: worker_op_keys(worker, &ids), vals, diffs: rows.iter().map(|(_, _, diff)| *diff).collect() })
}

fn keyed_rows(worker: i64, rows: HashMap<usize, i64>) -> Option<Updates> {
    let rows: Vec<(usize, i64)> = rows.into_iter().filter(|(_, n)| *n != 0).collect();
    if rows.is_empty() { return None; }
    let ids: Vec<usize> = rows.iter().map(|(id, _)| *id).collect();
    Some(Updates { keys: worker_op_keys(worker, &ids), vals: CValue::Unit(rows.len()), diffs: rows.iter().map(|(_, n)| *n).collect() })
}

fn retired_rows(worker: i64, rows: HashMap<(String, String, String), i64>) -> Option<Updates> {
    let rows: Vec<((String, String, String), i64)> = rows.into_iter().filter(|(_, n)| *n != 0).collect();
    if rows.is_empty() { return None; }
    let keys = CValue::Prod(vec![
        CValue::int(rows.iter().map(|_| worker)),
        text_column(rows.iter().map(|((p, _, _), _)| p.as_str())),
        text_column(rows.iter().map(|((_, s, _), _)| s.as_str())),
        text_column(rows.iter().map(|((_, _, n), _)| n.as_str())),
    ]);
    Some(Updates { keys, vals: CValue::Unit(rows.len()), diffs: rows.iter().map(|(_, n)| *n).collect() })
}

