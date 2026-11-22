//! The corgi `ProxyReduceBackend`: the value semantics for the DD `ProxyReduceTactic`.
//!
//! The tactic (differential's `operators::int_proxy::reduce`) owns the navigation of times over
//! integer proxies `(key_id, value_id, time, diff)`, for the keys that need it. A key with one
//! time to evaluate at is *direct* (differential's `int_proxy::direct`): the backend finds these
//! by comparing time lanes, before it presents anything, and completes them itself. A key the
//! tactic owes a carried time, one in `changed`, is presented instead, as the tactic holds that time.
//!
//! A direct key stays in corgi: chunks in, the reducer's graph, columns out.
//!   * Its input is rows of the input chunks. Where one chunk holds the key, at the key's time,
//!     the rows are already its values in order, netted: the reducer reads them where they lie.
//!     Otherwise the key's rows are sorted and netted by value across chunks, as for any key.
//!     Either way the rows are named by where they are, and gathered once, into the graph.
//!   * Its output is not matched against what the key has. The reducer's rows are recorded at the
//!     key's time, and beside them the key's existing output, negated, at that time. The batch is
//!     sorted and consolidated when it is sealed, which nets the two.
//!   * A time is built only for a record that is not at or before the key's time, to report the
//!     time it carries.
//!
//! For the keys it presents, this backend supplies only:
//!
//!   * ids — a key's id is the identifier its arrangement is sorted by (the key itself when it is a
//!     primitive integer, else its hash lane). A value's id names a row in a pool of columns: the
//!     chunks' own value columns, and the output columns this backend builds. Within a window, a
//!     key's equal input values share one id, found by corgi's segmented sort. A proven weighted
//!     sum compares projected values here; other reducers keep whole-value identity. Output values are
//!     matched to the current output by structural comparison. Nothing hashes values.
//!   * collisions — a hashed key's identifier may hold several real keys. Chunks sort each
//!     identifier's rows by real key, so a window finds them with two comparisons per run. Their
//!     ids are then per real key, input and output alike: a colliding bracket is split by real key
//!     before it is reduced, its output is matched by key as well as value, and every output id
//!     records the row holding its key, which is what `emit` reads.
//!   * the value callback — `reduce_lambda` runs ONE crossing per wave over every `(key, time)`
//!     bracket, evaluating its expression over every bracket's key and `(val, diff)` list.
//!     The resulting `(val, diff)` lists supply the desired output columns, never DDIR rows.
//!     Only what the expression reads is gathered: its demand on its input, found once when the
//!     backend is built, decides whether keys are gathered, and which fields of the values.
//!   * materialize — gather the emitted keys and values from the pools' columns and seal a
//!     `CorgiChunk` batch column-natively.
//!
//! Presentation assigns value ids in structural order (signed integers, lexicographic lists).
//! Reduction expressions read their input lists in that order.
//!
//! The changed-key restriction is honored by presenting only the changed keys: novel batches are
//! read whole (delta-sized), the accumulated history is scanned and filtered to the changed hashes
//! (a columnar semijoin — matching the row-wise tactic's read).

use std::ops::Range;
use std::rc::Rc;

use differential_dataflow::consolidation::consolidate;
use differential_dataflow::trace::Description;
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::operators::int_proxy::{KeyPosition, ProxyBridge};
use differential_dataflow::operators::int_proxy::reduce::{ProxyReduceBackend, ReduceInstance, ReduceWindow};

use corgi::arrange::{compare_at, gather_lanes, sort_blocks};
use corgi::{Bounds, Demand, Graph, NumOp, Shape, Value as CValue};

use crate::runtime::col_times::{ColTime, ColTimes};
use crate::runtime::search::matching_ranges;
use crate::runtime::chunk::{colliding_keys, columns_to_nonempty_batch, ident, key_is_hashed, recover_key, recover_key_ref, CorgiChunk};
use crate::lambda::compile_reduce;
use crate::ir::{Diff, Reducer};

type CBatch<T> = Rc<ChunkBatch<CorgiChunk<T, Diff>>>;

/// Values named by id: id `i` is row `refs[i].1` of `columns[refs[i].0]`.
#[derive(Default)]
struct Pool {
    columns: Vec<CValue>,
    refs: Vec<(usize, usize)>,
}

impl Pool {
    /// The values `ids` name, as one column.
    fn gather(&self, ids: &[u64]) -> CValue {
        gather_refs(&self.columns, ids.iter().map(|&id| self.refs[id as usize]))
    }
}

/// The rows `refs` names, each a `(column, row)` of `columns`, as one column.
fn gather_refs(columns: &[CValue], refs: impl Iterator<Item = (usize, usize)>) -> CValue {
    let srcs: Vec<Option<&CValue>> = columns.iter().map(Some).collect();
    let (tags, offs): (Vec<usize>, Vec<usize>) = refs.unzip();
    gather_lanes(&srcs, &tags, &offs)
}

/// Where each of a list of chunks holds some ascending keys: per chunk, the `(key index, rows)` of
/// each key it holds, in key order.
type Matches = Vec<Vec<(usize, Range<usize>)>>;

/// Each bracket of `input`, ending at `ends`, cut to its first `k` entries, or `None` if none is
/// longer. A bracket's ids ascend in value order, so these are its first `k` values.
fn first_entries(k: usize, ends: &[usize], input: &[(u64, Diff)]) -> Option<(Vec<usize>, Vec<(u64, Diff)>)> {
    let mut start = 0;
    if !ends.iter().any(|&end| end - std::mem::replace(&mut start, end) > k) { return None; }
    let (mut kept_ends, mut kept) = (Vec::with_capacity(ends.len()), Vec::new());
    start = 0;
    for &end in ends {
        kept.extend_from_slice(&input[start..end.min(start + k)]);
        kept_ends.push(kept.len());
        start = end;
    }
    Some((kept_ends, kept))
}

/// What a reduce's lambda reads of its input `(key, vals)`, `vals` a list of `(val, diff)`, as
/// Corgi's demand analysis finds it. Each key's list keeps its length and order whatever is read.
struct Reads {
    /// Whether the key is read at all. A key read only in part is gathered whole.
    key: bool,
    /// The demand on each value: the value columns are projected by it before they are gathered.
    value: Demand,
    /// The demand on the input once its values are gathered so, which projects it to the shape
    /// the narrowed graph takes.
    argument: Demand,
    /// That shape, `(key, vals)` projected by the lambda's demand.
    shape: Shape,
}

impl Reads {
    /// `demand` is the lambda's demand on `input`, its `(key, vals)` shape.
    fn new(demand: Demand, input: &Shape) -> Self {
        let shape = demand.projected_shape(input).expect("a demand on the reduce's input");
        let Demand::Prod(parts) = &demand else {
            let read = demand != Demand::None;
            let value = if read { Demand::All } else { Demand::None };
            return Reads { key: read, value, argument: demand, shape };
        };
        // The values are projected by their demand before they are gathered, so the argument then
        // takes them whole (or not at all), with whatever it reads of the diffs.
        let (value, list) = match &parts[1] {
            Demand::List(element) => match &**element {
                Demand::Prod(pair) => {
                    let gathered = if pair[0] == Demand::None { Demand::None } else { Demand::All };
                    (pair[0].clone(), Demand::List(Box::new(Demand::Prod(vec![gathered, pair[1].clone()]))))
                }
                other => (other.clone(), parts[1].clone()),
            },
            other => (other.clone(), other.clone()),
        };
        Reads { key: parts[0] != Demand::None, value, argument: Demand::Prod(vec![parts[0].clone(), list]), shape }
    }
}

/// A corgi reduce backend for a single `Reducer`.
///
/// Each retire goes: `begin`; then per window, `next_window` presents a run of keys' input and
/// output as integer records, `reduce_corrections` computes the output each key wants and how it
/// differs from what it has, and `emit` records the difference; finally `finish` builds the batch.
pub(crate) struct CorgiReduceBackend<T> {
    /// A `Reducer::Expr`'s graph, compiled at the key and value shapes it reads and narrowed to
    /// what it reads of them: it takes its input projected by `reads.argument`.
    graph: Graph<NumOp>,
    /// What the graph reads of its input.
    reads: Reads,
    /// The reducer is invariant under consolidation of equal projected values.
    project_values: bool,
    /// How many of each key's first values the graph is handed, if not all.
    limit: Option<usize>,
    /// The current window's input values: its chunks' value columns, projected by `reads.value`.
    input: Pool,
    /// The current retire's output values: its output chunks' value columns, then those built here.
    output: Pool,
    /// For each output id, the row of `key_columns` holding its key. An output id names a key and a
    /// value together, as two keys may share an identifier and hold equal values.
    output_keys: Vec<(usize, usize)>,
    /// The current retire's input then output chunks' key columns.
    key_columns: Vec<CValue>,
    /// The output emitted so far: key rows in `key_columns`, value ids, times, and diffs.
    rows: (Vec<(usize, usize)>, Vec<u64>, ColTimes<T>, Vec<Diff>),
    /// The output of the keys completed here, as columns, in no order and not consolidated.
    completed: Vec<Columns<T>>,
    /// Input records per window, bounding what the presentations cost at once.
    window_size: usize,
    /// The retire in progress.
    retire: Retire,
    /// The retire's upper bound, each element as coordinates: a direct key's time is not at or
    /// beyond it, and the times its records carry are.
    upper: Vec<Vec<u64>>,
}

/// Updates as columns: keys in their arrangement form, values, times and diffs.
struct Columns<T> {
    keys: CValue,
    vals: CValue,
    times: ColTimes<T>,
    diffs: Vec<Diff>,
}

/// A retire's keys and where its input chunks hold them, found once by its first window.
#[derive(Default)]
struct Retire {
    /// The retire's keys, ascending.
    keys: Vec<i64>,
    /// The input records each key holds, novel and prior.
    held: Vec<usize>,
    /// Where the input chunks, prior then novel, hold the keys.
    input: Matches,
    /// The current window, as a range of `keys`.
    window: Range<usize>,
    /// The window's identifiers that hold more than one real key, ascending.
    colliding: Vec<i64>,
    /// Whether each key is owed a carried time by the tactic, which the tactic must then evaluate.
    owed: Vec<bool>,
}

impl Retire {
    /// Advance to the next window: the next keys, until they hold `budget` input records. A key is
    /// never split, so a window holds at least one. Returns `false` once there are no keys left.
    fn advance(&mut self, budget: usize) -> bool {
        let start = self.window.end;
        let (mut stop, mut records) = (start, 0);
        while stop < self.keys.len() && (records < budget || self.held[stop] == 0) {
            records += self.held[stop];
            stop += 1;
        }
        self.window = start..stop;
        stop > start
    }

    /// Where the input chunks hold the window's keys, as indices into the window. Moves the matches
    /// out, rather than copying them, when the window is the whole retire.
    fn input_matches(&mut self) -> Matches {
        let Range { start, end } = self.window.clone();
        if end - start == self.keys.len() {
            return std::mem::take(&mut self.input);
        }
        self.input.iter().map(|found| {
            let (lo, hi) = (found.partition_point(|m| m.0 < start), found.partition_point(|m| m.0 < end));
            found[lo..hi].iter().map(|(index, rows)| (index - start, rows.clone())).collect()
        }).collect()
    }
}

impl<T> CorgiReduceBackend<T> {
    /// A backend for input of `shape`, covering the key space in windows of `1 << 12` input
    /// records.
    pub(crate) fn new(reducer: Reducer, limit: Option<usize>, shape: (Shape, Shape)) -> Self { Self::with_window(reducer, limit, shape, 1 << 12) }

    /// A backend with an explicit window budget, in presented input records. A `Reducer::Expr`
    /// compiles here, at `shape`; a type error is a panic with corgi's message, which install's
    /// type check keeps from being reached.
    pub(crate) fn with_window(reducer: Reducer, limit: Option<usize>, (kshape, vshape): (Shape, Shape), window_size: usize) -> Self {
        let Reducer::Expr(lambda) = reducer;
        let compiled = compile_reduce(&lambda, &kshape, &vshape)
            .unwrap_or_else(|e| panic!("reduce: type error at shapes ({kshape}, {vshape}): {e}"));
        // The graph rebuilt to read only what it reads, and the demand saying what that is.
        let input = Shape::Prod(vec![kshape, Shape::List(Box::new(Shape::Prod(vec![vshape, Shape::Int])))]);
        // A limit observes whole-value order and entry boundaries. Demand alone does not prove
        // that merging those entries is valid; the sum analysis also checks disappearing groups.
        let project_values = limit.is_none() && crate::passes::reduce_demand::projection(&compiled).is_some();
        let narrowed = corgi::narrow(compiled, &Demand::All)
            .unwrap_or_else(|e| panic!("reduce: narrowing a compiled graph at {input}: {e}"));
        CorgiReduceBackend {
            graph: narrowed.graph,
            reads: Reads::new(narrowed.input, &input),
            project_values,
            limit,
            input: Pool::default(),
            output: Pool::default(),
            output_keys: Vec::new(),
            key_columns: Vec::new(),
            rows: (Vec::new(), Vec::new(), ColTimes::default(), Vec::new()),
            completed: Vec::new(),
            window_size: window_size.max(1),
            retire: Retire::default(),
            upper: Vec::new(),
        }
    }
}

/// Where `chunks` hold the ascending `keys`.
///
/// Both the keys and stored identifier lane are sorted; [`matching_ranges`] chooses between
/// merging them and searching the keys in lockstep.
fn search<T: ColTime>(chunks: &[&CorgiChunk<T, Diff>], keys: &[i64]) -> Matches {
    let mut scratch = Vec::new();
    chunks.iter().map(|chunk| {
        let mut found = Vec::new();
        if chunk.diffs().is_empty() { return found; }
        matching_ranges(keys, ident(chunk.keys()), &mut scratch, &mut found);
        found
    }).collect()
}

/// Present the records `chunks` hold for `keys`, at `matches`, into `bridge`, sorted by
/// `((key, id), time)` and consolidated.
///
/// Each key's rows are sorted by value, and each run of equal values gets one id: the next index
/// of `refs`, where it is recorded as the `(chunk, row)` of one of its rows. The keys `colliding`
/// marks (by index; empty when none do) hold several real keys under one identifier: their rows are
/// first split by real key, so that an id names one real key's value, and the row it is recorded
/// at holds that key.
///
/// The keys `skip` marks (by index; empty when none are) are not presented: the backend completes them.
fn present<T: ColTime + Ord>(
    chunks: &[&CorgiChunk<T, Diff>],
    keys: &[i64],
    matches: &Matches,
    colliding: &[bool],
    skip: &[bool],
    projected: Option<&[CValue]>,
    refs: &mut Vec<(usize, usize)>,
    bridge: &mut ProxyBridge<T, Diff, i64>,
) {
    // The matched `(chunk, row)`s, key by key, and each one's key index.
    let mut runs: Vec<_> = matches.iter().enumerate()
        .flat_map(|(chunk, found)| found.iter().map(move |(key, rows)| (*key, chunk, rows.clone())))
        .filter(|run| !skip.get(run.0).copied().unwrap_or(false))
        .collect();
    runs.sort_by_key(|run| run.0);
    let (mut labels, mut rows) = (Vec::new(), Vec::new());
    for (key, chunk, range) in runs {
        labels.extend(range.clone().map(|_| key as u64));
        rows.extend(range.map(|row| (chunk, row)));
    }
    if rows.is_empty() {
        return;
    }
    // Each label's key index; the identity unless real keys split some labels.
    let mut label_keys = None;
    if colliding.contains(&true) {
        (labels, rows, label_keys) = split_real_keys(chunks, colliding, labels, rows);
    }
    let key_of = |label: u64| label_keys.as_ref().map_or(label as usize, |keys: &Vec<usize>| keys[label as usize]);
    // Output and unproven input reducers retain whole-value identity and order.
    let whole;
    let vals = if let Some(projected) = projected { projected } else {
        whole = chunks.iter().map(|chunk| chunk.vals().clone()).collect::<Vec<_>>();
        &whole
    };
    let (sorted, groups) = sort_blocks(&labels, &gather_refs(vals, rows.iter().copied()));
    let update = |(chunk, row): (usize, usize)| (chunks[chunk].times().get(row), chunks[chunk].diffs()[row]);
    let (mut start, mut updates) = (0, Vec::new());
    for group in groups.chunk_by(|a, b| a == b) {
        let members = &sorted[start..start + group.len()];
        start += group.len();
        updates.extend(members.iter().map(|&i| update(rows[i])));
        consolidate(&mut updates);
        if !updates.is_empty() {
            let (key, id) = (keys[key_of(labels[members[0]])], refs.len() as u64);
            refs.push(rows[members[0]]);
            bridge.extend(updates.drain(..).map(|(time, diff)| ((key, id), time, diff)));
        }
    }
}

/// Relabel the rows of the `colliding` key indices so that each real key gets its own label.
///
/// Takes rows labelled by key index, ascending, and returns them reordered so the new labels
/// ascend, with each new label's key index. Rows of other keys keep one label per key.
fn split_real_keys<T: ColTime>(
    chunks: &[&CorgiChunk<T, Diff>],
    colliding: &[bool],
    labels: Vec<u64>,
    rows: Vec<(usize, usize)>,
) -> (Vec<u64>, Vec<(usize, usize)>, Option<Vec<usize>>) {
    // Rank the colliding rows' real keys within their key index.
    let subset: Vec<usize> = (0..rows.len()).filter(|&i| colliding[labels[i] as usize]).collect();
    let key_columns: Vec<CValue> = chunks.iter().map(|chunk| chunk.keys().clone()).collect();
    let subset_labels: Vec<u64> = subset.iter().map(|&i| labels[i]).collect();
    let (perm, ranks) = sort_blocks(&subset_labels, &gather_refs(&key_columns, subset.iter().map(|&i| rows[i])));
    let mut rank = vec![0; rows.len()];
    for (&at, &r) in perm.iter().zip(&ranks) { rank[subset[at]] = r; }
    // Each row's `(key index, rank)`, densely renumbered.
    let mut pairs: Vec<(u64, u64)> = labels.iter().copied().zip(rank).collect();
    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_by_key(|&i| pairs[i]);
    let rows = order.iter().map(|&i| rows[i]).collect();
    pairs = order.into_iter().map(|i| pairs[i]).collect();
    let (mut labels, mut label_keys) = (Vec::with_capacity(pairs.len()), Vec::new());
    for (i, pair) in pairs.iter().enumerate() {
        if i == 0 || pairs[i - 1] != *pair { label_keys.push(pair.0 as usize); }
        labels.push(label_keys.len() as u64 - 1);
    }
    (labels, rows, Some(label_keys))
}

fn chunks_of<T>(batches: &[CBatch<T>]) -> Vec<&CorgiChunk<T, Diff>>
where
    T: ColTime,
{
    batches.iter().flat_map(|b| b.chunks.iter()).collect()
}

/// A retire's keys, ascending: those the novel batches touch, and those the harness says changed.
///
/// Each chunk holds its keys in order, so its distinct keys are read in one pass, and the chunks'
/// lists and the changed keys are sorted together only if there is more than one of them.
fn retire_keys<T: ColTime>(novel_chunks: &[&CorgiChunk<T, Diff>], changed: &[i64]) -> Vec<i64> {
    let (mut keys, mut lists) = (Vec::new(), 0);
    for chunk in novel_chunks {
        let (lane, before) = (ident(chunk.keys()), keys.len());
        keys.extend(lane.iter().enumerate().filter(|(row, key)| *row == 0 || lane[row - 1] != **key).map(|(_, key)| *key));
        lists += (keys.len() > before) as usize;
    }
    let before = keys.len();
    keys.extend_from_slice(changed);
    lists += (keys.len() > before) as usize;
    if lists > 1 {
        // A stable sort merges runs that are already in order.
        keys.sort();
        keys.dedup();
    }
    keys
}

/// The rows that some chunks hold for a window's candidate keys, key by key: where each is, its
/// diff, and whether its time is at or before its key's.
///
/// The times are tested where they are stored, as the rows are found, and the answer is a lane
/// of conclusions. The times themselves are not copied.
struct Rows {
    /// Each row's key, by index; they ascend.
    labels: Vec<u64>,
    /// Where each row is: its chunk, by index, and its row there.
    at: Vec<(usize, usize)>,
    diffs: Vec<Diff>,
    /// Each key's end among the rows.
    ends: Vec<usize>,
    /// Whether each row is at or before its key's time.
    before: Vec<bool>,
}

impl Rows {
    /// No rows, of no keys.
    fn none() -> Self { Rows { labels: Vec::new(), at: Vec::new(), diffs: Vec::new(), ends: Vec::new(), before: Vec::new() } }

    /// The rows `chunks` hold at `matches` for the keys `direct` has as candidates. A key's rows
    /// are in the order of its chunks.
    ///
    /// The rows of the chunks before `novel` are tested against their key's candidate time: a
    /// row at or before it is marked so, and a row that is neither that nor joins with it to a
    /// time at or beyond `upper` ends the key's candidacy. The rows of later chunks are novel
    /// records, which are at their key's time.
    fn new<T: ColTime>(
        chunks: &[&CorgiChunk<T, Diff>],
        matches: &[Vec<(usize, Range<usize>)>],
        novel: usize,
        direct: &mut Direct,
        upper: &[Vec<u64>],
    ) -> Self {
        // Candidacies end as the rows are read; the keys whose rows are read are fixed first.
        let read: Vec<bool> = direct.keys.clone();
        let mut ends = vec![0; read.len()];
        for (index, rows) in matches.iter().flatten() { if read[*index] { ends[*index] += rows.len(); } }
        // Each key's next free row.
        let mut next = Vec::with_capacity(ends.len());
        let mut total = 0;
        for end in ends.iter_mut() {
            next.push(total);
            total += *end;
            *end = total;
        }
        let (mut labels, mut at, mut diffs, mut before) = (vec![0; total], vec![(0, 0); total], vec![0; total], vec![true; total]);
        for (chunk, found) in matches.iter().enumerate() {
            let (times, changes) = (chunks[chunk].times().raw_lanes(), chunks[chunk].diffs());
            // Runs are mostly a row or two: read a row at a time.
            for (index, rows) in found {
                if !read[*index] { continue; }
                for row in rows.clone() {
                    let to = next[*index];
                    next[*index] += 1;
                    labels[to] = *index as u64;
                    at[to] = (chunk, row);
                    diffs[to] = changes[row];
                    if chunk >= novel || !direct.keys[*index] { continue; }
                    // A coordinate the chunk does not hold is zero, which is before anything.
                    let time = &direct.times[index * direct.width..][..direct.width];
                    if times.iter().zip(time).all(|(lane, x)| lane[row] <= *x) { continue; }
                    before[to] = false;
                    direct.keys[*index] = beyond(upper, &|j| times.get(j).map_or(0, |lane| lane[row]).max(time[j]));
                }
            }
        }
        Rows { labels, at, diffs, ends, before }
    }

    /// The rows of key `index`.
    fn key(&self, index: usize) -> Range<usize> {
        (if index == 0 { 0 } else { self.ends[index - 1] })..self.ends[index]
    }
}

/// The window's direct keys, and each one's time.
///
/// A key is direct (differential's `int_proxy::direct`) when its novel records' times are one
/// time, not at or beyond the retire's upper bound, and every one of its input and output records
/// is at or before that time, or joins with it to a time at or beyond the bound.
/// Such a key is evaluated once, over its records at or before its time.
/// A key the tactic owes a carried time is presented, as the tactic evaluates it there.
///
/// [`Direct::candidates`] finds the keys with one such seed, and [`Rows::new`] tests their
/// records against it. Both read time lanes, and build no time.
struct Direct {
    /// Whether each key of the window is direct: at first, whether it is a candidate.
    keys: Vec<bool>,
    /// Each key's candidate time as coordinates, `width` to a key; a direct key's is its time.
    times: Vec<u64>,
    width: usize,
}

impl Direct {
    /// No direct keys: every key is presented.
    fn none() -> Self { Direct { keys: Vec::new(), times: Vec::new(), width: 0 } }

    /// Whether key `index` is direct.
    fn is(&self, index: usize) -> bool { self.keys.get(index).copied().unwrap_or(false) }

    /// The time of key `index`, as coordinates.
    fn time(&self, index: usize) -> &[u64] { &self.times[index * self.width..][..self.width] }

    /// The candidates among `keys`: those whose novel records' times, in the `novel` chunks at
    /// their matches, are one time that is not at or beyond `upper`. The keys `owed` or
    /// `colliding` marks are left to be presented. All times are `width` coordinates wide.
    fn candidates<T: ColTime>(
        keys: &[i64],
        owed: &[bool],
        upper: &[Vec<u64>],
        novel: (&[&CorgiChunk<T, Diff>], &[Vec<(usize, Range<usize>)>]),
        colliding: &[bool],
        width: usize,
    ) -> Self {
        // Each key's seeds so far: none, one time, or more than one.
        #[derive(Clone, Copy, PartialEq)]
        enum Seeds { None, One, Many }
        let mut seeds = vec![Seeds::None; keys.len()];
        let mut times = vec![0; keys.len() * width];
        // The novel records' times.
        for (chunk, found) in novel.0.iter().zip(novel.1) {
            let stored = chunk.times();
            for (index, rows) in found {
                let slot = &mut times[index * width..][..width];
                if seeds[*index] == Seeds::None {
                    for (j, x) in slot.iter_mut().enumerate() { *x = stored.coordinate(j, rows.start); }
                    seeds[*index] = Seeds::One;
                }
                if seeds[*index] == Seeds::One && !(stored.uniform(rows.clone()) && slot.iter().enumerate().all(|(j, x)| *x == stored.coordinate(j, rows.start))) {
                    seeds[*index] = Seeds::Many;
                }
            }
        }
        // A candidate at or beyond `upper` is carried, not evaluated. Neighbouring keys often share
        // a time, so the last verdict is kept.
        let mut direct: Vec<bool> = seeds.iter().zip(owed).zip(colliding.iter().chain(std::iter::repeat(&false))).map(|((seeds, owed), colliding)| *seeds == Seeds::One && !owed && !colliding).collect();
        let mut last: Option<(usize, bool)> = None;
        for index in 0..keys.len() {
            if !direct[index] { continue; }
            let time = &times[index * width..][..width];
            let carried = match last {
                Some((other, carried)) if times[other * width..][..width] == *time => carried,
                _ => beyond(upper, &|j| time[j]),
            };
            last = Some((index, carried));
            direct[index] = !carried;
        }
        Direct { keys: direct, times, width }
    }
}

/// A window's direct keys, with their input and output rows: what the backend completes itself.
struct Completion {
    direct: Direct,
    input: Rows,
    output: Rows,
}

impl Completion {
    /// No direct keys: every key is presented.
    fn none() -> Self { Completion { direct: Direct::none(), input: Rows::none(), output: Rows::none() } }

    /// The direct keys among `keys`: the candidates, whose records are then found, and tested
    /// against their times where they are stored. The input chunks are those before `prior`,
    /// then the novel ones; `owed` and `colliding` mark the keys left to be presented.
    fn find<T: ColTime>(
        keys: &[i64],
        owed: &[bool],
        upper: &[Vec<u64>],
        width: usize,
        colliding: &[bool],
        (in_chunks, in_matches, prior): (&[&CorgiChunk<T, Diff>], &Matches, usize),
        (out_chunks, out_matches): (&[&CorgiChunk<T, Diff>], &Matches),
    ) -> Self {
        let mut direct = Direct::candidates(keys, owed, upper, (&in_chunks[prior..], &in_matches[prior..]), colliding, width);
        let input = Rows::new(in_chunks, in_matches, prior, &mut direct, upper);
        let output = Rows::new(out_chunks, out_matches, out_chunks.len(), &mut direct, upper);
        Completion { direct, input, output }
    }
}

/// Whether the time with coordinates `time(j)` is at or beyond the frontier whose elements have
/// coordinates `upper`.
fn beyond(upper: &[Vec<u64>], time: &dyn Fn(usize) -> u64) -> bool {
    upper.iter().any(|bound| bound.iter().enumerate().all(|(j, x)| *x <= time(j)))
}

/// Net the rows `select` of `rows`, which are of direct keys and ascend, by value.
///
/// The rows are sorted by their `values` within each key, and each run of equal values is netted
/// two ways. Its rows at or before the key's time sum to the value's count there, and `value`
/// receives each that is not zero as the key's index, where one row of the value is, and the
/// count: for one key at a time, with values ascending. Its other rows carry their joins with
/// the key's time, less those that net to nothing, which go to `carried`; their times are read
/// from `chunks`, where the rows are.
fn net<T: ColTime>(
    rows: &Rows,
    select: &[usize],
    chunks: &[&CorgiChunk<T, Diff>],
    values: &[CValue],
    direct: &Direct,
    keys: &[i64],
    mut value: impl FnMut(usize, (usize, usize), Diff),
    carried_times: &mut Vec<(T, i64)>,
) {
    if select.is_empty() { return; }
    let labels: Vec<u64> = select.iter().map(|&row| rows.labels[row]).collect();
    let (sorted, groups) = sort_blocks(&labels, &gather_refs(values, select.iter().map(|&row| rows.at[row])));
    // Coordinate `j` of a row's time.
    let coordinate = |row: usize, j: usize| chunks[rows.at[row].0].times().coordinate(j, rows.at[row].1);
    // The current value's rows that are not at or before the key's time, netted by their joins with it.
    let mut carried: Vec<(usize, Diff)> = Vec::new();
    // The last row to carry a time, whose key and time the next often repeats.
    let mut last: Option<usize> = None;
    let mut start = 0;
    while start < sorted.len() {
        let mut end = start + 1;
        while end < sorted.len() && groups[end] == groups[start] { end += 1; }
        let first = select[sorted[start]];
        let index = rows.labels[first] as usize;
        let at = direct.time(index);
        // Whether two rows of this key join with its time to one time.
        let same = |a: usize, b: usize| at.iter().enumerate().all(|(j, x)| coordinate(a, j).max(*x) == coordinate(b, j).max(*x));
        let mut count = 0;
        carried.clear();
        for &position in &sorted[start..end] {
            let row = select[position];
            if rows.before[row] { count += rows.diffs[row]; }
            else {
                match carried.iter_mut().find(|entry| same(entry.0, row)) {
                    Some(entry) => entry.1 += rows.diffs[row],
                    None => carried.push((row, rows.diffs[row])),
                }
            }
        }
        if count != 0 { value(index, rows.at[first], count); }
        for &(row, change) in &carried {
            if change == 0 || last.is_some_and(|other| rows.labels[other] == rows.labels[row] && same(other, row)) { continue; }
            last = Some(row);
            carried_times.push((T::from_coordinates(at.iter().enumerate().map(|(j, x)| coordinate(row, j).max(*x))), keys[index]));
        }
        start = end;
    }
}

impl<T> CorgiReduceBackend<T>
where
    T: ColTime + Ord,
{
    /// The lambda's graph over lists of input rows, one list to a key.
    ///
    /// The lists end at `ends`, none of them empty. Their rows are `rows` of the input `chunks`
    /// (by index), each counted `diffs` times, with distinct values ascending in each list where
    /// the values are read whole. `keys` are each list's key, as a row of `key_columns`.
    /// The values are gathered once, projected, from where they are stored.
    /// Returns the output values, their diffs, and each list's end among them.
    fn reduce_rows(&self, ends: Vec<usize>, chunks: &[usize], rows: &[usize], diffs: Vec<Diff>, keys: Vec<(usize, usize)>) -> (CValue, Vec<Diff>, Vec<usize>) {
        let vals = match self.reads.value {
            Demand::None => CValue::Unit(rows.len()),
            _ => {
                let sources: Vec<Option<&CValue>> = self.input.columns.iter().map(Some).collect();
                gather_lanes(&sources, chunks, rows)
            }
        };
        // Projected values may tie; whole ones ascend.
        if cfg!(debug_assertions) && self.reads.value == Demand::All {
            let mut start = 0;
            for &end in &ends {
                for i in start + 1..end {
                    assert!(compare_at(&vals, i - 1, &vals, i).is_lt(), "reduce: a list's values are not strictly ascending");
                }
                start = end;
            }
            assert!(diffs.iter().all(|&diff| diff != 0), "reduce: a list holds a zero diff");
        }
        let keys = if self.reads.key { recover_key(&gather_refs(&self.key_columns, keys.into_iter())) } else { CValue::Unit(keys.len()) };
        let lists = CValue::List(Bounds::offsets(ends), Box::new(CValue::Prod(vec![vals, CValue::i64(diffs)])));
        let argument = self.reads.argument.project(CValue::Prod(vec![keys, lists])).expect("the reduce's input has the shape its demand is on");
        let shape = corgi::shape_of_value(&argument);
        assert!(shape == self.reads.shape, "reduce: input read as {shape} reached an operator narrowed to {}", self.reads.shape);
        let (bounds, elems) = corgi::eval_graph(&self.graph, argument).into_list("reduce output").unwrap();
        let mut fields = elems.into_prod("reduce output").unwrap();
        let out_diffs = fields.pop().unwrap().into_i64("reduce output diffs").unwrap().to_vec();
        let out_vals = fields.pop().unwrap();
        (out_vals, out_diffs, bounds.to_vec())
    }

    /// The expression crossing for a wave: every
    /// bracket's key and its values, as a list of `(val, diff)`, through the lambda's graph at once.
    /// Returns the output values, their diffs, and each bracket's end among them.
    ///
    /// The harness presents each bracket consolidated, so its diffs are non-zero and its value ids
    /// ascend, which is value order: `present` numbers each key's values in sorted order. The
    /// brackets are presented as they are. One with no input has no output, and the lambda does not
    /// see it.
    fn reduce_lambda(&self, ends: &[usize], input: &[(u64, Diff)]) -> (CValue, Vec<Diff>, Vec<usize>) {
        // A limit hands the lambda each bracket's first values only; nothing past them is gathered.
        let limited = self.limit.and_then(|k| first_entries(k, ends, input));
        let (ends, input) = limited.as_ref().map_or((ends, input), |(ends, input)| (&ends[..], &input[..]));
        // Per bracket with input: its index, its end in `input`, and the row holding its key.
        let (mut blocks, mut key_refs) = (Vec::new(), Vec::new());
        let mut start = 0;
        for (bi, &end) in ends.iter().enumerate() {
            if end > start {
                blocks.push((bi, end));
                key_refs.push(self.input.refs[input[start].0 as usize]);
            }
            start = end;
        }
        if blocks.is_empty() {
            return (CValue::Unit(0), Vec::new(), vec![0; ends.len()]);
        }
        let (chunks, rows): (Vec<usize>, Vec<usize>) = input.iter().map(|&(id, _)| self.input.refs[id as usize]).unzip();
        let diffs = input.iter().map(|&(_, diff)| diff).collect();
        let block_ends: Vec<usize> = blocks.iter().map(|b| b.1).collect();
        let (out_vals, out_diffs, list_ends) = self.reduce_rows(block_ends, &chunks, &rows, diffs, key_refs);

        // Each bracket's end among the output rows; brackets without input add none.
        let (mut out_ends, mut blocks, mut emitted) = (Vec::with_capacity(ends.len()), blocks.iter().zip(list_ends).peekable(), 0);
        for bi in 0..ends.len() {
            if let Some((_, end)) = blocks.next_if(|((b, _), _)| *b == bi) { emitted = end; }
            out_ends.push(emitted);
        }
        (out_vals, out_diffs, out_ends)
    }

    /// The output of the window's direct keys, `completion`, as columns: the reducer's rows for
    /// each, at its time, and its existing output at or before its time, negated, at its time.
    /// The times the keys' other records carry go to `carried`.
    ///
    /// The reducer's input is one list of rows to a key, with a count for each. A limit keeps
    /// each list's first rows. A key with nothing to read has nothing to evaluate, as in the
    /// tactic's sweep.
    fn complete(
        &self,
        keys: &[i64],
        in_chunks: &[&CorgiChunk<T, Diff>],
        out_chunks: &[&CorgiChunk<T, Diff>],
        prior: usize,
        completion: &Completion,
        carried: &mut Vec<(T, i64)>,
    ) -> Vec<Columns<T>> {
        let Completion { direct, input, output } = completion;
        let is_direct = |index: usize| direct.is(index);
        let projected = (self.project_values && self.reads.value != Demand::All).then_some(&self.input.columns[..]);
        let mut parts = Vec::new();
        let limit = self.limit.unwrap_or(usize::MAX);
        let (mut lists, mut ends, mut diffs) = (Vec::new(), Vec::new(), Vec::new());
        let (mut chunks, mut rows) = (Vec::new(), Vec::new());
        // A direct key is plain when one novel chunk holds it: its rows are at the
        // key's time, and a chunk holds a value once at a time, so they are the key's values, in
        // order and netted, and there is nothing to sort.
        // Any other direct key's rows are sorted and netted by value.
        let mut select = Vec::new();
        for index in (0..keys.len()).filter(|index| is_direct(*index)) {
            let block = input.key(index);
            let Some(&(chunk, row)) = input.at.get(block.start).filter(|_| !block.is_empty()) else { continue };
            let plain = chunk >= prior && block.clone().all(|row| input.at[row].0 == chunk);
            if plain {
                let block = block.start..block.end.min(block.start.saturating_add(limit));
                chunks.extend(std::iter::repeat_n(chunk, block.len()));
                rows.extend(row..row + block.len());
                diffs.extend_from_slice(&input.diffs[block]);
                lists.push((index, (chunk, row)));
                ends.push(rows.len());
            } else {
                select.extend(block);
            }
        }
        let whole;
        let values = if let Some(projected) = projected { projected } else {
            whole = in_chunks.iter().map(|chunk| chunk.vals().clone()).collect::<Vec<_>>();
            &whole
        };
        // The list being built, by its key's index, and its length.
        let mut open: Option<(usize, usize)> = None;
        net(input, &select, &in_chunks, values, direct, keys, |index, at, count| {
            match &mut open {
                Some((key, length)) if *key == index => *length += 1,
                _ => {
                    if open.is_some() { ends.push(rows.len()); }
                    lists.push((index, at));
                    open = Some((index, 1));
                }
            }
            if open.is_some_and(|(_, length)| length <= limit) {
                chunks.push(at.0);
                rows.push(at.1);
                diffs.push(count);
            }
        }, carried);
        if open.is_some() { ends.push(rows.len()); }
        // The times the direct keys' output records carry.
        let select: Vec<usize> = (0..output.labels.len()).filter(|row| !output.before[*row] && is_direct(output.labels[*row] as usize)).collect();
        net(output, &select, &out_chunks, &self.output.columns[..out_chunks.len()], direct, keys, |_, _, _| unreachable!("the selected rows are not at or before their key's time"), carried);

        // The reducer's rows, each at its key's time.
        if !lists.is_empty() {
            let (vals, diffs, ends) = self.reduce_rows(ends, &chunks, &rows, diffs, lists.iter().map(|list| list.1).collect());
            let mut lanes = vec![Vec::with_capacity(diffs.len()); direct.width];
            let mut key_rows = Vec::with_capacity(diffs.len());
            let mut start = 0;
            for ((index, key), end) in lists.into_iter().zip(ends) {
                key_rows.extend(std::iter::repeat_n(key, end - start));
                for (lane, coordinate) in lanes.iter_mut().zip(direct.time(index)) { lane.extend(std::iter::repeat_n(*coordinate, end - start)); }
                start = end;
            }
            let times = ColTimes::from_raw_lanes(lanes, diffs.len());
            parts.push(Columns { keys: gather_refs(&self.key_columns, key_rows.into_iter()), vals, times, diffs });
        }
        // The direct keys' existing output at or before their times, negated, at their times.
        // Sealing the batch nets it against the reducer's rows.
        let mut lanes = vec![Vec::new(); direct.width];
        let (mut chunks, mut rows, mut diffs) = (Vec::new(), Vec::new(), Vec::new());
        for row in 0..output.labels.len() {
            let index = output.labels[row] as usize;
            if !is_direct(index) || !output.before[row] { continue; }
            chunks.push(output.at[row].0);
            rows.push(output.at[row].1);
            diffs.push(-output.diffs[row]);
            for (lane, coordinate) in lanes.iter_mut().zip(direct.time(index)) { lane.push(*coordinate); }
        }
        if !rows.is_empty() {
            let keys = gather_refs(&self.key_columns, chunks.iter().map(|chunk| in_chunks.len() + chunk).zip(rows.iter().copied()));
            let sources: Vec<Option<&CValue>> = self.output.columns[..out_chunks.len()].iter().map(Some).collect();
            let vals = gather_lanes(&sources, &chunks, &rows);
            let times = ColTimes::from_raw_lanes(lanes, diffs.len());
            parts.push(Columns { keys, vals, times, diffs });
        }
        parts
    }
}

impl<T> ProxyReduceBackend<T, CBatch<T>, CBatch<T>> for CorgiReduceBackend<T>
where
    T: ColTime + Ord,
{
    type Key = i64;
    type VIn = u64;
    type VOut = u64;
    type RIn = Diff;
    type ROut = Diff;

    fn begin(&mut self, description: Description<T>) {
        self.rows = (Vec::new(), Vec::new(), ColTimes::default(), Vec::new());
        self.completed.clear();
        self.upper = description.upper().iter().map(|time| time.coordinates().collect()).collect();
    }

    fn next_window(&mut self, instance: &ReduceInstance<'_, T, CBatch<T>, CBatch<T>>, changed: &[i64], from: &mut KeyPosition<i64>, window: &mut ReduceWindow<T, Diff, Diff, i64>) {
        if *from == KeyPosition::End {
            return;
        }
        let novel_chunks = chunks_of(instance.input_batches);
        let mut in_chunks = chunks_of(instance.source_batches);
        let prior = in_chunks.len();
        in_chunks.extend(novel_chunks.iter().copied());
        let out_chunks = chunks_of(instance.output_batches);

        // The first window finds the retire's keys, where the input holds them, and the columns
        // that the retire's ids will name rows of.
        if *from == KeyPosition::Start {
            let keys = retire_keys(&novel_chunks, changed);
            let input = search(&in_chunks, &keys);
            let mut held = vec![0; keys.len()];
            for (index, rows) in input.iter().flatten() { held[*index] += rows.len(); }
            // The changed keys are those the tactic owes a carried time; both lists ascend.
            let mut next = changed.iter().peekable();
            let owed = keys.iter().map(|key| next.next_if(|changed| *changed == key).is_some()).collect();
            self.retire = Retire { keys, held, input, window: 0..0, colliding: Vec::new(), owed };
            let value = &self.reads.value;
            self.input.columns = in_chunks.iter().map(|chunk| value.project(chunk.vals().clone()).expect("a chunk's values have the reduce's value shape")).collect();
            self.output = Pool { columns: out_chunks.iter().map(|chunk| chunk.vals().clone()).collect(), refs: Vec::new() };
            self.output_keys.clear();
            self.key_columns = in_chunks.iter().chain(&out_chunks).map(|chunk| chunk.keys().clone()).collect();
        }

        // Windows are bounded, as their presentations are all live at once.
        let retire = &mut self.retire;
        if !retire.advance(self.window_size) {
            *from = KeyPosition::End;
            return;
        }
        let matches = retire.input_matches();
        let keys = &retire.keys[retire.window.clone()];
        let out_matches = search(&out_chunks, keys);
        // The identifiers that hold several real keys, across input and output alike.
        let colliding = if in_chunks.iter().chain(&out_chunks).any(|chunk| key_is_hashed(chunk.keys())) {
            let columns: Vec<_> = in_chunks.iter().chain(&out_chunks).map(|chunk| recover_key_ref(chunk.keys())).collect();
            let runs = matches.iter().chain(&out_matches).enumerate().flat_map(|(column, found)| {
                found.iter().map(move |(key, rows)| (*key, column, rows.start, rows.end))
            });
            colliding_keys(keys.len(), &columns, runs)
        } else { vec![false; keys.len()] };
        retire.colliding = keys.iter().zip(&colliding).filter(|(_, c)| **c).map(|(key, _)| *key).collect();
        let colliding = if retire.colliding.is_empty() { &[][..] } else { &colliding[..] };

        let width = in_chunks.iter().chain(&out_chunks).map(|chunk| chunk.times().width())
            .chain(self.upper.iter().map(Vec::len)).max().unwrap_or(0);
        // The direct keys, which are completed here rather than presented.
        let completion = if instance.present_all { Completion::none() } else {
            let owed = &retire.owed[retire.window.clone()];
            Completion::find(keys, owed, &self.upper, width, colliding, (&in_chunks, &matches, prior), (&out_chunks, &out_matches))
        };
        let is_direct = |index: usize| completion.direct.is(index);

        // The seeds are the novel batches' RAW (key, time) support, recorded here — before the
        // merged presentation below, whose consolidation may net a novel record away entirely.
        window.seeds.reserve(matches[prior..].iter().flatten().filter(|(index, _)| !is_direct(*index)).map(|(_, rows)| rows.len()).sum());
        for (chunk, found) in in_chunks[prior..].iter().zip(&matches[prior..]) {
            for (index, rows) in found {
                if is_direct(*index) { continue; }
                window.seeds.extend(rows.clone().map(|row| (keys[*index], chunk.times().get(row))));
            }
        }
        window.seeds.sort_unstable();
        window.seeds.dedup();

        // ONE merged input presentation: novel and prior together, netted by the consolidation —
        // equal values share an id, so an exactly cancelling pair vanishes here, and its time
        // survives in `window.seeds` above. Input ids name rows for this window only.
        // Only the keys that are not direct are presented.
        self.input.refs.clear();
        let projected = (self.project_values && self.reads.value != Demand::All).then_some(&self.input.columns[..]);
        present(&in_chunks, keys, &matches, colliding, &completion.direct.keys, projected, &mut self.input.refs, &mut window.input);
        // The output history, same keys. Output ids name rows for the whole retire, and the rows
        // that hold their keys are at the same positions of the output chunks' key columns.
        let fresh = self.output.refs.len();
        present(&out_chunks, keys, &out_matches, colliding, &completion.direct.keys, None, &mut self.output.refs, &mut window.output);
        self.output_keys.extend(self.output.refs[fresh..].iter().map(|&(chunk, row)| (in_chunks.len() + chunk, row)));

        *from = retire.keys.get(retire.window.end).map_or(KeyPosition::End, |key| KeyPosition::At(*key));
        if completion.direct.keys.is_empty() { return; }
        let keys = &self.retire.keys[self.retire.window.clone()];
        let parts = self.complete(keys, &in_chunks, &out_chunks, prior, &completion, &mut window.carried);
        self.completed.extend(parts);
    }

    fn reduce_corrections(&mut self, keys: &[i64], in_ends: &[usize], input: &[(u64, Diff)], out_ends: &[usize], output: &[(u64, Diff)]) -> (Vec<(u64, Diff)>, Vec<usize>) {
        // Each key wants its rows of `desired`, each with its diff (once each, but for a lambda's), and
        // has `(id, diff)`s of output. The
        // correction nets the two by value: a wanted row equal to an output id's value counts
        // toward that id, and one equal to none gets a new id. Keys usually have a handful of each;
        // a key with many desired rows is sorted and consolidated instead (see below).
        //
        // A colliding identifier's bracket holds several real keys' values. It is split into one
        // bracket per real key, each reduced on its own, and its rows are matched against output
        // by key as well as value. Its input ids ascend by real key, so each key's are contiguous.
        let split = keys.iter().any(|key| self.retire.colliding.binary_search(key).is_ok());
        let (mut sub_ends, mut sub_input, mut bracket_ends) = (Vec::new(), Vec::new(), Vec::new());
        if split {
            let mut start = 0;
            for (key, &end) in keys.iter().zip(in_ends) {
                let lo = sub_input.len();
                sub_input.extend_from_slice(&input[start..end]);
                if self.retire.colliding.binary_search(key).is_ok() {
                    sub_input[lo..].sort_by_key(|&(id, _)| id);
                    for i in lo + 1..sub_input.len() {
                        let ((c0, r0), (c1, r1)) = (self.input.refs[sub_input[i - 1].0 as usize], self.input.refs[sub_input[i].0 as usize]);
                        if compare_at(&self.key_columns[c0], r0, &self.key_columns[c1], r1).is_ne() { sub_ends.push(i); }
                    }
                }
                sub_ends.push(sub_input.len());
                bracket_ends.push(sub_ends.len());
                start = end;
            }
        }
        let (in_ends, input) = if split { (&sub_ends[..], &sub_input[..]) } else { (in_ends, input) };
        // The desired rows, and each one's diff where it is not one copy.
        let (desired, desired_diffs, desired_ends) = self.reduce_lambda(in_ends, input);
        let fresh = self.output.columns.len();
        let prior_refs = self.output.refs.len();
        if !desired.is_empty() { self.output.columns.push(desired); }
        let Pool { columns, refs } = &mut self.output;
        let (output_keys, key_columns) = (&mut self.output_keys, &self.key_columns);
        let (mut corr, mut corr_ends) = (Vec::new(), Vec::with_capacity(keys.len()));
        // Scratch for keys with many rows, reused across keys.
        let mut order: Vec<(usize, usize, bool, usize)> = Vec::new();
        let (mut target, mut alloc): (Vec<Result<usize, usize>>, Vec<usize>) = (Vec::new(), Vec::new());
        let (mut bs, mut os) = (0, 0);
        for (b, (key, &oe)) in keys.iter().zip(out_ends).enumerate() {
            let be = if split { bracket_ends[b] } else { b + 1 };
            let checked = split && self.retire.colliding.binary_search(key).is_ok();
            // Reconcile directly in this key's tail of the batched corrections. A separate Vec
            // here used to allocate once for every key, even for an initial singleton result.
            let net_start = corr.len();
            corr.extend(output[os..oe].iter().map(|&(id, d)| (id, -d)));
            // A key with many desired rows: diff and consolidate. The search below costs the
            // key's rows for each desired row, quadratic in them; instead order the key's existing
            // and desired rows together by value, and give each desired row the entry that search
            // would find: the first existing row of its value, else the first desired row of its
            // value, which allocates. A key that is not colliding has one bracket, `bs`.
            let (lo, hi) = (if bs == 0 { 0 } else { desired_ends[bs - 1] }, desired_ends[be - 1]);
            let rows = (oe - os) + (hi - lo);
            if !checked && hi - lo > 16 && hi - lo > 2 * (usize::BITS - rows.leading_zeros()) as usize {
                let first = if bs == 0 { 0 } else { in_ends[bs - 1] };
                let (kc, kr) = self.input.refs[input[first].0 as usize];
                // (column, row, desired, position): of equal values, existing rows come first,
                // each side in its own order, so a run's first item is the search's answer.
                order.clear();
                order.extend(output[os..oe].iter().enumerate().map(|(i, &(id, _))| { let (c, r) = refs[id as usize]; (c, r, false, i) }));
                order.extend((lo..hi).map(|row| (fresh, row, true, row)));
                order.sort_by(|x, y| compare_at(&columns[x.0], x.1, &columns[y.0], y.1).then((x.2, x.3).cmp(&(y.2, y.3))));
                // Each desired row's entry: `Ok(i)` the key's `i`th existing row, `Err(r)` the
                // entry desired row `r` allocates.
                target.clear();
                target.resize(hi - lo, Err(0));
                let mut start = 0;
                while start < order.len() {
                    let (c, r, _, _) = order[start];
                    let mut end = start + 1;
                    while end < order.len() && compare_at(&columns[c], r, &columns[order[end].0], order[end].1).is_eq() { end += 1; }
                    let to = if order[start].2 { Err(order[start].3) } else { Ok(order[start].3) };
                    for &(_, _, wanted, row) in &order[start..end] {
                        if wanted { target[row - lo] = to; }
                    }
                    start = end;
                }
                alloc.clear();
                alloc.resize(hi - lo, 0);
                for row in lo..hi {
                    let diff = desired_diffs[row];
                    match target[row - lo] {
                        Ok(i) => corr[net_start + i].1 += diff,
                        Err(r) if r == row => {
                            alloc[row - lo] = corr.len();
                            corr.push((refs.len() as u64, diff));
                            refs.push((fresh, row));
                            output_keys.push((kc, kr));
                        }
                        Err(r) => corr[alloc[r - lo]].1 += diff,
                    }
                }
            } else {
                for s in bs..be {
                    let (ds, de) = (if s == 0 { 0 } else { desired_ends[s - 1] }, desired_ends[s]);
                    if ds == de { continue; }
                    // A desired row's key: that of the input it was reduced from, which is non-empty.
                    let first = if s == 0 { 0 } else { in_ends[s - 1] };
                    let (kc, kr) = self.input.refs[input[first].0 as usize];
                    for row in ds..de {
                        let diff = desired_diffs[row];
                        let equal = |&(id, _): &(u64, Diff)| {
                            let (column, r) = refs[id as usize];
                            compare_at(&columns[fresh], row, &columns[column], r).is_eq() && (!checked || {
                                let (oc, or) = output_keys[id as usize];
                                compare_at(&key_columns[kc], kr, &key_columns[oc], or).is_eq()
                            })
                        };
                        match corr[net_start..].iter().position(equal) {
                            Some(at) => corr[net_start + at].1 += diff,
                            None => {
                                corr.push((refs.len() as u64, diff));
                                refs.push((fresh, row));
                                output_keys.push((kc, kr));
                            }
                        }
                    }
                }
            }
            let mut keep = net_start;
            for read in net_start..corr.len() {
                if corr[read].1 != 0 {
                    corr[keep] = corr[read];
                    keep += 1;
                }
            }
            corr.truncate(keep);
            corr_ends.push(corr.len());
            (bs, os) = (be, oe);
        }
        // Desired rows equal to existing ids need no retained payload. Keep this column
        // only if a newly allocated id names it; later waves can reuse the vacant slot.
        if refs.len() == prior_refs { columns.truncate(fresh); }
        (corr, corr_ends)
    }

    fn emit(&mut self, records: &[((i64, u64), T, Diff)]) {
        // An output id names its key's row, as two keys may share an identifier.
        let (key_rows, ids, times, diffs) = &mut self.rows;
        for ((_, id), time, diff) in records {
            key_rows.push(self.output_keys[*id as usize]);
            ids.push(*id);
            times.push(time);
            diffs.push(*diff);
        }
    }

    fn finish(&mut self) -> Option<CBatch<T>> {
        // The retire's output: what the keys completed here recorded as columns, and what the
        // tactic emitted for the keys it was presented.
        let (key_rows, ids, times, diffs) = std::mem::take(&mut self.rows);
        let mut parts = std::mem::take(&mut self.completed);
        if !times.is_empty() {
            let keys = gather_refs(&self.key_columns, key_rows.into_iter());
            parts.push(Columns { keys, vals: self.output.gather(&ids), times, diffs });
        }
        let whole = if parts.len() > 1 {
            let (mut tags, mut offs) = (Vec::new(), Vec::new());
            let (mut times, mut diffs) = (ColTimes::default(), Vec::new());
            for (tag, part) in parts.iter().enumerate() {
                tags.extend(std::iter::repeat_n(tag, part.diffs.len()));
                offs.extend(0..part.diffs.len());
                times.push_range(&part.times, 0, part.diffs.len());
                diffs.extend_from_slice(&part.diffs);
            }
            let keys = gather_lanes(&parts.iter().map(|part| Some(&part.keys)).collect::<Vec<_>>(), &tags, &offs);
            let vals = gather_lanes(&parts.iter().map(|part| Some(&part.vals)).collect::<Vec<_>>(), &tags, &offs);
            Columns { keys, vals, times, diffs }
        } else {
            parts.pop()?
        };
        // The parts may net to nothing, as a key's rows may repeat what it has.
        columns_to_nonempty_batch(whole.keys, whole.vals, whole.times, whole.diffs).map(Rc::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::chunk::key_ids;
    use std::collections::{BTreeMap, BTreeSet};
    use crate::runtime::chunk::present_key;
    use crate::ir::Time;

    /// Demand is insufficient: entry counts, limits, extrema on signed lists and zero-emitting
    /// sums must keep whole-value identity. A weighted sum that suppresses zero may consolidate.
    #[test]
    fn projected_values_require_a_mergeable_reducer() {
        let shape = (Shape::Int, Shape::Prod(vec![Shape::Int, Shape::Int]));
        let sum = "let n = vals map (((v, _), d) -> (v, d) mul) fold_add in (n, (n, 1)) enlist filter";
        for (body, limit, expected) in [
            (sum, None, true), (sum, Some(1), false),
            ("(vals map ((_, d) -> d) fold_add, 1) enlist", None, false),
            ("(vals len, 1) enlist", None, false),
            ("(vals head.0.0, 1) enlist", Some(1), false),
            ("(vals map (((v, _), _) -> v) fold_min, 1) enlist", None, false),
            ("(vals map (((v, _), _) -> v) fold_max, 1) enlist", None, false),
        ] {
            let lambda = crate::ir::Lambda::new(format!("let (_, vals) = input in {body}"), &corgi::Scope::default());
            let backend = CorgiReduceBackend::<Time>::new(Reducer::Expr(lambda), limit, shape.clone());
            assert_eq!(backend.project_values, expected, "{body}, limit={limit:?}");
        }
        let backend = CorgiReduceBackend::<Time>::new(Reducer::Expr(crate::parse::Reducer::Count.lambda()), None, shape);
        assert!(backend.project_values);
    }

    /// The rows of a column of `Int` leaves, products of them, or units, as vectors.
    fn rows_of(col: &CValue) -> Vec<Vec<u64>> {
        let fields: Vec<Vec<i64>> = match col {
            CValue::Unit(_) => Vec::new(),
            CValue::Prod(fields) => fields.iter().map(|field| field.as_i64("rows_of").unwrap().into_owned()).collect(),
            leaf => vec![leaf.as_i64("rows_of").unwrap().into_owned()],
        };
        (0..col.len()).map(|i| fields.iter().map(|field| field[i] as u64).collect()).collect()
    }

    /// An integer column of these values.
    fn ints(xs: Vec<u64>) -> CValue { CValue::i64(xs.into_iter().map(|x| x as i64).collect()) }

    /// Random `(key, value, time, diff)` rows over few keys and values, so that they collide.
    fn random_rows(seed: u64, count: usize, keys: u64) -> Vec<(u64, (u64, u64), Time, Diff)> {
        let stamp = |outer, coords: &[u64]| Time::new(std::iter::once(outer).chain(coords.iter().copied()).collect());
        let times = [stamp(0, &[]), stamp(1, &[2, 3]), stamp(2, &[1, 4]), stamp(3, &[2])];
        let mut state = seed;
        let mut next = |n: u64| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) % n
        };
        (0..count).map(|_| (next(keys), (next(3), next(3)), times[next(4) as usize].clone(), [1, -1, 2][next(3) as usize])).collect()
    }

    const KEY_SHAPES: [fn(&[u64]) -> CValue; 2] = [
        |k| ints(k.to_vec()),
        |k| present_key(CValue::Prod(vec![ints(k.to_vec()), ints(k.iter().map(|k| k * 7).collect())])),
    ];
    const VAL_SHAPES: [fn(&[(u64, u64)]) -> CValue; 3] = [
        |v| ints(v.iter().map(|v| v.0).collect()),
        |v| CValue::Prod(vec![ints(v.iter().map(|v| v.0).collect()), ints(v.iter().map(|v| v.1).collect())]),
        |v| CValue::Unit(v.len()),
    ];

    /// One chunk per run of `size` rows, in the given key and value shapes.
    fn chunks(rows: &[(u64, (u64, u64), Time, Diff)], size: usize, key_shape: fn(&[u64]) -> CValue, val_shape: fn(&[(u64, u64)]) -> CValue) -> Vec<CorgiChunk<Time, Diff>> {
        rows.chunks(size).map(|rows| CorgiChunk::from_columns(
            key_shape(&rows.iter().map(|r| r.0).collect::<Vec<_>>()),
            val_shape(&rows.iter().map(|r| r.1).collect::<Vec<_>>()),
            rows.iter().map(|r| r.2.clone()).collect(),
            rows.iter().map(|r| r.3).collect(),
        )).filter(|chunk| !chunk.diffs().is_empty()).collect()
    }

    /// The netted records of `keys` in `chunks`, by key id and value.
    fn netted(chunks: &[&CorgiChunk<Time, Diff>], keys: &BTreeSet<i64>) -> BTreeMap<(i64, Vec<u64>, Time), Diff> {
        let mut netted = BTreeMap::new();
        for chunk in chunks {
            for (i, (key, value)) in key_ids(chunk.keys()).into_iter().zip(rows_of(chunk.vals())).enumerate() {
                if keys.contains(&key) {
                    *netted.entry((key, value, chunk.times().get(i))).or_insert(0) += chunk.diffs()[i];
                }
            }
        }
        netted.retain(|_, diff| *diff != 0);
        netted
    }

    /// `present` gives what the chunks hold, sorted and consolidated, with one id per value under a
    /// key, across key and value shapes, chunkings, and cancellations.
    #[test]
    fn present_groups_values_across_chunks() {
        let rows = random_rows(0x9E37_79B9_7F4A_7C15, 300, 6);
        for (key_shape, val_shape) in KEY_SHAPES.iter().flat_map(|k| VAL_SHAPES.iter().map(move |v| (*k, *v))) {
            let mut keys = key_ids(&key_shape(&[0, 2, 3, 5, 9]));
            keys.sort();
            for size in [1, 7, rows.len()] {
                let owned = chunks(&rows, size, key_shape, val_shape);
                let chunks: Vec<_> = owned.iter().collect();
                let (mut refs, mut bridge) = (Vec::new(), Vec::new());
                present(&chunks, &keys, &search(&chunks, &keys), &[], &[], None, &mut refs, &mut bridge);
                assert!(bridge.windows(2).all(|w| (w[0].0, &w[0].1) < (w[1].0, &w[1].1)));
                let pool = Pool { columns: chunks.iter().map(|chunk| chunk.vals().clone()).collect(), refs };
                let values = rows_of(&pool.gather(&bridge.iter().map(|r| r.0.1).collect::<Vec<_>>()));
                let (mut actual, mut named) = (BTreeMap::new(), BTreeMap::new());
                for (((key, id), time, diff), value) in bridge.iter().zip(values) {
                    assert_eq!(named.entry((*key, value.clone())).or_insert(*id), id, "one id per value");
                    actual.insert((*key, value, time.clone()), *diff);
                }
                assert_eq!(actual, netted(&chunks, &keys.iter().copied().collect()));
            }
        }
    }

    /// Corrections net desired against current output by value, whichever ids hold it: two ids of
    /// one value count together, and a new value gets a new id naming its row.
    #[test]
    fn corrections_net_by_value() {
        let mut backend = CorgiReduceBackend::<Time>::new(Reducer::Expr(crate::parse::Reducer::Count.lambda()), None, (Shape::Int, Shape::Int));
        backend.output = Pool { columns: vec![ints(vec![5, 5, 7])], refs: vec![(0, 0), (0, 1), (0, 2)] };
        backend.output_keys = vec![(0, 0), (0, 0), (0, 1)];
        backend.input.refs = vec![(0, 0), (0, 1)];
        backend.input.columns = vec![ints(vec![0, 1])];
        backend.key_columns = vec![ints(vec![10, 11, 12])];
        // Key 0 counts 5 and has output 5 twice; key 1 counts 3 and has output 7; key 2 counts 0.
        let input = [(0, 2), (1, 3), (0, 3), (0, 1), (1, -1)];
        let output = [(0, 1), (1, 1), (2, 1)];
        let (corr, ends) = backend.reduce_corrections(&[10, 11, 12], &[2, 3, 5], &input, &[2, 3, 3], &output);
        assert_eq!((corr.clone(), ends), (vec![(1, -1), (2, -1), (3, 1)], vec![1, 3, 3]));
        assert_eq!(rows_of(&backend.output.gather(&[3])), vec![vec![3]]);
    }

    /// Unchanged waves borrow old ids and must not accumulate unused desired columns.
    /// A changed wave's column stays alive when a later unchanged wave reuses its id.
    #[test]
    fn corrections_retain_only_named_output_columns() {
        let mut backend = CorgiReduceBackend::<Time>::new(Reducer::Expr(crate::parse::Reducer::Count.lambda()), None, (Shape::Int, Shape::Int));
        backend.input.refs = vec![(0, 0)];
        backend.input.columns = vec![ints(vec![0])];
        backend.key_columns = vec![ints(vec![10])];
        backend.output = Pool { columns: vec![ints(vec![5])], refs: vec![(0, 0)] };
        backend.output_keys = vec![(0, 0)];
        for _ in 0..64 {
            assert_eq!(backend.reduce_corrections(&[10], &[1], &[(0, 5)], &[1], &[(0, 1)]), (vec![], vec![0]));
            assert_eq!((backend.output.columns.len(), backend.output.refs.len()), (1, 1));
        }
        assert_eq!(backend.reduce_corrections(&[10], &[1], &[(0, 3)], &[1], &[(0, 1)]), (vec![(0, -1), (1, 1)], vec![2]));
        assert_eq!((backend.output.columns.len(), backend.output.refs.len()), (2, 2));
        for _ in 0..64 {
            assert_eq!(backend.reduce_corrections(&[10], &[1], &[(0, 3)], &[1], &[(1, 1)]), (vec![], vec![0]));
            assert_eq!((backend.output.columns.len(), backend.output.refs.len()), (2, 2));
        }
        assert_eq!(rows_of(&backend.output.gather(&[0, 1])), vec![vec![5], vec![3]]);
    }

    /// corgi orders `(Int, List(Int))` rows as Rust orders `(i64, Vec<i64>)`: signed integers, and
    /// lists lexicographically, a prefix first.
    #[test]
    fn corgi_order_matches_rust_for_ragged_lists_and_signed_products() {
        let mut rows = Vec::new();
        for n in [-3, 0, 7] {
            for list in [vec![], vec![0], vec![1], vec![1, -1], vec![1, 0], vec![2], vec![2, -2, 0]] {
                rows.push((n, list));
            }
        }
        rows.reverse();
        let ends: Vec<usize> = rows.iter().scan(0, |end, (_, list)| { *end += list.len(); Some(*end) }).collect();
        let columns = CValue::Prod(vec![
            CValue::int(rows.iter().map(|r| r.0)),
            CValue::List(ends.into(), Box::new(CValue::int(rows.iter().flat_map(|r| r.1.iter().copied())))),
        ]);
        let (perm, _) = sort_blocks(&vec![0; rows.len()], &columns);
        let actual: Vec<_> = perm.into_iter().map(|i| rows[i].clone()).collect();
        rows.sort();
        assert_eq!(actual, rows);
    }

    /// Windows partition a retire: at any budget each window stops once it holds the budget, and
    /// together they present what one window would, which is what the chunks hold.
    #[test]
    fn windows_partition_a_retire() {
        use timely::progress::{Antichain, Timestamp};
        let prior = random_rows(0x2545_F491_4F6C_DD1D, 300, 40);
        let mut novel = random_rows(0x1234_5678, 60, 40);
        novel.extend(prior.iter().step_by(25).map(|(k, v, t, d)| (*k, *v, t.clone(), -d)));
        let output = random_rows(0x8765_4321, 80, 40);
        let lower = Antichain::from_elem(Time::minimum());
        for (key_shape, val_shape) in KEY_SHAPES.iter().flat_map(|k| VAL_SHAPES[..2].iter().map(move |v| (*k, *v))) {
            let batches = |rows, size| -> Vec<CBatch<Time>> {
                chunks(rows, size, key_shape, val_shape).into_iter().map(|chunk| Rc::new(ChunkBatch::new(vec![chunk]))).collect()
            };
            let (source, input, out) = (batches(&prior, 70), batches(&novel, 30), batches(&output, 50));
            // The windows are read as presented, so the backend is to complete no key itself.
            let instance = ReduceInstance { source_batches: &source, input_batches: &input, output_batches: &out, lower: lower.borrow(), present_all: true };
            let mut in_chunks = chunks_of(&source);
            in_chunks.extend(chunks_of(&input));
            // Changed keys, two of them held by no batch, and the input records each retire key holds.
            let mut changed = key_ids(&key_shape(&[3, 17, 41, 45]));
            changed.sort();
            let retire: BTreeSet<i64> = chunks_of(&input).iter().flat_map(|chunk| key_ids(chunk.keys())).chain(changed.iter().copied()).collect();
            let mut held = BTreeMap::new();
            for id in in_chunks.iter().flat_map(|chunk| key_ids(chunk.keys())).filter(|id| retire.contains(id)) {
                *held.entry(id).or_insert(0) += 1;
            }
            let run = |window_size| {
                let shape = (corgi::shape_of_value(&recover_key(&key_shape(&[0]))), corgi::shape_of_value(&val_shape(&[(0, 0)])));
                // Distinct reads its values whole, so the input pool holds what was presented.
                let mut backend = CorgiReduceBackend::<Time>::with_window(Reducer::Expr(crate::parse::Reducer::Distinct.lambda()), None, shape, window_size);
                backend.begin(Description::new(lower.clone(), Antichain::new(), lower.clone()));
                let (mut inputs, mut seeds, mut outputs) = (BTreeMap::new(), BTreeSet::new(), BTreeMap::new());
                let (mut window, mut from) = (ReduceWindow::default(), KeyPosition::Start);
                while from != KeyPosition::End {
                    let before = from;
                    window.clear();
                    backend.next_window(&instance, &changed, &mut from, &mut window);
                    assert!(from > before);
                    let within = |key: i64| before <= KeyPosition::At(key) && KeyPosition::At(key) < from;
                    let counts: Vec<usize> = held.iter().filter(|(key, _)| within(**key)).map(|(_, count)| *count).collect();
                    assert!(counts.iter().rev().skip(1).sum::<usize>() < window_size, "a window stops once it holds the budget");
                    assert!(from == KeyPosition::End || counts.iter().sum::<usize>() >= window_size, "a window holds the budget");
                    for (bridge, pool, into) in [(&window.input, &backend.input, &mut inputs), (&window.output, &backend.output, &mut outputs)] {
                        let values = rows_of(&pool.gather(&bridge.iter().map(|r| r.0.1).collect::<Vec<_>>()));
                        for (((key, _), time, diff), value) in bridge.iter().zip(values) {
                            assert!(within(*key));
                            assert!(into.insert((*key, value, time.clone()), *diff).is_none());
                        }
                    }
                    for (key, time) in window.seeds.iter() {
                        assert!(within(*key) && seeds.insert((*key, time.clone())));
                    }
                }
                (inputs, seeds, outputs)
            };
            let support: BTreeSet<_> = chunks_of(&input).iter()
                .flat_map(|chunk| key_ids(chunk.keys()).into_iter().enumerate().map(|(i, key)| (key, chunk.times().get(i))))
                .collect();
            assert!(run(usize::MAX) == (netted(&in_chunks, &retire), support, netted(&chunks_of(&out), &retire)));
            for window_size in [1, 3, 17] {
                assert!(run(window_size) == run(usize::MAX), "window_size={window_size}");
            }
        }
    }

    /// Real keys sharing an identifier are reduced apart. Over two retires — the second against the
    /// first's input and output — a forged identifier lane holding three real keys gives what the
    /// true hash lane gives, for each reducer, at every window size.
    #[test]
    fn colliding_keys_reduce_apart() {
        use timely::progress::{Antichain, Timestamp};
        use differential_dataflow::operators::int_proxy::reduce::ProxyReduceTactic;
        use differential_dataflow::operators::reduce::ReduceTactic;
        use crate::runtime::chunk::recover_key;
        use crate::runtime::feed::print;
        use corgi::Shape;

        let pairs = |rows: &[(u64, (u64, u64), Time, Diff)], pair: fn(&(u64, (u64, u64), Time, Diff)) -> (u64, u64)| {
            let (a, b): (Vec<u64>, Vec<u64>) = rows.iter().map(pair).unzip();
            CValue::Prod(vec![ints(a), ints(b)])
        };
        let shape = Shape::Prod(vec![Shape::Int, Shape::Int]);
        // Two retires' inputs: the first at outer times 0 and 1, the second from 2 on.
        let early = random_rows(0x51ED_270B_2A4F_3C11, 200, 6);
        let late: Vec<_> = random_rows(0x0BAD_5EED, 120, 6).into_iter()
            .map(|(k, v, t, d)| (k, v, Time::new(std::iter::once(t.first().copied().unwrap_or(0) + 2).chain(t.iter().skip(1).copied()).collect()), d)).collect();
        let early: Vec<_> = early.into_iter().filter(|r| r.2.first().copied().unwrap_or(0) < 2).collect();
        let stamp = |outer| crate::server::host_time(outer);

        // A lambda: the weighted sum of the values' first fields, once, and how many values, twice negated.
        let lambda = crate::ir::Lambda::new("let (_, vals) = input in \
            ((((0, vals) fold ((acc, ((a, _), d)) -> (acc, (a, d) mul) add)), 1) enlist, ((vals len), -2) enlist) append".into(), &corgi::Scope::default());
        // Each sugar with its limit, `min`'s splitting a colliding identifier's real keys first.
        use crate::parse::Reducer as Sugar;
        let sugars = [Sugar::Count, Sugar::Distinct, Sugar::Min, Sugar::Collect].map(|sugar| (Reducer::Expr(sugar.lambda()), sugar.limit()));
        for (reducer, limit) in sugars.into_iter().chain([(Reducer::Expr(lambda), None)]) {
            // Keys `(a, 7a)`, identified by their hash or by `a % 2`.
            let run = |forged: bool, window_size: usize| {
                let batch = |rows: &[(u64, (u64, u64), Time, Diff)]| -> CBatch<Time> {
                    let keys = pairs(rows, |r| (r.0, r.0 * 7));
                    let keys = if forged { CValue::Prod(vec![ints(rows.iter().map(|r| r.0 % 2).collect()), keys]) } else { present_key(keys) };
                    let vals = pairs(rows, |r| r.1);
                    let chunk = CorgiChunk::from_columns(keys, vals, rows.iter().map(|r| r.2.clone()).collect(), rows.iter().map(|r| r.3).collect());
                    Rc::new(ChunkBatch::new(vec![chunk]))
                };
                let mut tactic = ProxyReduceTactic::new(CorgiReduceBackend::<Time>::with_window(reducer.clone(), limit, (shape.clone(), shape.clone()), window_size));
                let held = Antichain::from_elem(Time::minimum());
                let (first, _) = tactic.retire(Vec::new(), Vec::new(), vec![batch(&early)], &held, &Antichain::from_elem(stamp(2)), &held);
                let first = first.and_then(|span| span.inner).into_iter().collect::<Vec<_>>();
                let (second, _) = tactic.retire(vec![batch(&early)], first.clone(), vec![batch(&late)], &Antichain::from_elem(stamp(2)), &Antichain::new(), &held);
                let mut netted = BTreeMap::new();
                for batch in first.iter().chain(second.and_then(|span| span.inner).as_ref()) {
                    for chunk in batch.chunks.iter() {
                        let keys = recover_key(chunk.keys());
                        for i in 0..chunk.diffs().len() {
                            *netted.entry((print(&keys, i), print(chunk.vals(), i), chunk.times().get(i))).or_insert(0) += chunk.diffs()[i];
                        }
                    }
                }
                netted.retain(|_, diff| *diff != 0);
                netted
            };
            let expected = run(false, usize::MAX);
            assert!(!expected.is_empty());
            for window_size in [1, 5, usize::MAX] {
                assert_eq!(run(true, window_size), expected, "{reducer:?}, window_size={window_size}");
            }
        }
    }
}
