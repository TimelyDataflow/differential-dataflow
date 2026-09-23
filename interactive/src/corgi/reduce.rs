//! The corgi `ProxyReduceBackend`: the value semantics for the DD `ProxyReduceTactic`.
//!
//! The tactic (differential's `operators::int_proxy::reduce`) owns ALL time/lattice logic over
//! integer proxies `(key_id, value_id, time, diff)`; this backend supplies only:
//!
//!   * ids — a key's id is the identifier its arrangement is sorted by (the key itself when it is a
//!     primitive integer, else its hash lane). A value's id names a row in a pool of columns: the
//!     chunks' own value columns, and the output columns this backend builds. Within a window, a
//!     key's equal input values share one id, found by corgi's segmented sort; output values are
//!     matched to the current output by structural comparison. Nothing hashes values.
//!   * the value callback — `reduce_brackets` runs ONE crossing per wave over every `(key, time)`
//!     bracket, building the output value COLUMNS directly (Count → a `u64` prim, Distinct → a
//!     `Unit`, Min → the chosen input rows, Collect → a `List`), never through DDIR rows.
//!   * materialize — gather the emitted keys and values from the pools' columns and seal a
//!     `CorgiChunk` batch column-natively.
//!
//! Min/Collect values use a segmented structural sort over an order-only columnar view: signed
//! integer leaves are swizzled, and lists become lexicographic ranks. The winning rows are still
//! gathered from the original columns.
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
use corgi::{ArithOp, Bounds, NumOp, OpLike, Value as CValue};

use crate::corgi::col_times::RowTime;
use crate::corgi::col_times::{ColTime, ColTimes};
use crate::corgi::search::matching_ranges;
use crate::corgi::chunk::{columns_to_batch, key_ids, key_lane, CorgiChunk};
use crate::ir::{Diff, Reducer};

type CBatch<T> = Rc<ChunkBatch<CorgiChunk<T, Diff>>>;

/// Build a sortable view matching DDIR's signed leaves and lexicographic lists.
///
/// DDIR's leaf scalar is `Int`, transcoded into a Corgi primitive as its raw
/// bits. Corgi's radix sort is unsigned, so XORing each payload leaf's sign bit
/// turns signed order into unsigned order. Sum discriminants remain untouched;
/// only their payload lanes recurse. This consumes freshly gathered candidate
/// columns, allowing Corgi to swizzle their buffers in place when unshared.
fn signed_order_view(value: CValue) -> CValue {
    match value {
        value @ CValue::Prim(_) => NumOp::from(ArithOp::ToSigned).eval(value).expect("ToSigned on a leaf"),
        CValue::Prod(fields) => {
            CValue::Prod(fields.into_iter().map(signed_order_view).collect())
        }
        CValue::Sum(tags, variants) => {
            // the lane assignment is untouched — only the payload lanes are swizzled.
            CValue::Sum(tags, variants.into_iter().map(signed_order_view).collect())
        }
        CValue::List(bounds, values) => lexicographic_list_ranks(bounds, signed_order_view(*values)),
        CValue::Unit(len) => CValue::Unit(len),
    }
}

/// An order-only integer rank for each list, using DDIR's lexicographic order.
/// Corgi's general structural order is intentionally length-first. Preserve
/// that contract and adapt here: rank the element columns, then refine tied
/// list prefixes in column batches, with end-of-list preceding every element.
/// No DDIR rows or per-comparison interpreter calls are materialized.
///
/// This is a correctness adapter, not a performance-neutral view: it eagerly
/// ranks all elements, including unused tails, and each prefix round scans all
/// lists and allocates fresh scratch even when most prefixes are resolved.
/// Cost therefore grows with total element count and unresolved prefix depth.
/// Only list-valued subcolumns pay this ranking cost (including strings encoded
/// as lists); physical arrangement-key ordering is unchanged.
fn lexicographic_list_ranks(bounds: Bounds, ordered_elements: CValue) -> CValue {
    let ends = bounds.to_vec();
    let rows = ends.len();
    let (element_perm, element_labels) = sort_blocks(&vec![0; ordered_elements.len()], &ordered_elements);
    let mut element_ranks = vec![0; element_perm.len()];
    for (i, &row) in element_perm.iter().enumerate() { element_ranks[row] = element_labels[i]; }
    let starts: Vec<_> = std::iter::once(0).chain(ends.iter().copied()).take(rows).collect();
    let mut perm: Vec<_> = (0..rows).collect();
    let mut labels = vec![0; rows];
    let mut position = 0;
    loop {
        let mut present = vec![0; rows];
        let mut keys = vec![0; rows];
        let mut active = false;
        for i in 0..rows {
            let tied = (i > 0 && labels[i] == labels[i-1]) || (i+1 < rows && labels[i] == labels[i+1]);
            let row = perm[i];
            if tied && position < ends[row]-starts[row] {
                present[i] = 1;
                keys[i] = element_ranks[starts[row]+position];
                active = true;
            }
        }
        if !active { break; }
        let (order, refined) = sort_blocks(&labels, &CValue::Prod(vec![CValue::u64(present), CValue::u64(keys)]));
        perm = order.into_iter().map(|i| perm[i]).collect();
        labels = refined;
        position += 1;
    }
    let mut ranks = vec![0; rows];
    for (i, &row) in perm.iter().enumerate() { ranks[row] = labels[i]; }
    CValue::u64(ranks)
}

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

/// A corgi reduce backend for a single `Reducer`.
///
/// Each retire goes: `begin`; then per window, `next_window` presents a run of keys' input and
/// output as integer records, `reduce_corrections` computes the output each key wants and how it
/// differs from what it has, and `emit` records the difference; finally `finish` builds the batch.
///
/// The tactic reasons in `I`: the column's own time `T`, or a fixed-width representation of it.
pub struct CorgiReduceBackend<T, I = T> {
    reducer: Reducer,
    /// The current window's input values: its chunks' value columns.
    input: Pool,
    /// The current retire's output values: its output chunks' value columns, then those built here.
    output: Pool,
    /// The current retire's input then output chunks' key columns.
    key_columns: Vec<CValue>,
    /// The output emitted so far: key rows in `key_columns`, value ids, times, and diffs.
    rows: (Vec<(usize, usize)>, Vec<u64>, ColTimes<T>, Vec<Diff>),
    /// Input records per window, bounding what the presentations cost at once.
    window_size: usize,
    /// The retire in progress.
    retire: Retire,
    _time: std::marker::PhantomData<I>,
}

/// A retire's keys and where its input chunks hold them, found once by its first window.
#[derive(Default)]
struct Retire {
    /// The retire's keys, ascending.
    keys: Vec<u64>,
    /// The input records each key holds, novel and prior.
    held: Vec<usize>,
    /// Where the input chunks, prior then novel, hold the keys.
    input: Matches,
    /// The current window, as a range of `keys`.
    window: Range<usize>,
    /// A row of `key_columns` holding each key of the window.
    key_rows: Vec<Option<(usize, usize)>>,
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

impl<T, I> CorgiReduceBackend<T, I> {
    /// A backend covering the key space in windows of `1 << 12` input records.
    pub fn new(reducer: Reducer) -> Self { Self::with_window(reducer, 1 << 12) }

    /// A backend with an explicit window budget, in presented input records.
    pub fn with_window(reducer: Reducer, window_size: usize) -> Self {
        CorgiReduceBackend {
            reducer,
            input: Pool::default(),
            output: Pool::default(),
            key_columns: Vec::new(),
            rows: (Vec::new(), Vec::new(), ColTimes::default(), Vec::new()),
            window_size: window_size.max(1),
            retire: Retire::default(),
            _time: std::marker::PhantomData,
        }
    }
}

/// Where `chunks` hold the ascending `keys`.
///
/// Both the keys and stored identifier lane are sorted; [`matching_ranges`] chooses between
/// merging them and searching the keys in lockstep.
fn search<T: ColTime>(chunks: &[&CorgiChunk<T, Diff>], keys: &[u64]) -> Matches {
    let mut scratch = Vec::new();
    chunks.iter().map(|chunk| {
        let mut found = Vec::new();
        if chunk.diffs().is_empty() { return found; }
        let lane = corgi::arrange::leaf_slice(key_lane(chunk.keys())).expect("the identifier lane is a u64 leaf");
        matching_ranges(keys, lane, &mut scratch, &mut found);
        found
    }).collect()
}

/// Present the records `chunks` hold for `keys`, at `matches`, into `bridge`, sorted by
/// `((key, id), time)` and consolidated.
///
/// Each key's rows are sorted by value, and each run of equal values gets one id: the next index
/// of `refs`, where it is recorded as the `(chunk, row)` of one of its rows.
fn present<T: ColTime + Ord, I: RowTime<T>>(
    chunks: &[&CorgiChunk<T, Diff>],
    keys: &[u64],
    matches: &Matches,
    refs: &mut Vec<(usize, usize)>,
    bridge: &mut ProxyBridge<I, Diff>,
) {
    // The matched `(chunk, row)`s, key by key, and each one's key index.
    let mut runs: Vec<_> = matches.iter().enumerate()
        .flat_map(|(chunk, found)| found.iter().map(move |(key, rows)| (*key, chunk, rows.clone())))
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
    let vals: Vec<CValue> = chunks.iter().map(|chunk| chunk.vals().clone()).collect();
    let (sorted, groups) = sort_blocks(&labels, &gather_refs(&vals, rows.iter().copied()));
    let update = |(chunk, row): (usize, usize)| (I::read(chunks[chunk].times(), row), chunks[chunk].diffs()[row]);
    let (mut start, mut updates) = (0, Vec::new());
    for group in groups.chunk_by(|a, b| a == b) {
        let members = &sorted[start..start + group.len()];
        start += group.len();
        updates.extend(members.iter().map(|&i| update(rows[i])));
        consolidate(&mut updates);
        if !updates.is_empty() {
            let (key, id) = (keys[labels[members[0]] as usize], refs.len() as u64);
            refs.push(rows[members[0]]);
            bridge.extend(updates.drain(..).map(|(time, diff)| ((key, id), time, diff)));
        }
    }
}

fn chunks_of<T>(batches: &[CBatch<T>]) -> Vec<&CorgiChunk<T, Diff>>
where
    T: ColTime,
{
    batches.iter().flat_map(|b| b.chunks.iter()).collect()
}

/// A retire's keys, ascending: those the novel batches touch, and the `changed` set the harness
/// supplies.
fn retire_keys<T: ColTime>(novel_chunks: &[&CorgiChunk<T, Diff>], changed: &[u64]) -> Vec<u64> {
    let mut keys: Vec<u64> = novel_chunks.iter().flat_map(|chunk| key_ids(chunk.keys())).chain(changed.iter().copied()).collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

impl<T, I> CorgiReduceBackend<T, I>
where
    T: ColTime + Ord,
{
    /// The one value crossing for a wave: every `(key, time)` bracket at once. Builds a column of the
    /// desired output values, one row each, and returns it with each bracket's end among its rows.
    /// `input[k] = (value_id, accumulated diff)`; bracket `i` is `input[ends[i-1]..ends[i]]`, non-empty.
    fn reduce_brackets(&mut self, ends: &[usize], input: &[(u64, Diff)]) -> (CValue, Vec<usize>) {
        let mut out_ends: Vec<usize> = Vec::with_capacity(ends.len());
        match self.reducer {
            Reducer::Count => {
                // Per-bracket sum of diffs; survivors become a `Tuple([Int(sum)])` = corgi `Prod([u64])`.
                let mut sums: Vec<u64> = Vec::new();
                let mut start = 0;
                for &end in ends {
                    let c: Diff = input[start..end].iter().map(|&(_, d)| d).sum();
                    if c > 0 {
                        sums.push(c as u64);
                    }
                    out_ends.push(sums.len());
                    start = end;
                }
                (CValue::Prod(vec![CValue::u64(sums)]), out_ends)
            }
            Reducer::Distinct => {
                // Present iff any value has NON-ZERO net -- the sign does not matter. DD's `reduce`
                // presents every value whose accumulation is non-zero, negatives included, and
                // `backend::vec`'s Distinct then emits `1` without looking at the diffs at all. A
                // `> 0` test here silently drops a key whose values all accumulate negative, which
                // is exactly what a negated collection produces. Output value is unit (a `Unit` column).
                let mut present = 0usize;
                let mut start = 0;
                for &end in ends {
                    if input[start..end].iter().any(|&(_, d)| d != 0) {
                        present += 1;
                    }
                    out_ends.push(present);
                    start = end;
                }
                (CValue::Unit(present), out_ends)
            }
            Reducer::Min => {
                // The structural minimum over values with NON-ZERO net. The sign does not select
                // candidates: DD presents every non-zero accumulation. Filtering to `> 0` here both
                // drops all-negative keys and can pick a different minimum when a bracket mixes signs.
                // Gather all candidates across brackets into one column, segment by
                // bracket, and one corgi `sort_blocks` gives every bracket's argmin at once
                // (`perm[block_start]`). The winning ROW is taken columnar.
                let mut cand_reps: Vec<u64> = Vec::new(); // input value ID per candidate
                let mut labels: Vec<u64> = Vec::new(); // dense segment id per candidate
                let mut block_starts: Vec<usize> = Vec::new(); // per emitted bracket: start offset in cand_reps
                let mut start = 0;
                for &end in ends {
                    let lo = cand_reps.len();
                    let seg = block_starts.len() as u64;
                    for k in start..end {
                        if input[k].1 != 0 {
                            cand_reps.push(input[k].0);
                            labels.push(seg);
                        }
                    }
                    if cand_reps.len() > lo {
                        block_starts.push(lo);
                    }
                    out_ends.push(block_starts.len());
                    start = end;
                }
                if cand_reps.is_empty() {
                    return (CValue::Unit(0), out_ends);
                }
                let candidates = self.input.gather(&cand_reps);
                // Integer values need no sort: scan each block for its least as signed.
                let min_reps: Vec<u64> = if let Some(values) = corgi::arrange::leaf_slice(&candidates) {
                    let block_ends = block_starts.iter().skip(1).copied().chain([cand_reps.len()]);
                    block_starts.iter().zip(block_ends).map(|(&lo, hi)| {
                        cand_reps[(lo..hi).min_by_key(|&k| values[k] as i64).expect("blocks are non-empty")]
                    }).collect()
                } else {
                    let (perm, _) = sort_blocks(&labels, &signed_order_view(candidates));
                    block_starts.iter().map(|&lo| cand_reps[perm[lo]]).collect()
                };
                (self.input.gather(&min_reps), out_ends)
            }
            Reducer::Collect => {
                // One row per bracket: the values sorted in DDIR observable order,
                // each repeated by its diff, as a `List`. One `sort_blocks` orders every bracket's
                // entries at once; element rows are then taken columnar. A bracket emits iff some
                // value has NON-ZERO net (as Distinct/Min: DD invokes the reducer only for a key
                // with input, and the row reducer then lists the positive copies — an empty list
                // when every net is negative). A bracket whose values all cancelled is a key with
                // no input: it must emit nothing, or a retracted key keeps a stale (empty) list.
                let mut entry_reps: Vec<u64> = Vec::new();
                let mut entry_diffs: Vec<Diff> = Vec::new();
                let mut labels: Vec<u64> = Vec::new();
                let mut blocks: Vec<(usize, usize)> = Vec::with_capacity(ends.len());
                let mut start = 0;
                for (bi, &end) in ends.iter().enumerate() {
                    if input[start..end].iter().any(|&(_, d)| d != 0) {
                        let lo = entry_reps.len();
                        for k in start..end {
                            entry_reps.push(input[k].0);
                            entry_diffs.push(input[k].1);
                            labels.push(bi as u64);
                        }
                        blocks.push((lo, entry_reps.len()));
                    }
                    out_ends.push(blocks.len());
                    start = end;
                }
                if blocks.is_empty() {
                    return (CValue::Unit(0), out_ends);
                }
                let perm = sort_blocks(&labels, &signed_order_view(self.input.gather(&entry_reps))).0;
                // Expand each bracket's sorted entries by their diff (max(0, ·) copies).
                let mut elem_reps: Vec<u64> = Vec::new();
                let mut bracket_ends: Vec<usize> = Vec::with_capacity(ends.len());
                for (lo, hi) in blocks {
                    for &e in &perm[lo..hi] {
                        for _ in 0..entry_diffs[e].max(0) {
                            elem_reps.push(entry_reps[e]);
                        }
                    }
                    bracket_ends.push(elem_reps.len());
                }
                // A window whose lists are all empty still has an element SHAPE — the input
                // values' — and the column must carry it, or this batch's `List<()>` meets the
                // next batch's `List<T>` where the two are concatenated. `gather` at no indices
                // is the empty column of that shape.
                let elems = self.input.gather(&elem_reps);
                (CValue::List(Bounds::offsets(bracket_ends), Box::new(elems)), out_ends)
            }
        }
    }
}

impl<T, I> ProxyReduceBackend<T, CBatch<T>, CBatch<T>> for CorgiReduceBackend<T, I>
where
    T: ColTime + Ord,
    I: RowTime<T>,
{
    type Time = I;
    type Key = u64;
    type VIn = u64;
    type VOut = u64;
    type RIn = Diff;
    type ROut = Diff;

    fn begin(&mut self, _description: Description<T>) {
        self.rows = (Vec::new(), Vec::new(), ColTimes::default(), Vec::new());
    }

    fn next_window(&mut self, instance: &ReduceInstance<'_, T, CBatch<T>, CBatch<T>>, changed: &[u64], from: &mut KeyPosition<u64>, window: &mut ReduceWindow<I, Diff, Diff>) {
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
            self.retire = Retire { keys, held, input, window: 0..0, key_rows: Vec::new() };
            self.input.columns = in_chunks.iter().map(|chunk| chunk.vals().clone()).collect();
            self.output = Pool { columns: out_chunks.iter().map(|chunk| chunk.vals().clone()).collect(), refs: Vec::new() };
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
        // A row holding each key: its first in the input chunks, else its first in the output chunks.
        retire.key_rows = vec![None; keys.len()];
        for (base, found) in [(0, &matches), (in_chunks.len(), &out_matches)] {
            for (chunk, list) in found.iter().enumerate() {
                for (index, rows) in list { retire.key_rows[*index].get_or_insert((base + chunk, rows.start)); }
            }
        }

        // The seeds are the novel batches' RAW (key, time) support, recorded here — before the
        // merged presentation below, whose consolidation may net a novel record away entirely.
        window.seeds.reserve(matches[prior..].iter().flatten().map(|(_, rows)| rows.len()).sum());
        for (chunk, found) in in_chunks[prior..].iter().zip(&matches[prior..]) {
            for (index, rows) in found {
                window.seeds.extend(rows.clone().map(|row| (keys[*index], I::read(chunk.times(), row))));
            }
        }
        window.seeds.sort_unstable();
        window.seeds.dedup();

        // ONE merged input presentation: novel and prior together, netted by the consolidation —
        // equal values share an id, so an exactly cancelling pair vanishes here, and its time
        // survives in `window.seeds` above. Input ids name rows for this window only.
        self.input.refs.clear();
        present(&in_chunks, keys, &matches, &mut self.input.refs, &mut window.input);
        // The output history, same keys. Output ids name rows for the whole retire.
        present(&out_chunks, keys, &out_matches, &mut self.output.refs, &mut window.output);

        *from = retire.keys.get(retire.window.end).map_or(KeyPosition::End, |key| KeyPosition::At(*key));
    }

    fn reduce_corrections(&mut self, keys: &[u64], in_ends: &[usize], input: &[(u64, Diff)], out_ends: &[usize], output: &[(u64, Diff)]) -> (Vec<(u64, Diff)>, Vec<usize>) {
        // Each key wants its rows of `desired`, once each, and has `(id, diff)`s of output. The
        // correction nets the two by value: a wanted row equal to an output id's value counts
        // toward that id, and one equal to none gets a new id. Keys have a handful of each.
        let (desired, desired_ends) = self.reduce_brackets(in_ends, input);
        let fresh = self.output.columns.len();
        if !desired.is_empty() { self.output.columns.push(desired); }
        let Pool { columns, refs } = &mut self.output;
        let (mut corr, mut corr_ends) = (Vec::new(), Vec::with_capacity(keys.len()));
        let (mut ds, mut os) = (0, 0);
        for (&de, &oe) in desired_ends.iter().zip(out_ends) {
            let mut net: Vec<(u64, Diff)> = output[os..oe].iter().map(|&(id, d)| (id, -d)).collect();
            for row in ds..de {
                let equal = |&(id, _): &(u64, Diff)| {
                    let (column, r) = refs[id as usize];
                    compare_at(&columns[fresh], row, &columns[column], r).is_eq()
                };
                match net.iter().position(equal) {
                    Some(at) => net[at].1 += 1,
                    None => { net.push((refs.len() as u64, 1)); refs.push((fresh, row)); }
                }
            }
            corr.extend(net.into_iter().filter(|&(_, d)| d != 0));
            corr_ends.push(corr.len());
            (ds, os) = (de, oe);
        }
        (corr, corr_ends)
    }

    fn emit(&mut self, records: &[((u64, u64), I, Diff)]) {
        // Every emitted key is in the current window, and was presented there.
        let keys = &self.retire.keys[self.retire.window.clone()];
        let (key_rows, ids, times, diffs) = &mut self.rows;
        for ((key, id), time, diff) in records {
            let index = keys.binary_search(key).expect("emitted keys are in the window");
            key_rows.push(self.retire.key_rows[index].expect("emitted keys were presented"));
            ids.push(*id);
            time.write(times);
            diffs.push(*diff);
        }
    }

    fn finish(&mut self) -> Option<CBatch<T>> {
        let (key_rows, ids, times, diffs) = std::mem::take(&mut self.rows);
        if times.is_empty() { return None; }
        let keys = gather_refs(&self.key_columns, key_rows.into_iter());
        Some(Rc::new(columns_to_batch(keys, self.output.gather(&ids), times, diffs)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use differential_dataflow::dynamic::pointstamp::PointStamp;
    use crate::corgi::chunk::present_key;
    use crate::ir::Time;

    /// The rows of a column of `u64` leaves, products of them, or units, as vectors.
    fn rows_of(col: &CValue) -> Vec<Vec<u64>> {
        let fields: Vec<&[u64]> = match col {
            CValue::Unit(_) => Vec::new(),
            CValue::Prod(fields) => fields.iter().map(|field| corgi::arrange::leaf_slice(field).unwrap()).collect(),
            leaf => vec![corgi::arrange::leaf_slice(leaf).unwrap()],
        };
        (0..col.len()).map(|i| fields.iter().map(|field| field[i]).collect()).collect()
    }

    /// Random `(key, value, time, diff)` rows over few keys and values, so that they collide.
    fn random_rows(seed: u64, count: usize, keys: u64) -> Vec<(u64, (u64, u64), Time, Diff)> {
        let stamp = |outer, coords: &[u64]| Time::new(outer, PointStamp::new(coords.iter().copied().collect()));
        let times = [stamp(0, &[]), stamp(1, &[2, 3]), stamp(2, &[1, 4]), stamp(3, &[2])];
        let mut state = seed;
        let mut next = |n: u64| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) % n
        };
        (0..count).map(|_| (next(keys), (next(3), next(3)), times[next(4) as usize].clone(), [1, -1, 2][next(3) as usize])).collect()
    }

    const KEY_SHAPES: [fn(&[u64]) -> CValue; 2] = [
        |k| CValue::u64(k.to_vec()),
        |k| present_key(CValue::Prod(vec![CValue::u64(k.to_vec()), CValue::u64(k.iter().map(|k| k * 7).collect())])),
    ];
    const VAL_SHAPES: [fn(&[(u64, u64)]) -> CValue; 3] = [
        |v| CValue::u64(v.iter().map(|v| v.0).collect()),
        |v| CValue::Prod(vec![CValue::u64(v.iter().map(|v| v.0).collect()), CValue::u64(v.iter().map(|v| v.1).collect())]),
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
    fn netted(chunks: &[&CorgiChunk<Time, Diff>], keys: &BTreeSet<u64>) -> BTreeMap<(u64, Vec<u64>, Time), Diff> {
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
                let (mut refs, mut bridge) = (Vec::new(), ProxyBridge::<Time, Diff>::new());
                present(&chunks, &keys, &search(&chunks, &keys), &mut refs, &mut bridge);
                assert!(bridge.windows(2).all(|w| (w[0].0, &w[0].1) < (w[1].0, &w[1].1)));
                // The same presentation in a fixed-width representation.
                let mut flat = ProxyBridge::<crate::corgi::flat::Flat<3>, Diff>::new();
                present(&chunks, &keys, &search(&chunks, &keys), &mut Vec::new(), &mut flat);
                assert_eq!(flat.into_iter().map(|(ids, time, diff)| (ids, Time::from(time), diff)).collect::<Vec<_>>(), bridge);
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
        let mut backend = CorgiReduceBackend::<Time>::new(Reducer::Count);
        backend.output = Pool { columns: vec![CValue::Prod(vec![CValue::u64(vec![5, 5, 7])])], refs: vec![(0, 0), (0, 1), (0, 2)] };
        // Key 0 counts 5 and has output 5 twice; key 1 counts 3 and has output 7; key 2 counts 0.
        let input = [(0, 2), (1, 3), (0, 3), (0, 1), (1, -1)];
        let output = [(0, 1), (1, 1), (2, 1)];
        let (corr, ends) = backend.reduce_corrections(&[10, 11, 12], &[2, 3, 5], &input, &[2, 3, 3], &output);
        assert_eq!((corr.clone(), ends), (vec![(1, -1), (2, -1), (3, 1)], vec![1, 3, 3]));
        assert_eq!(rows_of(&backend.output.gather(&[3])), vec![vec![3]]);
    }

    #[test]
    fn order_view_matches_ddir_for_ragged_lists_and_signed_products() {
        use crate::ir::Value as V;
        use crate::corgi::logic::{transcode, shape_of_row};
        let mut rows = Vec::new();
        for n in [-3, 0, 7] {
            for bytes in [vec![], vec![0], vec![1], vec![1, -1], vec![1, 0], vec![2], vec![2, -2, 0]] {
                rows.push(V::Tuple(vec![V::Int(n), V::List(bytes.into_iter().map(V::Int).collect())]));
            }
        }
        rows.reverse();
        // Infer from a non-empty representative; the first row need not carry
        // data for every nested-list position once the schema is declared.
        let shape = shape_of_row(&rows[0]).unwrap();
        let columns = transcode(&rows, &shape);
        let (perm, _) = sort_blocks(&vec![0; rows.len()], &signed_order_view(columns));
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
            let instance = ReduceInstance { source_batches: &source, input_batches: &input, output_batches: &out, lower: lower.borrow() };
            let mut in_chunks = chunks_of(&source);
            in_chunks.extend(chunks_of(&input));
            // Changed keys, two of them held by no batch, and the input records each retire key holds.
            let mut changed = key_ids(&key_shape(&[3, 17, 41, 45]));
            changed.sort();
            let retire: BTreeSet<u64> = chunks_of(&input).iter().flat_map(|chunk| key_ids(chunk.keys())).chain(changed.iter().copied()).collect();
            let mut held = BTreeMap::new();
            for id in in_chunks.iter().flat_map(|chunk| key_ids(chunk.keys())).filter(|id| retire.contains(id)) {
                *held.entry(id).or_insert(0) += 1;
            }
            let run = |window_size| {
                let mut backend = CorgiReduceBackend::<Time>::with_window(Reducer::Count, window_size);
                backend.begin(Description::new(lower.clone(), Antichain::new(), lower.clone()));
                let (mut inputs, mut seeds, mut outputs) = (BTreeMap::new(), BTreeSet::new(), BTreeMap::new());
                let (mut window, mut from) = (ReduceWindow::default(), KeyPosition::Start);
                while from != KeyPosition::End {
                    let before = from;
                    window.clear();
                    backend.next_window(&instance, &changed, &mut from, &mut window);
                    assert!(from > before);
                    let within = |key: u64| before <= KeyPosition::At(key) && KeyPosition::At(key) < from;
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
}
