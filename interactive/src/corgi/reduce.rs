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
//!   * collisions — a hashed key's identifier may hold several real keys. Chunks sort each
//!     identifier's rows by real key, so a window finds them with two comparisons per run. Their
//!     ids are then per real key, input and output alike: a colliding bracket is split by real key
//!     before it is reduced, its output is matched by key as well as value, and every output id
//!     records the row holding its key, which is what `emit` reads.
//!   * the value callback — `reduce_brackets` runs ONE crossing per wave over every `(key, time)`
//!     bracket, building the output value COLUMNS directly (Count → an `Int` column, Distinct → a
//!     `Unit`, Min → the chosen input rows, Collect → a `List`, a lambda → its graph's output over
//!     every bracket's key and `(val, diff)` list), never through DDIR rows.
//!   * materialize — gather the emitted keys and values from the pools' columns and seal a
//!     `CorgiChunk` batch column-natively.
//!
//! Presentation assigns value ids in structural order (signed integers, lexicographic lists).
//! Min/Collect reuse that order rather than sorting the candidate columns a second time.
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

use corgi::arrange::{compare_at, equal_idx, gather_lanes, sort_blocks};
use corgi::{Bounds, Graph, NumOp, Shape, Value as CValue};

use crate::corgi::col_times::{ColTime, ColTimes};
use crate::corgi::search::matching_ranges;
use crate::corgi::chunk::{colliding_keys, columns_to_batch, ident, key_ids, key_is_hashed, recover_key, recover_key_ref, CorgiChunk};
use crate::corgi::logic::compile_reduce;
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

/// A corgi reduce backend for a single `Reducer`.
///
/// Each retire goes: `begin`; then per window, `next_window` presents a run of keys' input and
/// output as integer records, `reduce_corrections` computes the output each key wants and how it
/// differs from what it has, and `emit` records the difference; finally `finish` builds the batch.
pub struct CorgiReduceBackend<T> {
    reducer: Reducer,
    /// A `Reducer::Expr`'s graph, compiled at the key and value shapes it reads.
    graph: Option<(Shape, Shape, Graph<NumOp>)>,
    /// The current window's input values: its chunks' value columns.
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
    /// Input records per window, bounding what the presentations cost at once.
    window_size: usize,
    /// The retire in progress.
    retire: Retire,
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
    pub fn new(reducer: Reducer, shape: (Shape, Shape)) -> Self { Self::with_window(reducer, shape, 1 << 12) }

    /// A backend with an explicit window budget, in presented input records. A `Reducer::Expr`
    /// compiles here, at `shape`; a type error is a panic with corgi's message, which install's
    /// type check keeps from being reached.
    pub fn with_window(reducer: Reducer, (kshape, vshape): (Shape, Shape), window_size: usize) -> Self {
        let graph = match &reducer {
            Reducer::Expr(lambda) => {
                let g = compile_reduce(lambda, &kshape, &vshape)
                    .unwrap_or_else(|e| panic!("reduce: type error at shapes ({kshape}, {vshape}): {e}"));
                Some((kshape, vshape, g))
            }
            _ => None,
        };
        CorgiReduceBackend {
            reducer,
            graph,
            input: Pool::default(),
            output: Pool::default(),
            output_keys: Vec::new(),
            key_columns: Vec::new(),
            rows: (Vec::new(), Vec::new(), ColTimes::default(), Vec::new()),
            window_size: window_size.max(1),
            retire: Retire::default(),
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
fn present<T: ColTime + Ord>(
    chunks: &[&CorgiChunk<T, Diff>],
    keys: &[i64],
    matches: &Matches,
    colliding: &[bool],
    refs: &mut Vec<(usize, usize)>,
    bridge: &mut ProxyBridge<T, Diff, i64>,
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
    // Each label's key index; the identity unless real keys split some labels.
    let mut label_keys = None;
    if colliding.contains(&true) {
        (labels, rows, label_keys) = split_real_keys(chunks, colliding, labels, rows);
    }
    let key_of = |label: u64| label_keys.as_ref().map_or(label as usize, |keys: &Vec<usize>| keys[label as usize]);
    let vals: Vec<CValue> = chunks.iter().map(|chunk| chunk.vals().clone()).collect();
    let (sorted, groups) = sort_blocks(&labels, &gather_refs(&vals, rows.iter().copied()));
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

/// A retire's keys, ascending: those the novel batches touch, and the `changed` set the harness
/// supplies.
fn retire_keys<T: ColTime>(novel_chunks: &[&CorgiChunk<T, Diff>], changed: &[i64]) -> Vec<i64> {
    let mut keys: Vec<i64> = novel_chunks.iter().flat_map(|chunk| key_ids(chunk.keys())).chain(changed.iter().copied()).collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

impl<T> CorgiReduceBackend<T>
where
    T: ColTime + Ord,
{
    /// The one value crossing for a wave: every `(key, time)` bracket at once. Builds a column of the
    /// desired output values, one row each, and returns it with each bracket's end among its rows.
    /// `input[k] = (value_id, accumulated diff)`; bracket `i` is `input[ends[i-1]..ends[i]]`, non-empty.
    /// As with `reduce_lambda`, the harness consolidates each bracket in ascending value-id order.
    fn reduce_brackets(&mut self, ends: &[usize], input: &[(u64, Diff)]) -> (CValue, Vec<usize>) {
        let mut out_ends: Vec<usize> = Vec::with_capacity(ends.len());
        match self.reducer {
            Reducer::Count => {
                // Per-bracket sum of diffs, of either sign, as DD's `count`; a non-zero sum becomes an
                // `Int`.
                let mut sums: Vec<i64> = Vec::new();
                let mut start = 0;
                for &end in ends {
                    let c: Diff = input[start..end].iter().map(|&(_, d)| d).sum();
                    if c != 0 {
                        sums.push(c);
                    }
                    out_ends.push(sums.len());
                    start = end;
                }
                (CValue::i64(sums), out_ends)
            }
            Reducer::Distinct => {
                // Present iff any value has NON-ZERO net -- the sign does not matter. DD's `reduce`
                // presents every value whose accumulation is non-zero, negatives included. A
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
                // Presentation has already ordered each real key's values. Its first non-zero
                // value id is the minimum; gather just the winners, across all brackets at once.
                let mut min_reps = Vec::with_capacity(ends.len());
                let mut start = 0;
                for &end in ends {
                    if let Some(&(id, _)) = input[start..end].iter().find(|&&(_, diff)| diff != 0) {
                        min_reps.push(id);
                    }
                    out_ends.push(min_reps.len());
                    start = end;
                }
                if min_reps.is_empty() {
                    return (CValue::Unit(0), out_ends);
                }
                (self.input.gather(&min_reps), out_ends)
            }
            Reducer::Expr(_) => unreachable!("a lambda's brackets are `reduce_lambda`'s"),
            Reducer::Collect => {
                // One row per bracket: the values sorted in DDIR observable order,
                // each repeated by its diff, as a `List`. Ascending ids already give that order;
                // element rows are taken columnar. A bracket emits iff some
                // value has NON-ZERO net (as Distinct/Min: DD invokes the reducer only for a key
                // with input, and the row reducer then lists the positive copies — an empty list
                // when every net is negative). A bracket whose values all cancelled is a key with
                // no input: it must emit nothing, or a retracted key keeps a stale (empty) list.
                let mut elem_reps = Vec::new();
                let mut bracket_ends = Vec::with_capacity(ends.len());
                let mut start = 0;
                for &end in ends {
                    if input[start..end].iter().any(|&(_, d)| d != 0) {
                        for &(id, diff) in &input[start..end] {
                            elem_reps.extend(std::iter::repeat_n(id, diff.max(0) as usize));
                        }
                        bracket_ends.push(elem_reps.len());
                    }
                    out_ends.push(bracket_ends.len());
                    start = end;
                }
                if bracket_ends.is_empty() {
                    return (CValue::Unit(0), out_ends);
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

    /// As [`Self::reduce_brackets`] for `Reducer::Expr`, whose output rows carry diffs: every
    /// bracket's key and its values, as a list of `(val, diff)`, through the lambda's graph at once.
    /// Returns the output values, their diffs, and each bracket's end among them.
    ///
    /// The harness presents each bracket consolidated, so its diffs are non-zero and its value ids
    /// ascend, which is value order: `present` numbers each key's values in sorted order. The
    /// brackets are presented as they are. One with no input has no output, and the lambda does not
    /// see it.
    fn reduce_lambda(&self, ends: &[usize], input: &[(u64, Diff)]) -> (CValue, Vec<Diff>, Vec<usize>) {
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
        let vals = self.input.gather(&input.iter().map(|&(id, _)| id).collect::<Vec<_>>());
        let diffs = CValue::i64(input.iter().map(|&(_, diff)| diff).collect());
        if cfg!(debug_assertions) {
            let mut start = 0;
            for &(_, end) in &blocks {
                for i in start + 1..end {
                    assert!(compare_at(&vals, i - 1, &vals, i).is_lt(), "reduce: a bracket's values are not strictly ascending");
                }
                start = end;
            }
            assert!(input.iter().all(|&(_, diff)| diff != 0), "reduce: a bracket holds a zero diff");
        }
        let keys = recover_key(&gather_refs(&self.key_columns, key_refs.into_iter()));
        let (kshape, vshape) = (corgi::shape_of_value(&keys), corgi::shape_of_value(&vals));
        let lists = CValue::List(Bounds::offsets(blocks.iter().map(|b| b.1).collect()), Box::new(CValue::Prod(vec![vals, diffs])));

        let (k, v, graph) = self.graph.as_ref().expect("an expression reducer compiles at construction");
        assert!(*k == kshape && *v == vshape, "reduce: input of shape ({kshape}, {vshape}) reached an operator compiled at ({k}, {v})");
        let (bounds, elems) = corgi::eval_graph(graph, CValue::Prod(vec![keys, lists])).into_list("reduce output").unwrap();
        let mut fields = elems.into_prod("reduce output").unwrap();
        let out_diffs = fields.pop().unwrap().into_i64("reduce output diffs").unwrap().to_vec();
        let out_vals = fields.pop().unwrap();

        // Each bracket's end among the output rows; brackets without input add none.
        let list_ends = bounds.to_vec();
        let (mut out_ends, mut blocks, mut emitted) = (Vec::with_capacity(ends.len()), blocks.iter().zip(list_ends).peekable(), 0);
        for bi in 0..ends.len() {
            if let Some((_, end)) = blocks.next_if(|((b, _), _)| *b == bi) { emitted = end; }
            out_ends.push(emitted);
        }
        (out_vals, out_diffs, out_ends)
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

    fn begin(&mut self, _description: Description<T>) {
        self.rows = (Vec::new(), Vec::new(), ColTimes::default(), Vec::new());
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
            self.retire = Retire { keys, held, input, window: 0..0, colliding: Vec::new() };
            self.input.columns = in_chunks.iter().map(|chunk| chunk.vals().clone()).collect();
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

        // The seeds are the novel batches' RAW (key, time) support, recorded here — before the
        // merged presentation below, whose consolidation may net a novel record away entirely.
        window.seeds.reserve(matches[prior..].iter().flatten().map(|(_, rows)| rows.len()).sum());
        for (chunk, found) in in_chunks[prior..].iter().zip(&matches[prior..]) {
            for (index, rows) in found {
                window.seeds.extend(rows.clone().map(|row| (keys[*index], chunk.times().get(row))));
            }
        }
        window.seeds.sort_unstable();
        window.seeds.dedup();

        // ONE merged input presentation: novel and prior together, netted by the consolidation —
        // equal values share an id, so an exactly cancelling pair vanishes here, and its time
        // survives in `window.seeds` above. Input ids name rows for this window only.
        self.input.refs.clear();
        present(&in_chunks, keys, &matches, colliding, &mut self.input.refs, &mut window.input);
        // The output history, same keys. Output ids name rows for the whole retire, and the rows
        // that hold their keys are at the same positions of the output chunks' key columns.
        let fresh = self.output.refs.len();
        present(&out_chunks, keys, &out_matches, colliding, &mut self.output.refs, &mut window.output);
        self.output_keys.extend(self.output.refs[fresh..].iter().map(|&(chunk, row)| (in_chunks.len() + chunk, row)));

        *from = retire.keys.get(retire.window.end).map_or(KeyPosition::End, |key| KeyPosition::At(*key));
    }

    fn reduce_corrections(&mut self, keys: &[i64], in_ends: &[usize], input: &[(u64, Diff)], out_ends: &[usize], output: &[(u64, Diff)]) -> (Vec<(u64, Diff)>, Vec<usize>) {
        // Each key wants its rows of `desired`, each with its diff (once each, but for a lambda's), and
        // has `(id, diff)`s of output. The
        // correction nets the two by value: a wanted row equal to an output id's value counts
        // toward that id, and one equal to none gets a new id. Keys have a handful of each.
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
        let (desired, desired_diffs, desired_ends) = match self.reducer {
            Reducer::Expr(_) => { let (d, diffs, e) = self.reduce_lambda(in_ends, input); (d, Some(diffs), e) }
            _ => { let (d, e) = self.reduce_brackets(in_ends, input); (d, None, e) }
        };
        let fresh = self.output.columns.len();
        let prior_refs = self.output.refs.len();
        if !desired.is_empty() { self.output.columns.push(desired); }
        // The usual reducers (including single-result lambdas) reconcile one desired value with
        // at most one old value per real key. Compare all those pairs at once, by coordinates into
        // the existing columns. Keys with multiple outputs retain the general reconciliation path;
        // batching their Cartesian candidate sets would add work that an early match avoids.
        let mut singleton_matches = Vec::new();
        if !output.is_empty() {
            singleton_matches.resize(keys.len(), None);
            let capacity = keys.len().min(output.len());
            let mut pairs = Vec::with_capacity(capacity);
            let (mut bs, mut os) = (0, 0);
            for (b, (key, &oe)) in keys.iter().zip(out_ends).enumerate() {
                let be = if split { bracket_ends[b] } else { b + 1 };
                let ds = if bs == 0 { 0 } else { desired_ends[bs - 1] };
                let de = desired_ends[be - 1];
                if self.retire.colliding.binary_search(key).is_err() && de - ds <= 1 && oe - os <= 1 {
                    singleton_matches[b] = Some(false);
                    if de > ds && oe > os {
                        let (column, row) = self.output.refs[output[os].0 as usize];
                        pairs.push((column, ds, row, b));
                    }
                }
                (bs, os) = (be, oe);
            }
            pairs.sort_unstable_by_key(|p| (p.0, p.1));
            let (mut left, mut right) = (Vec::new(), Vec::new());
            for group in pairs.chunk_by(|a, b| a.0 == b.0) {
                left.clear(); right.clear();
                left.extend(group.iter().map(|p| p.1));
                right.extend(group.iter().map(|p| p.2));
                let signs = equal_idx(&self.output.columns[fresh], &self.output.columns[group[0].0], &left, &right);
                for (pair, sign) in group.iter().zip(signs) { singleton_matches[pair.3] = Some(sign == 0); }
            }
        }
        let Pool { columns, refs } = &mut self.output;
        let (output_keys, key_columns) = (&mut self.output_keys, &self.key_columns);
        let (mut corr, mut corr_ends) = (Vec::new(), Vec::with_capacity(keys.len()));
        let (mut bs, mut os) = (0, 0);
        for (b, (key, &oe)) in keys.iter().zip(out_ends).enumerate() {
            let be = if split { bracket_ends[b] } else { b + 1 };
            let checked = split && self.retire.colliding.binary_search(key).is_ok();
            // Reconcile directly in this key's tail of the batched corrections. A separate Vec
            // here used to allocate once for every key, even for an initial singleton result.
            let net_start = corr.len();
            corr.extend(output[os..oe].iter().map(|&(id, d)| (id, -d)));
            for s in bs..be {
                let (ds, de) = (if s == 0 { 0 } else { desired_ends[s - 1] }, desired_ends[s]);
                if ds == de { continue; }
                // A desired row's key: that of the input it was reduced from, which is non-empty.
                let first = if s == 0 { 0 } else { in_ends[s - 1] };
                let (kc, kr) = self.input.refs[input[first].0 as usize];
                for row in ds..de {
                    let diff = desired_diffs.as_ref().map_or(1, |diffs| diffs[row]);
                    let equal = |&(id, _): &(u64, Diff)| {
                        let (column, r) = refs[id as usize];
                        compare_at(&columns[fresh], row, &columns[column], r).is_eq() && (!checked || {
                            let (oc, or) = output_keys[id as usize];
                            compare_at(&key_columns[kc], kr, &key_columns[oc], or).is_eq()
                        })
                    };
                    let found = match singleton_matches.get(b).copied().flatten() {
                        Some(true) => Some(0),
                        Some(false) => None,
                        None => corr[net_start..].iter().position(equal),
                    };
                    match found {
                        Some(at) => corr[net_start + at].1 += diff,
                        None => {
                            corr.push((refs.len() as u64, diff));
                            refs.push((fresh, row));
                            output_keys.push((kc, kr));
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
                present(&chunks, &keys, &search(&chunks, &keys), &[], &mut refs, &mut bridge);
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
        let mut backend = CorgiReduceBackend::<Time>::new(Reducer::Count, (Shape::Int, Shape::Int));
        backend.output = Pool { columns: vec![ints(vec![5, 5, 7])], refs: vec![(0, 0), (0, 1), (0, 2)] };
        backend.output_keys = vec![(0, 0), (0, 0), (0, 1)];
        backend.input.refs = vec![(0, 0), (0, 1)];
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
        let mut backend = CorgiReduceBackend::<Time>::new(Reducer::Count, (Shape::Int, Shape::Int));
        backend.input.refs = vec![(0, 0)];
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
            let instance = ReduceInstance { source_batches: &source, input_batches: &input, output_batches: &out, lower: lower.borrow() };
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
                let mut backend = CorgiReduceBackend::<Time>::with_window(Reducer::Count, shape, window_size);
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
        use crate::corgi::chunk::recover_key;
        use crate::corgi::feed::print;
        use corgi::Shape;

        let pairs = |rows: &[(u64, (u64, u64), Time, Diff)], pair: fn(&(u64, (u64, u64), Time, Diff)) -> (u64, u64)| {
            let (a, b): (Vec<u64>, Vec<u64>) = rows.iter().map(pair).unzip();
            CValue::Prod(vec![ints(a), ints(b)])
        };
        let shape = Shape::Prod(vec![Shape::Int, Shape::Int]);
        // Two retires' inputs: the first at outer times 0 and 1, the second from 2 on.
        let early = random_rows(0x51ED_270B_2A4F_3C11, 200, 6);
        let late: Vec<_> = random_rows(0x0BAD_5EED, 120, 6).into_iter()
            .map(|(k, v, t, d)| (k, v, Time::new(t.outer + 2, t.inner), d)).collect();
        let early: Vec<_> = early.into_iter().filter(|r| r.2.outer < 2).collect();
        let stamp = |outer| Time::new(outer, PointStamp::new(std::iter::empty().collect()));

        // A lambda: the weighted sum of the values' first fields, once, and how many values, twice negated.
        let lambda = crate::ir::Lambda::new("let (_, vals) = input in \
            ((((0, vals) fold ((acc, ((a, _), d)) -> (acc, (a, d) mul) add)), 1) enlist, ((vals len), -2) enlist) append".into(), &corgi::Scope::default());
        for reducer in [Reducer::Count, Reducer::Distinct, Reducer::Min, Reducer::Collect, Reducer::Expr(lambda)] {
            // Keys `(a, 7a)`, identified by their hash or by `a % 2`.
            let run = |forged: bool, window_size: usize| {
                let batch = |rows: &[(u64, (u64, u64), Time, Diff)]| -> CBatch<Time> {
                    let keys = pairs(rows, |r| (r.0, r.0 * 7));
                    let keys = if forged { CValue::Prod(vec![ints(rows.iter().map(|r| r.0 % 2).collect()), keys]) } else { present_key(keys) };
                    let vals = pairs(rows, |r| r.1);
                    let chunk = CorgiChunk::from_columns(keys, vals, rows.iter().map(|r| r.2.clone()).collect(), rows.iter().map(|r| r.3).collect());
                    Rc::new(ChunkBatch::new(vec![chunk]))
                };
                let mut tactic = ProxyReduceTactic::new(CorgiReduceBackend::<Time>::with_window(reducer.clone(), (shape.clone(), shape.clone()), window_size));
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
