//! The corgi `ProxyReduceBackend`: the value semantics for the DD `ProxyReduceTactic`.
//!
//! The tactic (differential's `operators::int_proxy::reduce`) owns ALL time/lattice logic over
//! integer proxies `(key_id, value_id, time, diff)`; this backend supplies only:
//!
//!   * ids — a key's id is the identifier its arrangement is sorted by (the key itself when it is a
//!     primitive integer, else its hash lane). A value's id names a row in a pool of columns: the
//!     chunks' own value columns, and the output columns this backend builds. Within a window, a
//!     key's equal input values share one id, found by corgi's segmented sort. A proven weighted
//!     sum compares projected values here; other reducers keep whole-value identity. Output values are
//!     matched to the current output by structural comparison. Nothing hashes values.
//!   * integer values — a value that is a primitive integer (perhaps in one-field products) is its
//!     own id, mapped into `u64` with its sign bit flipped so that ids ascend in value order. Equal
//!     values share an id with no sort, and no pool row names them: the reduction reads its values
//!     from the ids, and emission writes the ids back as the value column. Input and output decide
//!     this apart, by the shapes of the values they compare (for input, the projected values where
//!     a weighted sum compares those, so a wide value read at one integer field counts), in each
//!     window whose identifiers each hold one real key: always for integer keys, and for hashed
//!     keys unless the window has a collision.
//!   * collisions — a hashed key's identifier may hold several real keys. Chunks sort each
//!     identifier's rows by real key, so a window finds them with two comparisons per run. Their
//!     ids are then per real key, input and output alike: a colliding bracket is split by real key
//!     before it is reduced, its output is matched by key as well as value, and every output id
//!     records the row holding its key, which is what `emit` reads. A value is not its own id in
//!     such a window, as two real keys under one identifier may hold equal values.
//!   * the value callback — `reduce_lambda` runs ONE crossing per wave over every `(key, time)`
//!     bracket, evaluating its expression over every bracket's key and `(val, diff)` list.
//!     The resulting `(val, diff)` lists supply the desired output columns, never DDIR rows.
//!     Only what the expression reads is gathered: its demand on its input, found once when the
//!     backend is built, decides whether keys are gathered, and which fields of the values.
//!   * materialize — gather the emitted keys and values from the pools' columns and seal a
//!     `CorgiChunk` batch column-natively.
//!
//! Presentation assigns value ids in structural order (signed integers, lexicographic lists),
//! whether ids name rows or are the values themselves. Reduction expressions read their input
//! lists in that order.
//!
//! The changed-key restriction is honored by presenting only the changed keys: novel batches are
//! read whole (delta-sized), the accumulated history is scanned and filtered to the changed hashes
//! (a columnar semijoin — matching the row-wise tactic's read).

use std::borrow::Cow;
use std::ops::Range;
use std::rc::Rc;

use differential_dataflow::consolidation::{consolidate, consolidate_from};
use differential_dataflow::trace::Description;
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::operators::int_proxy::{KeyPosition, ProxyBridge};
use differential_dataflow::operators::int_proxy::reduce::{ProxyReduceBackend, ReduceInstance, ReduceWindow};

use corgi::arrange::{compare_at, equal_idx, gather_lanes, leaf_slice, sort_blocks};
use corgi::{Bounds, Demand, Graph, NumOp, Shape, Value as CValue};

use crate::runtime::col_times::{ColTime, ColTimes};
use crate::runtime::search::matching_ranges;
use crate::runtime::chunk::{colliding_keys, columns_to_batch, ident, int_lane, key_ids, key_is_hashed, recover_key, recover_key_ref, CorgiChunk};
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

/// How many one-field products wrap an integer leaf of this shape, if it is one. Such a value
/// orders as its integer does, and can be its own id.
fn int_leaf(shape: &Shape) -> Option<usize> {
    match shape {
        Shape::Int => Some(0),
        Shape::Prod(fields) if fields.len() == 1 => int_leaf(&fields[0]).map(|depth| depth + 1),
        _ => None,
    }
}

/// An integer's id: its bits with the sign bit flipped, so that ids ascend as integers do.
fn int_id(value: i64) -> u64 { (value as u64) ^ (1 << 63) }

/// The integer an id names: the inverse of [`int_id`].
fn id_int(id: u64) -> i64 { (id ^ (1 << 63)) as i64 }

/// The integers `ids` name, as a column in `depth` one-field products.
fn int_column(depth: usize, ids: impl Iterator<Item = u64>) -> CValue {
    let mut column = CValue::i64(ids.map(id_int).collect());
    for _ in 0..depth { column = CValue::Prod(vec![column]); }
    column
}

/// The ids of the integers at `rows`, each a `(column, row)` of `columns`. Columns held as `i64`s,
/// as chunks hold integer values, are read where they are. If a row's column is held narrower,
/// as a projected field may be, the rows are gathered and read together instead.
fn int_ids(columns: &[CValue], rows: &[(usize, usize)]) -> Vec<u64> {
    let lanes: Vec<Option<&[i64]>> = columns.iter().map(int_lane).collect();
    if rows.iter().all(|&(column, _)| lanes[column].is_some()) {
        return rows.iter().map(|&(column, row)| int_id(lanes[column].unwrap()[row])).collect();
    }
    let gathered = gather_refs(columns, rows.iter().copied());
    leaf_slice(&gathered).expect("integer values").iter().map(|&value| int_id(value)).collect()
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
    /// The emitted rows whose ids are output values rather than rows of `output`, as ranges.
    value_rows: Vec<Range<usize>>,
    /// If the input's values are integers, in how many one-field products.
    int_input: Option<usize>,
    /// If the output's values are integers, in how many one-field products.
    int_output: Option<usize>,
    /// How the current window's ids name values.
    window_ids: WindowIds,
    /// Input records per window, bounding what the presentations cost at once.
    window_size: usize,
    /// The retire in progress.
    retire: Retire,
}

/// How the current window's ids name values: by rows of the pools, or as the integers themselves.
/// Integers name themselves where the values are integers and each of the window's identifiers
/// holds one real key.
#[derive(Default)]
struct WindowIds {
    /// Whether input ids are the integer values themselves.
    input_values: bool,
    /// Whether output ids are the integer values themselves.
    output_values: bool,
    /// When either is, a row of `key_columns` holding each of the window's keys, if any does, as
    /// such ids record none.
    key_rows: Vec<Option<(usize, usize)>>,
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
    pub(crate) fn new(reducer: Reducer, limit: Option<usize>, shape: (Shape, Shape)) -> Self { Self::with_window(reducer, limit, shape, 1 << 12) }

    /// A backend with an explicit window budget, in presented input records. A `Reducer::Expr`
    /// compiles here, at `shape`; a type error is a panic with corgi's message, which install's
    /// type check keeps from being reached.
    pub(crate) fn with_window(reducer: Reducer, limit: Option<usize>, (kshape, vshape): (Shape, Shape), window_size: usize) -> Self {
        let Reducer::Expr(lambda) = reducer;
        let graph = compile_reduce(&lambda, &kshape, &vshape)
            .unwrap_or_else(|e| panic!("reduce: type error at shapes ({kshape}, {vshape}): {e}"));
        let value_shape = vshape.clone();
        // The graph rebuilt to read only what it reads, and the demand saying what that is.
        let input = Shape::Prod(vec![kshape, Shape::List(Box::new(Shape::Prod(vec![vshape, Shape::Int])))]);
        let compiled = corgi::compile(graph, &input)
            .unwrap_or_else(|e| panic!("reduce: compiling at {input}: {e}"));
        // A limit observes whole-value order and entry boundaries. Demand alone does not prove
        // that merging those entries is valid; the sum analysis also checks disappearing groups.
        let project_values = limit.is_none() && crate::passes::reduce_demand::projection(&compiled).is_some();
        let narrowed = corgi::narrow(compiled, &Demand::All)
            .unwrap_or_else(|e| panic!("reduce: narrowing a compiled graph at {input}: {e}"));
        let reads = Reads::new(narrowed.input, &input);
        // Input ids name the values `present` compares: projected, where the reducer merges equal
        // projections (see `projects`), else whole.
        let compared = if project_values && reads.value != Demand::All {
            reads.value.projected_shape(&value_shape).expect("a demand on the reduce's values")
        } else { value_shape };
        let int_input = int_leaf(&compared);
        // The graph's output is a list of `(val, diff)`.
        let int_output = match corgi::shape_of(&narrowed.graph, &reads.shape) {
            Ok(Shape::List(element)) => match *element {
                Shape::Prod(fields) if fields.len() == 2 => int_leaf(&fields[0]),
                _ => None,
            },
            _ => None,
        };
        CorgiReduceBackend {
            graph: narrowed.graph,
            reads,
            project_values,
            limit,
            input: Pool::default(),
            output: Pool::default(),
            output_keys: Vec::new(),
            key_columns: Vec::new(),
            rows: (Vec::new(), Vec::new(), ColTimes::default(), Vec::new()),
            value_rows: Vec::new(),
            int_input,
            int_output,
            window_ids: WindowIds::default(),
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

/// How [`present`] names each run of equal values under a key.
enum Naming<'a> {
    /// By a row: the next index of `refs`, where the run is recorded as the `(chunk, row)` of one
    /// of its rows.
    Rows(&'a mut Vec<(usize, usize)>),
    /// By the value itself, an integer. Exact only where each identifier holds one real key.
    Ints,
}

/// Present the records `chunks` hold for `keys`, at `matches`, into `bridge`, sorted by
/// `((key, id), time)` and consolidated.
///
/// Values are compared as `projected` holds them, a column per chunk, or else whole. Each key's
/// rows are ordered by value, and each run of equal values gets one id, as `naming` says.
/// Integers are ordered by their ids, which a chunk holds in order, so only a key that several
/// chunks hold is sorted, and as integers. Otherwise rows are sorted by value. The keys
/// `colliding` marks (by index; empty when none do) hold several real keys under one identifier:
/// their rows are first split by real key, so that an id names one real key's value, and the row
/// it is recorded at holds that key. Their values are never named by themselves.
fn present<T: ColTime + Ord>(
    chunks: &[&CorgiChunk<T, Diff>],
    keys: &[i64],
    matches: &Matches,
    colliding: &[bool],
    projected: Option<&[CValue]>,
    naming: Naming<'_>,
    bridge: &mut ProxyBridge<T, Diff, i64>,
) {
    // The matched `(chunk, row)`s, key by key, and each one's key index.
    let mut runs: Vec<_> = matches.iter().enumerate()
        .flat_map(|(chunk, found)| found.iter().map(move |(key, rows)| (*key, chunk, rows.clone())))
        .collect();
    runs.sort_by_key(|run| run.0);
    // At their final size: grown by doubling, window after window, these cost a presentation up to
    // half again its time, by the allocator's state.
    let total = runs.iter().map(|run| run.2.len()).sum();
    let (mut labels, mut rows) = (Vec::with_capacity(total), Vec::with_capacity(total));
    for (key, chunk, range) in runs {
        labels.extend(range.clone().map(|_| key as u64));
        rows.extend(range.map(|row| (chunk, row)));
    }
    if rows.is_empty() {
        return;
    }
    // Output and unproven input reducers retain whole-value identity and order.
    let whole;
    let vals = if let Some(projected) = projected { projected } else {
        whole = chunks.iter().map(|chunk| chunk.vals().clone()).collect::<Vec<_>>();
        &whole
    };
    let update = |(chunk, row): (usize, usize)| (chunks[chunk].times().get(row), chunks[chunk].diffs()[row]);
    let mut updates = Vec::new();
    let refs = match naming {
        Naming::Rows(refs) => refs,
        Naming::Ints => {
            assert!(!colliding.contains(&true), "present: a value names itself only under an identifier holding one real key");
            let ids = int_ids(vals, &rows);
            let mut order: Vec<usize> = (0..rows.len()).collect();
            for block in order.chunk_by_mut(|&a, &b| labels[a] == labels[b]) {
                if !block.is_sorted_by_key(|&i| ids[i]) { block.sort_by_key(|&i| ids[i]); }
            }
            for group in order.chunk_by(|&a, &b| labels[a] == labels[b] && ids[a] == ids[b]) {
                updates.extend(group.iter().map(|&i| update(rows[i])));
                consolidate(&mut updates);
                let (key, id) = (keys[labels[group[0]] as usize], ids[group[0]]);
                bridge.extend(updates.drain(..).map(|(time, diff)| ((key, id), time, diff)));
            }
            return;
        }
    };
    // Each label's key index; the identity unless real keys split some labels.
    let mut label_keys = None;
    if colliding.contains(&true) {
        (labels, rows, label_keys) = split_real_keys(chunks, colliding, labels, rows);
    }
    let key_of = |label: u64| label_keys.as_ref().map_or(label as usize, |keys: &Vec<usize>| keys[label as usize]);
    let (sorted, groups) = sort_blocks(&labels, &gather_refs(vals, rows.iter().copied()));
    let mut start = 0;
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
    /// The expression crossing for a wave: every
    /// bracket's key and its values, as a list of `(val, diff)`, through the lambda's graph at once.
    /// Returns the output values, their diffs, and each bracket's end among them.
    ///
    /// The harness presents each bracket consolidated, so its diffs are non-zero and its value ids
    /// ascend, which is value order: `present` numbers each key's values in sorted order. The
    /// brackets are presented as they are. One with no input has no output, and the lambda does not
    /// see it. `key_rows` holds the row holding each bracket's key, for those with input.
    fn reduce_lambda(&self, ends: &[usize], input: &[(u64, Diff)], key_rows: &[Option<(usize, usize)>]) -> (CValue, Vec<Diff>, Vec<usize>) {
        // A limit hands the lambda each bracket's first values only; nothing past them is gathered.
        let limited = self.limit.and_then(|k| first_entries(k, ends, input));
        let (ends, input) = limited.as_ref().map_or((ends, input), |(ends, input)| (&ends[..], &input[..]));
        // Per bracket with input: its index, its end in `input`, and the row holding its key.
        let (mut blocks, mut key_refs) = (Vec::new(), Vec::new());
        let mut start = 0;
        for (bi, &end) in ends.iter().enumerate() {
            if end > start {
                blocks.push((bi, end));
                key_refs.push(key_rows[bi].expect("a bracket with input has a row holding its key"));
            }
            start = end;
        }
        if blocks.is_empty() {
            return (CValue::Unit(0), Vec::new(), vec![0; ends.len()]);
        }
        let vals = match self.reads.value {
            Demand::None => CValue::Unit(input.len()),
            _ => self.input_values(input.iter().map(|&(id, _)| id)),
        };
        let diffs = CValue::i64(input.iter().map(|&(_, diff)| diff).collect());
        // Projected values may tie; whole ones ascend.
        if cfg!(debug_assertions) && self.reads.value == Demand::All {
            let mut start = 0;
            for &(_, end) in &blocks {
                for i in start + 1..end {
                    assert!(compare_at(&vals, i - 1, &vals, i).is_lt(), "reduce: a bracket's values are not strictly ascending");
                }
                start = end;
            }
            assert!(input.iter().all(|&(_, diff)| diff != 0), "reduce: a bracket holds a zero diff");
        }
        let keys = if self.reads.key { recover_key(&gather_refs(&self.key_columns, key_refs.into_iter())) } else { CValue::Unit(key_refs.len()) };
        let lists = CValue::List(Bounds::offsets(blocks.iter().map(|b| b.1).collect()), Box::new(CValue::Prod(vec![vals, diffs])));
        let argument = self.reads.argument.project(CValue::Prod(vec![keys, lists])).expect("the reduce's input has the shape its demand is on");
        let shape = corgi::shape_of_value(&argument);
        assert!(shape == self.reads.shape, "reduce: input read as {shape} reached an operator narrowed to {}", self.reads.shape);
        let (bounds, elems) = corgi::eval_graph(&self.graph, argument).into_list("reduce output").unwrap();
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

    /// The current window's input values that `ids` name, projected by what the graph reads of
    /// them.
    fn input_values(&self, ids: impl Iterator<Item = u64>) -> CValue {
        match self.int_input.filter(|_| self.window_ids.input_values) {
            // Ids of projected values are already what the graph reads.
            Some(depth) if self.projects() => int_column(depth, ids),
            Some(depth) => self.reads.value.project(int_column(depth, ids)).expect("an integer has the reduce's value shape"),
            None => self.input.gather(&ids.collect::<Vec<_>>()),
        }
    }

    /// Whether input values are compared as the graph reads them: the reducer is proven to merge
    /// equal projected values, and it does not read them whole.
    fn projects(&self) -> bool { self.project_values && self.reads.value != Demand::All }

    /// The row of `key_columns` holding each bracket's key, for the brackets with input. Ids that
    /// name rows of the input pool name rows holding their keys. Ids that are values do not, and
    /// then each bracket is a key of the window (no identifier collides), whose row it has.
    fn bracket_rows(&self, keys: &[i64], ends: &[usize], input: &[(u64, Diff)]) -> Vec<Option<(usize, usize)>> {
        let window = &self.retire.keys[self.retire.window.clone()];
        debug_assert!(!self.window_ids.input_values || keys.len() == ends.len(), "reduce: a bracket per key");
        let mut start = 0;
        ends.iter().enumerate().map(|(bracket, &end)| {
            let first = std::mem::replace(&mut start, end);
            (end > first).then(|| if self.window_ids.input_values {
                let at = window.binary_search(&keys[bracket]).expect("a bracket's key is in its window");
                self.window_ids.key_rows[at].expect("a key with input has a row holding it")
            } else {
                self.input.refs[input[first].0 as usize]
            })
        }).collect()
    }

    /// [`ProxyReduceBackend::reduce_corrections`] where output ids are values: each key's desired
    /// rows, as ids, less its output, consolidated. Brackets are keys, as no identifier collides.
    fn value_corrections(&self, keys: &[i64], in_ends: &[usize], input: &[(u64, Diff)], out_ends: &[usize], output: &[(u64, Diff)]) -> (Vec<(u64, Diff)>, Vec<usize>) {
        let key_rows = self.bracket_rows(keys, in_ends, input);
        let (column, diffs, ends) = self.reduce_lambda(in_ends, input, &key_rows);
        let desired = if diffs.is_empty() { Cow::Borrowed(&[][..]) } else { leaf_slice(&column).expect("the reduce's output values are integers") };
        let (mut corr, mut corr_ends) = (Vec::with_capacity(output.len() + diffs.len()), Vec::with_capacity(keys.len()));
        let (mut ds, mut os) = (0, 0);
        for (&de, &oe) in ends.iter().zip(out_ends) {
            let start = corr.len();
            corr.extend(output[os..oe].iter().map(|&(id, diff)| (id, -diff)));
            corr.extend(desired[ds..de].iter().zip(&diffs[ds..de]).map(|(&value, &diff)| (int_id(value), diff)));
            consolidate_from(&mut corr, start);
            corr_ends.push(corr.len());
            (ds, os) = (de, oe);
        }
        (corr, corr_ends)
    }

    /// The output values the emitted `ids` name: the ids themselves in `value_rows`, and rows of
    /// the output pool elsewhere.
    fn output_values(&self, ids: &[u64], value_rows: &[Range<usize>]) -> CValue {
        let Some(depth) = self.int_output.filter(|_| !value_rows.is_empty()) else { return self.output.gather(ids) };
        let values = int_column(depth, ids.iter().copied());
        if value_rows.len() == 1 && value_rows[0] == (0..ids.len()) { return values; }
        // Some windows named rows of the pool (an identifier collided): gather from both.
        let mut named = vec![false; ids.len()];
        for rows in value_rows { named[rows.clone()].fill(true); }
        let mut columns = self.output.columns.clone();
        columns.push(values);
        let at = columns.len() - 1;
        gather_refs(&columns, ids.iter().zip(named).enumerate().map(|(row, (&id, named))| if named { (at, row) } else { self.output.refs[id as usize] }))
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
        self.value_rows.clear();
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
            let value = &self.reads.value;
            self.input.columns = in_chunks.iter().map(|chunk| value.project(chunk.vals().clone()).expect("a chunk's values have the reduce's value shape")).collect();
            self.output = Pool { columns: out_chunks.iter().map(|chunk| chunk.vals().clone()).collect(), refs: Vec::new() };
            self.output_keys.clear();
            self.key_columns = in_chunks.iter().chain(&out_chunks).map(|chunk| chunk.keys().clone()).collect();
        }

        // Windows are bounded, as their presentations are all live at once.
        let projects = self.projects();
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

        // Integer values name themselves where each identifier holds one real key. Their ids then
        // record no rows, so the window notes a row holding each key, input or output.
        let ids = &mut self.window_ids;
        ids.input_values = colliding.is_empty() && self.int_input.is_some();
        ids.output_values = colliding.is_empty() && self.int_output.is_some();
        ids.key_rows.clear();
        if ids.input_values || ids.output_values {
            ids.key_rows.resize(keys.len(), None);
            for (column, found) in matches.iter().chain(&out_matches).enumerate() {
                for (key, rows) in found { ids.key_rows[*key].get_or_insert((column, rows.start)); }
            }
        }

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
        let projected = projects.then_some(&self.input.columns[..]);
        let naming = if ids.input_values { Naming::Ints } else { Naming::Rows(&mut self.input.refs) };
        present(&in_chunks, keys, &matches, colliding, projected, naming, &mut window.input);
        // The output history, same keys. Output ids name rows for the whole retire, and the rows
        // that hold their keys are at the same positions of the output chunks' key columns.
        if ids.output_values {
            present(&out_chunks, keys, &out_matches, colliding, None, Naming::Ints, &mut window.output);
        } else {
            let fresh = self.output.refs.len();
            present(&out_chunks, keys, &out_matches, colliding, None, Naming::Rows(&mut self.output.refs), &mut window.output);
            self.output_keys.extend(self.output.refs[fresh..].iter().map(|&(chunk, row)| (in_chunks.len() + chunk, row)));
        }

        *from = retire.keys.get(retire.window.end).map_or(KeyPosition::End, |key| KeyPosition::At(*key));
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
        //
        // Where output ids are the values themselves, equal values are equal ids, and the
        // correction is a consolidation.
        if self.window_ids.output_values {
            return self.value_corrections(keys, in_ends, input, out_ends, output);
        }
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
        let key_rows = self.bracket_rows(keys, in_ends, input);
        let (desired, desired_diffs, desired_ends) = self.reduce_lambda(in_ends, input, &key_rows);
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
            let resolved = singleton_matches.get(b).copied().flatten().is_some();
            let (lo, hi) = (if bs == 0 { 0 } else { desired_ends[bs - 1] }, desired_ends[be - 1]);
            let rows = (oe - os) + (hi - lo);
            if !checked && !resolved && hi - lo > 16 && hi - lo > 2 * (usize::BITS - rows.leading_zeros()) as usize {
                let (kc, kr) = key_rows[bs].expect("a bracket with desired rows has input");
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
                    let (kc, kr) = key_rows[s].expect("a bracket with desired rows has input");
                    for row in ds..de {
                        let diff = desired_diffs[row];
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
        let (key_rows, ids, times, diffs) = &mut self.rows;
        let start = ids.len();
        if self.window_ids.output_values {
            // An output id is the value, and the window holds a row of each key; records ascend
            // by key, so the window's keys are walked once.
            let window = &self.retire.keys[self.retire.window.clone()];
            let mut at = 0;
            for ((key, id), time, diff) in records {
                if window.get(at) != Some(key) { at = window.binary_search(key).expect("an emitted key is in its window"); }
                key_rows.push(self.window_ids.key_rows[at].expect("an emitted key has a row holding it"));
                ids.push(*id);
                times.push(time);
                diffs.push(*diff);
            }
            match self.value_rows.last_mut() {
                Some(rows) if rows.end == start => rows.end = ids.len(),
                _ if ids.len() > start => self.value_rows.push(start..ids.len()),
                _ => {}
            }
        } else {
            // An output id names its key's row, as two keys may share an identifier.
            for ((_, id), time, diff) in records {
                key_rows.push(self.output_keys[*id as usize]);
                ids.push(*id);
                times.push(time);
                diffs.push(*diff);
            }
        }
    }

    fn finish(&mut self) -> Option<CBatch<T>> {
        let (key_rows, ids, times, diffs) = std::mem::take(&mut self.rows);
        let value_rows = std::mem::take(&mut self.value_rows);
        if times.is_empty() { return None; }
        let keys = gather_refs(&self.key_columns, key_rows.into_iter());
        Some(Rc::new(columns_to_batch(keys, self.output_values(&ids, &value_rows), times, diffs)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
                present(&chunks, &keys, &search(&chunks, &keys), &[], None, Naming::Rows(&mut refs), &mut bridge);
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

    /// Integers named by themselves: `present` gives what the chunks hold, sorted and consolidated,
    /// with each value's own id, so a key's ids ascend in signed order, across key shapes, a
    /// one-field product, a pair projected to a field held as bytes, chunkings, ties, and
    /// cancellations.
    #[test]
    fn present_names_integers_by_themselves() {
        const SIGNED: [i64; 3] = [i64::MIN, -1, 256];
        let rows = random_rows(0x9E37_79B9_7F4A_7C15, 300, 6);
        // Each shape, and whether values are compared at their first field.
        let val_shapes: [(fn(&[(u64, u64)]) -> CValue, bool); 3] = [
            (|v| CValue::i64(v.iter().map(|v| SIGNED[v.0 as usize]).collect()), false),
            (|v| CValue::Prod(vec![CValue::int(v.iter().map(|v| v.1 as i64))]), false),
            (|v| CValue::Prod(vec![CValue::int(v.iter().map(|v| v.0 as i64)), CValue::int(v.iter().map(|v| v.1 as i64))]), true),
        ];
        let first = Demand::Prod(vec![Demand::All, Demand::None]);
        for (key_shape, (val_shape, projected)) in KEY_SHAPES.iter().flat_map(|k| val_shapes.iter().map(move |v| (*k, *v))) {
            let mut keys = key_ids(&key_shape(&[0, 2, 3, 5, 9]));
            keys.sort();
            for size in [1, 7, rows.len()] {
                let owned = chunks(&rows, size, key_shape, val_shape);
                let chunks: Vec<_> = owned.iter().collect();
                let columns: Vec<CValue> = if projected { chunks.iter().map(|chunk| first.project(chunk.vals().clone()).unwrap()).collect() } else { Vec::new() };
                let mut bridge = Vec::new();
                present(&chunks, &keys, &search(&chunks, &keys), &[], projected.then_some(&columns[..]), Naming::Ints, &mut bridge);
                assert!(bridge.windows(2).all(|w| (w[0].0, &w[0].1) < (w[1].0, &w[1].1)));
                assert!(bridge.windows(2).all(|w| w[0].0.0 != w[1].0.0 || id_int(w[0].0.1) <= id_int(w[1].0.1)), "ids ascend in signed order");
                let values = rows_of(&int_column(0, bridge.iter().map(|r| r.0.1)));
                let actual: BTreeMap<_, _> = bridge.iter().zip(values).map(|(((key, _), time, diff), value)| ((*key, value, time.clone()), *diff)).collect();
                // What the chunks hold, netted at the compared values.
                let mut expected = BTreeMap::new();
                for ((key, value, time), diff) in netted(&chunks, &keys.iter().copied().collect()) {
                    let value = if projected { value[..1].to_vec() } else { value };
                    *expected.entry((key, value, time)).or_insert(0) += diff;
                }
                expected.retain(|_, diff| *diff != 0);
                assert_eq!(actual, expected);
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
                    // The values the window's ids name, whether rows or the values themselves.
                    let input_values = backend.input_values(window.input.iter().map(|r| r.0.1));
                    let output_ids: Vec<u64> = window.output.iter().map(|r| r.0.1).collect();
                    let all = 0..output_ids.len();
                    let named = if backend.window_ids.output_values { std::slice::from_ref(&all) } else { &[] };
                    let output_values = backend.output_values(&output_ids, named);
                    for (bridge, values, into) in [(&window.input, input_values, &mut inputs), (&window.output, output_values, &mut outputs)] {
                        for (((key, _), time, diff), value) in bridge.iter().zip(rows_of(&values)) {
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

    /// Integer values named by themselves reduce as values named by rows do. Over two retires of
    /// signed values with ties and cancellations, bare, in a one-field product, or as the field a
    /// weighted sum reads of a pair (whose projection is the integer), each reducer gives what it
    /// gives under forged identifiers that all collide, where every window names
    /// rows: with integer keys, hashed keys, and forged identifiers of which only one collides,
    /// so that some windows name values and others rows, at every window size.
    #[test]
    fn integer_values_reduce_as_rows_do() {
        use timely::progress::{Antichain, Timestamp};
        use differential_dataflow::operators::int_proxy::reduce::ProxyReduceTactic;
        use differential_dataflow::operators::reduce::ReduceTactic;
        use crate::runtime::feed::print;
        use crate::parse::Reducer as Sugar;

        /// How keys `(a, 7a)` are identified.
        #[derive(Clone, Copy, Debug)]
        enum Ident { Int, Hash, OneCollides, AllCollide }
        let key_column = |ident: Ident, keys: &[u64]| -> CValue {
            let pair = CValue::Prod(vec![ints(keys.to_vec()), ints(keys.iter().map(|k| k * 7).collect())]);
            match ident {
                Ident::Int => ints(keys.to_vec()),
                Ident::Hash => present_key(pair),
                Ident::OneCollides => CValue::Prod(vec![ints(keys.iter().map(|&k| if k < 2 { 0 } else { k }).collect()), pair]),
                Ident::AllCollide => CValue::Prod(vec![ints(keys.iter().map(|k| k % 2).collect()), pair]),
            }
        };
        // Signed values at the edges of the sign flip and of byte storage, with ties.
        const VALUES: [i64; 8] = [i64::MIN, -300, -1, 0, 1, 255, 256, i64::MAX];
        // Each value with a second field, which a pair's projection drops: values equal in their
        // first field may differ in it, and merge only when projected.
        let signed = |seed, count| -> Vec<(u64, (i64, i64), Time, Diff)> {
            random_rows(seed, count, 6).into_iter().map(|(k, (a, b), t, d)| (k, (VALUES[(a * 3 + b) as usize % 8], a as i64 - 1), t, d)).collect()
        };
        let stamp = |outer| crate::server::host_time(outer);
        let later = |t: &Time| Time::new(std::iter::once(t.first().copied().unwrap_or(0) + 2).chain(t.iter().skip(1).copied()).collect());
        // The first retire's input, at outer times 0 and 1. The second's, from 2 on: new rows,
        // rows of the first negated, and a batch cancelling some new rows exactly.
        let early: Vec<_> = signed(0x51ED_270B_2A4F_3C11, 200).into_iter().filter(|r| r.2.first().copied().unwrap_or(0) < 2).collect();
        let mut late: Vec<_> = signed(0x0BAD_5EED, 120).into_iter().map(|(k, v, t, d)| (k, v, later(&t), d)).collect();
        late.extend(early.iter().step_by(7).map(|(k, v, t, d)| (*k, *v, later(t), -d)));
        let cancel: Vec<_> = late.iter().step_by(3).map(|(k, v, t, d)| (*k, *v, t.clone(), -d)).collect();

        /// How the values are held.
        #[derive(Clone, Copy, Debug)]
        enum Values { Bare, Wrapped, Pair }
        for values in [Values::Bare, Values::Wrapped, Values::Pair] {
            // Bare and wrapped: each value, and a constant for each, with its diff, so that equal
            // outputs consolidate. A pair: the weighted sum of first fields, which reads only them.
            let body = match values {
                Values::Bare => "(vals map ((v, d) -> (v, d)), vals map ((_, d) -> (7, d))) append",
                Values::Wrapped => "(vals map ((v, d) -> (v, d)), vals map ((_, d) -> ((7), d))) append",
                Values::Pair => "let n = vals map (((v, _), d) -> (v, d) mul) fold_add in (n, (n, 1)) enlist filter",
            };
            let lambda = crate::ir::Lambda::new(format!("let (_, vals) = input in {body}"), &corgi::Scope::default());
            let (vshape, column): (Shape, fn(&[(i64, i64)]) -> CValue) = match values {
                Values::Bare => (Shape::Int, |vs| CValue::i64(vs.iter().map(|v| v.0).collect())),
                Values::Wrapped => (Shape::Prod(vec![Shape::Int]), |vs| CValue::Prod(vec![CValue::i64(vs.iter().map(|v| v.0).collect())])),
                Values::Pair => (Shape::Prod(vec![Shape::Int, Shape::Int]), |vs| CValue::Prod(vec![CValue::i64(vs.iter().map(|v| v.0).collect()), CValue::int(vs.iter().map(|v| v.1))])),
            };
            // The sum compares its pairs' projections, single integers, so they name themselves.
            if let Values::Pair = values {
                let backend = CorgiReduceBackend::<Time>::new(Reducer::Expr(lambda.clone()), None, (Shape::Int, vshape.clone()));
                assert!(backend.projects() && backend.int_input == Some(1) && backend.int_output == Some(0));
            }
            let sugars = [Sugar::Count, Sugar::Distinct, Sugar::Min, Sugar::Collect].map(|sugar| (Reducer::Expr(sugar.lambda()), sugar.limit()));
            for (reducer, limit) in sugars.into_iter().chain([(Reducer::Expr(lambda), None)]) {
                let run = |ident: Ident, window_size: usize| {
                    let batch = |rows: &[(u64, (i64, i64), Time, Diff)]| -> CBatch<Time> {
                        let keys = key_column(ident, &rows.iter().map(|r| r.0).collect::<Vec<_>>());
                        let chunk = CorgiChunk::from_columns(keys, column(&rows.iter().map(|r| r.1).collect::<Vec<_>>()), rows.iter().map(|r| r.2.clone()).collect(), rows.iter().map(|r| r.3).collect());
                        Rc::new(ChunkBatch::new(vec![chunk]))
                    };
                    let kshape = if let Ident::Int = ident { Shape::Int } else { Shape::Prod(vec![Shape::Int, Shape::Int]) };
                    let mut tactic = ProxyReduceTactic::new(CorgiReduceBackend::<Time>::with_window(reducer.clone(), limit, (kshape, vshape.clone()), window_size));
                    let held = Antichain::from_elem(Time::minimum());
                    let (first, _) = tactic.retire(Vec::new(), Vec::new(), vec![batch(&early)], &held, &Antichain::from_elem(stamp(2)), &held);
                    let first = first.and_then(|span| span.inner).into_iter().collect::<Vec<_>>();
                    let (second, _) = tactic.retire(vec![batch(&early)], first.clone(), vec![batch(&late), batch(&cancel)], &Antichain::from_elem(stamp(2)), &Antichain::new(), &held);
                    // Output by `a`, the value as printed, and time.
                    let mut netted = BTreeMap::new();
                    for out in first.iter().chain(second.and_then(|span| span.inner).as_ref()) {
                        for chunk in out.chunks.iter() {
                            let keys = recover_key(chunk.keys());
                            let a = match &keys { CValue::Prod(fields) => fields[0].as_i64("a").unwrap().into_owned(), leaf => leaf.as_i64("a").unwrap().into_owned() };
                            for (i, (key, diff)) in a.iter().zip(chunk.diffs()).enumerate() {
                                *netted.entry((*key, print(chunk.vals(), i), chunk.times().get(i))).or_insert(0) += diff;
                            }
                        }
                    }
                    netted.retain(|_, diff| *diff != 0);
                    netted
                };
                let expected = run(Ident::AllCollide, usize::MAX);
                assert!(!expected.is_empty());
                for ident in [Ident::Int, Ident::Hash, Ident::OneCollides, Ident::AllCollide] {
                    for window_size in [1, 5, usize::MAX] {
                        assert_eq!(run(ident, window_size), expected, "{reducer:?}, {ident:?}, {values:?}, window_size={window_size}");
                    }
                }
            }
        }
    }
}
