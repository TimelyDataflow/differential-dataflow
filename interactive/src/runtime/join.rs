//! The corgi JOIN backend for the `int_proxy` proxy-join seam.
//!
//! [`CorgiJoinBackend`] implements [`ProxyJoinBackend`]: `advance` stages blocks of the two
//! inputs' key intersection as `(coord, time, diff)` entries, and `cross` redeems
//! matched IDs into primitive columns or gathers coordinates from the instance's chunks, runs the
//! compiled projection, and cuts `TARGET_OUT`-sized [`CorgiContainer`]s. Peak state is one
//! block's bridges plus one container, however large the unit.
//!
//! `advance` completes a key itself when the tactic would not replay it ([`replays`]): when either
//! side holds fewer than 16 rows of it. Such a key is not staged at all: its output is every pair of its rows, which `advance`
//! records as row coordinates, and `cross` then gathers the block's pairs from the chunks' columns,
//! with times joined lane-wise, straight into the projection. Nothing is named, consolidated,
//! or built a row at a time. Only a key with long histories on both sides is staged and presented
//! in the bridges, for the tactic's replay.
//!
//! # Tokens
//!
//! *   The **group token** is the arrangement's leading `i64` identifier: the key itself for
//!     primitive integer keys, or the carried content hash for structured keys. Chunk order is
//!     identifier order, which makes `from` seekable and every key shape resumably blockable.
//!     A hash collision remains under that token through proxy matching; only then does `cross`
//!     compare the recorded real-key coordinates and discard unequal pairs.
//!
//! *   Values are compared after projection to the fields the join reads. Unread values share
//!     one token per real key. With primitive keys, a read primitive **value token** is the
//!     value itself (including singleton-product wrappers). Equal values consolidate without
//!     choosing a representative, and matched columns are reconstructed from IDs. Other values
//!     use a *canonical coordinate*: `(chunk << 48) | row` of the value's first occurrence among
//!     its side's chunks. Coordinates redeem against the instance alone (no backend state, no
//!     reliance on call adjacency), and value columns are never copied into the presentation —
//!     they are gathered once, straight from chunk storage into the projection input, and only
//!     for rows that matched. Assigning *equal projected* values (found by the per-key merge
//!     across chunks) the *same* canonical coordinate is what lets bridge consolidation cancel
//!     cross-batch churn: tokens are the unit of cancellation, and location-distinct tokens for
//!     equal data would silently opt out.
//!
//! Times are advanced by `instance.lower` on presentation (consolidate on load), and a key
//! whose side nets to zero after consolidation is suppressed entirely (the harness requires
//! keys common to both bridges).

use std::borrow::Cow;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::rc::Rc;

use differential_dataflow::operators::int_proxy::{KeyPosition, JoinInstance, ProxyBridge, ProxyJoinBackend};
use differential_dataflow::operators::int_proxy::join::JoinMatches;
use differential_dataflow::trace::chunk::{Chunk, ChunkBatch};

use corgi::arrange::{compare_at, equal_idx, gather_lanes, sort_blocks};
use crate::runtime::search::matching_ranges;
use corgi::{shape_of_value, Demand, Graph, NumOp, Shape, Value as CValue};

use crate::runtime::chunk::{colliding_keys, int_lane, key_is_hashed, recover_key_ref, rewrap, CorgiChunk};
use crate::runtime::col_times::{ColTime, ColTimes};
use crate::runtime::container::CorgiContainer;
use crate::lambda::compile_projection;
use crate::ir::{Diff, Lambda};

type CBatch<T> = Rc<ChunkBatch<CorgiChunk<T, Diff>>>;

/// Matches per output container cut by `cross` (mirrors the chunk grading `TARGET`).
const TARGET_OUT: usize = 1 << 18;
/// Coordinate packing: chunk index in the high bits, row in the low `COORD_BITS`.
pub(super) const COORD_BITS: u32 = 48;

/// Whether a key with `rows0` and `rows1` rows on its two sides is worth the tactic's replay.
///
/// The replay walks the two histories in time order, consolidating each as it goes, so its work is
/// bounded by the netted accumulations rather than by the product of the histories. That can only
/// pay for itself when both sides are long; otherwise every pair of rows is cheaper.
/// This is the threshold the tactic's own `join_key` uses for the keys it is presented.
/// The two need not agree for correctness: a completed key's output is every pair of its rows either way.
fn replays(rows0: usize, rows1: usize) -> bool {
    rows0 >= 16 && rows1 >= 16
}

/// What a projection reads from the columns gathered for each match.
pub(super) struct Reads<const N: usize> {
    /// Each input's demand, applied to its columns before gathering.
    pub(super) columns: [Demand; N],
    /// Once gathered, retained inputs are taken whole; unread inputs are dropped.
    pub(super) argument: Demand,
    /// The shape the narrowed graph takes.
    pub(super) shape: Shape,
}

impl<const N: usize> Reads<N> {
    pub(super) fn new(demand: Demand, input: &Shape) -> Self {
        let shape = demand.projected_shape(input).expect("a demand on the join's input");
        let Demand::Prod(parts) = &demand else {
            return Reads { columns: std::array::from_fn(|_| demand.clone()), argument: demand, shape };
        };
        let columns = parts.clone().try_into().expect("one demand per input column");
        let argument = Demand::Prod(parts.iter().map(|part| if *part == Demand::None { Demand::None } else { Demand::All }).collect());
        Reads { columns, argument, shape }
    }
}

/// The corgi [`ProxyJoinBackend`]: holds the projection, a lambda over `(key, val0, val1)`,
/// compiled at the shapes of the columns it meets.
pub(crate) struct CorgiJoinBackend<T: ColTime> {
    /// Identifier tokens in the current block that cover more than one real key. `advance` writes
    /// this and the immediately following `cross` reads it before the next `advance`; only matches
    /// under these astronomically rare tokens need a real-key comparison.
    colliding: Vec<i64>,
    /// The row pairs of the keys the current block completes. `advance` writes them and the
    /// immediately following `cross` crosses and clears them: they are not kept between blocks,
    /// as one fat block would pin its pairs' memory for the life of the operator.
    completed: Pairs,
    /// The projection, narrowed to take its input projected by `reads.argument`.
    graph: Graph<NumOp>,
    /// What the graph reads of its input.
    reads: Reads<3>,
    _t: PhantomData<T>,
}

/// Pairs of rows, one of each input, as `(chunk, row)` coordinates in four columns.
#[derive(Default)]
struct Pairs {
    chunk0: Vec<usize>,
    row0: Vec<usize>,
    chunk1: Vec<usize>,
    row1: Vec<usize>,
    /// The diffs of the pairs from `netted_from` on, which are of rows netted before pairing.
    /// The pairs before it take the product of their two rows' stored diffs.
    netted_from: Option<usize>,
    diffs: Vec<Diff>,
}

impl Pairs {
    /// Every pair of a row of `runs0` with a row of `runs1`, runs of the first and second input.
    fn cross(&mut self, runs0: &[Run], runs1: &[Run]) {
        for &(chunk0, s0, e0) in runs0 {
            for &(chunk1, s1, e1) in runs1 {
                for row0 in s0..e0 {
                    let rows = e1 - s1;
                    self.chunk0.extend(std::iter::repeat_n(chunk0, rows));
                    self.row0.extend(std::iter::repeat_n(row0, rows));
                    self.chunk1.extend(std::iter::repeat_n(chunk1, rows));
                    self.row1.extend(s1..e1);
                }
            }
        }
    }
}

/// Whether a key with these runs on its two sides is completed as every pair of its rows.
///
/// It must be a key the tactic would not replay ([`replays`]): the replay bounds the work by the
/// netted accumulations, and pays for itself only on a key long on both sides.
/// A side whose values the projection reads only in part is staged
/// to net the rows that the projection cannot tell apart, unless it is one row, with nothing to net.
/// A completed key with rows of both signs on one side has that side netted first ([`net_side`]).
fn completes(runs: (&[Run], &[Run]), kinds: (ProjKind, ProjKind)) -> bool {
    let rows = |runs: &[Run]| runs.iter().map(|run| run.2 - run.1).sum::<usize>();
    let (rows0, rows1) = (rows(runs.0), rows(runs.1));
    let plain = |kind: ProjKind, rows: usize| rows == 1 || kind == ProjKind::Whole;
    !replays(rows0, rows1) && plain(kinds.0, rows0) && plain(kinds.1, rows1)
}

/// Whether the rows of `runs` have diffs of both signs.
///
/// Rows of one side can only cancel when they do: a value added in one batch and taken away
/// in a later one. Every pair of such a key's rows would carry the cancelling rows on to the
/// next operator, so the side is netted before its rows are paired ([`net_side`]).
/// Rows of one sign cannot cancel, whatever their values.
/// It is asked only of a completed key's rows, which the pairing reads anyway.
fn mixed_signs<T: ColTime>(chunks: &[&CorgiChunk<T, Diff>], runs: &[Run]) -> bool {
    // One row has one sign.
    if let [(_, s, e)] = runs { if e - s == 1 { return false; } }
    let (mut positive, mut negative) = (false, false);
    for &(chunk, s, e) in runs {
        for &diff in &chunks[chunk].diffs()[s..e] {
            positive |= diff > 0;
            negative |= diff < 0;
        }
    }
    positive && negative
}

/// One side's rows of several keys, netted: for each key of `keys`, in order, the rows that are
/// left once equal values at equal times, advanced by `lower`, have their diffs summed and the
/// zeros dropped. Each is `(key's index in keys, chunk, row, diff)`, the row standing for all
/// the rows summed into it.
///
/// This is one sort of all the keys' rows, by value and time within each key, and not a pass
/// for each key.
fn net_side<T: ColTime>(chunks: &[&CorgiChunk<T, Diff>], lower: &T, keys: &[&[Run]]) -> Vec<(usize, usize, usize, Diff)> {
    let (mut labels, mut tags, mut offs) = (Vec::new(), Vec::new(), Vec::new());
    for (label, runs) in keys.iter().enumerate() {
        for &(chunk, s, e) in runs.iter() {
            labels.extend(std::iter::repeat_n(label as u64, e - s));
            tags.extend(std::iter::repeat_n(chunk, e - s));
            offs.extend(s..e);
        }
    }
    let vals: Vec<Option<&CValue>> = chunks.iter().map(|c| Some(c.vals())).collect();
    let bound: Vec<u64> = lower.coordinates().collect();
    let width = chunks.iter().map(|c| c.times().width()).max().unwrap_or(0).max(bound.len());
    let mut fields = vec![gather_lanes(&vals, &tags, &offs)];
    // The times only need to group, not to order, so each lane is read as it is stored.
    fields.extend((0..width).map(|j| {
        let low = bound.get(j).copied().unwrap_or(0);
        CValue::i64(tags.iter().zip(&offs).map(|(&chunk, &row)| chunks[chunk].times().coordinate(j, row).max(low) as i64).collect())
    }));
    let (perm, groups) = sort_blocks(&labels, &CValue::Prod(fields));
    let mut kept = Vec::new();
    let mut start = 0;
    for group in groups.chunk_by(|a, b| a == b) {
        let end = start + group.len();
        let diff: Diff = perm[start..end].iter().map(|&i| chunks[tags[i]].diffs()[offs[i]]).sum();
        // The sort keeps each key's rows in the key's place, so the label there is still the key's.
        if diff != 0 { kept.push((labels[start] as usize, tags[perm[start]], offs[perm[start]], diff)); }
        start = end;
    }
    kept
}

impl<T: ColTime> CorgiJoinBackend<T> {
    /// The projection over inputs of the key's and both values' shapes. A type error is a panic
    /// with corgi's message: install type-checked the program, so it is not reached.
    pub(crate) fn new(projection: &Lambda, [k, v0, v1]: [Shape; 3]) -> Self {
        let compiled = compile_projection(projection, &[k.clone(), v0.clone(), v1.clone()])
            .unwrap_or_else(|e| panic!("join projection: type error at shapes ({k}, {v0}, {v1}): {e}"));
        let input = Shape::Prod(vec![k, v0, v1]);
        let narrowed = corgi::narrow(compiled, &Demand::All)
            .unwrap_or_else(|e| panic!("join projection: narrowing a compiled graph at {input}: {e}"));
        CorgiJoinBackend { colliding: Vec::new(), completed: Pairs::default(), graph: narrowed.graph, reads: Reads::new(narrowed.input, &input), _t: PhantomData }
    }

    /// The output container of matched rows: their `(key, val0, val1)` columns, as gathered for
    /// what the projection reads, through the projection, with their output times and diffs.
    fn project(&self, [key, val0, val1]: [CValue; 3], times: ColTimes<T>, diffs: Vec<Diff>) -> CorgiContainer<T, Diff> {
        let argument = self.reads.argument.project(CValue::Prod(vec![key, val0, val1])).expect("the join's input has the shape its demand is on");
        let shape = shape_of_value(&argument);
        assert!(shape == self.reads.shape, "join projection: input read as {shape} reached an operator narrowed to {}", self.reads.shape);
        let mut cols = corgi::eval_graph(&self.graph, argument).into_prod("corgi join projection").unwrap();
        let vals = cols.pop().unwrap();
        let keys = cols.pop().unwrap();
        CorgiContainer { keys, vals, times, diffs }
    }

    /// The output of row `pairs` of the two inputs' chunks: their columns gathered into the
    /// projection, times joined lane-wise with each other and with `lower`, and diffs multiplied.
    fn cross_rows(&self, chunks0: &[&CorgiChunk<T, Diff>], chunks1: &[&CorgiChunk<T, Diff>], pairs: &Pairs, lower: &T, output: &mut Vec<CorgiContainer<T, Diff>>) {
        let [dk, d0, d1] = &self.reads.columns;
        let keys0 = demanded(dk, chunks0.iter().map(|c| recover_key_ref(c.keys())));
        let vals0 = demanded(d0, chunks0.iter().map(|c| c.vals()));
        let vals1 = demanded(d1, chunks1.iter().map(|c| c.vals()));
        let keys0: Vec<Option<&CValue>> = keys0.iter().map(|c| Some(&**c)).collect();
        let vals0: Vec<Option<&CValue>> = vals0.iter().map(|c| Some(&**c)).collect();
        let vals1: Vec<Option<&CValue>> = vals1.iter().map(|c| Some(&**c)).collect();
        let bound: Vec<u64> = lower.coordinates().collect();
        let width = chunks0.iter().chain(chunks1).map(|c| c.times().width()).max().unwrap_or(0).max(bound.len());
        let mut start = 0;
        while start < pairs.row0.len() {
            let end = (start + TARGET_OUT).min(pairs.row0.len());
            let (chunk0, row0, chunk1, row1) = (&pairs.chunk0[start..end], &pairs.row0[start..end], &pairs.chunk1[start..end], &pairs.row1[start..end]);
            let rows = end - start;
            let kc = if *dk == Demand::None { CValue::Unit(rows) } else { gather_lanes(&keys0, chunk0, row0) };
            let v0 = if *d0 == Demand::None { CValue::Unit(rows) } else { gather_lanes(&vals0, chunk0, row0) };
            let v1 = if *d1 == Demand::None { CValue::Unit(rows) } else { gather_lanes(&vals1, chunk1, row1) };
            let lanes = (0..width).map(|j| {
                let low = bound.get(j).copied().unwrap_or(0);
                (0..rows).map(|i| chunks0[chunk0[i]].times().coordinate(j, row0[i]).max(chunks1[chunk1[i]].times().coordinate(j, row1[i])).max(low)).collect()
            }).collect();
            let diffs = (0..rows).map(|i| match pairs.netted_from {
                Some(from) if start + i >= from => pairs.diffs[start + i - from],
                _ => chunks0[chunk0[i]].diffs()[row0[i]] * chunks1[chunk1[i]].diffs()[row1[i]],
            }).collect();
            output.push(self.project([kc, v0, v1], ColTimes::from_raw_lanes(lanes, rows), diffs));
            start = end;
        }
    }
}

impl<T: ColTime> ProxyJoinBackend<T, CBatch<T>, CBatch<T>> for CorgiJoinBackend<T> {
    type Key = i64;
    type V0 = u64;
    type V1 = u64;
    type R0 = Diff;
    type R1 = Diff;
    type ROut = Diff;
    type Output = CorgiContainer<T, Diff>;

    fn advance(
        &mut self,
        instance: &JoinInstance<T, CBatch<T>, CBatch<T>>,
        from: &mut KeyPosition<i64>,
        bridge0: &mut ProxyBridge<T, Diff, i64>,
        bridge1: &mut ProxyBridge<T, Diff, i64>,
    ) {
        debug_assert!(self.completed.row0.is_empty(), "`cross` follows every `advance`, and takes its completed pairs");
        let mut next = match *from {
            KeyPosition::Start => Some(i64::MIN),
            KeyPosition::At(key) => Some(key),
            KeyPosition::End => return,
        };
        self.colliding.clear();
        let chunks0 = side_chunks(instance.batches0);
        let chunks1 = side_chunks(instance.batches1);
        if chunks0.is_empty() || chunks1.is_empty() {
            *from = KeyPosition::End;
            return;
        }
        debug_assert_eq!(
            key_is_hashed(chunks0[0].keys()),
            key_is_hashed(chunks1[0].keys()),
            "join sides disagree on whether the arrangement key carries a hash lane",
        );
        debug_assert!(
            chunks0.iter().all(|chunk| key_is_hashed(chunk.keys()) == key_is_hashed(chunks0[0].keys()))
                && chunks1.iter().all(|chunk| key_is_hashed(chunk.keys()) == key_is_hashed(chunks1[0].keys())),
            "chunks of one join key disagree on whether they carry a hash lane",
        );
        // Every arrangement key leads with its identifier and is sorted by it, so one resumable
        // leaf walk applies to every key shape. A hash collision is retained in proxy space and
        // recorded here; `cross` then filters only that token's unequal real-key pairs. Restarting
        // through a whole-key walk is not valid after an earlier block has already retired.
        // Tokens are minted on values projected to what the projection reads, so values that
        // differ only in unread fields share a token and consolidate before matching. `cross`
        // reads the projected columns at the token's coordinate, which any row of the token serves.
        let proj0 = SideProj::new(&chunks0, &self.reads.columns[1]);
        let proj1 = SideProj::new(&chunks1, &self.reads.columns[2]);
        advance_leaf(
            &chunks0,
            &chunks1,
            instance.lower,
            &mut next,
            bridge0,
            bridge1,
            &mut self.colliding,
            (&proj0, &proj1),
            (!instance.present_all).then_some(&mut self.completed),
        );
        *from = next.map_or(KeyPosition::End, KeyPosition::At);
    }

    fn cross(
        &mut self,
        instance: &JoinInstance<T, CBatch<T>, CBatch<T>>,
        matches: &mut JoinMatches<T, Diff, i64>,
        output: &mut Vec<CorgiContainer<T, Diff>>,
    ) {
        let completed = std::mem::take(&mut self.completed);
        // The tactic calls `cross` after every block, including those with nothing to cross.
        if completed.row0.is_empty() && matches.ids.is_empty() { return; }
        let chunks0 = side_chunks(instance.batches0);
        let chunks1 = side_chunks(instance.batches1);
        // The completed keys' pairs first, then the tactic's matches.
        if !completed.row0.is_empty() {
            self.cross_rows(&chunks0, &chunks1, &completed, instance.lower, output);
        }
        if matches.ids.is_empty() { return; }
        // Project columns before gathering; collision checks still compare the original keys.
        let [dk, d0, d1] = &self.reads.columns;
        let keys0 = demanded(dk, chunks0.iter().map(|c| recover_key_ref(c.keys())));
        let vals0 = demanded(d0, chunks0.iter().map(|c| c.vals()));
        let vals1 = demanded(d1, chunks1.iter().map(|c| c.vals()));
        let keys0: Vec<Option<&CValue>> = keys0.iter().map(|c| Some(&**c)).collect();
        let vals0: Vec<Option<&CValue>> = vals0.iter().map(|c| Some(&**c)).collect();
        let vals1: Vec<Option<&CValue>> = vals1.iter().map(|c| Some(&**c)).collect();

        let collision_columns: Vec<_> = if self.colliding.is_empty() { Vec::new() } else {
            chunks0.iter().chain(&chunks1).map(|c| recover_key_ref(c.keys())).collect()
        };
        let primitive_keys = !key_is_hashed(chunks0[0].keys());
        let primitive0 = primitive_values(chunks0[0]);
        let primitive1 = primitive_values(chunks1[0]);
        let n = matches.ids.len();
        let (mut tag0, mut off0) = (Vec::new(), Vec::new());
        let (mut tag1, mut off1) = (Vec::new(), Vec::new());
        let mut start = 0usize;
        while start < n {
            let end = (start + TARGET_OUT).min(n);
            tag0.clear(); off0.clear(); tag1.clear(); off1.clear();
            let kept = if self.colliding.is_empty() {
                // Only coordinate-backed values need gather indices.
                for (_, (c0, c1)) in &matches.ids[start..end] {
                    if !primitive0 {
                        tag0.push((c0 >> COORD_BITS) as usize);
                        off0.push((c0 & ((1 << COORD_BITS) - 1)) as usize);
                    }
                    if !primitive1 {
                        tag1.push((c1 >> COORD_BITS) as usize);
                        off1.push((c1 & ((1 << COORD_BITS) - 1)) as usize);
                    }
                }
                None
            } else {
                let mut pairs = Vec::new();
                for (index, (token, (c0, c1))) in matches.ids[start..end].iter().enumerate() {
                    if self.colliding.binary_search(token).is_ok() {
                        pairs.push(((c0 >> COORD_BITS) as usize, chunks0.len() + (c1 >> COORD_BITS) as usize,
                                    (c0 & ((1 << COORD_BITS) - 1)) as usize, (c1 & ((1 << COORD_BITS) - 1)) as usize, index));
                    }
                }
                let mut keep = vec![true; end - start];
                pairs.sort_unstable_by_key(|p| (p.0, p.1, p.4));
                let (mut left, mut right) = (Vec::new(), Vec::new());
                for group in pairs.chunk_by(|a, b| (a.0, a.1) == (b.0, b.1)) {
                    left.clear(); right.clear();
                    left.extend(group.iter().map(|p| p.2));
                    right.extend(group.iter().map(|p| p.3));
                    let signs = equal_idx(collision_columns[group[0].0], collision_columns[group[0].1], &left, &right);
                    for (pair, sign) in group.iter().zip(signs) { keep[pair.4] = sign == 0; }
                }
                let mut kept = Vec::with_capacity(end - start);
                for (index, (_, (c0, c1))) in matches.ids[start..end].iter().enumerate() {
                    let c0_chunk = (c0 >> COORD_BITS) as usize;
                    let c0_row = (c0 & ((1 << COORD_BITS) - 1)) as usize;
                    let c1_chunk = (c1 >> COORD_BITS) as usize;
                    let c1_row = (c1 & ((1 << COORD_BITS) - 1)) as usize;
                    if !keep[index] { continue; }
                    kept.push(start + index);
                    tag0.push(c0_chunk);
                    off0.push(c0_row);
                    tag1.push(c1_chunk);
                    off1.push(c1_row);
                }
                Some(kept)
            };
            if kept.as_ref().is_some_and(Vec::is_empty) {
                start = end;
                continue;
            }
            // Gather only the declared key borrowed above: the arrangement's leading
            // identifier is not part of the projection input. A downstream arrange derives
            // the identifier for the projected key.
            let ids = &matches.ids[start..end];
            let rows = kept.as_ref().map_or(end - start, Vec::len);
            let kc = if *dk == Demand::None { CValue::Unit(rows) } else if primitive_keys {
                dk.project(rewrap(chunks0[0].keys(), ids.iter().map(|x| x.0).collect())).expect("the join key's demand")
            } else { gather_lanes(&keys0, &tag0, &off0) };
            let v0 = if *d0 == Demand::None { CValue::Unit(rows) } else if primitive0 {
                d0.project(rewrap(chunks0[0].vals(), ids.iter().map(|x| x.1.0 as i64).collect())).expect("the join value 0's demand")
            } else { gather_lanes(&vals0, &tag0, &off0) };
            let v1 = if *d1 == Demand::None { CValue::Unit(rows) } else if primitive1 {
                d1.project(rewrap(chunks1[0].vals(), ids.iter().map(|x| x.1.1 as i64).collect())).expect("the join value 1's demand")
            } else { gather_lanes(&vals1, &tag1, &off1) };
            let times = kept.as_ref().map_or_else(
                || matches.times[start..end].iter().collect(),
                |kept| kept.iter().map(|&index| &matches.times[index]).collect(),
            );
            let diffs = kept.as_ref().map_or_else(
                || matches.diffs[start..end].to_vec(),
                |kept| kept.iter().map(|&index| matches.diffs[index]).collect(),
            );
            output.push(self.project([kc, v0, v1], times, diffs));
            start = end;
        }
    }
}

/// Borrow whole columns; projecting partial ones shares their leaves and costs only their shape.
pub(super) fn demanded<'a>(demand: &Demand, cols: impl Iterator<Item = &'a CValue>) -> Vec<Cow<'a, CValue>> {
    if *demand == Demand::None { return Vec::new(); }
    cols.map(|col| match demand {
        Demand::All => Cow::Borrowed(col),
        _ => Cow::Owned(demand.project(col.clone()).expect("the join column's demand")),
    }).collect()
}

/// Coordinates remain necessary for structured keys, including collision validation.
pub(super) fn primitive_values<T: ColTime>(chunk: &CorgiChunk<T, Diff>) -> bool {
    !key_is_hashed(chunk.keys()) && int_lane(chunk.vals()).is_some()
}

/// The instance's chunks on one side, in the deterministic order coordinates index
/// (batches in order, chunks in order) — `advance` and `cross` must agree on it.
fn side_chunks<T: ColTime>(batches: &[CBatch<T>]) -> Vec<&CorgiChunk<T, Diff>> {
    // Coordinates use this same filtered ordering in `advance` and `cross`. Excluding empties here
    // makes the convention explicit and keeps `ident` from inspecting a shape-less `Unit(0)` key.
    let chunks: Vec<&CorgiChunk<T, Diff>> = batches
        .iter()
        .flat_map(|batch| batch.chunks.iter())
        .filter(|chunk| chunk.len() > 0)
        .collect();
    assert!(chunks.len() < (1 << (64 - COORD_BITS)), "too many chunks for coordinate packing");
    chunks
}

/// The column flattened to `i64` leaf lanes: `Some(lanes)` when it is a (possibly nested)
/// product of integer leaves held as `i64`s, so row order
/// is the lexicographic order of the lane tuples. `Sum`/`List`/float leaves, and integers held
/// narrower, give `None` (structural compares).
fn leaf_lanes(col: &CValue) -> Option<Vec<&[i64]>> {
    fn walk<'a>(col: &'a CValue, out: &mut Vec<&'a [i64]>) -> bool {
        match col {
            CValue::Prim(_) => match int_lane(col) { Some(lane) => { out.push(lane); true }, None => false },
            CValue::Prod(fields) => fields.iter().all(|f| walk(f, out)),
            CValue::Unit(_) => true,
            _ => false,
        }
    }
    let mut out = Vec::new();
    if walk(col, &mut out) { Some(out) } else { None }
}

/// Whether to borrow comparison lanes. Primitive tokens read their source lane directly.
pub(super) fn leaf_valued<T: ColTime>(chunks: &[&CorgiChunk<T, Diff>]) -> bool {
    !primitive_values(chunks[0])
        && chunks.iter().filter(|c| c.len() > 0).all(|c| leaf_lanes(c.vals()).is_some())
}

/// How one side's values are compared when minting tokens: projected to what the projection
/// reads of them. Equal projected values share one token, so values that differ only in unread
/// fields consolidate before matching.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum ProjKind {
    /// The projection reads the whole value: tokens as before.
    Whole,
    /// The projection reads nothing of the value: one token per key.
    Collapse,
    /// It reads leading leaf lanes, so the stored order still groups equal projected values.
    Grouped,
    /// It reads fields the stored order does not group by: sort to find equal projected values.
    Ungrouped,
}

/// One side's projection for tokens, over its chunks.
pub(super) struct SideProj {
    pub(super) kind: ProjKind,
    /// Whether the values are leaf-laned (and not primitive tokens), so lanes are borrowed.
    pub(super) leaf: bool,
    /// The kept leaf lanes, in `leaf_lanes` order, when `leaf`.
    keep: Vec<usize>,
    /// Per chunk, the projected column for structural compares, when not `leaf`.
    cols: Vec<CValue>,
}

impl SideProj {
    /// For `demand` on the values of `chunks` (in coordinate order).
    pub(super) fn new<T: ColTime>(chunks: &[&CorgiChunk<T, Diff>], demand: &Demand) -> Self {
        let leaf = !chunks.is_empty() && leaf_valued(chunks);
        let whole = SideProj { kind: ProjKind::Whole, leaf, keep: Vec::new(), cols: Vec::new() };
        let Some(first) = chunks.first() else { return whole };
        if *demand == Demand::All { return whole; }
        if *demand == Demand::None || matches!(demand.project(first.vals().clone()), Ok(CValue::Unit(_))) {
            return SideProj { kind: ProjKind::Collapse, leaf, keep: Vec::new(), cols: Vec::new() };
        }
        if primitive_values(first) { return whole; }
        if leaf {
            let mut keep = Vec::new();
            demanded_lanes(first.vals(), demand, &mut 0, &mut keep);
            let total = leaf_lanes(first.vals()).map_or(0, |l| l.len());
            if keep.is_empty() { return SideProj { kind: ProjKind::Collapse, leaf, keep, cols: Vec::new() }; }
            if keep.len() == total { return whole; }
            let grouped = keep.iter().enumerate().all(|(i, &l)| i == l);
            SideProj { kind: if grouped { ProjKind::Grouped } else { ProjKind::Ungrouped }, leaf, keep, cols: Vec::new() }
        } else {
            let cols = chunks.iter().map(|c| demand.project(c.vals().clone()).expect("the join value's demand")).collect();
            SideProj { kind: ProjKind::Ungrouped, leaf, keep: Vec::new(), cols }
        }
    }
    /// The comparison lanes of `chunk`: its kept leaf lanes.
    fn lanes<'a, T: ColTime>(&self, chunk: &'a CorgiChunk<T, Diff>) -> Option<Vec<&'a [i64]>> {
        if !self.leaf { return None; }
        let lanes = leaf_lanes(chunk.vals()).expect("a leaf-laned side");
        Some(match self.kind {
            ProjKind::Whole => lanes,
            ProjKind::Collapse => Vec::new(),
            _ => self.keep.iter().map(|&i| lanes[i]).collect(),
        })
    }
    /// The column of chunk `cid` to compare structurally, when projected.
    fn pcol(&self, cid: usize) -> Option<&CValue> {
        self.cols.get(cid)
    }
}

/// The leaf lanes of `col`, in `leaf_lanes` order, that `demand` keeps.
fn demanded_lanes(col: &CValue, demand: &Demand, next: &mut usize, keep: &mut Vec<usize>) {
    match col {
        CValue::Prim(_) => { if *demand != Demand::None { keep.push(*next); } *next += 1; }
        CValue::Prod(fields) => match demand {
            Demand::Prod(needs) => for (f, d) in fields.iter().zip(needs) { demanded_lanes(f, d, next, keep) },
            d => for f in fields { demanded_lanes(f, d, next, keep) },
        },
        CValue::Unit(_) => {}
        _ => unreachable!("leaf-laned values are products of integer leaves"),
    }
}

/// One key's records in one chunk: absolute rows `[s, e)`. When the vals are leaf-laned,
/// `vals` borrows the chunk's kept leaf lanes, indexed by absolute row; `None`
/// falls back to structural compares against `pcol`, or the chunk itself.
pub(super) struct RunRef<'a, T: ColTime> {
    pub(super) chunk: &'a CorgiChunk<T, Diff>,
    pub(super) cid: usize,
    pub(super) s: usize,
    pub(super) e: usize,
    pub(super) vals: Option<&'a [&'a [i64]]>,
    pub(super) pcol: Option<&'a CValue>,
    pub(super) kind: ProjKind,
}

impl<'a, T: ColTime> RunRef<'a, T> {
    fn col(&self) -> &'a CValue {
        self.pcol.unwrap_or(self.chunk.vals())
    }
    fn val_cmp(&self, row: usize, other: &Self, orow: usize) -> Ordering {
        if self.kind == ProjKind::Collapse { return Ordering::Equal; }
        match (self.vals, other.vals) {
            (Some(a), Some(b)) => a.iter().zip(b).map(|(la, lb)| la[row].cmp(&lb[orow])).find(|o| *o != Ordering::Equal).unwrap_or(Ordering::Equal),
            _ => compare_at(self.col(), row, other.col(), orow),
        }
    }
    fn val_less(&self, row: usize, other: &Self, orow: usize) -> bool {
        self.val_cmp(row, other, orow) == Ordering::Less
    }
    fn val_eq(&self, row: usize, other: &Self, orow: usize) -> bool {
        self.val_cmp(row, other, orow) == Ordering::Equal
    }
    /// One past the end of the value run starting at `row`.
    fn val_run_end(&self, row: usize) -> usize {
        let mut r = row + 1;
        while r < self.e && self.val_eq(r, self, row) { r += 1; }
        r
    }
}

/// Per-key staging for one side: consolidated `(token, time, diff)` entries, built in scratch
/// so a side that nets to zero suppresses the key before anything reaches the bridges.
pub(super) struct SideScratch<T> {
    pub(super) entries: Vec<(u64, T, Diff)>,
    /// `(time, diff)` scratch for one value's cross-chunk merge.
    tds: Vec<(T, Diff)>,
    /// Cross-chunk value-merge heads, reused across keys.
    heads: Vec<(usize, usize)>,
    /// Row indices sorted by projected value, reused across keys.
    idx: Vec<(u32, u32)>,
}

impl<T: ColTime> SideScratch<T> {
    pub(super) fn new() -> Self {
        SideScratch { entries: Vec::new(), tds: Vec::new(), heads: Vec::new(), idx: Vec::new() }
    }

    fn push(&mut self, coord: u64, time: T, diff: Diff) {
        match self.entries.last_mut() {
            Some((lc, lt, ld)) if *lc == coord && *lt == time => *ld += diff,
            _ => self.entries.push((coord, time, diff)),
        }
    }

    /// Stage one key's records from `runs` (its equal-key row ranges, one per chunk holding it):
    /// primitive values identify themselves; other values share their least occurrence's
    /// coordinate across chunks. Times are advanced by `lower`, consolidated, zeros dropped.
    /// Entries end sorted by `(token, time)`.
    pub(super) fn stage_runs(&mut self, runs: &[RunRef<'_, T>], lower: &T) {
        self.entries.clear();
        let Some(first) = runs.first() else { return };
        if primitive_values(first.chunk) {
            for run in runs {
                let ids = int_lane(run.chunk.vals()).unwrap();
                // An unread primitive value is one token (`cross` never reads it back).
                let collapse = run.kind == ProjKind::Collapse;
                self.entries.extend((run.s..run.e).map(|row| {
                    let time = run.chunk.times().get_joined(row, lower);
                    (if collapse { 0 } else { ids[row] as u64 }, time, run.chunk.diffs()[row])
                }));
            }
            differential_dataflow::consolidation::consolidate_updates(&mut self.entries);
            return;
        }
        if first.kind == ProjKind::Ungrouped && (runs.len() != 1 || first.e - first.s > 1) {
            // The stored order does not group equal projected values: sort the key's rows by
            // projected value, then consolidate each value's times. The canonical coordinate
            // is the least contributing (chunk, row).
            // Move the scratch out while `push` borrows `self`, then return its capacity.
            let mut idx = std::mem::take(&mut self.idx);
            idx.clear();
            for (i, run) in runs.iter().enumerate() { idx.extend((run.s..run.e).map(|row| (i as u32, row as u32))); }
            idx.sort_unstable_by(|a, b| runs[a.0 as usize].val_cmp(a.1 as usize, &runs[b.0 as usize], b.1 as usize));
            let mut start = 0;
            while start < idx.len() {
                let (ri, row) = (idx[start].0 as usize, idx[start].1 as usize);
                let mut end = start + 1;
                while end < idx.len() && runs[idx[end].0 as usize].val_eq(idx[end].1 as usize, &runs[ri], row) { end += 1; }
                let mut coord = u64::MAX;
                self.tds.clear();
                for &(i, r) in &idx[start..end] {
                    let run = &runs[i as usize];
                    coord = coord.min(((run.cid as u64) << COORD_BITS) | r as u64);
                    self.tds.push((run.chunk.times().get_joined(r as usize, lower), run.chunk.diffs()[r as usize]));
                }
                self.tds.sort_by(|a, b| a.0.cmp(&b.0));
                let tds = std::mem::take(&mut self.tds);
                for (time, diff) in tds.iter() { self.push(coord, time.clone(), *diff); }
                self.tds = tds;
                start = end;
            }
            self.idx = idx;
            self.entries.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
            self.entries.retain(|(_, _, d)| *d != 0);
            return;
        }
        if let [run] = runs {
            // Single-chunk run: values are grouped and coordinates of successive value
            // runs ascend. Joining product times need not preserve their total order,
            // so adjacent consolidation suffices only if advanced times stay ordered.
            let coord_hi = (run.cid as u64) << COORD_BITS;
            let mut v_start = run.s;
            for row in run.s..run.e {
                if row > v_start && !run.val_eq(row, run, v_start) {
                    v_start = row;
                }
                let time = run.chunk.times().get_joined(row, lower);
                self.push(coord_hi | v_start as u64, time, run.chunk.diffs()[row]);
            }
            if self.entries.windows(2).any(|w| w[0].0 == w[1].0 && w[0].1 > w[1].1) {
                differential_dataflow::consolidation::consolidate_updates(&mut self.entries);
            }
        } else {
            // Cross-chunk merge by value content: heads are (run index, row); each step takes
            // the least value among heads, drains every chunk's sub-run of it into `tds`,
            // and consolidates. The canonical coordinate is the least contributing (chunk, row).
            // Move the scratch out while `push` borrows `self`, then return its capacity.
            let mut heads = std::mem::take(&mut self.heads);
            heads.clear();
            heads.extend(runs.iter().enumerate().map(|(i, r)| (i, r.s)));
            while !heads.is_empty() {
                let mut min = 0usize;
                for h in 1..heads.len() {
                    if runs[heads[h].0].val_less(heads[h].1, &runs[heads[min].0], heads[min].1) {
                        min = h;
                    }
                }
                let (ri_min, row_min) = heads[min];
                self.tds.clear();
                let mut coord = u64::MAX;
                let mut h = 0usize;
                while h < heads.len() {
                    let (ri, row) = heads[h];
                    let run = &runs[ri];
                    if run.val_eq(row, &runs[ri_min], row_min) {
                        let r_end = run.val_run_end(row);
                        for r in row..r_end {
                            let time = run.chunk.times().get_joined(r, lower);
                            self.tds.push((time, run.chunk.diffs()[r]));
                        }
                        coord = coord.min(((run.cid as u64) << COORD_BITS) | row as u64);
                        if r_end < run.e { heads[h] = (ri, r_end); h += 1; } else { heads.swap_remove(h); }
                    } else {
                        h += 1;
                    }
                }
                self.tds.sort_by(|a, b| a.0.cmp(&b.0));
                let tds = std::mem::take(&mut self.tds);
                for (time, diff) in tds.iter() {
                    self.push(coord, time.clone(), *diff);
                }
                self.tds = tds;
                self.tds.clear();
            }
            self.heads = heads;
            // Canonical coordinates interleave chunks; restore bridge order.
            self.entries.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
        }
        self.entries.retain(|(_, _, d)| *d != 0);
    }

    /// Move the staged entries into `bridge` under group token `k`.
    pub(super) fn emit(&mut self, k: i64, bridge: &mut ProxyBridge<T, Diff, i64>) -> usize {
        let n = self.entries.len();
        bridge.extend(self.entries.drain(..).map(|(coord, t, d)| ((k, coord), t, d)));
        n
    }
}

/// The identifier column of a chunk's keys, as a sorted `i64` slice. Every arrangement key
/// leads with its identifier ([`present_key`](crate::runtime::chunk::present_key)), so this is
/// always available and always ordered — no gather, no copy.
fn ident<'a, T: ColTime>(chunk: &'a CorgiChunk<T, Diff>) -> &'a [i64] {
    crate::runtime::chunk::ident(chunk.keys())
}

/// The block's exclusive identifier bound: for each chunk with more than `budget` rows left,
/// take the identifier `budget` rows in and bump past its whole run, then choose the least of
/// those bounds. A chunk may therefore contribute `budget + run length` rows rather than obeying
/// a hard cap; the expansion is what keeps one identifier wholly within one block. `None` when no
/// chunk is over budget — the block runs to the end and the walk is exhausted.
///
/// This is the whole point of a totally ordered identifier: the block's extent is decided by
/// reading ONE value per chunk, before anything is read in bulk, so each chunk can then be
/// read exactly once over exactly the rows the block needs. Deciding it the other way round —
/// read a fixed number of rows, then discover how far the block can reach — leaves whatever
/// was read past the bound to be discarded and read again next block, and the further apart
/// the chunks' key densities are, the more that is.
fn block_horizon<T: ColTime>(chunks: &[&CorgiChunk<T, Diff>], starts: &[usize], budget: usize) -> Option<i64> {
    let mut horizon: Option<i64> = None;
    for (c, &s) in chunks.iter().zip(starts) {
        let lane = ident(c);
        if lane.len() - s <= budget {
            continue;
        }
        // Past the whole run of the identifier at the budget, so the block ends at a key
        // boundary. `None` on overflow means nothing can exceed it: run to the end.
        let Some(bound) = lane[s + budget].checked_add(1) else { continue };
        horizon = Some(horizon.map_or(bound, |h: i64| h.min(bound)));
    }
    horizon
}

/// Where each chunk's block ends: the first row at or past `horizon`, or its end.
fn block_ends<T: ColTime>(chunks: &[&CorgiChunk<T, Diff>], horizon: Option<i64>) -> Vec<usize> {
    chunks.iter().map(|c| match horizon {
        Some(h) => ident(c).partition_point(|&x| x < h),
        None => c.len(),
    }).collect()
}

/// Rows per chunk per block. Bounds the block's working set; nothing outside it is read.
const PULL: usize = 1 << 14;

/// One key's rows in one chunk: the chunk, and its rows `[s, e)`.
type Run = (usize, usize, usize);

/// One side's keys in a block, ascending and distinct, each with its runs in chunk order.
struct SideRuns {
    keys: Vec<i64>,
    /// The runs of `keys[j]` are `runs[ends[j - 1]..ends[j]]`.
    ends: Vec<usize>,
    runs: Vec<Run>,
}

impl SideRuns {
    /// The runs of key `j`.
    fn of(&self, j: usize) -> &[Run] {
        &self.runs[if j == 0 { 0 } else { self.ends[j - 1] }..self.ends[j]]
    }

    /// Group `found`, runs with their keys, by key. The runs are in key order, or else in key
    /// order within each chunk and the chunks in order.
    fn group(mut found: Vec<(i64, Run)>) -> Self {
        // A stable sort keeps a key's runs in chunk order.
        if !found.is_sorted_by_key(|run| run.0) { found.sort_by_key(|run| run.0); }
        let mut side = SideRuns { keys: Vec::new(), ends: Vec::new(), runs: Vec::with_capacity(found.len()) };
        for (key, run) in found {
            if side.keys.last() == Some(&key) { *side.ends.last_mut().unwrap() += 1; }
            else { side.keys.push(key); side.ends.push(side.runs.len() + 1); }
            side.runs.push(run);
        }
        side
    }

    /// The runs of rows `starts[c]..ends[c]` of each chunk `c`: one pass over each chunk's identifiers.
    fn scan<T: ColTime>(chunks: &[&CorgiChunk<T, Diff>], starts: &[usize], ends: &[usize]) -> Self {
        let mut found = Vec::new();
        for (cid, chunk) in chunks.iter().enumerate() {
            let lane = &ident(chunk)[..ends[cid]];
            let mut s = starts[cid];
            while s < lane.len() {
                let key = lane[s];
                let mut e = s + 1;
                while e < lane.len() && lane[e] == key { e += 1; }
                found.push((key, (cid, s, e)));
                s = e;
            }
        }
        Self::group(found)
    }

    /// The runs of `keys`, ascending and distinct, in `chunks`: one search of each chunk.
    /// Only the keys some chunk holds are kept.
    fn probe<T: ColTime>(chunks: &[&CorgiChunk<T, Diff>], keys: &[i64]) -> Self {
        let (mut found, mut ranges, mut scratch) = (Vec::new(), Vec::new(), Vec::new());
        for (cid, chunk) in chunks.iter().enumerate() {
            matching_ranges(keys, ident(chunk), &mut scratch, &mut ranges);
            found.extend(ranges.iter().map(|(j, range)| (keys[*j], (cid, range.start, range.end))));
        }
        Self::group(found)
    }
}

/// The keys both sides hold, ascending: each as its index in the one side and in the other.
fn matched(side0: &SideRuns, side1: &SideRuns) -> Vec<(usize, usize)> {
    let mut both = Vec::new();
    let (mut j0, mut j1) = (0, 0);
    while j0 < side0.keys.len() && j1 < side1.keys.len() {
        match side0.keys[j0].cmp(&side1.keys[j1]) {
            Ordering::Less => j0 += 1,
            Ordering::Greater => j1 += 1,
            Ordering::Equal => { both.push((j0, j1)); j0 += 1; j1 += 1; }
        }
    }
    both
}

/// One side's chunks as the staging paths read them: each chunk's comparison lanes, borrowed.
struct SideView<'a, T: ColTime> {
    chunks: &'a [&'a CorgiChunk<T, Diff>],
    proj: &'a SideProj,
    lanes: Vec<Option<Vec<&'a [i64]>>>,
}

impl<'a, T: ColTime> SideView<'a, T> {
    fn new(chunks: &'a [&'a CorgiChunk<T, Diff>], proj: &'a SideProj) -> Self {
        SideView { chunks, proj, lanes: chunks.iter().map(|chunk| proj.lanes(chunk)).collect() }
    }
    /// `runs` as [`RunRef`]s.
    fn refs(&self, runs: &[Run]) -> smallvec::SmallVec<[RunRef<'_, T>; 4]> {
        runs.iter().map(|&(cid, s, e)| RunRef {
            chunk: self.chunks[cid],
            cid,
            s,
            e,
            vals: self.lanes[cid].as_deref(),
            pcol: self.proj.pcol(cid),
            kind: self.proj.kind,
        }).collect()
    }
}

/// The batched probe of one PROBEE-side chunk: per driver key, its equal-range in the
/// chunk, borrowing its full value lanes when leaf-shaped. Matched rows index the
/// original lanes directly, without a gather or an index buffer.
pub(super) struct Probe<'a, T: ColTime> {
    pub(super) chunk: &'a CorgiChunk<T, Diff>,
    pub(super) cid: usize,
    pub(super) lo: Vec<usize>,
    pub(super) hi: Vec<usize>,
    vals: Option<Vec<&'a [i64]>>,
    pcol: Option<&'a CValue>,
    kind: ProjKind,
}

impl<'a, T: ColTime> Probe<'a, T> {
    pub(super) fn new(chunk: &'a CorgiChunk<T, Diff>, cid: usize, needles: &[i64], proj: &'a SideProj) -> Self {
        let keys = ident(chunk);
        let (mut lo, mut hi) = (vec![0; needles.len()], vec![0; needles.len()]);
        let mut found = Vec::new();
        matching_ranges(needles, keys, &mut Vec::new(), &mut found);
        for (j, range) in found {
            lo[j] = range.start;
            hi[j] = range.end;
        }
        Probe { chunk, cid, lo, hi, vals: proj.lanes(chunk), pcol: proj.pcol(cid), kind: proj.kind }
    }
    /// The run of driver key `j` in this chunk, if any.
    pub(super) fn run_ref(&self, j: usize) -> Option<RunRef<'_, T>> {
        let (s, e) = (self.lo[j], self.hi[j]);
        if s == e { return None; }
        Some(RunRef {
            chunk: self.chunk,
            cid: self.cid,
            s,
            e,
            vals: self.vals.as_deref(),
            pcol: self.pcol,
            kind: self.kind,
        })
    }
}

/// Stage a colliding identifier without allowing equal values from different real keys to
/// consolidate together. Each real key is staged independently, but all retain the identifier as
/// their proxy token; `cross` uses the coordinates to discard unequal-key pairs afterward.
pub(super) fn stage_collision<T: ColTime>(
    runs: &[RunRef<'_, T>],
    lower: &T,
    token: i64,
    bridge: &mut ProxyBridge<T, Diff, i64>,
) {
    let mut positions: Vec<usize> = runs.iter().map(|run| run.s).collect();
    let mut scratch = SideScratch::new();
    let mut staged = Vec::new();
    loop {
        let Some(min) = positions
            .iter()
            .enumerate()
            .filter(|(index, position)| **position < runs[*index].e)
            .min_by(|(ai, ap), (bi, bp)| {
                compare_at(runs[*ai].chunk.keys(), **ap, runs[*bi].chunk.keys(), **bp)
            })
            .map(|(index, _)| index)
        else {
            break;
        };
        let (reference, row) = (runs[min].chunk.keys(), positions[min]);
        let mut equal_runs = Vec::new();
        for (index, run) in runs.iter().enumerate() {
            let start = positions[index];
            if start == run.e
                || compare_at(run.chunk.keys(), start, reference, row) != Ordering::Equal
            {
                continue;
            }
            let end = start
                + (start..run.e)
                    .position(|candidate| {
                        compare_at(run.chunk.keys(), candidate, reference, row) != Ordering::Equal
                    })
                    .unwrap_or(run.e - start);
            equal_runs.push(RunRef { chunk: run.chunk, cid: run.cid, s: start, e: end, vals: run.vals, pcol: run.pcol, kind: run.kind });
            positions[index] = end;
        }
        scratch.stage_runs(&equal_runs, lower);
        staged.extend(scratch.entries.drain(..).map(|(coord, time, diff)| ((token, coord), time, diff)));
    }
    staged.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
    bridge.extend(staged);
}

/// Blockwise `advance` over every arrangement key shape: group token = the leading identifier
/// lane (the key itself or its carried hash), so blocks resume by seeking `from` and end at
/// identifier boundaries.
///
/// A block's work is passes over its keys, not a walk of them. Each side's runs are found chunk
/// by chunk, the keys both sides hold by one merge, and the colliding keys by one comparison.
/// What is left for each key is to record its pairs, or to stage it.
///
/// Two regimes: when one side is much smaller (the fresh delta against an accumulated
/// trace), the small side DRIVES and the large side is searched only at the driver's keys
/// (sorted probes — cost tracks the driver plus matches). When the sides are
/// comparable, both sides are scanned instead.
fn advance_leaf<T: ColTime>(
    chunks0: &[&CorgiChunk<T, Diff>],
    chunks1: &[&CorgiChunk<T, Diff>],
    lower: &T,
    from: &mut Option<i64>,
    bridge0: &mut ProxyBridge<T, Diff, i64>,
    bridge1: &mut ProxyBridge<T, Diff, i64>,
    colliding: &mut Vec<i64>,
    (proj0, proj1): (&SideProj, &SideProj),
    mut completed: Option<&mut Pairs>,
) {
    let start = from.expect("advance called on an exhausted unit");
    let hashed = key_is_hashed(chunks0[0].keys());
    // Resume: the first row of each chunk at or past `start`, by binary search on its identifier
    // lane. The lane is a sorted `i64` slice, so this is a slice operation, not a column probe.
    let seek = |chunks: &[&CorgiChunk<T, Diff>]| -> Vec<usize> {
        chunks.iter().map(|c| ident(c).partition_point(|&x| x < start)).collect()
    };
    let start0 = seek(chunks0);
    let start1 = seek(chunks1);
    let remaining = |chunks: &[&CorgiChunk<T, Diff>], starts: &[usize]| -> usize {
        chunks.iter().zip(starts).map(|(c, &s)| c.len() - s).sum()
    };
    let (r0, r1) = (remaining(chunks0, &start0), remaining(chunks1, &start1));
    let (h, runs0, runs1) = if r0.max(r1) >= 2 * r0.min(r1) {
        // Lopsided: only the driver is read in bulk, so only the driver bounds the block.
        if r0 <= r1 {
            let h = block_horizon(chunks0, &start0, PULL);
            let end0 = block_ends(chunks0, h);
            let runs0 = SideRuns::scan(chunks0, &start0, &end0);
            let runs1 = SideRuns::probe(chunks1, &runs0.keys);
            (h, runs0, runs1)
        } else {
            let h = block_horizon(chunks1, &start1, PULL);
            let end1 = block_ends(chunks1, h);
            let runs1 = SideRuns::scan(chunks1, &start1, &end1);
            let runs0 = SideRuns::probe(chunks0, &runs1.keys);
            (h, runs0, runs1)
        }
    } else {
        // Symmetric: both sides are read, so both bound the block.
        let h = block_horizon(chunks0, &start0, PULL).into_iter()
            .chain(block_horizon(chunks1, &start1, PULL))
            .min();
        let (end0, end1) = (block_ends(chunks0, h), block_ends(chunks1, h));
        (h, SideRuns::scan(chunks0, &start0, &end0), SideRuns::scan(chunks1, &start1, &end1))
    };
    // The block ran to the horizon; resume there (`None` = a side is exhausted, and with it
    // the intersection).
    *from = h;
    let both = matched(&runs0, &runs1);
    if both.is_empty() { return; }

    // Check the whole block's matched keys before staging any one key. Unmatched keys need no
    // real-key checks; only coordinates into the existing columns cross into Corgi.
    let collisions = if hashed {
        let columns: Vec<_> = chunks0.iter().chain(chunks1).map(|c| recover_key_ref(c.keys())).collect();
        let offset = chunks0.len();
        let of0 = both.iter().enumerate().flat_map(|(m, &(j0, _))| runs0.of(j0).iter().map(move |&(cid, s, e)| (m, cid, s, e)));
        let of1 = both.iter().enumerate().flat_map(|(m, &(_, j1))| runs1.of(j1).iter().map(move |&(cid, s, e)| (m, offset + cid, s, e)));
        colliding_keys(both.len(), &columns, of0.chain(of1))
    } else { Vec::new() };

    // Each matched key in order: its pairs recorded, if it is completed, or else staged, and
    // presented in the bridges if both sides survive consolidation.
    let kinds = (proj0.kind, proj1.kind);
    let (view0, view1) = (SideView::new(chunks0, proj0), SideView::new(chunks1, proj1));
    let (mut scratch0, mut scratch1) = (SideScratch::new(), SideScratch::new());
    // The completed keys with a side to net first: their runs, and which sides.
    let mut netting: Vec<(&[Run], &[Run], (bool, bool))> = Vec::new();
    for (m, &(j0, j1)) in both.iter().enumerate() {
        let key = runs0.keys[j0];
        let (of0, of1) = (runs0.of(j0), runs1.of(j1));
        if hashed && collisions[m] {
            colliding.push(key);
            let (mut staged0, mut staged1) = (Vec::new(), Vec::new());
            stage_collision(&view0.refs(of0), lower, key, &mut staged0);
            stage_collision(&view1.refs(of1), lower, key, &mut staged1);
            if !staged0.is_empty() && !staged1.is_empty() {
                bridge0.extend(staged0);
                bridge1.extend(staged1);
            }
            continue;
        }
        if let Some(pairs) = completed.as_mut() {
            if completes((of0, of1), kinds) {
                let mixed = (mixed_signs(chunks0, of0), mixed_signs(chunks1, of1));
                if mixed == (false, false) { pairs.cross(of0, of1); } else { netting.push((of0, of1, mixed)); }
                continue;
            }
        }
        scratch0.stage_runs(&view0.refs(of0), lower);
        scratch1.stage_runs(&view1.refs(of1), lower);
        if !scratch0.entries.is_empty() && !scratch1.entries.is_empty() {
            scratch0.emit(key, bridge0);
            scratch1.emit(key, bridge1);
        }
    }

    // The keys set aside: each side with both signs is netted, all such keys at once, and what
    // is left is paired, with the diffs the netting gave.
    if !netting.is_empty() {
        let pairs = completed.expect("a key is only set aside to net when keys are completed");
        let net0 = net_side(chunks0, lower, &netting.iter().filter(|key| key.2.0).map(|key| key.0).collect::<Vec<_>>());
        let net1 = net_side(chunks1, lower, &netting.iter().filter(|key| key.2.1).map(|key| key.1).collect::<Vec<_>>());
        // One side's rows of a key: the next netted key's, or its stored rows as they are.
        fn side_rows<T: ColTime>(
            rows: &mut Vec<(usize, usize, Diff)>, chunks: &[&CorgiChunk<T, Diff>], runs: &[Run],
            mixed: bool, net: &[(usize, usize, usize, Diff)], at: &mut usize, label: &mut usize,
        ) {
            rows.clear();
            if mixed {
                while *at < net.len() && net[*at].0 == *label {
                    rows.push((net[*at].1, net[*at].2, net[*at].3));
                    *at += 1;
                }
                *label += 1;
            } else {
                for &(chunk, s, e) in runs { rows.extend((s..e).map(|row| (chunk, row, chunks[chunk].diffs()[row]))); }
            }
        }
        pairs.netted_from = Some(pairs.row0.len());
        let (mut rows0, mut rows1) = (Vec::new(), Vec::new());
        let (mut at0, mut at1, mut label0, mut label1) = (0, 0, 0, 0);
        for &(of0, of1, (mixed0, mixed1)) in &netting {
            side_rows(&mut rows0, chunks0, of0, mixed0, &net0, &mut at0, &mut label0);
            side_rows(&mut rows1, chunks1, of1, mixed1, &net1, &mut at1, &mut label1);
            for &(chunk0, row0, diff0) in &rows0 {
                for &(chunk1, row1, diff1) in &rows1 {
                    pairs.chunk0.push(chunk0);
                    pairs.row0.push(row0);
                    pairs.chunk1.push(chunk1);
                    pairs.row1.push(row1);
                    pairs.diffs.push(diff0 * diff1);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An integer column of these values.
    fn ints(xs: Vec<u64>) -> CValue { CValue::i64(xs.into_iter().map(|x| x as i64).collect()) }

    fn batch(rows: &[(u64, u64, u64)]) -> CBatch<u64> {
        let keys = CValue::Prod(vec![
            ints(rows.iter().map(|row| row.0).collect()),
            CValue::Prod(vec![
                ints(rows.iter().map(|row| row.1).collect()),
                ints(rows.iter().map(|row| row.2).collect()),
            ]),
        ]);
        // Deliberately equal across different real keys: collision staging must not consolidate
        // these together before `cross` has a chance to compare their keys.
        let vals = ints(vec![0; rows.len()]);
        let chunk = CorgiChunk::from_columns(keys, vals, (0..rows.len()).map(|_| 0).collect(), vec![1; rows.len()]);
        Rc::new(ChunkBatch::new(vec![chunk]))
    }

    /// A backend projecting `(key, (val0, val1))` from inputs of these key and value shapes.
    fn backend(shapes: [Shape; 3]) -> CorgiJoinBackend<u64> {
        CorgiJoinBackend::new(&Lambda::new("let (k, v0, v1) = input in (k, (v0, v1))".into(), &corgi::Scope::default()), shapes)
    }

    fn pair() -> Shape { Shape::Prod(vec![Shape::Int, Shape::Int]) }

    #[test]
    fn narrow_projection_matches_unnarrowed_join() {
        for hashed in [false, true] {
            let make = |wide, row| {
                let keys = if hashed {
                    // Same hash and first field, different unread second field.
                    CValue::Prod(vec![ints(vec![7]), CValue::Prod(vec![ints(vec![1]), ints(vec![row])])])
                } else { CValue::Prod(vec![ints(vec![7])]) };
                let vals = CValue::Prod(if wide {
                    (0..4).map(|field| ints(vec![10 * field + row])).collect()
                } else { vec![ints(vec![row])] });
                Rc::new(ChunkBatch::new(vec![CorgiChunk::from_columns(
                    keys, vals, [row].into_iter().collect(), vec![if row == 0 { 2 } else { -3 }],
                )]))
            };
            let instance = Unit { batches0: vec![make(true, 0), make(true, 1)], batches1: vec![make(false, 0), make(false, 1)], lower: 5 };
            let shapes = [if hashed { pair() } else { Shape::Prod(vec![Shape::Int]) }, Shape::Prod(vec![Shape::Int; 4]), Shape::Prod(vec![Shape::Int])];
            for body in ["((), c)", "(k, c)", "(k, v)", "((), ())", if hashed { "let (x, y) = k in (x, c)" } else { "(k, (v, c))" }] {
                let lambda = Lambda::new(format!("let (k, (a, b, c, d), v) = input in {body}"), &corgi::Scope::default());
                let mut narrowed = CorgiJoinBackend::new(&lambda, shapes.clone());
                let mut original = CorgiJoinBackend {
                    completed: Pairs::default(),
                    colliding: Vec::new(), graph: compile_projection(&lambda, &shapes).unwrap().graph().clone(),
                    reads: Reads::new(Demand::All, &Shape::Prod(shapes.to_vec())), _t: PhantomData,
                };
                let run = |backend: &mut CorgiJoinBackend<u64>| {
                    let (mut left, mut right) = (Vec::new(), Vec::new());
                    backend.advance(&instance.instance(), &mut KeyPosition::Start, &mut left, &mut right);
                    let mut matches = JoinMatches::default();
                    for a in &left {
                        for b in &right {
                            if a.0.0 == b.0.0 {
                                matches.ids.push((a.0.0, (a.0.1, b.0.1)));
                                matches.times.push(a.1.max(b.1));
                                matches.diffs.push(a.2 * b.2);
                            }
                        }
                    }
                    let mut output = Vec::new();
                    backend.cross(&instance.instance(), &mut matches, &mut output);
                    // Tokens minted on projected values can merge rows before `cross`: compare
                    // the consolidated updates, not the column layout.
                    let mut rows: Vec<_> = output.into_iter().flat_map(|c| c.into_updates()).collect();
                    differential_dataflow::consolidation::consolidate_updates(&mut rows);
                    rows
                };
                assert_eq!(run(&mut narrowed), run(&mut original), "hashed={hashed}, body={body}");
            }
        }
    }

    /// A unit of work, owned. Its backend must present every key, as these tests read the
    /// presentations.
    struct Unit {
        batches0: Vec<CBatch<u64>>,
        batches1: Vec<CBatch<u64>>,
        lower: u64,
    }

    impl Unit {
        fn instance(&self) -> JoinInstance<'_, u64, CBatch<u64>, CBatch<u64>> {
            JoinInstance { batches0: &self.batches0, batches1: &self.batches1, lower: &self.lower, present_all: true }
        }
    }

    fn cross_bridges(
        backend: &mut CorgiJoinBackend<u64>,
        instance: &JoinInstance<u64, CBatch<u64>, CBatch<u64>>,
        left: &ProxyBridge<u64, Diff, i64>,
        right: &ProxyBridge<u64, Diff, i64>,
    ) -> usize {
        let mut matches = JoinMatches::default();
        for a in left {
            for b in right {
                if a.0.0 == b.0.0 {
                    matches.ids.push((a.0.0, (a.0.1, b.0.1)));
                    matches.times.push(a.1.max(b.1));
                    matches.diffs.push(a.2 * b.2);
                }
            }
        }
        let mut output = Vec::new();
        backend.cross(instance, &mut matches, &mut output);
        output.iter().map(|container| container.diffs.len()).sum()
    }

    /// Unread-only changes cancel before any matching, including a side read only for existence.
    #[test]
    fn unread_changes_cancel_before_matching() {
        for body in ["(k, ())", "(k, a)", "(k, c)"] {
            let make = |noise, diff| Rc::new(ChunkBatch::new(vec![CorgiChunk::from_columns(
                ints(vec![7]), CValue::Prod(vec![ints(vec![1]), ints(vec![noise]), ints(vec![3])]),
                [0].into_iter().collect(), vec![diff],
            )]));
            let instance = Unit { batches0: vec![make(10, -1), make(11, 1)], batches1: vec![make(12, 1)], lower: 0 };
            let lambda = Lambda::new(format!("let (k, (a, _, c), _) = input in {body}"), &corgi::Scope::default());
            let shape = Shape::Prod(vec![Shape::Int; 3]);
            let mut backend = CorgiJoinBackend::new(&lambda, [Shape::Int, shape.clone(), shape]);
            let (mut left, mut right) = (Vec::new(), Vec::new());
            backend.advance(&instance.instance(), &mut KeyPosition::Start, &mut left, &mut right);
            assert!(left.is_empty() && right.is_empty(), "{body}");
        }
    }

    /// A value added in one batch and taken away in a later one nets to nothing before the key
    /// is matched, though the key is small enough to complete. Rows of one sign are completed.
    #[test]
    fn completed_keys_do_not_carry_cancelling_rows() {
        let make = |val, diff| Rc::new(ChunkBatch::new(vec![CorgiChunk::from_columns(
            ints(vec![7]), ints(vec![val]), [0].into_iter().collect(), vec![diff],
        )]));
        for (diffs, rows) in [([1, -1], 0), ([1, 1], 2), ([-1, -1], 2)] {
            let (batches0, batches1) = (vec![make(1, diffs[0]), make(1, diffs[1])], vec![make(2, 1)]);
            let instance = JoinInstance { batches0: &batches0, batches1: &batches1, lower: &0, present_all: false };
            let mut backend = backend([Shape::Int, Shape::Int, Shape::Int]);
            let (mut left, mut right, mut output) = (Vec::new(), Vec::new(), Vec::new());
            backend.advance(&instance, &mut KeyPosition::Start, &mut left, &mut right);
            backend.cross(&instance, &mut JoinMatches::default(), &mut output);
            assert!(left.is_empty() && right.is_empty(), "{diffs:?}");
            assert_eq!(output.iter().map(|container| container.diffs.len()).sum::<usize>(), rows, "{diffs:?}");
        }
        // What does not cancel is kept, with its summed diff, and nothing is presented:
        // value 1 nets to +2, value 3 to nothing, value 4 stays -1; the other side nets 2 to +1.
        let batches0 = vec![make(1, 3), make(1, -1), make(3, 1), make(3, -1), make(4, -1)];
        let batches1 = vec![make(2, 2), make(2, -1)];
        let instance = JoinInstance { batches0: &batches0, batches1: &batches1, lower: &0, present_all: false };
        let mut backend = backend([Shape::Int, Shape::Int, Shape::Int]);
        let (mut left, mut right, mut output) = (Vec::new(), Vec::new(), Vec::new());
        backend.advance(&instance, &mut KeyPosition::Start, &mut left, &mut right);
        backend.cross(&instance, &mut JoinMatches::default(), &mut output);
        assert!(left.is_empty() && right.is_empty());
        let mut diffs: Vec<Diff> = output.iter().flat_map(|container| container.diffs.iter().copied()).collect();
        diffs.sort();
        assert_eq!(diffs, vec![-1, 2]);
    }

    #[test]
    fn primitive_tokens_cancel_across_chunks_and_reconstruct_mixed_shapes() {
        for depth in 0..3 {
            for (compound0, compound1) in [(false, false), (false, true), (true, false), (true, true)] {
                for padded_side in [None, Some(false), Some(true)] {
                    let wrap = |mut col| {
                        for _ in 0..depth { col = CValue::Prod(vec![col]); }
                        col
                    };
                    let values = |ids: Vec<u64>, compound| {
                        let n = ids.len();
                        let col = wrap(ints(ids));
                        if compound { CValue::Prod(vec![col, ints(vec![9; n])]) } else { col }
                    };
                    let make = |compound, ids: Vec<u64>, diffs: Vec<Diff>, time| {
                        let n = ids.len();
                        Rc::new(ChunkBatch::new(vec![CorgiChunk::from_columns(
                            wrap(ints(vec![7; n])), values(ids, compound),
                            (0..n).map(|_| time).collect(), diffs,
                        )]))
                    };
                    let side = |compound| vec![
                        make(compound, vec![0, i64::MIN as u64, u64::MAX], vec![1, 1, 2], 0),
                        make(compound, vec![0, i64::MIN as u64], vec![-1, -1], 3),
                    ];
                    let mut instance = Unit { batches0: side(compound0), batches1: side(compound1), lower: 5 };
                    if let Some(right) = padded_side {
                        let extra = CorgiChunk::from_columns(
                            wrap(ints(vec![8; 20])), values(vec![0; 20], if right { compound1 } else { compound0 }),
                            (0..20).collect(), vec![1; 20],
                        );
                        if right { &mut instance.batches1 } else { &mut instance.batches0 }
                            .push(Rc::new(ChunkBatch::new(vec![extra])));
                    }
                    let shape = |v: &CValue| corgi::shape_of_value(v);
                    let mut backend = backend([shape(&wrap(ints(vec![7]))), shape(&values(vec![0], compound0)), shape(&values(vec![0], compound1))]);
                    let (mut left, mut right) = (Vec::new(), Vec::new());
                    backend.advance(&instance.instance(), &mut KeyPosition::Start, &mut left, &mut right);
                    assert_eq!((left.len(), right.len()), (1, 1));
                    assert_eq!((left[0].1, left[0].2), (5, 2));
                    if !compound0 { assert_eq!(left[0].0.1, u64::MAX); }
                    if !compound1 { assert_eq!(right[0].0.1, u64::MAX); }
                    let mut matches = JoinMatches { ids: vec![(7, (left[0].0.1, right[0].0.1))], times: vec![5], diffs: vec![4] };
                    let mut output = Vec::new();
                    backend.cross(&instance.instance(), &mut matches, &mut output);
                    assert_eq!(output.len(), 1);
                    assert_eq!(output[0].keys, wrap(ints(vec![7])));
                    assert_eq!(output[0].vals, CValue::Prod(vec![values(vec![u64::MAX], compound0), values(vec![u64::MAX], compound1)]));
                    assert_eq!(output[0].diffs, vec![4]);
                    assert_eq!(output[0].times.get(0), 5);
                    instance.batches0.push(make(compound0, vec![u64::MAX], vec![-2], 3));
                    left.clear(); right.clear();
                    backend.advance(&instance.instance(), &mut KeyPosition::Start, &mut left, &mut right);
                    assert!(left.is_empty() && right.is_empty(), "suppress a fully cancelled key");
                }
            }
        }
    }

    /// Keys are ordered as signed integers, so a walk from the start reaches the negative ones.
    #[test]
    fn negative_keys_join() {
        let make = |keys: Vec<i64>| Rc::new(ChunkBatch::new(vec![CorgiChunk::from_columns(
            CValue::i64(keys.clone()), CValue::i64(keys.clone()), keys.iter().map(|_| 0u64).collect(), vec![1; keys.len()],
        )]));
        let instance = Unit { batches0: vec![make(vec![i64::MIN, -5, 3])], batches1: vec![make(vec![-5, -1, 3])], lower: 0 };
        let mut backend = backend([Shape::Int, Shape::Int, Shape::Int]);
        let (mut from, mut left, mut right) = (KeyPosition::Start, Vec::new(), Vec::new());
        while from != KeyPosition::End {
            backend.advance(&instance.instance(), &mut from, &mut left, &mut right);
        }
        let keys = |bridge: &ProxyBridge<u64, Diff, i64>| bridge.iter().map(|r| r.0.0).collect::<Vec<_>>();
        assert_eq!((keys(&left), keys(&right)), (vec![-5, 3], vec![-5, 3]));
    }

    #[test]
    fn many_chunk_compound_runs_cancel_and_reuse_staging_across_keys() {
        check_many_chunk_runs(false);
        check_many_chunk_runs(true);
    }

    fn check_many_chunk_runs(hashed: bool) {
        // Spill past four runs, cancel the first key, then reuse fewer runs for the last key.
        let batches: Vec<_> = (0..7).map(|chunk| {
            let rows: Vec<_> = (0..6).flat_map(|key| (0..3).map(move |val| (key, val)))
                .filter(|&(key, _)| (key != 0 || chunk != 0) && (key != 5 || chunk < 3)).collect();
            let keys = CValue::i64(rows.iter().map(|r| r.0).collect());
            let keys = if hashed { CValue::Prod(vec![keys.clone(), keys]) } else { keys };
            Rc::new(ChunkBatch::new(vec![CorgiChunk::from_columns(
                keys,
                CValue::Prod(vec![
                    CValue::i64(rows.iter().map(|r| r.1).collect()),
                    CValue::i64(rows.iter().map(|r| r.1 * 7).collect()),
                ]),
                std::iter::repeat_n(0, rows.len()).collect(),
                vec![if chunk < 4 { 1 } else { -1 }; rows.len()],
            )]))
        }).collect();
        let instance = Unit { batches0: batches.clone(), batches1: batches, lower: 5 };
        let mut backend = backend([Shape::Int, pair(), pair()]);
        let (mut left, mut right) = (Vec::new(), Vec::new());
        backend.advance(&instance.instance(), &mut KeyPosition::Start, &mut left, &mut right);
        let expected: Vec<_> = (1..6).flat_map(|key| std::iter::repeat_n((key, 5, if key == 5 { 3 } else { 1 }), 3)).collect();
        assert_eq!(left.iter().map(|r| (r.0.0, r.1, r.2)).collect::<Vec<_>>(), expected);
        assert_eq!(left, right);
        assert_eq!(cross_bridges(&mut backend, &instance.instance(), &left, &right), 5 * 3 * 3);
    }

    #[test]
    fn borrowed_comparisons_match_structural_rows_after_skips() {
        let make = |keys: Vec<u64>, salt: u64| {
            let n = keys.len();
            CorgiChunk::from_columns(
                ints(keys),
                CValue::Prod(vec![
                    ints((0..n).map(|i| (i as u64 ^ salt) % 3).collect()),
                    CValue::Prod(vec![CValue::Unit(n), ints((0..n).map(|i| u64::MAX - i as u64).collect())]),
                ]),
                (0..n).map(|_| 0u64).collect(), vec![1; n],
            )
        };
        let left = make(vec![0, 0, 1, 2, 2, 2, 4, 4, 5], 1);
        let right = make(vec![0, 1, 1, 4, 5], 2);
        let (lproj, rproj) = (SideProj::new(&[&left], &Demand::All), SideProj::new(&[&right], &Demand::All));
        let runs = SideRuns::scan(&[&left], &[2], &[8]);
        assert_eq!(runs.keys, vec![1, 2, 4]);
        assert_eq!(runs.runs, vec![(0, 2, 3), (0, 3, 6), (0, 6, 8)]);
        let chunks = [&left];
        let view = SideView::new(&chunks, &lproj);
        let probe = Probe::new(&right, 1, &[0, 1, 3, 5], &rproj);
        assert!(probe.run_ref(2).is_none());
        for run in &runs.runs {
            let a = &view.refs(std::slice::from_ref(run))[0];
            for j in [0, 1, 3] {
                let b = probe.run_ref(j).unwrap();
                for i in a.s..a.e {
                    for k in b.s..b.e {
                        let expected = compare_at(a.chunk.vals(), i, b.chunk.vals(), k);
                        assert_eq!(a.val_eq(i, &b, k), expected == Ordering::Equal);
                        assert_eq!(a.val_less(i, &b, k), expected == Ordering::Less);
                        assert_eq!(b.val_less(k, &a, i), expected == Ordering::Greater);
                    }
                }
            }
        }
    }

    #[test]
    fn primitive_tokens_consolidate_after_product_time_reordering() {
        let t = |epoch, iter| crate::ir::Time::new(smallvec::smallvec![epoch, iter]);
        let chunk = CorgiChunk::from_columns(
            ints(vec![7; 3]), ints(vec![11; 3]),
            [t(0, 2), t(1, 0), t(1, 2)].into_iter().collect(),
            vec![1, 1, -1],
        );
        let mut scratch = SideScratch::new();
        scratch.stage_runs(
            &[RunRef { chunk: &chunk, cid: 0, s: 0, e: 3, vals: None, pcol: None, kind: ProjKind::Whole }],
            &t(1, 0),
        );
        assert_eq!(scratch.entries, vec![(11, t(1, 0), 1)]);
    }

    #[test]
    fn compound_tokens_consolidate_after_product_time_reordering() {
        let t = |epoch, iter| crate::ir::Time::new(smallvec::smallvec![epoch, iter]);
        let chunk = CorgiChunk::from_columns(
            ints(vec![7; 3]),
            CValue::Prod(vec![ints(vec![11; 3]), ints(vec![12; 3])]),
            [t(0, 2), t(1, 0), t(1, 2)].into_iter().collect(),
            vec![1, 1, -1],
        );
        let mut scratch = SideScratch::new();
        scratch.stage_runs(
            &[RunRef { chunk: &chunk, cid: 0, s: 0, e: 3, vals: None, pcol: None, kind: ProjKind::Whole }],
            &t(1, 0),
        );
        assert_eq!(scratch.entries, vec![(0, t(1, 0), 1)]);
    }

    #[test]
    fn collision_after_completed_block_filters_real_keys_without_restarting() {
        let collision = PULL as u64 + 1;
        let mut rows: Vec<_> = (0..collision).map(|id| (id, id, 0)).collect();
        rows.extend([(collision, 7, 0), (collision, 8, 0)]);
        let instance = Unit {
            batches0: vec![batch(&rows)],
            batches1: vec![batch(&rows)],
            lower: 0,
        };
        let mut backend = backend([pair(), Shape::Int, Shape::Int]);
        let mut from = KeyPosition::Start;
        let (mut left, mut right) = (Vec::new(), Vec::new());

        backend.advance(&instance.instance(), &mut from, &mut left, &mut right);
        assert!(from != KeyPosition::End, "the first block must leave work for the collision block");
        assert!(backend.colliding.is_empty());

        left.clear();
        right.clear();
        backend.advance(&instance.instance(), &mut from, &mut left, &mut right);
        assert_eq!(backend.colliding, vec![collision as i64]);
        assert_eq!(cross_bridges(&mut backend, &instance.instance(), &left, &right), 2);
    }

    /// Singleton runs can disagree across sides or chunks even though each is internally one
    /// key. Validate all endpoints together in both symmetric and either lopsided direction.
    #[test]
    fn block_checks_compare_real_keys_across_sides_and_chunks() {
        for padded in [None, Some(false), Some(true)] {
            let mut instance = Unit {
                batches0: vec![batch(&[(4, 1, 0), (5, 2, 0), (6, 3, 0)])],
                batches1: vec![batch(&[(4, 9, 0), (5, 2, 0), (6, 3, 0)]), batch(&[(6, 4, 0)])],
                lower: 0,
            };
            if let Some(right) = padded {
                let padding: Vec<_> = (100..150).map(|id| (id, id, 0)).collect();
                if right { &mut instance.batches1 } else { &mut instance.batches0 }.push(batch(&padding));
            }
            let mut backend = backend([pair(), Shape::Int, Shape::Int]);
            let (mut left, mut right) = (Vec::new(), Vec::new());
            backend.advance(&instance.instance(), &mut KeyPosition::Start, &mut left, &mut right);
            assert_eq!(backend.colliding, vec![4, 6]);
            assert_eq!(cross_bridges(&mut backend, &instance.instance(), &left, &right), 2);
        }
    }

    #[test]
    fn lopsided_collision_filters_real_keys() {
        let collision = 42;
        let left_rows = [(collision, 7, 0), (collision, 8, 0)];
        let mut right_rows: Vec<_> = (0..8).map(|id| (id, id, 0)).collect();
        right_rows.extend([(collision, 7, 0), (collision, 8, 0)]);
        let instance = Unit {
            batches0: vec![batch(&left_rows)],
            batches1: vec![batch(&right_rows)],
            lower: 0,
        };
        let mut backend = backend([pair(), Shape::Int, Shape::Int]);
        let mut from = KeyPosition::Start;
        let (mut left, mut right) = (Vec::new(), Vec::new());

        backend.advance(&instance.instance(), &mut from, &mut left, &mut right);
        assert_eq!(backend.colliding, vec![collision as i64]);
        assert_eq!(cross_bridges(&mut backend, &instance.instance(), &left, &right), 2);
    }
}
