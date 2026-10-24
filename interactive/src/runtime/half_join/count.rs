//! The count tactic: each released update, at its own time and diff, with the number of updates
//! the arrangement holds at its key, projected. See `scope_ir::Node::Count`.
//!
//! It runs under dogs3's `half_join_with_tactic` with the half join's batcher, so an update is
//! answered only once the arrangement's frontier has passed its time: every arranged update that
//! could precede it in the total order has arrived, and a key with no updates at all is one no
//! half join against the arrangement would match. Those rows are dropped, which is exact. The
//! number itself only steers work, so it reads no times: it is the rows under the update's
//! identifier across the arrangement's chunks, each a pair of binary searches. Retractions pending
//! compaction, and other real keys under a colliding hashed identifier, are counted too.

use std::rc::Rc;

use timely::progress::Antichain;

use differential_dogs3::operators::half_join::HalfJoinTactic;
use differential_dataflow::trace::chunk::Chunk;

use corgi::arrange::gather;
use corgi::{Graph, NumOp, Shape, Value as CValue};

use crate::runtime::chunk::{ident, recover_key_ref, CorgiChunk};
use crate::runtime::container::CorgiContainer;
use crate::runtime::search::matching_ranges;
use crate::lambda::compile_projection;
use crate::ir::{Diff, Lambda, Time};

use super::{NativeBatch, Run, CC};

/// The corgi count tactic: the projection over `(key, val, Int)`, compiled once at the shapes it
/// reads, and shared by every unit of work.
pub(crate) struct CorgiCountTactic {
    projection: Rc<([Shape; 3], Graph<NumOp>)>,
}

impl CorgiCountTactic {
    /// The projection over a key and a value of these shapes, and the count. A type error is a
    /// panic with corgi's message: install type-checked the program, so it is not reached.
    pub(crate) fn new(projection: &Lambda, [k, v]: [Shape; 2]) -> Self {
        let shapes = [k, v, Shape::Int];
        let g = compile_projection(projection, &shapes)
            .unwrap_or_else(|e| panic!("count: type error at shapes ({}, {}, Int): {e}", shapes[0], shapes[1]));
        CorgiCountTactic { projection: Rc::new((shapes, g)) }
    }
}

impl HalfJoinTactic<Time, NativeBatch, Run, CC> for CorgiCountTactic {
    /// One unit of work: each released run counted against `batches`, one container per run that
    /// keeps any row. Every row keeps its own time, which the unit's capabilities cover.
    fn prep(&mut self, runs: Vec<Run>, batches: Vec<NativeBatch>, _lower: Antichain<Time>) -> Box<dyn Iterator<Item = CC>> {
        let projection = Rc::clone(&self.projection);
        Box::new(runs.into_iter().filter_map(move |run| {
            let chunks: Vec<&CorgiChunk<Time, Diff>> = batches.iter().flat_map(|b| b.chunks.iter().filter(|c| c.len() > 0)).collect();
            count_run(&run, &chunks, &projection)
        }))
    }
}

/// The run's rows whose identifier `chunks` hold updates for, with the number of those updates,
/// projected; `None` when there are none.
fn count_run(run: &Run, chunks: &[&CorgiChunk<Time, Diff>], (pinned, graph): &([Shape; 3], Graph<NumOp>)) -> Option<CC> {
    if run.len() == 0 || chunks.is_empty() { return None; }
    // The run's distinct identifiers, in order, and the updates each has across the chunks.
    let ids = ident(run.keys());
    let mut needles: Vec<i64> = ids.to_vec();
    needles.dedup();
    let (first, last) = (needles[0], needles[needles.len() - 1]);
    let mut counts = vec![0i64; needles.len()];
    let (mut scratch, mut found) = (Vec::new(), Vec::new());
    for chunk in chunks {
        let hay = ident(chunk.keys());
        if hay[0] > last || hay[hay.len() - 1] < first { continue; }
        matching_ranges(&needles, hay, &mut scratch, &mut found);
        for (j, range) in found.drain(..) { counts[j] += range.len() as i64; }
    }
    // The rows kept: those under an identifier with any updates.
    let (mut kept, mut ns) = (Vec::new(), Vec::new());
    let mut j = 0;
    for (i, &id) in ids.iter().enumerate() {
        while needles[j] < id { j += 1; }
        if counts[j] > 0 { kept.push(i); ns.push(counts[j]); }
    }
    if kept.is_empty() { return None; }

    let keys = gather(recover_key_ref(run.keys()), &kept);
    let vals = gather(run.vals(), &kept);
    let shapes = [corgi::shape_of_value(&keys), corgi::shape_of_value(&vals), Shape::Int];
    assert!(*pinned == shapes, "count: a run of shape {shapes:?} reached a projection compiled at {pinned:?}");
    let mut out = corgi::eval_graph(graph, CValue::Prod(vec![keys, vals, CValue::i64(ns)])).into_prod("count projection").unwrap();
    let out_val = out.pop().unwrap();
    let out_key = out.pop().unwrap();
    Some(CorgiContainer {
        keys: out_key,
        vals: out_val,
        times: run.times().gather(&kept),
        diffs: kept.iter().map(|&i| run.diffs()[i]).collect(),
    })
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::*;
    use super::super::test_util::*;
    use crate::runtime::chunk::{columns_to_batch, present_key};

    /// The projection `(k, (v, n))`.
    fn with_count() -> Lambda { Lambda::new("let (k, v, n) = input in (k, (v, n))".into(), &corgi::Scope::default()) }

    /// Runs `tactic` over one run of `keys`, against the arranged chunks given as `(keys, diffs)`
    /// per chunk, and returns the rows `(key, (val, n))` with their times and diffs.
    fn count(keys: &[i64], arranged: &[(&[i64], &[Diff])]) -> Vec<((String, String), Time, Diff)> {
        let batches: Vec<NativeBatch> = arranged.iter().map(|(ks, ds)| {
            let vals = ints(&(0..ks.len() as i64).collect::<Vec<_>>());
            let times = ks.iter().enumerate().map(|(i, _)| t(0, i as u64)).collect();
            Rc::new(columns_to_batch(present_key(ints(ks)), vals, times, ds.to_vec()))
        }).collect();
        let vals: Vec<i64> = keys.iter().map(|k| k * 10).collect();
        let times: Vec<Time> = keys.iter().map(|_| t(5, 0)).collect();
        let diffs: Vec<Diff> = keys.iter().map(|k| if k % 2 == 0 { 1 } else { -2 }).collect();
        let run = Run::new(present_key(ints(keys)), ints(&vals), times.into_iter().collect(), diffs);
        let mut tactic = CorgiCountTactic::new(&with_count(), [Shape::Int, Shape::Int]);
        let mut rows: Vec<_> = tactic.prep(vec![run], batches, Antichain::from_elem(t(5, 0))).flat_map(|c| c.into_updates()).collect();
        rows.sort();
        rows
    }

    /// `n` is the number of arranged updates under the key, summed over chunks, retractions
    /// included; rows at keys with no updates are dropped; each row keeps its time and diff.
    #[test]
    fn counts_updates_across_chunks_and_drops_absent_keys() {
        let rows = count(&[1, 2, 2, 3, 4], &[(&[1, 1, 2, 4, 4, 4], &[1, -1, 1, 1, 1, 1]), (&[2, 2, 5], &[1, 1, 1])]);
        let row = |k: i64, n: i64| ((int(k), tup(vec![int(k * 10), int(n)])), t(5, 0), if k % 2 == 0 { 1 } else { -2 });
        assert_eq!(rows, vec![row(1, 2), row(2, 3), row(2, 3), row(4, 3)]);
    }

    /// An empty arrangement holds no key, so every row is dropped.
    #[test]
    fn an_empty_arrangement_drops_everything() {
        assert!(count(&[1, 2, 3], &[]).is_empty());
        assert!(count(&[1, 2, 3], &[(&[7, 8], &[1, 1])]).is_empty());
    }
}
