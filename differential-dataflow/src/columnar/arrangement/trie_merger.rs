//! Batch-at-a-time merging of sorted, consolidated `Updates` chains.
//!
//! The core is `TrieMerger::merge_batches`, which walks pairs of chunks via
//! `merge_batch`, building a chain of merged outputs with `ChainBuilder`.
//! `survey` maps the interleaving of the two inputs at each trie layer,
//! `write_from_surveys` (via `write_layer` and `write_diffs`) copies the
//! ranges that the surveys identify into the output trie.

use columnar::{Columnar, Len};
use timely::progress::frontier::{Antichain, AntichainRef};
use crate::trace::implementations::merge_batcher::Merger;

use super::super::layout::ColumnarUpdate as Update;
use super::super::updates::Updates;

/// Merge-batcher merger that melds sorted, consolidated `Updates` tries.
pub struct TrieMerger<U: Update> {
    _marker: std::marker::PhantomData<U>,
}

impl<U: Update> Default for TrieMerger<U> {
    fn default() -> Self { Self { _marker: std::marker::PhantomData } }
}

/// A merging iterator over two sorted iterators.
struct Merging<I1: Iterator, I2: Iterator> {
    iter1: std::iter::Peekable<I1>,
    iter2: std::iter::Peekable<I2>,
}

impl<K, V, T, D, I1, I2> Iterator for Merging<I1, I2>
where
    K: Copy + Ord,
    V: Copy + Ord,
    T: Copy + Ord,
    I1: Iterator<Item = (K, V, T, D)>,
    I2: Iterator<Item = (K, V, T, D)>,
{
    type Item = (K, V, T, D);
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match (self.iter1.peek(), self.iter2.peek()) {
            (Some(a), Some(b)) => {
                if (a.0, a.1, a.2) <= (b.0, b.1, b.2) {
                    self.iter1.next()
                } else {
                    self.iter2.next()
                }
            }
            (Some(_), None) => self.iter1.next(),
            (None, Some(_)) => self.iter2.next(),
            (None, None) => None,
        }
    }
}

/// Build sorted `Updates` chunks from a sorted iterator of refs,
/// using `Updates::form` (which consolidates internally) on batches.
fn form_chunks<'a, U: Update>(
    sorted: impl Iterator<Item = columnar::Ref<'a, super::super::updates::Tuple<U>>>,
    output: &mut Vec<Updates<U>>,
) {
    let mut sorted = sorted.peekable();
    while sorted.peek().is_some() {
        let chunk = Updates::<U>::form((&mut sorted).take(crate::columnar::LINK_TARGET));
        if chunk.len() > 0 {
            output.push(chunk);
        }
    }
}

impl<U: Update> Merger for TrieMerger<U>
where
    U::Time: 'static,
{
    type Chunk = Updates<U>;
    type Time = U::Time;

    fn merge(
        &mut self,
        list1: Vec<Updates<U>>,
        list2: Vec<Updates<U>>,
        output: &mut Vec<Updates<U>>,
        _stash: &mut Vec<Updates<U>>,
    ) {
        Self::merge_batches(list1, list2, output, _stash);
    }

    fn extract(
        &mut self,
        mut merged: Vec<Self::Chunk>,
        upper: AntichainRef<Self::Time>,
        frontier: &mut Antichain<Self::Time>,
        ship: &mut Vec<Self::Chunk>,
        kept: &mut Vec<Self::Chunk>,
        _stash: &mut Vec<Self::Chunk>,
    ) {
        use columnar::{Borrow, Container, ContainerOf, Index, Push};
        use columnar::primitive::offsets::Strides;
        use crate::columnar::updates::{Lists, retain_items};

        // TODO: rework to move from trie structure to trie structure.
        let mut time_owned = U::Time::default();
        let mut bitmap = Vec::new();    // update should be kept.
        for chunk in merged.drain(..) {
            bitmap.clear();
            let times = chunk.times.values.borrow();
            for idx in 0 .. times.len() {
                Columnar::copy_from(&mut time_owned, times.get(idx));
                if upper.less_equal(&time_owned) {
                    frontier.insert_ref(&time_owned);
                    bitmap.push(true);
                }
                else { bitmap.push(false); }
            }
            if bitmap.iter().all(|x| *x) { kept.push(chunk); }
            else if bitmap.iter().all(|x| !*x) { ship.push(chunk); }
            else {

                let (times, temp) = retain_items::<ContainerOf<U::Time>>(chunk.times.borrow(), &bitmap[..]);
                let (vals, temp) = retain_items::<ContainerOf<U::Val>>(chunk.vals.borrow(), &temp[..]);
                let (keys, _temp) = retain_items::<ContainerOf<U::Key>>(chunk.keys.borrow(), &temp[..]);
                let d_borrow = chunk.diffs.borrow();
                let mut diffs = <Lists::<ContainerOf<U::Diff>> as Container>::with_capacity_for([d_borrow].into_iter());
                for (index, bit) in bitmap.iter().enumerate() {
                    if *bit { diffs.values.push(d_borrow.values.get(index)); }
                }
                diffs.bounds = Strides::new(1, times.values.len() as u64);
                kept.push(Updates {
                    keys,
                    vals,
                    times,
                    diffs,
                });

                for bit in bitmap.iter_mut() { *bit = !*bit; }

                let (times, temp) = retain_items::<ContainerOf<U::Time>>(chunk.times.borrow(), &bitmap[..]);
                let (vals, temp) = retain_items::<ContainerOf<U::Val>>(chunk.vals.borrow(), &temp[..]);
                let (keys, _temp) = retain_items::<ContainerOf<U::Key>>(chunk.keys.borrow(), &temp[..]);
                let d_borrow = chunk.diffs.borrow();
                let mut diffs = <Lists::<ContainerOf<U::Diff>> as Container>::with_capacity_for([d_borrow].into_iter());
                for (index, bit) in bitmap.iter().enumerate() {
                    if *bit { diffs.values.push(d_borrow.values.get(index)); }
                }
                diffs.bounds = Strides::new(1, times.values.len() as u64);
                ship.push(Updates {
                    keys,
                    vals,
                    times,
                    diffs,
                });
            }
        }
    }

    fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize) {
        use timely::Accountable;
        (chunk.record_count() as usize, 0, 0, 0)
    }
}

impl<U: Update> TrieMerger<U>
where
    U::Time: 'static,
{
    /// Iterator-based merge: flatten, merge, consolidate, form.
    /// Correct but slow — used as fallback.
    #[allow(dead_code)]
    fn merge_iterator(
        list1: &[Updates<U>],
        list2: &[Updates<U>],
        output: &mut Vec<Updates<U>>,
    ) {
        let iter1 = list1.iter().flat_map(|chunk| chunk.iter());
        let iter2 = list2.iter().flat_map(|chunk| chunk.iter());

        let merged = Merging {
            iter1: iter1.peekable(),
            iter2: iter2.peekable(),
        };

        form_chunks::<U>(merged, output);
    }

    /// A merge implementation that operates batch-at-a-time.
    #[inline(never)]
    fn merge_batches(
        list1: Vec<Updates<U>>,
        list2: Vec<Updates<U>>,
        output: &mut Vec<Updates<U>>,
        stash: &mut Vec<Updates<U>>,
    ) {

        // The design for efficient "batch" merginging of chains of links is:
        // 0.   We choose a target link size, K, and will keep the average link size at least K and the max size at 2k.
        //      K should be large enough to amortize some set-up, but not so large that one or two extra break the bank.
        // 1.   We will repeatedly consider pairs of links, and fully merge one with a prefix of the other.
        //      The last elements of each link will tell us which of the two suffixes must be held back.
        // 2.   We then have a chain of as many links as we started with, with potential defects to correct:
        //      a.  A link may contain some number of zeros: we can remove them if we are eager, based on size.
        //      b.  A link may contain more than 2K updates; we can split it.
        //      c.  Two adjacent links may contain fewer than 2K updates; we can meld (careful append) them.
        // 3.   After a pass of the above, we should have restored the invariant.
        //      We can try and me smarter and fuse some of the above work rather than explicitly stage results.
        //
        // The challenging moment is the merge that can start with a suffix of one link, involving a prefix of one link.
        // These could be the same link, different links, and generally there is the potential for complexity here.

        let mut builder = ChainBuilder::default();

        let mut queue1: std::collections::VecDeque<_> = list1.into();
        let mut queue2: std::collections::VecDeque<_> = list2.into();

        // The first unconsumed update in each block, via (k_idx, v_idx, t_idx), or None if exhausted.
        // These are (0,0,0) for a new block, and should become None once there are no remaining updates.
        let mut cursor1 = queue1.pop_front().map(|b| ((0,0,0), b));
        let mut cursor2 = queue2.pop_front().map(|b| ((0,0,0), b));

        // For each pair of batches
        while cursor1.is_some() && cursor2.is_some() {
            Self::merge_batch(&mut cursor1, &mut cursor2, &mut builder, stash);
            if cursor1.is_none() { cursor1 = queue1.pop_front().map(|b| ((0,0,0), b)); }
            if cursor2.is_none() { cursor2 = queue2.pop_front().map(|b| ((0,0,0), b)); }
        }

        // TODO: create batch for the non-empty cursor.
        if let Some(((k,v,t),batch)) = cursor1 {
            let mut out_batch = stash.pop().unwrap_or_default();
            let empty: Updates<U> = Default::default();
            write_from_surveys(
                &batch,
                &empty,
                &[Report::This(0, 1)],
                &[Report::This(k, batch.keys.values.len())],
                &[Report::This(v, batch.vals.values.len())],
                &[Report::This(t, batch.times.values.len())],
                &mut out_batch,
            );
            builder.push(out_batch);
        }
        if let Some(((k,v,t),batch)) = cursor2 {
            let mut out_batch = stash.pop().unwrap_or_default();
            let empty: Updates<U> = Default::default();
            write_from_surveys(
                &empty,
                &batch,
                &[Report::That(0, 1)],
                &[Report::That(k, batch.keys.values.len())],
                &[Report::That(v, batch.vals.values.len())],
                &[Report::That(t, batch.times.values.len())],
                &mut out_batch,
            );
            builder.push(out_batch);
        }

        builder.extend(queue1);
        builder.extend(queue2);
        *output = builder.done();
        // TODO: Tidy output to satisfy structural invariants.
    }

    /// Merge two batches, one completely and another through the corresponding prefix.
    ///
    /// Each invocation determines the maximum amount of both batches we can merge, determined
    /// by comparing the elements at the tails of each batch, and locating the lesser in other.
    /// We will merge the whole of the batch containing the lesser, and the prefix up through
    /// the lesser element in the other batch, setting the cursor to the first element strictly
    /// greater than that lesser element.
    ///
    /// The algorithm uses a list of `Report` findings to map the interleavings of the layers.
    /// Each indicates either a range exclusive to one of the inputs, or a one element common
    /// to the layers from both inputs, which must be further explored. This map would normally
    /// allow the full merge to happen, but we need to carefully start at each cursor, and end
    /// just before the first element greater than the lesser bound.
    ///
    /// The consumed prefix and disjoint suffix should be single report entries, and it seems
    /// fine to first produce all reports and then reflect on the cursors, rather than use the
    /// cursors as part of the mapping.
    #[inline(never)]
    fn merge_batch(
        batch1: &mut Option<((usize, usize, usize), Updates<U>)>,
        batch2: &mut Option<((usize, usize, usize), Updates<U>)>,
        builder: &mut ChainBuilder<U>,
        stash: &mut Vec<Updates<U>>,
    ) {
        // TODO: Optimization for one batch exceeding the other.

        let ((k0_idx, v0_idx, t0_idx), updates0) = batch1.take().unwrap();
        let ((k1_idx, v1_idx, t1_idx), updates1) = batch2.take().unwrap();

        use columnar::Borrow;
        let keys0 = updates0.keys.borrow();
        let keys1 = updates1.keys.borrow();
        let vals0 = updates0.vals.borrow();
        let vals1 = updates1.vals.borrow();
        let times0 = updates0.times.borrow();
        let times1 = updates1.times.borrow();

        // Survey the interleaving of the two inputs.
        let mut key_survey = survey::<columnar::ContainerOf<U::Key>>(keys0, keys1, &[Report::Both(0,0)]);
        let mut val_survey = survey::<columnar::ContainerOf<U::Val>>(vals0, vals1, &key_survey);
        let mut time_survey = survey::<columnar::ContainerOf<U::Time>>(times0, times1, &val_survey);

        // We now know enough to start writing into an output batch.
        // We should update the input surveys to reflect the subset
        // of data that we want.
        //
        // At most one cursor should be non-zero (assert!).
        // A non-zero cursor must correspond to the first entry of the surveys,
        // as there is at least one consumed update that precedes the other batch.
        // We need to nudge that report forward to align with the cursor, potentially
        // squeezing the report to nothing (to the upper bound).

        // We start by updating the surveys to reflect the cursors.
        // If either cursor is set, then its batch has an element strictly less than the other batch.
        // We therefore expect to find a prefix of This/That at the start of the survey.
        if (k0_idx, v0_idx, t0_idx) != (0,0,0) {
            let mut done = false; while !done { if let Report::This(l,u) = &mut key_survey[0] { if *u <= k0_idx { key_survey.remove(0); } else { *l = k0_idx; done = true; } } else { done = true; } }
            let mut done = false; while !done { if let Report::This(l,u) = &mut val_survey[0] { if *u <= v0_idx { val_survey.remove(0); } else { *l = v0_idx; done = true; } } else { done = true; } }
            let mut done = false; while !done { if let Report::This(l,u) = &mut time_survey[0] { if *u <= t0_idx { time_survey.remove(0); } else { *l = t0_idx; done = true; } } else { done = true; } }
        }

        if (k1_idx, v1_idx, t1_idx) != (0,0,0) {
            let mut done = false; while !done { if let Report::That(l,u) = &mut key_survey[0] { if *u <= k1_idx { key_survey.remove(0); } else { *l = k1_idx; done = true; } } else { done = true; } }
            let mut done = false; while !done { if let Report::That(l,u) = &mut val_survey[0] { if *u <= v1_idx { val_survey.remove(0); } else { *l = v1_idx; done = true; } } else { done = true; } }
            let mut done = false; while !done { if let Report::That(l,u) = &mut time_survey[0] { if *u <= t1_idx { time_survey.remove(0); } else { *l = t1_idx; done = true; } } else { done = true; } }
        }

        // We want to trim the tails of the surveys to only cover ranges present in both inputs.
        // We can determine which was "longer" by looking at the last entry of the bottom layer,
        // which tells us which input (or both) contained the last element.
        //
        // From the bottom layer up, we'll identify the index of the last item, and then determine
        // the index of the list it belongs to. We use that index in the next layer, to locate the
        // index of the list it belongs to, on upward.
        let next_cursor = match time_survey.last().unwrap() {
            Report::This(_,_) => {
                // Collect the last value indexes known to strictly exceed an entry in the other batch.
                let mut t = times0.values.len();
                while let Some(Report::This(l,_)) = time_survey.last() { t = *l; time_survey.pop(); }
                let mut v = vals0.values.len();
                while let Some(Report::This(l,_)) = val_survey.last() { v = *l; val_survey.pop(); }
                let mut k = keys0.values.len();
                while let Some(Report::This(l,_)) = key_survey.last() { k = *l; key_survey.pop(); }
                // Now we may need to correct by nudging down.
                if v == times0.len() || times0.bounds.bounds(v).0 > t { v -= 1; }
                if k == vals0.len() || vals0.bounds.bounds(k).0 > v { k -= 1; }
                Some(Ok((k,v,t)))
            }
            Report::Both(_,_) => { None }
            Report::That(_,_) => {
                // Collect the last value indexes known to strictly exceed an entry in the other batch.
                let mut t = times1.values.len();
                while let Some(Report::That(l,_)) = time_survey.last() { t = *l; time_survey.pop(); }
                let mut v = vals1.values.len();
                while let Some(Report::That(l,_)) = val_survey.last() { v = *l; val_survey.pop(); }
                let mut k = keys1.values.len();
                while let Some(Report::That(l,_)) = key_survey.last() { k = *l; key_survey.pop(); }
                // Now we may need to correct by nudging down.
                if v == times1.len() || times1.bounds.bounds(v).0 > t { v -= 1; }
                if k == vals1.len() || vals1.bounds.bounds(k).0 > v { k -= 1; }
                Some(Err((k,v,t)))
            }
        };

        // Having updated the surveys, we now copy over the ranges they identify.
        let mut out_batch = stash.pop().unwrap_or_default();
        // TODO: We should be able to size `out_batch` pretty accurately from the survey.
        write_from_surveys(&updates0, &updates1, &[Report::Both(0,0)], &key_survey, &val_survey, &time_survey, &mut out_batch);
        builder.push(out_batch);

        match next_cursor {
            Some(Ok(kvt)) => { *batch1 = Some((kvt, updates0)); }
            Some(Err(kvt)) => {*batch2 = Some((kvt, updates1)); }
            None => { }
        }
    }

}

/// Write merged output from four levels of survey reports.
///
/// Each layer is written independently: `write_layer` handles keys, vals,
/// and times; `write_diffs` handles diff consolidation.
#[inline(never)]
fn write_from_surveys<U: Update>(
    updates0: &Updates<U>,
    updates1: &Updates<U>,
    root_survey: &[Report],
    key_survey: &[Report],
    val_survey: &[Report],
    time_survey: &[Report],
    output: &mut Updates<U>,
) {
    use columnar::Borrow;

    write_layer(updates0.keys.borrow(), updates1.keys.borrow(), root_survey, key_survey, &mut output.keys);
    write_layer(updates0.vals.borrow(), updates1.vals.borrow(), key_survey, val_survey, &mut output.vals);
    write_layer(updates0.times.borrow(), updates1.times.borrow(), val_survey, time_survey, &mut output.times);
    write_diffs::<U>(updates0.diffs.borrow(), updates1.diffs.borrow(), time_survey, &mut output.diffs);
}

/// From two sequences of interleaved lists, map out the interleaving of their values.
///
/// The sequence of input reports identify constraints on the sorted order of lists in the two inputs,
/// callout out ranges of each that are exclusively order, and elements that have equal prefixes and
/// therefore "overlap" and should be further investigated through the values of the lists.
///
/// The output should have the same form but for the next layer: subject to the ordering of `reports`,
/// a similar report for the values of the two lists, appropriate for the next layer.
#[inline(never)]
pub fn survey<'a, C: columnar::Container<Ref<'a>: Ord>>(
    lists0: <super::super::updates::Lists<C> as columnar::Borrow>::Borrowed<'a>,
    lists1: <super::super::updates::Lists<C> as columnar::Borrow>::Borrowed<'a>,
    reports: &[Report],
) -> Vec<Report> {
    use columnar::Index;
    let mut output = Vec::with_capacity(reports.len()); // may grow larger, but at least this large.
    for report in reports.iter() {
        match report {
            Report::This(lower0, upper0) => {
                let (new_lower, _) = lists0.bounds.bounds(*lower0);
                let (_, new_upper) = lists0.bounds.bounds(*upper0-1);
                output.push(Report::This(new_lower, new_upper));
            }
            Report::Both(index0, index1) => {

                // Fetch the bounds from the layers.
                let (mut lower0, upper0) = lists0.bounds.bounds(*index0);
                let (mut lower1, upper1) = lists1.bounds.bounds(*index1);

                // Scour the intersecting range for matches.
                while lower0 < upper0 && lower1 < upper1 {
                    let val0 = lists0.values.get(lower0);
                    let val1 = lists1.values.get(lower1);
                    match val0.cmp(&val1) {
                        std::cmp::Ordering::Less => {
                            let start = lower0;
                            lower0 += 1;
                            gallop(lists0.values, &mut lower0, upper0, |x| x < val1);
                            output.push(Report::This(start, lower0));
                        },
                        std::cmp::Ordering::Equal => {
                            output.push(Report::Both(lower0, lower1));
                            lower0 += 1;
                            lower1 += 1;
                        },
                        std::cmp::Ordering::Greater => {
                            let start = lower1;
                            lower1 += 1;
                            gallop(lists1.values, &mut lower1, upper1, |x| x < val0);
                            output.push(Report::That(start, lower1));
                        },
                    }
                }
                if lower0 < upper0 { output.push(Report::This(lower0, upper0)); }
                if lower1 < upper1 { output.push(Report::That(lower1, upper1)); }

            }
            Report::That(lower1, upper1) => {
                let (new_lower, _) = lists1.bounds.bounds(*lower1);
                let (_, new_upper) = lists1.bounds.bounds(*upper1-1);
                output.push(Report::That(new_lower, new_upper));
            }
        }
    }

    output
}

/// Write one layer of merged output from a list survey and item survey.
///
/// The list survey describes which lists to produce (from the layer above).
/// The item survey describes how the items within those lists interleave.
/// Both surveys are consumed completely; a mismatch is a bug.
///
/// Pruning (from cursor adjustments) can affect the first and last list
/// survey entries: the item survey's ranges may not match the natural
/// bounds of those lists. Middle entries are guaranteed unpruned and can
/// be bulk-copied.
#[inline(never)]
pub fn write_layer<'a, C: columnar::Container<Ref<'a>: Ord>>(
    lists0: <super::super::updates::Lists<C> as columnar::Borrow>::Borrowed<'a>,
    lists1: <super::super::updates::Lists<C> as columnar::Borrow>::Borrowed<'a>,
    list_survey: &[Report],
    item_survey: &[Report],
    output: &mut super::super::updates::Lists<C>,
) {
    use columnar::{Container, Index};

    let mut item_idx = 0;

    for (pos, list_report) in list_survey.iter().enumerate() {
        let is_first = pos == 0;
        let is_last = pos == list_survey.len() - 1;
        let may_be_pruned = is_first || is_last;

        match list_report {
            Report::This(lo, hi) => {
                let Report::This(item_lo, item_hi) = item_survey[item_idx] else { unreachable!("Expected This in item survey for This list") };
                item_idx += 1;
                if may_be_pruned {
                    // Item range may not match natural bounds; copy items in bulk
                    // but compute per-list bounds from natural bounds clamped to
                    // the item range.
                    let base = output.values.len();
                    output.values.extend_from_self(lists0.values, item_lo..item_hi);
                    for i in *lo..*hi {
                        let (_, nat_hi) = lists0.bounds.bounds(i);
                        output.bounds.push((base + nat_hi.min(item_hi) - item_lo) as u64);
                    }
                } else {
                    output.extend_from_self(lists0, *lo..*hi);
                }
            }
            Report::That(lo, hi) => {
                let Report::That(item_lo, item_hi) = item_survey[item_idx] else { unreachable!("Expected That in item survey for That list") };
                item_idx += 1;
                if may_be_pruned {
                    let base = output.values.len();
                    output.values.extend_from_self(lists1.values, item_lo..item_hi);
                    for i in *lo..*hi {
                        let (_, nat_hi) = lists1.bounds.bounds(i);
                        output.bounds.push((base + nat_hi.min(item_hi) - item_lo) as u64);
                    }
                } else {
                    output.extend_from_self(lists1, *lo..*hi);
                }
            }
            Report::Both(i0, i1) => {
                // Merge: consume item survey entries until both sides are covered.
                let (mut c0, end0) = lists0.bounds.bounds(*i0);
                let (mut c1, end1) = lists1.bounds.bounds(*i1);
                while (c0 < end0 || c1 < end1) && item_idx < item_survey.len() {
                    match item_survey[item_idx] {
                        Report::This(lo, hi) => {
                            if lo >= end0 { break; }
                            output.values.extend_from_self(lists0.values, lo..hi);
                            c0 = hi;
                        }
                        Report::That(lo, hi) => {
                            if lo >= end1 { break; }
                            output.values.extend_from_self(lists1.values, lo..hi);
                            c1 = hi;
                        }
                        Report::Both(v0, v1) => {
                            if v0 >= end0 && v1 >= end1 { break; }
                            output.values.push(lists0.values.get(v0));
                            c0 = v0 + 1;
                            c1 = v1 + 1;
                        }
                    }
                    item_idx += 1;
                }
                output.bounds.push(output.values.len() as u64);
            }
        }
    }
}

/// Write the diff layer from a time survey and two diff inputs.
///
/// The time survey is the item-level survey for the time layer, which
/// doubles as the list survey for diffs (one diff list per time entry).
///
/// - `This(lo, hi)`: bulk-copy diff lists from input 0.
/// - `That(lo, hi)`: bulk-copy diff lists from input 1.
/// - `Both(t0, t1)`: consolidate the two singleton diffs. Push `[sum]`
///   if non-zero, or an empty list `[]` if they cancel.
#[inline(never)]
pub fn write_diffs<U: super::super::layout::ColumnarUpdate>(
    diffs0: <super::super::updates::Lists<columnar::ContainerOf<U::Diff>> as columnar::Borrow>::Borrowed<'_>,
    diffs1: <super::super::updates::Lists<columnar::ContainerOf<U::Diff>> as columnar::Borrow>::Borrowed<'_>,
    time_survey: &[Report],
    output: &mut super::super::updates::Lists<columnar::ContainerOf<U::Diff>>,
) {
    use columnar::{Columnar, Container, Index, Len, Push};
    use crate::difference::{Semigroup, IsZero};

    for report in time_survey.iter() {
        match report {
            Report::This(lo, hi) => { output.extend_from_self(diffs0, *lo..*hi); }
            Report::That(lo, hi) => { output.extend_from_self(diffs1, *lo..*hi); }
            Report::Both(t0, t1) => {
                // Read singleton diffs via list bounds, consolidate.
                let (d0_lo, d0_hi) = diffs0.bounds.bounds(*t0);
                let (d1_lo, d1_hi) = diffs1.bounds.bounds(*t1);
                assert_eq!(d0_hi - d0_lo, 1, "Expected singleton diff list at t0={t0}");
                assert_eq!(d1_hi - d1_lo, 1, "Expected singleton diff list at t1={t1}");
                let mut diff: U::Diff = Columnar::into_owned(diffs0.values.get(d0_lo));
                diff.plus_equals(&Columnar::into_owned(diffs1.values.get(d1_lo)));
                if !diff.is_zero() { output.values.push(&diff); }
                output.bounds.push(output.values.len() as u64);
            }
        }
    }
}

/// Increments `index` until just after the last element of `input` to satisfy `cmp`.
///
/// The method assumes that `cmp` is monotonic, never becoming true once it is false.
/// If an `upper` is supplied, it acts as a constraint on the interval of `input` explored.
#[inline(always)]
pub(crate) fn gallop<C: columnar::Index>(input: C, lower: &mut usize, upper: usize, mut cmp: impl FnMut(<C as columnar::Index>::Ref) -> bool) {
    // if empty input, or already >= element, return
    if *lower < upper && cmp(input.get(*lower)) {
        let mut step = 1;
        while *lower + step < upper && cmp(input.get(*lower + step)) {
            *lower += step;
            step <<= 1;
        }

        step >>= 1;
        while step > 0 {
            if *lower + step < upper && cmp(input.get(*lower + step)) {
                *lower += step;
            }
            step >>= 1;
        }

        *lower += 1;
    }
}

/// A report we would expect to see in a sequence about two layers.
///
/// A sequence of these reports reveal an ordered traversal of the keys
/// of two layers, with ranges exclusive to one, ranges exclusive to the
/// other, and individual elements (not ranges) common to both.
#[derive(Copy, Clone, columnar::Columnar, Debug)]
pub enum Report {
    /// Range of indices in this input.
    This(usize, usize),
    /// Range of indices in that input.
    That(usize, usize),
    /// Matching indices in both inputs.
    Both(usize, usize),
}

/// Accumulates a sequence of `Updates` chunks, merging the tail when a new
/// chunk would extend the current run rather than start a new one.
pub struct ChainBuilder<U: super::super::layout::ColumnarUpdate> { updates: Vec<Updates<U>> }

impl<U: super::super::layout::ColumnarUpdate> Default for ChainBuilder<U> { fn default() -> Self { Self { updates: Default::default() } } }

impl<U: super::super::layout::ColumnarUpdate> ChainBuilder<U> {
    fn push(&mut self, mut link: Updates<U>) {
        link = link.filter_zero();
        if link.len() > 0 {
            if let Some(last) = self.updates.last_mut() {
                if last.len() + link.len() < 2 * crate::columnar::LINK_TARGET {
                    let mut build = super::super::updates::UpdatesBuilder::new_from(std::mem::take(last));
                    build.meld(&link);
                    *last = build.done();
                }
                else { self.updates.push(link); }

            }
            else { self.updates.push(link); }
        }
    }
    fn extend(&mut self, iter: impl IntoIterator<Item=Updates<U>>) { for link in iter { self.push(link); }}
    fn done(self) -> Vec<Updates<U>> { self.updates }
}
