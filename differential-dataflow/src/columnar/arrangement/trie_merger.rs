//! Batch-at-a-time merging of sorted, consolidated `UpdatesTyped` chains.
//!
//! The core is `merge_batches`, which walks pairs of chunks via
//! `merge_batch`, building a chain of merged outputs with `ChainBuilder`.
//! `survey` maps the interleaving of the two inputs at each trie layer,
//! `write_from_surveys` (via `write_layer` and `write_diffs`) copies the
//! ranges that the surveys identify into the output trie.

use columnar::{Columnar, Len};
use timely::progress::frontier::{Antichain, AntichainRef};

use super::super::layout::ColumnarUpdate as Update;
use super::super::updates::UpdatesTyped;

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

/// Build sorted `UpdatesTyped` chunks from a sorted iterator of refs,
/// using `UpdatesTyped::form` (which consolidates internally) on batches.
fn form_chunks<'a, U: Update>(
    sorted: impl Iterator<Item = columnar::Ref<'a, super::super::updates::Tuple<U>>>,
    output: &mut Vec<UpdatesTyped<U>>,
) {
    let mut sorted = sorted.peekable();
    while sorted.peek().is_some() {
        let chunk = UpdatesTyped::<U>::form((&mut sorted).take(crate::columnar::LINK_TARGET));
        if chunk.len() > 0 {
            output.push(chunk);
        }
    }
}

/// Partition `merged` into chunks ready to ship (times strictly less than `upper`)
/// and chunks kept for future seals (times at-or-after `upper`), updating
/// `frontier` to the antichain of kept times. `merged` is consumed lazily,
/// and outputs flow through `ship` / `kept` sinks so the caller can spill or
/// forward as chunks are produced rather than buffering them.
pub fn extract<U, I, FShip, FKept>(
    merged: I,
    upper: AntichainRef<U::Time>,
    frontier: &mut Antichain<U::Time>,
    mut ship: FShip,
    mut kept: FKept,
)
where
    U: Update,
    U::Time: 'static,
    I: IntoIterator<Item = UpdatesTyped<U>>,
    FShip: FnMut(UpdatesTyped<U>),
    FKept: FnMut(UpdatesTyped<U>),
{
    use columnar::{Container, ContainerOf, Index, Push};
    use columnar::primitive::offsets::Strides;
    use crate::columnar::updates::{Lists, retain_items};

    // TODO: rework to move from trie structure to trie structure.
    let mut time_owned = U::Time::default();
    let mut bitmap = Vec::new();    // update should be kept.
    for chunk in merged {
        bitmap.clear();
        let view = chunk.view();
        let times = view.times.values;
        for idx in 0 .. times.len() {
            Columnar::copy_from(&mut time_owned, times.get(idx));
            if upper.less_equal(&time_owned) {
                frontier.insert_ref(&time_owned);
                bitmap.push(true);
            }
            else { bitmap.push(false); }
        }
        if bitmap.iter().all(|x| *x) { kept(chunk); }
        else if bitmap.iter().all(|x| !*x) { ship(chunk); }
        else {

            let (times, temp) = retain_items::<ContainerOf<U::Time>>(view.times, &bitmap[..]);
            let (vals, temp) = retain_items::<ContainerOf<U::Val>>(view.vals, &temp[..]);
            let (keys, _temp) = retain_items::<ContainerOf<U::Key>>(view.keys, &temp[..]);
            let d_borrow = view.diffs;
            let mut diffs = <Lists::<ContainerOf<U::Diff>> as Container>::with_capacity_for([d_borrow].into_iter());
            for (index, bit) in bitmap.iter().enumerate() {
                if *bit { diffs.values.push(d_borrow.values.get(index)); }
            }
            diffs.bounds = Strides::new(1, times.values.len() as u64);
            kept(UpdatesTyped {
                keys,
                vals,
                times,
                diffs,
            });

            for bit in bitmap.iter_mut() { *bit = !*bit; }

            let (times, temp) = retain_items::<ContainerOf<U::Time>>(view.times, &bitmap[..]);
            let (vals, temp) = retain_items::<ContainerOf<U::Val>>(view.vals, &temp[..]);
            let (keys, _temp) = retain_items::<ContainerOf<U::Key>>(view.keys, &temp[..]);
            let d_borrow = view.diffs;
            let mut diffs = <Lists::<ContainerOf<U::Diff>> as Container>::with_capacity_for([d_borrow].into_iter());
            for (index, bit) in bitmap.iter().enumerate() {
                if *bit { diffs.values.push(d_borrow.values.get(index)); }
            }
            diffs.bounds = Strides::new(1, times.values.len() as u64);
            ship(UpdatesTyped {
                keys,
                vals,
                times,
                diffs,
            });
        }
    }
}

/// Iterator-based merge: flatten, merge, consolidate, form.
/// Correct but slow — used as fallback.
#[allow(dead_code)]
fn merge_iterator<U: Update>(
    list1: &[UpdatesTyped<U>],
    list2: &[UpdatesTyped<U>],
    output: &mut Vec<UpdatesTyped<U>>,
)
where
    U::Time: 'static,
{
    let iter1 = list1.iter().flat_map(|chunk| chunk.iter());
    let iter2 = list2.iter().flat_map(|chunk| chunk.iter());

    let merged = Merging {
        iter1: iter1.peekable(),
        iter2: iter2.peekable(),
    };

    form_chunks::<U>(merged, output);
}

/// A merge implementation that operates batch-at-a-time.
///
/// Inputs are taken as `IntoIterator` so the caller can stream chunks in
/// lazily — e.g. fetching paged-out chunks one group at a time — rather than
/// materializing entire chains up front. Output chunks are emitted via the
/// caller-supplied `sink` as they become stable, so the caller can apply a
/// spill policy mid-merge rather than buffering the full result.
#[inline(never)]
pub fn merge_batches<U, I1, I2, S>(
    list1: I1,
    list2: I2,
    sink: S,
)
where
    U: Update,
    U::Time: 'static,
    I1: IntoIterator<Item = UpdatesTyped<U>>,
    I2: IntoIterator<Item = UpdatesTyped<U>>,
    S: FnMut(UpdatesTyped<U>),
{

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

    let mut builder = ChainBuilder::new(sink);

    let mut iter1 = list1.into_iter();
    let mut iter2 = list2.into_iter();

    // The first unconsumed update in each block, via (k_idx, v_idx, t_idx), or None if exhausted.
    // These are (0,0,0) for a new block, and should become None once there are no remaining updates.
    let mut cursor1 = iter1.next().map(|b| ((0,0,0), b));
    let mut cursor2 = iter2.next().map(|b| ((0,0,0), b));

    // For each pair of batches
    while cursor1.is_some() && cursor2.is_some() {
        merge_batch(&mut cursor1, &mut cursor2, &mut builder);
        if cursor1.is_none() { cursor1 = iter1.next().map(|b| ((0,0,0), b)); }
        if cursor2.is_none() { cursor2 = iter2.next().map(|b| ((0,0,0), b)); }
    }

    // TODO: create batch for the non-empty cursor.
    if let Some(((k,v,t),batch)) = cursor1 {
        let mut out_batch = UpdatesTyped::<U>::default();
        let empty: UpdatesTyped<U> = Default::default();
        let view = batch.view();
        write_from_surveys(
            &batch,
            &empty,
            &[Report::This(0, 1)],
            &[Report::This(k, view.keys.values.len())],
            &[Report::This(v, view.vals.values.len())],
            &[Report::This(t, view.times.values.len())],
            &mut out_batch,
        );
        builder.push(out_batch);
    }
    if let Some(((k,v,t),batch)) = cursor2 {
        let mut out_batch = UpdatesTyped::<U>::default();
        let empty: UpdatesTyped<U> = Default::default();
        let view = batch.view();
        write_from_surveys(
            &empty,
            &batch,
            &[Report::That(0, 1)],
            &[Report::That(k, view.keys.values.len())],
            &[Report::That(v, view.vals.values.len())],
            &[Report::That(t, view.times.values.len())],
            &mut out_batch,
        );
        builder.push(out_batch);
    }

    builder.extend(iter1);
    builder.extend(iter2);
    builder.done();
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
fn merge_batch<U: Update, F: FnMut(UpdatesTyped<U>)>(
    batch1: &mut Option<((usize, usize, usize), UpdatesTyped<U>)>,
    batch2: &mut Option<((usize, usize, usize), UpdatesTyped<U>)>,
    builder: &mut ChainBuilder<U, F>,
)
where
    U::Time: 'static,
{
    // TODO: Optimization for one batch exceeding the other.

    let ((k0_idx, v0_idx, t0_idx), updates0) = batch1.take().unwrap();
    let ((k1_idx, v1_idx, t1_idx), updates1) = batch2.take().unwrap();

    let view0 = updates0.view();
    let view1 = updates1.view();
    let keys0 = view0.keys;
    let keys1 = view1.keys;
    let vals0 = view0.vals;
    let vals1 = view1.vals;
    let times0 = view0.times;
    let times1 = view1.times;

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
    let mut out_batch = UpdatesTyped::<U>::default();
    // TODO: We should be able to size `out_batch` pretty accurately from the survey.
    write_from_surveys(&updates0, &updates1, &[Report::Both(0,0)], &key_survey, &val_survey, &time_survey, &mut out_batch);
    builder.push(out_batch);

    match next_cursor {
        Some(Ok(kvt)) => { *batch1 = Some((kvt, updates0)); }
        Some(Err(kvt)) => {*batch2 = Some((kvt, updates1)); }
        None => { }
    }
}

/// Write merged output from four levels of survey reports.
///
/// Each layer is written independently: `write_layer` handles keys, vals,
/// and times; `write_diffs` handles diff consolidation.
#[inline(never)]
fn write_from_surveys<U: Update>(
    updates0: &UpdatesTyped<U>,
    updates1: &UpdatesTyped<U>,
    root_survey: &[Report],
    key_survey: &[Report],
    val_survey: &[Report],
    time_survey: &[Report],
    output: &mut UpdatesTyped<U>,
) {
    let view0 = updates0.view();
    let view1 = updates1.view();
    write_layer(view0.keys, view1.keys, root_survey, key_survey, &mut output.keys);
    write_layer(view0.vals, view1.vals, key_survey, val_survey, &mut output.vals);
    write_layer(view0.times, view1.times, val_survey, time_survey, &mut output.times);
    write_diffs::<U>(view0.diffs, view1.diffs, time_survey, &mut output.diffs);
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

/// Accumulates `UpdatesTyped` chunks one at a time, melding small adjacent
/// chunks. Holds at most one chunk in memory (the meld target); whenever a
/// push doesn't meld, the prior target becomes "stable" and is emitted via
/// the caller-provided sink. The sink can spill, count, or forward the chunk
/// however it likes.
pub struct ChainBuilder<U: super::super::layout::ColumnarUpdate, F: FnMut(UpdatesTyped<U>)> {
    last: Option<UpdatesTyped<U>>,
    sink: F,
}

impl<U: super::super::layout::ColumnarUpdate, F: FnMut(UpdatesTyped<U>)> ChainBuilder<U, F>
where
    U::Time: 'static,
{
    fn new(sink: F) -> Self { Self { last: None, sink } }

    fn push(&mut self, mut link: UpdatesTyped<U>) {
        link = link.filter_zero();
        if link.len() == 0 { return; }
        // Split links larger than twice the link target so downstream chains
        // have multiple entries — required for per-chain spill policies (e.g.
        // `Threshold` in the columnar_spill example) to actually spill anything.
        if link.len() > 2 * crate::columnar::LINK_TARGET {
            let (first, rest) = split_at::<U>(link, crate::columnar::LINK_TARGET);
            self.push(first);
            self.push(rest);
            return;
        }
        match self.last.as_mut() {
            Some(last) if last.len() + link.len() < 2 * crate::columnar::LINK_TARGET => {
                let mut build = super::super::updates::UpdatesBuilder::new_from(std::mem::take(last));
                build.meld(&link);
                *last = build.done();
            }
            _ => {
                if let Some(prev) = self.last.take() {
                    (self.sink)(prev);
                }
                self.last = Some(link);
            }
        }
    }
    fn extend(&mut self, iter: impl IntoIterator<Item=UpdatesTyped<U>>) {
        for link in iter { self.push(link); }
    }
    fn done(mut self) {
        if let Some(last) = self.last.take() {
            (self.sink)(last);
        }
    }
}

/// Split `chunk` into two `UpdatesTyped` parts at record index `n`: the first
/// `n` records and the remaining `chunk.len() - n`. Bitmap pattern mirrors
/// `extract`'s split between ship/kept halves.
fn split_at<U: Update>(chunk: UpdatesTyped<U>, n: usize) -> (UpdatesTyped<U>, UpdatesTyped<U>)
where
    U::Time: 'static,
{
    use columnar::{Container, ContainerOf, Index, Push};
    use columnar::primitive::offsets::Strides;
    use crate::columnar::updates::{Lists, retain_items};

    let total = chunk.len();
    if n == 0 { return (UpdatesTyped::default(), chunk); }
    if n >= total { return (chunk, UpdatesTyped::default()); }

    let view = chunk.view();
    let mut bitmap: Vec<bool> = (0..total).map(|i| i < n).collect();

    // First half: records [0, n).
    let (times, temp) = retain_items::<ContainerOf<U::Time>>(view.times, &bitmap[..]);
    let (vals, temp) = retain_items::<ContainerOf<U::Val>>(view.vals, &temp[..]);
    let (keys, _temp) = retain_items::<ContainerOf<U::Key>>(view.keys, &temp[..]);
    let d_borrow = view.diffs;
    let mut diffs = <Lists::<ContainerOf<U::Diff>> as Container>::with_capacity_for([d_borrow].into_iter());
    for (i, &bit) in bitmap.iter().enumerate() {
        if bit { diffs.values.push(d_borrow.values.get(i)); }
    }
    diffs.bounds = Strides::new(1, times.values.len() as u64);
    let first = UpdatesTyped { keys, vals, times, diffs };

    // Invert and build second half: records [n, total).
    for bit in bitmap.iter_mut() { *bit = !*bit; }
    let (times, temp) = retain_items::<ContainerOf<U::Time>>(view.times, &bitmap[..]);
    let (vals, temp) = retain_items::<ContainerOf<U::Val>>(view.vals, &temp[..]);
    let (keys, _temp) = retain_items::<ContainerOf<U::Key>>(view.keys, &temp[..]);
    let mut diffs = <Lists::<ContainerOf<U::Diff>> as Container>::with_capacity_for([d_borrow].into_iter());
    for (i, &bit) in bitmap.iter().enumerate() {
        if bit { diffs.values.push(d_borrow.values.get(i)); }
    }
    diffs.bounds = Strides::new(1, times.values.len() as u64);
    let second = UpdatesTyped { keys, vals, times, diffs };

    (first, second)
}
