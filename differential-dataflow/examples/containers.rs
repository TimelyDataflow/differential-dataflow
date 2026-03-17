//! Demonstrates the unified update container abstraction.
//!
//! This example uses a deliberately simple container implementation —
//! a sorted `Vec<(K, V, T, R)>` — to exercise the `MergeContainer` and
//! `ExtractVia` traits across all five sites where differential dataflow
//! manipulates bundles of updates:
//!
//! 1. Collection streams (sorted containers flowing through operators)
//! 2. Batcher (chain of sorted containers, merge-sort stacking)
//! 3. Batcher seal (extract by frontier)
//! 4. Trace batch merging (merge with time advancement, shared batches)
//! 5. Operator bulk loading (extract by key set with advancement)

use std::fmt::Debug;
use std::rc::Rc;

// ---------------------------------------------------------------------------
// Trait definitions
// ---------------------------------------------------------------------------

/// An antichain of times, used for frontier operations.
/// Simplified: just a set of incomparable times.
#[derive(Clone, Debug)]
struct Antichain<T> {
    elements: Vec<T>,
}

impl<T: Ord + Clone> Antichain<T> {
    fn from_elem(t: T) -> Self {
        Antichain { elements: vec![t] }
    }
    /// Returns true if `time` is not greater-or-equal to any element.
    /// For a total order, this is just: time < min(elements).
    fn less_than(&self, time: &T) -> bool {
        self.elements.iter().all(|e| time < e)
    }
    /// Advance `time` to the join (max, for a total order) with the frontier.
    fn advance(&self, time: &T) -> T where T: Ord + Clone {
        let mut result = time.clone();
        for e in &self.elements {
            if *e > result {
                result = e.clone();
            }
        }
        result
    }
}

/// A read position into a shared (or owned) container.
///
/// Containers in a trace are shared via `Rc` — multiple readers may
/// hold references to the same container. The offset tracks per-reader
/// consumption without mutating the underlying container.
struct ChainEntry<C> {
    container: Rc<C>,
    offset: usize,
}

impl<C> ChainEntry<C> {
    fn from_owned(container: C) -> Self {
        ChainEntry { container: Rc::new(container), offset: 0 }
    }
}

impl<C> Clone for ChainEntry<C> {
    fn clone(&self) -> Self {
        ChainEntry {
            container: Rc::clone(&self.container),
            offset: self.offset,
        }
    }
}

/// Core trait for sorted update containers.
///
/// A container is an opaque, bounded-size, immutable collection of sorted
/// updates. Methods take `ChainEntry<Self>` rather than `&mut Self`,
/// because containers in a trace are shared via `Rc`. The offset in the
/// `ChainEntry` tracks per-reader consumption.
///
/// The internal representation is not prescribed — this trait describes
/// what the container can do, not how it is structured.
trait MergeContainer: Sized {
    type Time: Ord + Clone + Debug;

    /// Merge from the heads of two peer containers into a new (owned)
    /// container of approximately `target_size` logical updates.
    ///
    /// Both entries advance their offsets to reflect consumed input.
    /// If `advance` is provided, times are advanced by the frontier
    /// and entries are re-sorted and consolidated within each
    /// (key, value) group.
    fn merge_step(
        left: &mut ChainEntry<Self>,
        right: &mut ChainEntry<Self>,
        advance: Option<&Antichain<Self::Time>>,
        target_size: usize,
    ) -> Self;

    /// Partition by time frontier: returns (extracted, kept) where
    /// `extracted` contains entries not beyond `frontier`, and `kept`
    /// contains the rest.
    ///
    /// Takes ownership — this is used by the batcher at seal time,
    /// before containers are shared via Rc.
    fn extract_frontier(
        self,
        frontier: &Antichain<Self::Time>,
    ) -> (Self, Self);

    /// Total number of logical updates in the container (ignoring offset).
    fn total_len(&self) -> usize;

    /// Number of logical updates remaining (after offset).
    fn remaining(entry: &ChainEntry<Self>) -> usize {
        entry.container.total_len() - entry.offset
    }

    /// Whether all entries have been consumed.
    fn exhausted(entry: &ChainEntry<Self>) -> bool {
        entry.offset >= entry.container.total_len()
    }
}

/// Key-filtered extraction with time advancement.
///
/// Separated from `MergeContainer` because not all consumers need it,
/// and the key set representation may vary (hence the type parameter).
trait ExtractVia<K: ?Sized>: MergeContainer {
    /// Extract entries matching `keys` into a new (owned) container.
    /// Times are advanced by `frontier` and consolidated.
    fn extract_keys(
        source: &mut ChainEntry<Self>,
        keys: &K,
        advance: &Antichain<Self::Time>,
        target_size: usize,
    ) -> Self;
}

// ---------------------------------------------------------------------------
// Simple implementation: Vec<(K, V, T, R)>
// ---------------------------------------------------------------------------

/// A sorted vector of (key, val, time, diff) tuples.
/// Immutable once constructed. Dead simple, intentionally unoptimized.
#[derive(Default)]
struct VecContainer<K, V, T, R> {
    data: Vec<(K, V, T, R)>,
}

impl<K: Ord + Clone + Debug, V: Ord + Clone + Debug, T: Ord + Clone + Debug, R: Ord + Clone + Debug + Diff>
    VecContainer<K, V, T, R>
{
    /// Construct from a possibly-unsorted vector. Sorts and consolidates.
    fn from_unsorted(mut data: Vec<(K, V, T, R)>) -> Self {
        data.sort_by(|a, b| (&a.0, &a.1, &a.2).cmp(&(&b.0, &b.1, &b.2)));
        consolidate(&mut data);
        VecContainer { data }
    }

    /// The live slice (everything after some offset).
    fn live(&self, offset: usize) -> &[(K, V, T, R)] {
        &self.data[offset..]
    }

    fn total_len(&self) -> usize {
        self.data.len()
    }
}

impl<K, V, T, R> std::fmt::Debug for VecContainer<K, V, T, R>
where K: Debug, V: Debug, T: Debug, R: Debug
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VecContainer({} entries)", self.data.len())
    }
}

/// Trait for diff types: can be added together, can be zero.
trait Diff: Clone {
    fn plus_equals(&mut self, other: &Self);
    fn is_zero(&self) -> bool;
}

impl Diff for i64 {
    fn plus_equals(&mut self, other: &Self) { *self += other; }
    fn is_zero(&self) -> bool { *self == 0 }
}

/// Sort by (K, V, T), then consolidate diffs for matching (K, V, T) triples.
/// Removes entries with zero diff.
fn consolidate<K: Ord, V: Ord, T: Ord, R: Diff>(data: &mut Vec<(K, V, T, R)>) {
    data.sort_by(|a, b| (&a.0, &a.1, &a.2).cmp(&(&b.0, &b.1, &b.2)));
    let mut write = 0;
    for read in 0..data.len() {
        if write > 0
            && data[write - 1].0 == data[read].0
            && data[write - 1].1 == data[read].1
            && data[write - 1].2 == data[read].2
        {
            let diff = data[read].3.clone();
            data[write - 1].3.plus_equals(&diff);
        } else {
            if write < read {
                data.swap(write, read);
            }
            write += 1;
        }
    }
    data.truncate(write);
    data.retain(|x| !x.3.is_zero());
}

/// In-place consolidation for already-sorted data (merge output).
fn consolidate_sorted<K: Eq, V: Eq, T: Eq, R: Diff>(data: &mut Vec<(K, V, T, R)>) {
    let mut write = 0;
    for read in 0..data.len() {
        if write > 0
            && data[write - 1].0 == data[read].0
            && data[write - 1].1 == data[read].1
            && data[write - 1].2 == data[read].2
        {
            let diff = data[read].3.clone();
            data[write - 1].3.plus_equals(&diff);
        } else {
            if write < read {
                data.swap(write, read);
            }
            write += 1;
        }
    }
    data.truncate(write);
    data.retain(|x| !x.3.is_zero());
}

impl<K, V, T, R> MergeContainer for VecContainer<K, V, T, R>
where
    K: Ord + Clone + Debug,
    V: Ord + Clone + Debug,
    T: Ord + Clone + Debug,
    R: Ord + Clone + Debug + Diff,
{
    type Time = T;

    fn merge_step(
        left: &mut ChainEntry<Self>,
        right: &mut ChainEntry<Self>,
        advance: Option<&Antichain<T>>,
        target_size: usize,
    ) -> Self {
        let mut result = Vec::with_capacity(target_size);
        let mut li = left.offset;
        let mut ri = right.offset;
        let ldata = &left.container.data;
        let rdata = &right.container.data;

        while result.len() < target_size && (li < ldata.len() || ri < rdata.len()) {
            let take_left = if li >= ldata.len() {
                false
            } else if ri >= rdata.len() {
                true
            } else {
                (&ldata[li].0, &ldata[li].1, &ldata[li].2)
                    <= (&rdata[ri].0, &rdata[ri].1, &rdata[ri].2)
            };

            let (k, v, t, r) = if take_left {
                let entry = &ldata[li];
                li += 1;
                (entry.0.clone(), entry.1.clone(), entry.2.clone(), entry.3.clone())
            } else {
                let entry = &rdata[ri];
                ri += 1;
                (entry.0.clone(), entry.1.clone(), entry.2.clone(), entry.3.clone())
            };

            let t = if let Some(frontier) = advance {
                frontier.advance(&t)
            } else {
                t
            };

            result.push((k, v, t, r));
        }

        left.offset = li;
        right.offset = ri;

        // If we advanced times, we may have created duplicates within (k,v) groups.
        if advance.is_some() {
            consolidate(&mut result);
        } else {
            consolidate_sorted(&mut result);
        }

        VecContainer { data: result }
    }

    fn extract_frontier(
        self,
        frontier: &Antichain<Self::Time>,
    ) -> (Self, Self) {
        let mut extracted = Vec::new();
        let mut kept = Vec::new();

        for item in self.data {
            if frontier.less_than(&item.2) {
                extracted.push(item);
            } else {
                kept.push(item);
            }
        }

        (VecContainer { data: extracted }, VecContainer { data: kept })
    }

    fn total_len(&self) -> usize {
        self.data.len()
    }
}

/// For bulk loading: extract by a sorted slice of keys.
impl<K, V, T, R> ExtractVia<[K]> for VecContainer<K, V, T, R>
where
    K: Ord + Clone + Debug,
    V: Ord + Clone + Debug,
    T: Ord + Clone + Debug,
    R: Ord + Clone + Debug + Diff,
{
    fn extract_keys(
        source: &mut ChainEntry<Self>,
        keys: &[K],
        advance: &Antichain<T>,
        target_size: usize,
    ) -> Self {
        let mut result = Vec::with_capacity(target_size);
        let mut ki = 0;
        let mut si = source.offset;
        let sdata = &source.container.data;

        while result.len() < target_size && ki < keys.len() && si < sdata.len() {
            match sdata[si].0.cmp(&keys[ki]) {
                std::cmp::Ordering::Less => {
                    si += 1;
                }
                std::cmp::Ordering::Equal => {
                    let entry = &sdata[si];
                    let t = advance.advance(&entry.2);
                    result.push((entry.0.clone(), entry.1.clone(), t, entry.3.clone()));
                    si += 1;
                }
                std::cmp::Ordering::Greater => {
                    ki += 1;
                }
            }
        }

        source.offset = si;
        consolidate(&mut result);

        VecContainer { data: result }
    }
}

// ---------------------------------------------------------------------------
// Chain management (generic over any MergeContainer)
// ---------------------------------------------------------------------------

/// Drop empty entries from the front of a chain.
fn drop_empty_prefix<C: MergeContainer>(chain: &mut Vec<ChainEntry<C>>) {
    while chain.first().map_or(false, |e| C::exhausted(e)) {
        chain.remove(0);
    }
}

/// Push a container onto an owned output chain, merging with the last
/// container if both are undersized (the stacking discipline).
///
/// Output chains hold owned containers (not yet shared). We wrap them
/// in ChainEntry temporarily for merge_step, then unwrap.
fn push_with_stacking<C: MergeContainer>(
    output: &mut Vec<C>,
    container: C,
    advance: Option<&Antichain<C::Time>>,
    target_size: usize,
) {
    output.push(container);
    while output.len() >= 2 {
        let n = output.len();
        if output[n - 1].total_len() + output[n - 2].total_len() <= target_size {
            let b = output.pop().unwrap();
            let a = output.pop().unwrap();
            let mut ea = ChainEntry::from_owned(a);
            let mut eb = ChainEntry::from_owned(b);
            let merged = C::merge_step(&mut ea, &mut eb, advance, target_size);
            output.push(merged);
        } else {
            break;
        }
    }
}

/// Merge two shared chains into a new owned chain, doing at most `fuel` work.
/// Input chains are mutated: consumed entries have their offsets advanced,
/// and fully-consumed entries are dropped (potentially releasing Rc memory).
///
/// When one chain is exhausted, the remaining chain is drained by merging
/// each entry with an empty partner. This handles time advancement for
/// single-chain drain and keeps the code generic.
fn merge_chains<C: MergeContainer + Default>(
    left: &mut Vec<ChainEntry<C>>,
    right: &mut Vec<ChainEntry<C>>,
    advance: Option<&Antichain<C::Time>>,
    target_size: usize,
    fuel: &mut usize,
) -> Vec<C> {
    let mut output = Vec::new();

    while *fuel > 0 {
        drop_empty_prefix(left);
        drop_empty_prefix(right);

        match (left.first_mut(), right.first_mut()) {
            (Some(l), Some(r)) => {
                let merged = C::merge_step(l, r, advance, target_size);
                let produced = merged.total_len();
                if produced == 0 { break; }
                *fuel = fuel.saturating_sub(produced);
                push_with_stacking(&mut output, merged, advance, target_size);
            }
            (Some(_), None) => {
                drain_chain(left, advance, target_size, fuel, &mut output);
            }
            (None, Some(_)) => {
                drain_chain(right, advance, target_size, fuel, &mut output);
            }
            (None, None) => break,
        }
    }

    output
}

/// Drain a single remaining chain by merging each entry with an empty container.
/// This handles time advancement correctly when draining.
fn drain_chain<C: MergeContainer + Default>(
    chain: &mut Vec<ChainEntry<C>>,
    advance: Option<&Antichain<C::Time>>,
    target_size: usize,
    fuel: &mut usize,
    output: &mut Vec<C>,
) {
    while *fuel > 0 {
        drop_empty_prefix(chain);
        if chain.is_empty() { break; }
        let mut empty = ChainEntry::from_owned(C::default());
        let merged = C::merge_step(chain.first_mut().unwrap(), &mut empty, advance, target_size);
        let produced = merged.total_len();
        if produced == 0 { break; }
        *fuel = fuel.saturating_sub(produced);
        push_with_stacking(output, merged, advance, target_size);
    }
}

/// Extract all entries not beyond `frontier` from an owned chain (batcher use).
/// Returns the extracted chain; the input chain is rebuilt with kept entries.
fn extract_from_owned_chain<C: MergeContainer>(
    chain: &mut Vec<C>,
    frontier: &Antichain<C::Time>,
    target_size: usize,
) -> Vec<C> {
    let mut output = Vec::new();
    let mut remaining = Vec::new();

    for container in chain.drain(..) {
        let (extracted, kept) = container.extract_frontier(frontier);
        if extracted.total_len() > 0 {
            push_with_stacking(&mut output, extracted, None, target_size);
        }
        if kept.total_len() > 0 {
            remaining.push(kept);
        }
    }

    *chain = remaining;
    output
}

/// Bulk-load from multiple shared chains (trace batches) by key set.
fn bulk_load<C: MergeContainer + ExtractVia<[K]> + Default, K>(
    chains: &mut Vec<Vec<ChainEntry<C>>>,
    keys: &[K],
    advance: &Antichain<C::Time>,
    target_size: usize,
) -> Vec<C> {
    let mut per_chain: Vec<Vec<C>> = Vec::new();
    for chain in chains.iter_mut() {
        let mut extracted = Vec::new();
        for entry in chain.iter_mut() {
            let e = C::extract_keys(entry, keys, advance, target_size);
            if e.total_len() > 0 {
                extracted.push(e);
            }
        }
        if !extracted.is_empty() {
            per_chain.push(extracted);
        }
    }

    // Merge all extracted chains pairwise.
    while per_chain.len() > 1 {
        let b = per_chain.pop().unwrap();
        let a = per_chain.pop().unwrap();
        let mut left: Vec<ChainEntry<C>> = a.into_iter().map(ChainEntry::from_owned).collect();
        let mut right: Vec<ChainEntry<C>> = b.into_iter().map(ChainEntry::from_owned).collect();
        let mut fuel = usize::MAX;
        let merged = merge_chains(&mut left, &mut right, None, target_size, &mut fuel);
        per_chain.push(merged);
    }

    per_chain.pop().unwrap_or_default()
}

/// Wrap owned containers into shared ChainEntries (simulate installing into trace).
fn share_chain<C>(owned: Vec<C>) -> Vec<ChainEntry<C>> {
    owned.into_iter().map(ChainEntry::from_owned).collect()
}

// ---------------------------------------------------------------------------
// Demonstration of all five sites
// ---------------------------------------------------------------------------

type Container = VecContainer<String, String, u64, i64>;

fn print_owned_chain(chain: &[Container]) {
    for (i, c) in chain.iter().enumerate() {
        println!("    container {}: {} entries", i, c.total_len());
        for entry in c.live(0) {
            println!("      {:?}", entry);
        }
    }
}

fn print_shared_chain(chain: &[ChainEntry<Container>]) {
    for (i, entry) in chain.iter().enumerate() {
        let len = Container::remaining(entry);
        println!("    container {}: {} entries (shared, rc={})", i, len, Rc::strong_count(&entry.container));
        for item in entry.container.live(entry.offset) {
            println!("      {:?}", item);
        }
    }
}

fn main() {
    let target_size = 8;

    println!("=== Site 1: Collection Stream ===");
    println!("Operators produce sorted containers.\n");

    let c1: Container = VecContainer::from_unsorted(vec![
        ("alice".into(), "v1".into(), 1, 1),
        ("bob".into(), "v2".into(), 1, 1),
        ("alice".into(), "v1".into(), 1, 1),  // duplicate, will consolidate
        ("carol".into(), "v3".into(), 2, 1),
    ]);
    println!("  Operator output: {} entries (after sort+consolidate)", c1.total_len());
    for entry in c1.live(0) {
        println!("    {:?}", entry);
    }

    let c2: Container = VecContainer::from_unsorted(vec![
        ("alice".into(), "v1".into(), 2, -1),
        ("dave".into(), "v4".into(), 2, 1),
        ("bob".into(), "v2".into(), 3, -1),
    ]);
    println!("  Operator output: {} entries", c2.total_len());

    let c3: Container = VecContainer::from_unsorted(vec![
        ("carol".into(), "v3".into(), 3, 1),
        ("eve".into(), "v5".into(), 3, 1),
    ]);
    println!("  Operator output: {} entries\n", c3.total_len());

    // --- Site 2: Batcher ---
    println!("=== Site 2: Batcher (Chain Merge-Sort) ===");
    println!("Sorted containers arrive; batcher maintains owned chain.\n");

    let mut batcher_chain: Vec<Container> = Vec::new();
    // Batcher receives sorted containers and applies stacking discipline.
    for c in vec![c1, c2, c3] {
        push_with_stacking(&mut batcher_chain, c, None, target_size);
    }

    println!("  Chain after stacking: {} containers", batcher_chain.len());
    print_owned_chain(&batcher_chain);
    println!();

    // --- Site 3: Batcher Seal (Extract by Frontier) ---
    println!("=== Site 3: Batcher Seal (Extract by Frontier) ===");
    let frontier = Antichain::from_elem(3u64);
    println!("  Extracting entries with time < {:?}\n", frontier.elements);

    let batch1_owned = extract_from_owned_chain(&mut batcher_chain, &frontier, target_size);

    println!("  Extracted batch: {} containers", batch1_owned.len());
    print_owned_chain(&batch1_owned);
    println!("  Remaining in batcher: {} containers", batcher_chain.len());
    print_owned_chain(&batcher_chain);
    println!();

    // Seal the rest.
    let frontier2 = Antichain::from_elem(10u64);
    let batch2_owned = extract_from_owned_chain(&mut batcher_chain, &frontier2, target_size);
    println!("  Second seal (frontier < 10): {} containers", batch2_owned.len());
    print_owned_chain(&batch2_owned);
    println!();

    // --- Install batches into trace (wrap in Rc for sharing) ---
    println!("  Installing batches into trace (wrapping in Rc)...\n");
    let batch1_shared = share_chain(batch1_owned);
    let batch2_shared = share_chain(batch2_owned);

    // Simulate multiple readers: clone the shared chains.
    let _reader_copy = batch1_shared.clone();
    println!("  batch1 refcounts after cloning for a reader:");
    print_shared_chain(&batch1_shared);
    println!();

    // --- Site 4: Trace Batch Merging (with advancement) ---
    println!("=== Site 4: Trace Batch Merging (with Advancement) ===");
    let compaction_frontier = Antichain::from_elem(5u64);
    println!("  Merging batch1 and batch2, advancing times to {:?}", compaction_frontier.elements);
    println!("  (Inputs are shared; merge produces new owned containers)\n");

    let mut left = batch1_shared;
    let mut right = batch2_shared;
    let mut fuel = 1000usize;
    let merged_owned = merge_chains(
        &mut left, &mut right,
        Some(&compaction_frontier),
        target_size,
        &mut fuel,
    );

    println!("  Merged batch (owned): {} containers", merged_owned.len());
    print_owned_chain(&merged_owned);
    println!("  (Note: times advanced to >= 5, diffs consolidated)");
    println!("  Reader's copy of batch1 is still valid (rc={})\n",
             Rc::strong_count(&_reader_copy[0].container));

    // Install merged batch into trace.
    let merged_shared = share_chain(merged_owned);

    // --- Site 5: Operator Bulk Loading ---
    println!("=== Site 5: Operator Bulk Loading ===");
    let query_keys = vec!["alice".to_string(), "carol".to_string()];
    println!("  Extracting keys {:?} with advancement to {:?}\n",
             query_keys, compaction_frontier.elements);

    let mut trace_batches = vec![merged_shared];
    let loaded = bulk_load(
        &mut trace_batches,
        &query_keys,
        &compaction_frontier,
        target_size,
    );

    println!("  Loaded: {} containers", loaded.len());
    print_owned_chain(&loaded);
    println!();

    // --- Summary ---
    println!("=== Summary ===");
    println!("  All five sites used the same MergeContainer/ExtractVia traits.");
    println!("  Containers are immutable once created; ChainEntry tracks offsets.");
    println!("  Trace batches share containers via Rc; merges produce new owned data.");
    println!("  Chain management (stacking, fuel, drop-empty) is generic.");
}
