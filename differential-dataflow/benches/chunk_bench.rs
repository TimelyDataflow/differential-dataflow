//! Microbenchmark: `VecChunk` (flat) vs `ColChunk` (columnar trie) at the
//! `Chunk` level — build, merge, full cursor scan, and resident memory — across
//! data shapes. Isolates the per-layout cost from the dataflow runtime (cf. the
//! `spines` example, which benchmarks whole trace spines end to end).
//!
//! ```text
//! cargo bench --bench chunk_bench -- [updates]
//! ```
//!
//! Shapes exercise where the trie should and shouldn't help: unique `u64` keys
//! with no vals (flat's home turf), few keys with many vals each (key + val-run
//! dedup), and `String` keys (dedup of repeated heap-backed keys — the "mandatory
//! for memory" case).

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

use timely::Container;
use timely::container::{PushInto, SizableContainer};

use differential_dataflow::consolidation::Consolidate;
use differential_dataflow::columnar::layout::ColumnarUpdate;
use differential_dataflow::trace::chunk::{merge_chains, Chunk, NavigableChunk};
use differential_dataflow::trace::chunk::vec::VecChunk;
use differential_dataflow::columnar::trace::ColChunk;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::implementations::chunker::ContainerChunker;

/// A global allocator that tracks currently-resident bytes, so we can snapshot
/// the heap footprint of a built chunk chain.
struct Counting;
static RESIDENT: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 { RESIDENT.fetch_add(l.size(), Relaxed); System.alloc(l) }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) { RESIDENT.fetch_sub(l.size(), Relaxed); System.dealloc(p, l) }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        if new >= l.size() { RESIDENT.fetch_add(new - l.size(), Relaxed); } else { RESIDENT.fetch_sub(l.size() - new, Relaxed); }
        System.realloc(p, l, new)
    }
}
#[global_allocator]
static ALLOC: Counting = Counting;
fn resident() -> usize { RESIDENT.load(Relaxed) }

/// Build a sorted, consolidated chunk chain from a flat update stream, through
/// the real `ContainerChunker` build path used by `arrange`.
fn build_chain<C, I>(mut updates: Vec<I>) -> Vec<C>
where
    C: Default + SizableContainer + Consolidate + Container + PushInto<I>,
{
    use timely::container::ContainerBuilder;
    let mut chunker = ContainerChunker::<C>::default();
    chunker.push_into(&mut updates);
    let mut out = Vec::new();
    while let Some(c) = chunker.finish() { out.push(std::mem::take(c)); }
    out
}

/// Walk every `(key, val, time, diff)` via the chunk cursors, counting updates
/// (a uniform proxy for scan cost across layouts).
fn scan<C: NavigableChunk>(chunks: &[C]) -> usize {
    let mut count = 0;
    for c in chunks {
        let mut cur = c.cursor();
        while cur.key_valid(c) {
            while cur.val_valid(c) {
                cur.map_times(c, |_t, _d| count += 1);
                cur.step_val(c);
            }
            cur.step_key(c);
        }
    }
    count
}

fn ms(t: Instant) -> f64 { t.elapsed().as_secs_f64() * 1000.0 }

/// Build / merge / scan / size one chunk type over the given updates, split into
/// two interleaving halves for the merge. Returns timings + footprint.
fn bench<C, I>(updates: &[I], half_a: &[I], half_b: &[I]) -> (f64, f64, f64, f64, usize, usize)
where
    C: NavigableChunk + Default + SizableContainer + Consolidate + Container + PushInto<I>,
    I: Clone,
{
    let (half_a, half_b) = (half_a.to_vec(), half_b.to_vec());

    // Build the full chain; measure its resident footprint and update count.
    let base = resident();
    let t = Instant::now();
    let chain = build_chain::<C, I>(updates.to_vec());
    let build_ms = ms(t);
    let bytes = resident().saturating_sub(base);
    let records: usize = chain.iter().map(<C as Chunk>::len).sum();

    // Merge two half-chains.
    let chain_a = build_chain::<C, I>(half_a);
    let chain_b = build_chain::<C, I>(half_b);
    let t = Instant::now();
    let mut merged = VecDeque::new();
    merge_chains::<C>(chain_a, chain_b, &mut merged);
    let merge_ms = ms(t);
    let merged: Vec<C> = merged.into();

    // Scan the merged chain.
    let t = Instant::now();
    let scanned = std::hint::black_box(scan(&merged));
    let scan_ms = ms(t);
    assert!(scanned <= records.max(1) * 2);

    drop((chain, merged));
    (build_ms, merge_ms, scan_ms, bytes as f64, records, bytes)
}

/// Even/odd index split — heavy interleaving (the merge's worst case).
fn alt<I: Clone>(updates: &[I]) -> (Vec<I>, Vec<I>) {
    (updates.iter().step_by(2).cloned().collect(), updates.iter().skip(1).step_by(2).cloned().collect())
}

/// Run both layouts over the same updates and print a comparison row.
fn compare<KV, I>(label: &str, updates: Vec<I>)
where
    I: Clone,
    VecChunk<KV, (), u64, i64>: Chunk + Default + SizableContainer + Consolidate + Container + PushInto<I>,
    ColChunk<(KV, (), u64, i64)>: Chunk + Default + SizableContainer + Consolidate + Container + PushInto<I>,
    (KV, (), u64, i64): ColumnarUpdate<Key = KV, Val = (), Time = u64, Diff = i64>,
    KV: Ord + Clone + 'static,
{
    let (a, b) = alt(&updates);
    let (vb, vm, vs, _vbytes, recs, vbytes) = bench::<VecChunk<KV, (), u64, i64>, I>(&updates, &a, &b);
    let (cb, cm, cs, _cbytes, _recs, cbytes) = bench::<ColChunk<(KV, (), u64, i64)>, I>(&updates, &a, &b);
    println!("\n== {label}  ({recs} records) ==");
    println!("            {:>12} {:>12} {:>10}", "vec(flat)", "col(trie)", "col/vec");
    println!("  build ms  {:>12.1} {:>12.1} {:>9.2}x", vb, cb, cb / vb);
    println!("  merge ms  {:>12.1} {:>12.1} {:>9.2}x", vm, cm, cm / vm);
    println!("  scan  ms  {:>12.1} {:>12.1} {:>9.2}x", vs, cs, cs / vs);
    println!("  bytes     {:>12} {:>12} {:>9.2}x", vbytes, cbytes, cbytes as f64 / vbytes as f64);
    println!("  bytes/rec {:>12.1} {:>12.1}", vbytes as f64 / recs as f64, cbytes as f64 / recs as f64);
}

fn main() {
    let n: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);

    // Deterministic xorshift PRNG.
    let mut seed = 0x2545F4914F6CDD1Du64;
    let mut rng = move || { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; seed };

    // Shape A: unique-ish u64 keys, no vals, single time. Flat's home turf.
    {
        let mut u: Vec<((u64, ()), u64, i64)> = (0..n as u64).map(|k| ((k, ()), 0u64, 1i64)).collect();
        for i in 0..u.len() { let j = (rng() as usize) % u.len(); u.swap(i, j); }
        compare::<u64, _>("A: unique u64 keys, () vals", u);
    }

    // Shape B: few keys, many random vals each — key dedup + val runs, but the
    // even/odd split interleaves vals (merge's hard case; flat's two-pointer wins).
    {
        let keys = (n as f64).sqrt() as u64;
        let mut u: Vec<((u64, u64), u64, i64)> =
            (0..n as u64).map(|_| { let k = rng() % keys; let v = rng() % keys; ((k, v), 0u64, 1i64) }).collect();
        u.sort();
        let (a, b) = alt(&u);
        bench_valbearing("B: few u64 keys, many random vals (interleaved merge)", u, a, b);
    }

    // Shape C: String keys (heap-backed, repeated), no vals — the memory case.
    {
        let cardinality = (n / 20).max(1) as u64; // ~20 repeats per key
        let mut u: Vec<((String, ()), u64, i64)> =
            (0..n as u64).map(|_| { let k = rng() % cardinality; ((format!("key-{k:012}"), ()), 0u64, 1i64) }).collect();
        u.sort();
        compare::<String, _>("C: String keys (~20x repeats), () vals", u);
    }

    // Shape D: few (key,val) pairs, many times each, halves split by time
    // (old vs new) — the LSM-append pattern. Each (key,val)'s time run is one
    // disjoint This-then-That, so the trie merge bulk-copies whole time runs.
    {
        let pairs = ((n as f64).sqrt() as u64).max(1);
        let half = n as u64 / 2;
        let mut u: Vec<((u64, u64), u64, i64)> =
            (0..n as u64).map(|i| { let p = i % pairs; let t = i / pairs; ((p, 0u64), t, 1i64) }).collect();
        u.sort();
        // Disjoint by time: half_a = early times, half_b = late times.
        let a: Vec<_> = u.iter().filter(|(_, t, _)| *t < half / pairs).cloned().collect();
        let b: Vec<_> = u.iter().filter(|(_, t, _)| *t >= half / pairs).cloned().collect();
        bench_valbearing("D: few (k,v), many times, time-disjoint merge (LSM append)", u, a, b);
    }

    // Shape X: read-side unloading — probe extraction vs cursor navigation.
    extraction::run(n);
}

/// Probe-extraction (`UnloadChunk`) vs cursor navigation over a `ChunkBatch` of
/// columnar chunks: both consume the probed updates into owned staging — the
/// cursor by per-probe seeks and owned copies, extraction by bulk column-range
/// melds. Resident and paged (cold spilled bytes) variants.
mod extraction {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;
    use std::time::Instant;

    use columnar::{Borrow, Columnar, ContainerOf, Index, Len, Push};
    use timely::progress::Antichain;

    use differential_dataflow::columnar::layout::{ColumnarUpdate, Coltainer};
    use differential_dataflow::columnar::trace::ColChunk;
    use differential_dataflow::columnar::trace::spill::{self, BytesSource, BytesStore, SpillStats};
    use differential_dataflow::trace::chunk::{Chunk, ChunkBatch};
    use differential_dataflow::trace::cursor::Cursor;
    use differential_dataflow::trace::implementations::BatchContainer;
    use differential_dataflow::trace::{Description, Navigable};

    use super::{build_chain, ms};

    /// In-memory backing store: an arena of byte blobs.
    struct MemStore(Rc<RefCell<Vec<Vec<u8>>>>);
    struct MemSource(Rc<RefCell<Vec<Vec<u8>>>>, usize);
    impl BytesStore for MemStore {
        fn store(&mut self, bytes: &[u8]) -> Box<dyn BytesSource> {
            let mut arena = self.0.borrow_mut();
            let id = arena.len();
            arena.push(bytes.to_vec());
            Box::new(MemSource(self.0.clone(), id))
        }
    }
    impl BytesSource for MemSource { fn load(&self) -> Vec<u8> { self.0.borrow()[self.1].clone() } }

    fn batch_of<U: ColumnarUpdate<Time = u64>>(chunks: Vec<ColChunk<U>>) -> ChunkBatch<ColChunk<U>> {
        let desc = Description::new(
            Antichain::from_elem(0u64), Antichain::from_elem(1u64), Antichain::from_elem(0u64));
        ChunkBatch::new(chunks, desc)
    }

    /// Page every chunk of `chain` out through `settle` (budget 0), leaving the
    /// spiller installed so reads count fetches.
    fn page_out<U: ColumnarUpdate<Time = u64>>(chain: Vec<ColChunk<U>>) -> Vec<ColChunk<U>> {
        let arena = Rc::new(RefCell::new(Vec::new()));
        spill::install(0, Box::new(MemStore(arena)), std::sync::Arc::new(SpillStats::default()));
        let mut input: VecDeque<_> = chain.into();
        let mut out = VecDeque::new();
        ColChunk::settle(&mut input, true, &mut out);
        out.into()
    }

    /// The cursor path: per-probe `seek_key` on the straddle cursor, copying the
    /// hit's key, vals, times, and diffs out as owned values. Returns updates staged.
    fn probe_cursor<U: ColumnarUpdate<Time = u64>>(batch: &ChunkBatch<ColChunk<U>>, probes: &ContainerOf<U::Key>) -> usize {
        let mut cursor = batch.cursor();
        let mut staged: Vec<(U::Key, U::Val, U::Time, U::Diff)> = Vec::new();
        let pb = probes.borrow();
        for i in 0..pb.len() {
            let probe = pb.get(i);
            cursor.seek_key(batch, probe);
            let Some(key) = cursor.get_key(batch) else { continue };
            if <Coltainer<U::Key> as BatchContainer>::reborrow(key)
                != <Coltainer<U::Key> as BatchContainer>::reborrow(probe) { continue; }
            let key = <U::Key as Columnar>::into_owned(key);
            while let Some(val) = cursor.get_val(batch) {
                let val = <U::Val as Columnar>::into_owned(val);
                let k = key.clone();
                cursor.map_times(batch, |t, d| staged.push((
                    k.clone(), val.clone(),
                    <U::Time as Columnar>::into_owned(t),
                    <U::Diff as Columnar>::into_owned(d),
                )));
                cursor.step_val(batch);
            }
        }
        staged.len()
    }

    /// The extraction path: one `extract_into` over the batch into chunk staging.
    fn probe_extract<U: ColumnarUpdate<Time = u64>>(batch: &ChunkBatch<ColChunk<U>>, probes: &ContainerOf<U::Key>) -> usize {
        use differential_dataflow::trace::Builder;
        let mut staging = differential_dataflow::trace::chunk::ChunkBatchBuilder::<ColChunk<U>>::default();
        batch.extract_into(probes.borrow(), &mut staging);
        let desc = Description::new(
            Antichain::from_elem(0u64), Antichain::new(), Antichain::from_elem(0u64));
        staging.done(desc).chunks.iter().map(Chunk::len).sum()
    }

    /// Best-of-`reps` wall clock; returns (ms, result) and asserts result stability.
    fn best(reps: usize, mut f: impl FnMut() -> usize) -> (f64, usize) {
        let (mut best, mut out) = (f64::MAX, 0);
        for r in 0..reps {
            let t = Instant::now();
            let got = std::hint::black_box(f());
            best = best.min(ms(t));
            if r > 0 { assert_eq!(got, out); }
            out = got;
        }
        (best, out)
    }

    fn row(label: &str, hits: usize, cursor_ms: f64, extract_ms: f64) {
        println!("  {label:<38} {hits:>9} {cursor_ms:>10.1} {extract_ms:>11.1} {:>10.2}x", cursor_ms / extract_ms);
    }

    /// One shape: resident best-of-3 both paths, then (optionally) a paged batch
    /// measured cold — a single pass each, on separately paged batches, since the
    /// cursor path materializes chunk caches as it reads (extraction does not).
    fn shape<U: ColumnarUpdate<Time = u64>>(label: &str, updates: Vec<((U::Key, U::Val), u64, i64)>, probes: &ContainerOf<U::Key>, paged: bool)
    where
        U: ColumnarUpdate<Diff = i64>,
        ColChunk<U>: timely::container::SizableContainer
            + differential_dataflow::consolidation::Consolidate
            + timely::container::PushInto<((U::Key, U::Val), u64, i64)>,
        U::Key: Clone, U::Val: Clone,
    {
        let chain = build_chain::<ColChunk<U>, _>(updates.clone());
        let batch = batch_of(chain);
        let (cursor_ms, cursor_hits) = best(3, || probe_cursor(&batch, probes));
        let (extract_ms, extract_hits) = best(3, || probe_extract(&batch, probes));
        assert_eq!(cursor_hits, extract_hits, "paths disagree on {label}");
        row(label, extract_hits, cursor_ms, extract_ms);

        if paged {
            // Cold cursor pass: first touch decodes and caches every chunk it opens.
            let batch = batch_of(page_out(build_chain::<ColChunk<U>, _>(updates.clone())));
            let t = Instant::now();
            let cursor_hits = std::hint::black_box(probe_cursor(&batch, probes));
            let cursor_ms = ms(t);
            spill::uninstall();
            // Extraction never populates caches, so every pass is cold; best-of-3.
            let batch = batch_of(page_out(build_chain::<ColChunk<U>, _>(updates)));
            let (extract_ms, extract_hits) = best(3, || probe_extract(&batch, probes));
            spill::uninstall();
            assert_eq!(cursor_hits, extract_hits, "paged paths disagree on {label}");
            row(&format!("{label}, paged (cold)"), extract_hits, cursor_ms, extract_ms);
        }
    }

    pub fn run(n: usize) {
        println!("\n== probe extraction (UnloadChunk) vs cursor navigation, col trie ==");
        println!("  {:<38} {:>9} {:>10} {:>11} {:>11}", "shape", "hits", "cursor ms", "extract ms", "cur/ext");

        // Dense String keys: every key probed — the design doc's headline case.
        {
            let key = |k: usize| format!("key-{k:012}");
            let updates: Vec<((String, ()), u64, i64)> = (0..n).map(|k| ((key(k), ()), 0, 1)).collect();
            let mut probes = ContainerOf::<String>::default();
            for k in 0..n { probes.push(&key(k)); }
            shape::<(String, (), u64, i64)>("dense probes, String keys", updates, &probes, true);
        }

        // Sparse (1%) u64 probes: navigation's best case — extraction wants parity.
        {
            let updates: Vec<((u64, ()), u64, i64)> = (0..n as u64).map(|k| ((k, ()), 0, 1)).collect();
            let mut probes = ContainerOf::<u64>::default();
            for k in (0..n as u64).step_by(100) { probes.push(k); }
            shape::<(u64, (), u64, i64)>("sparse probes (1%), u64 keys", updates, &probes, true);
        }

        // Dense u64 probes: the pure navigate-vs-copy comparison, no string compares.
        {
            let updates: Vec<((u64, ()), u64, i64)> = (0..n as u64).map(|k| ((k, ()), 0, 1)).collect();
            let mut probes = ContainerOf::<u64>::default();
            for k in 0..n as u64 { probes.push(k); }
            shape::<(u64, (), u64, i64)>("dense probes, u64 keys", updates, &probes, false);
        }

        // Fat values: 4 x 256B string vals per key, every key probed — value copies
        // dominate, and staging should run at memory bandwidth.
        {
            let keys = (n / 16).max(1) as u64;
            let fat = |k: u64, v: u64| {
                let mut s = format!("val-{k:012}-{v:02}-");
                s.push_str(&"x".repeat(256 - s.len()));
                s
            };
            let updates: Vec<((u64, String), u64, i64)> =
                (0..keys).flat_map(|k| (0..4).map(move |v| ((k, fat(k, v)), 0, 1))).collect();
            let mut probes = ContainerOf::<u64>::default();
            for k in 0..keys { probes.push(k); }
            shape::<(u64, String, u64, i64)>("dense probes, fat vals (4 x 256B)", updates, &probes, false);
        }
    }
}

/// Shape B/D need a `u64` val type (not `()`), so they get their own dispatch,
/// with an explicit merge split (`half_a` / `half_b`).
fn bench_valbearing(label: &str, updates: Vec<((u64, u64), u64, i64)>, half_a: Vec<((u64, u64), u64, i64)>, half_b: Vec<((u64, u64), u64, i64)>) {
    let (vb, vm, vs, _v, recs, vbytes) = bench::<VecChunk<u64, u64, u64, i64>, _>(&updates, &half_a, &half_b);
    let (cb, cm, cs, _c, _r, cbytes) = bench::<ColChunk<(u64, u64, u64, i64)>, _>(&updates, &half_a, &half_b);
    println!("\n== {label}  ({recs} records) ==");
    println!("            {:>12} {:>12} {:>10}", "vec(flat)", "col(trie)", "col/vec");
    println!("  build ms  {:>12.1} {:>12.1} {:>9.2}x", vb, cb, cb / vb);
    println!("  merge ms  {:>12.1} {:>12.1} {:>9.2}x", vm, cm, cm / vm);
    println!("  scan  ms  {:>12.1} {:>12.1} {:>9.2}x", vs, cs, cs / vs);
    println!("  bytes     {:>12} {:>12} {:>9.2}x", vbytes, cbytes, cbytes as f64 / vbytes as f64);
    println!("  bytes/rec {:>12.1} {:>12.1}", vbytes as f64 / recs as f64, cbytes as f64 / recs as f64);
}
