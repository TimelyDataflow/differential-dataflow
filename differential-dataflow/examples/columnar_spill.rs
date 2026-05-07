//! Example: file-backed spill for the columnar `MergeBatcher`.
//!
//! Demonstrates `Spill` / `Fetch` / `SpillPolicy` impls modeled on TD's
//! `communication/examples/spill_stress.rs`. Spills `UpdatesTyped<U>` chunks
//! to a tempfile via per-column `Stash::write_bytes`, fetches them back via
//! `Stash::try_from_bytes` and `Updates::into_typed`.
//!
//! Run with: `cargo run --example columnar_spill`

use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Static spill-policy config, read by `SpillBatcher::new` when `arrange_core`
// constructs each worker's batcher (we can't pass parameters through the
// `Batcher::new(logger, op_id)` constructor).
static ENABLE_SPILL: AtomicBool = AtomicBool::new(true);
static HEAD: AtomicUsize = AtomicUsize::new(10_000_000);
static THRESH: AtomicUsize = AtomicUsize::new(50_000_000);

/// Cumulative bytes serialized (pre-compression) and bytes written
/// (post-compression) across all `FileSpill` instances. Lets us report a
/// compression ratio at the end of a run.
static BYTES_DECOMPRESSED: AtomicUsize = AtomicUsize::new(0);
static BYTES_COMPRESSED: AtomicUsize = AtomicUsize::new(0);

/// Cross-worker registry of `Threshold` stats so we can sum them after a run.
static SHARED_STATS: OnceLock<Mutex<Vec<Arc<ThresholdStats>>>> = OnceLock::new();

fn register_stats(stats: Arc<ThresholdStats>) {
    SHARED_STATS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(stats);
}

fn collect_stats() -> (usize, usize) {
    if let Some(m) = SHARED_STATS.get() {
        let v = m.lock().unwrap();
        let fires: usize = v.iter().map(|s| s.fires.load(Ordering::Relaxed)).sum();
        let chunks: usize = v.iter().map(|s| s.chunks_spilled.load(Ordering::Relaxed)).sum();
        (fires, chunks)
    } else {
        (0, 0)
    }
}

fn reset_stats() {
    if let Some(m) = SHARED_STATS.get() {
        m.lock().unwrap().clear();
    }
    BYTES_DECOMPRESSED.store(0, Ordering::Relaxed);
    BYTES_COMPRESSED.store(0, Ordering::Relaxed);
}

use columnar::Push;
use columnar::bytes::stash::Stash;

use differential_dataflow::columnar::{RecordedUpdates, ValBuilder, ValColBuilder, ValSpine};
use differential_dataflow::columnar::batcher::MergeBatcher;
use differential_dataflow::columnar::layout::ColumnarUpdate as Update;
use differential_dataflow::columnar::spill::{Entry, Fetch, Spill, SpillPolicy};
use differential_dataflow::columnar::updates::{Updates, UpdatesTyped};
use differential_dataflow::logging::Logger;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::trace::{Batcher, Builder};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::probe::{Handle as ProbeHandle, Probe};
use timely::dataflow::operators::Input;
use timely::dataflow::InputHandle;
use timely::progress::frontier::AntichainRef;
use timely::progress::{frontier::Antichain, Timestamp};

/// File-backed `Spill`. Serializes each chunk into a reusable `Vec<u8>` and
/// writes it with one `write_all` per chunk. Rotates to a new tempfile every
/// `ROTATE_AFTER_BYTES` so disk space is reclaimed as `FileFetch` handles are
/// consumed: once a file's last handle is dropped, the `Arc` hits zero, the
/// (already-unlinked) tempfile closes, and the OS gives the space back.
pub struct FileSpill<U: Update> {
    /// Current write file. `None` until first spill, or after rotation if no
    /// chunks have been written to a fresh file yet.
    current: Option<Arc<Mutex<std::fs::File>>>,
    /// Bytes written to `current` so far.
    current_offset: u64,
    /// Reusable serialization buffer; grows to fit the largest chunk seen,
    /// then sticks at that capacity (no per-chunk allocation).
    buf: Vec<u8>,
    _marker: PhantomData<U>,
}

impl<U: Update> FileSpill<U> {
    /// Rotate to a new tempfile after this many bytes. Sized so each file
    /// holds many chunks (amortizing the file-open cost) but small enough
    /// that we don't accumulate hundreds of GB on disk before any can be
    /// reclaimed.
    const ROTATE_AFTER_BYTES: u64 = 1 << 30; // 1 GiB

    pub fn new() -> std::io::Result<Self> {
        Ok(Self {
            current: None,
            current_offset: 0,
            buf: Vec::new(),
            _marker: PhantomData,
        })
    }

    fn current_file(&mut self) -> std::io::Result<Arc<Mutex<std::fs::File>>> {
        if self.current.is_none() || self.current_offset >= Self::ROTATE_AFTER_BYTES {
            // Drop the previous Arc — outstanding `FileFetch` handles still
            // hold it; once they're all consumed, the file is unlinked-closed
            // and the OS reclaims its space.
            self.current = Some(Arc::new(Mutex::new(tempfile::tempfile()?)));
            self.current_offset = 0;
        }
        Ok(self.current.as_ref().unwrap().clone())
    }
}

impl<U: Update + 'static> Spill<UpdatesTyped<U>> for FileSpill<U> {
    fn spill(
        &mut self,
        chunks: &mut Vec<UpdatesTyped<U>>,
        handles: &mut Vec<Box<dyn Fetch<UpdatesTyped<U>>>>,
    ) {
        while let Some(chunk) = chunks.pop() {
            let updates: Updates<U, Vec<u8>> = chunk.into();
            let keys_len = updates.keys.length_in_bytes() as u64;
            let vals_len = updates.vals.length_in_bytes() as u64;
            let times_len = updates.times.length_in_bytes() as u64;
            let diffs_len = updates.diffs.length_in_bytes() as u64;
            let total = 32 + keys_len + vals_len + times_len + diffs_len;

            // Serialize the whole chunk (header + four columns) into the
            // reusable buffer.
            self.buf.clear();
            self.buf.extend_from_slice(&keys_len.to_le_bytes());
            self.buf.extend_from_slice(&vals_len.to_le_bytes());
            self.buf.extend_from_slice(&times_len.to_le_bytes());
            self.buf.extend_from_slice(&diffs_len.to_le_bytes());
            updates.keys.write_bytes(&mut self.buf).unwrap();
            updates.vals.write_bytes(&mut self.buf).unwrap();
            updates.times.write_bytes(&mut self.buf).unwrap();
            updates.diffs.write_bytes(&mut self.buf).unwrap();
            debug_assert_eq!(self.buf.len() as u64, total);

            // Compress before writing. lz4 block format: caller is responsible
            // for tracking the decompressed size, which we stash in the handle.
            let compressed = lz4_flex::block::compress(&self.buf);
            let comp_len = compressed.len() as u64;
            BYTES_DECOMPRESSED.fetch_add(total as usize, Ordering::Relaxed);
            BYTES_COMPRESSED.fetch_add(comp_len as usize, Ordering::Relaxed);

            let file = self.current_file().expect("tempfile");
            let start = self.current_offset;
            let mut f = file.lock().unwrap();
            f.seek(SeekFrom::Start(start)).unwrap();
            f.write_all(&compressed).unwrap();
            drop(f);
            self.current_offset += comp_len;

            handles.push(Box::new(FileFetch::<U> {
                file: file.clone(),
                offset: start,
                compressed_len: comp_len,
                decompressed_len: total,
                _marker: PhantomData,
            }));
        }
    }
}

/// Per-chunk fetch handle. On `fetch`, reads `compressed_len` bytes at
/// `offset`, decompresses to `decompressed_len`, then parses the 32-byte
/// header + four column payloads.
pub struct FileFetch<U: Update> {
    file: Arc<Mutex<std::fs::File>>,
    offset: u64,
    compressed_len: u64,
    decompressed_len: u64,
    _marker: PhantomData<U>,
}

impl<U: Update + 'static> Fetch<UpdatesTyped<U>> for FileFetch<U> {
    fn fetch(self: Box<Self>) -> Result<Vec<UpdatesTyped<U>>, Box<dyn Fetch<UpdatesTyped<U>>>> {
        // Read the compressed bytes in one shot.
        let mut compressed = vec![0u8; self.compressed_len as usize];
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(self.offset)).unwrap();
        file.read_exact(&mut compressed).unwrap();
        drop(file);

        let decompressed = lz4_flex::block::decompress(&compressed, self.decompressed_len as usize)
            .expect("lz4 decompress");

        // Parse the 32-byte header from the decompressed buffer.
        let header = &decompressed[0..32];
        let keys_len = u64::from_le_bytes(header[0..8].try_into().unwrap()) as usize;
        let vals_len = u64::from_le_bytes(header[8..16].try_into().unwrap()) as usize;
        let times_len = u64::from_le_bytes(header[16..24].try_into().unwrap()) as usize;
        let diffs_len = u64::from_le_bytes(header[24..32].try_into().unwrap()) as usize;

        // Slice the four columns out of the decompressed buffer, each into
        // its own owned `Vec<u8>` (`Stash::try_from_bytes` requires owned).
        let mut o = 32;
        let keys_bytes = decompressed[o..o + keys_len].to_vec();
        o += keys_len;
        let vals_bytes = decompressed[o..o + vals_len].to_vec();
        o += vals_len;
        let times_bytes = decompressed[o..o + times_len].to_vec();
        o += times_len;
        let diffs_bytes = decompressed[o..o + diffs_len].to_vec();

        let keys = Stash::try_from_bytes(keys_bytes).unwrap();
        let vals = Stash::try_from_bytes(vals_bytes).unwrap();
        let times = Stash::try_from_bytes(times_bytes).unwrap();
        let diffs = Stash::try_from_bytes(diffs_bytes).unwrap();
        let updates: Updates<U, Vec<u8>> = Updates { keys, vals, times, diffs };
        Ok(vec![updates.into_typed()])
    }
}

/// Trivial `SpillPolicy`: page out every `Typed` entry on each apply.
/// Useful for direct queue exercise; not intended as a real policy.
pub struct SpillEverything<U: Update> {
    spill: FileSpill<U>,
}

impl<U: Update + 'static> SpillPolicy<UpdatesTyped<U>> for SpillEverything<U> {
    fn apply(&mut self, queue: &mut std::collections::VecDeque<Entry<UpdatesTyped<U>>>) {
        let mut new_queue = std::collections::VecDeque::with_capacity(queue.len());
        let mut buf = Vec::new();
        let mut handles: Vec<Box<dyn Fetch<UpdatesTyped<U>>>> = Vec::new();
        for entry in queue.drain(..) {
            match entry {
                Entry::Typed(c) => {
                    buf.push(c);
                    self.spill.spill(&mut buf, &mut handles);
                    let handle = handles.pop().expect("FileSpill produces a handle per chunk");
                    new_queue.push_back(Entry::Paged(handle));
                }
                Entry::Paged(h) => new_queue.push_back(Entry::Paged(h)),
            }
        }
        *queue = new_queue;
    }
}

/// Threshold-based spill policy adapted from timely's
/// `communication::allocator::zero_copy::spill::threshold::Threshold`.
///
/// Counts records (not bytes) for the threshold check. When the queue's
/// resident records exceed `head_reserve_records + threshold_records`, spill
/// chunks past the head reserve. Unlike TD we don't carve out the last
/// entry — TD's last entry is a `try_merge` target being extended in place;
/// our chunks are all finished, so any of them can be spilled.
pub struct Threshold<U: Update> {
    spill: FileSpill<U>,
    /// Records near the head of the queue stay resident.
    pub head_reserve_records: usize,
    /// Spillable surplus: trigger when resident exceeds head + threshold.
    pub threshold_records: usize,
    /// Counters shared with the caller (chunks_spilled, fires).
    pub stats: Arc<ThresholdStats>,
}

#[derive(Default)]
pub struct ThresholdStats {
    pub fires: AtomicUsize,
    pub chunks_spilled: AtomicUsize,
}

impl<U: Update> Threshold<U> {
    pub fn new(spill: FileSpill<U>, head_reserve_records: usize, threshold_records: usize) -> Self {
        Self {
            spill,
            head_reserve_records,
            threshold_records,
            stats: Arc::new(ThresholdStats::default()),
        }
    }
}

impl<U: Update + 'static> SpillPolicy<UpdatesTyped<U>> for Threshold<U> {
    fn apply(&mut self, queue: &mut std::collections::VecDeque<Entry<UpdatesTyped<U>>>) {
        let resident: usize = queue.iter().map(|e| match e {
            Entry::Typed(c) => c.len(),
            Entry::Paged(_) => 0,
        }).sum();
        if resident <= self.head_reserve_records + self.threshold_records {
            return;
        }

        // Walk the queue, accumulating a head reserve. Past the reserve, mark
        // every Typed entry for spill.
        let mut cumulative = 0usize;
        let mut target_indices: Vec<usize> = Vec::new();
        for (i, entry) in queue.iter().enumerate() {
            if let Entry::Typed(c) = entry {
                if cumulative >= self.head_reserve_records {
                    target_indices.push(i);
                }
                cumulative += c.len();
            }
        }
        if target_indices.is_empty() { return; }

        // Take the targeted chunks out, leaving empty placeholders we overwrite below.
        let mut targets: Vec<UpdatesTyped<U>> = Vec::with_capacity(target_indices.len());
        for &i in &target_indices {
            if let Entry::Typed(c) = &mut queue[i] {
                targets.push(std::mem::take(c));
            }
        }

        let mut handles: Vec<Box<dyn Fetch<UpdatesTyped<U>>>> = Vec::new();
        self.spill.spill(&mut targets, &mut handles);
        // FileSpill drains via pop (LIFO); reverse so handles align with target_indices order.
        handles.reverse();
        assert_eq!(target_indices.len(), handles.len());
        self.stats.fires.fetch_add(1, Ordering::Relaxed);
        self.stats.chunks_spilled.fetch_add(handles.len(), Ordering::Relaxed);
        for (i, handle) in target_indices.into_iter().zip(handles) {
            queue[i] = Entry::Paged(handle);
        }
    }
}

/// `Batcher` wrapper that installs a `Threshold` policy on a `MergeBatcher`
/// at construction time, reading config from `HEAD` / `THRESH` / `ENABLE_SPILL`
/// statics. Slots into `arrange_core` in place of `ValBatcher` and lets the
/// timely operator drive a spilling merger without surgery to the `Batcher`
/// trait signature.
pub struct SpillBatcher<K, V, T, R>(MergeBatcher<(K, V, T, R)>)
where
    (K, V, T, R): Update;

impl<K, V, T, R> Batcher for SpillBatcher<K, V, T, R>
where
    K: columnar::Columnar + 'static,
    V: columnar::Columnar + 'static,
    T: columnar::Columnar + Timestamp + 'static,
    R: columnar::Columnar + 'static,
    (K, V, T, R): Update<Time = T> + 'static,
{
    type Input = RecordedUpdates<(K, V, T, R)>;
    type Time = T;
    type Output = UpdatesTyped<(K, V, T, R)>;

    fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        let mut inner = <MergeBatcher<(K, V, T, R)> as Batcher>::new(logger, operator_id);
        if ENABLE_SPILL.load(Ordering::Relaxed) {
            let head = HEAD.load(Ordering::Relaxed);
            let thresh = THRESH.load(Ordering::Relaxed);
            let policy = Threshold::<(K, V, T, R)>::new(
                FileSpill::new().expect("tempfile"),
                head,
                thresh,
            );
            register_stats(policy.stats.clone());
            inner.set_spill_policy(Box::new(policy));
        }
        Self(inner)
    }

    fn push_container(&mut self, container: &mut Self::Input) {
        self.0.push_container(container);
    }

    fn seal<B: Builder<Input = Self::Output, Time = Self::Time>>(
        &mut self,
        upper: Antichain<T>,
    ) -> B::Output {
        self.0.seal::<B>(upper)
    }

    fn frontier(&mut self) -> AntichainRef<'_, T> {
        self.0.frontier()
    }
}

type TestUpdate = (u64, u64, u64, i64);

fn make_chunk(updates: &[(u64, u64, u64, i64)]) -> UpdatesTyped<TestUpdate> {
    let mut out = UpdatesTyped::<TestUpdate>::default();
    for (k, v, t, d) in updates {
        out.push((k, v, t, d));
    }
    out.consolidate()
}

fn collect(chunk: &UpdatesTyped<TestUpdate>) -> Vec<(u64, u64, u64, i64)> {
    chunk.iter().map(|(k, v, t, d)| (*k, *v, *t, *d)).collect()
}

fn main() {
    // Build a few synthetic chunks.
    let chunk_a = make_chunk(&[
        (1, 10, 100, 1),
        (1, 10, 200, 2),
        (1, 20, 100, 3),
        (2, 20, 200, 5),
    ]);
    let chunk_b = make_chunk(&[
        (3, 30, 100, 7),
        (3, 30, 200, -7),
        (4, 40, 100, 11),
    ]);
    let chunk_c = make_chunk(&[
        (5, 50, 100, 1),
    ]);

    let originals = vec![chunk_a, chunk_b, chunk_c];
    let expected: Vec<Vec<_>> = originals.iter().map(collect).collect();

    // Direct Spill/Fetch roundtrip.
    {
        let mut spill = FileSpill::<TestUpdate>::new().unwrap();
        let mut chunks = originals.clone();
        let mut handles: Vec<Box<dyn Fetch<UpdatesTyped<TestUpdate>>>> = Vec::new();
        spill.spill(&mut chunks, &mut handles);
        assert!(chunks.is_empty(), "spill should drain chunks");
        assert_eq!(handles.len(), expected.len(), "expected one handle per chunk");

        // Spill drains in pop order (LIFO); reverse to align with original order.
        handles.reverse();

        for (i, handle) in handles.into_iter().enumerate() {
            let fetched = handle.fetch().unwrap_or_else(|_| panic!("fetch should succeed"));
            assert_eq!(fetched.len(), 1, "FileFetch returns one chunk per handle");
            let got = collect(&fetched[0]);
            assert_eq!(got, expected[i], "chunk {} mismatch after roundtrip", i);
        }
        println!("ok: direct Spill+Fetch roundtripped {} chunks", expected.len());
    }

    // SpillPolicy roundtrip via a queue: every Typed becomes Paged, then we
    // fetch each one back and compare.
    {
        let mut policy = SpillEverything {
            spill: FileSpill::<TestUpdate>::new().unwrap(),
        };
        let mut queue: std::collections::VecDeque<Entry<UpdatesTyped<TestUpdate>>> =
            originals.iter().cloned().map(Entry::Typed).collect();
        policy.apply(&mut queue);

        // Every entry should now be Paged, in original order.
        assert_eq!(queue.len(), expected.len());
        for (i, entry) in queue.into_iter().enumerate() {
            match entry {
                Entry::Paged(handle) => {
                    let fetched = handle.fetch().unwrap_or_else(|_| panic!("fetch should succeed"));
                    assert_eq!(fetched.len(), 1);
                    assert_eq!(collect(&fetched[0]), expected[i], "queue position {}", i);
                }
                Entry::Typed(_) => panic!("SpillEverything should leave nothing typed"),
            }
        }
        println!("ok: SpillEverything paged & retrieved {} chunks in order", expected.len());
    }

    // End-to-end demo: a real timely dataflow.
    //
    // Each worker generates its share of the cancellation workload (positives
    // then negatives) and feeds them into an `arrange_core` whose batcher is
    // our `SpillBatcher`. With multiple workers we get parallel mergers, each
    // with its own `Threshold` policy and tempfile.
    {
        let cfg = match parse_args() {
            Some(cfg) => cfg,
            None => return,
        };

        let total_records = (cfg.times * cfg.keys_per_time) as usize * 2;
        let bytes_per_record = std::mem::size_of::<TestUpdate>();
        let raw_gb = (total_records * bytes_per_record) as f64 / (1u64 << 30) as f64;
        let per_worker_head = cfg.head / cfg.workers.max(1);
        let per_worker_thresh = cfg.thresh / cfg.workers.max(1);
        println!(
            "config: times={} keys={} workers={} head={} ({} per worker) thresh={} ({} per worker) mode={:?} sample_secs={}",
            cfg.times, cfg.keys_per_time, cfg.workers,
            cfg.head, per_worker_head,
            cfg.thresh, per_worker_thresh,
            cfg.mode, cfg.sample_secs,
        );
        println!(
            "workload: {} records ({:.2} GB raw, {} bytes/record)",
            total_records, raw_gb, bytes_per_record,
        );

        if cfg.mode != Mode::Baseline {
            ENABLE_SPILL.store(true, Ordering::Relaxed);
            // Divide head/threshold across workers so the user-supplied values
            // are a global budget, not per-worker. With N workers, each gets
            // 1/N of the budget; total resident across workers is bounded by
            // the configured head + threshold.
            let per_worker_head = cfg.head / cfg.workers.max(1);
            let per_worker_thresh = cfg.thresh / cfg.workers.max(1);
            HEAD.store(per_worker_head, Ordering::Relaxed);
            THRESH.store(per_worker_thresh, Ordering::Relaxed);
            reset_stats();
            let elapsed = run_timely_dataflow(cfg.times, cfg.keys_per_time, cfg.workers, cfg.sample_secs, "spill");
            let (fires, chunks) = collect_stats();
            let dec = BYTES_DECOMPRESSED.load(Ordering::Relaxed);
            let comp = BYTES_COMPRESSED.load(Ordering::Relaxed);
            println!(
                "spill: {:.2}s | {:.2} M records/s | {:.2} GB/s | threshold fired {} times, spilled {} chunks",
                elapsed.as_secs_f64(),
                total_records as f64 / elapsed.as_secs_f64() / 1e6,
                raw_gb / elapsed.as_secs_f64(),
                fires, chunks,
            );
            if dec > 0 {
                let dec_gb = dec as f64 / (1u64 << 30) as f64;
                let comp_gb = comp as f64 / (1u64 << 30) as f64;
                println!(
                    "compression: {:.2} GB → {:.2} GB ({:.2}× ratio, lz4)",
                    dec_gb, comp_gb, dec as f64 / comp.max(1) as f64,
                );
            }
        }

        if cfg.mode != Mode::Spill {
            ENABLE_SPILL.store(false, Ordering::Relaxed);
            reset_stats();
            let elapsed = run_timely_dataflow(cfg.times, cfg.keys_per_time, cfg.workers, cfg.sample_secs, "baseline");
            println!(
                "baseline: {:.2}s | {:.2} M records/s | {:.2} GB/s",
                elapsed.as_secs_f64(),
                total_records as f64 / elapsed.as_secs_f64() / 1e6,
                raw_gb / elapsed.as_secs_f64(),
            );
        }
    }
}

/// Run a single timely dataflow with `workers` worker threads. Each worker
/// generates `keys_per_time / workers` records per timestamp, feeds them to
/// an `arrange_core` over our `SpillBatcher`, advances time, and waits for
/// the probe. Returns elapsed wall time.
fn run_timely_dataflow(
    times: u64,
    keys_per_time: u64,
    workers: usize,
    sample_secs: u64,
    label: &str,
) -> std::time::Duration {
    let stop = Arc::new(AtomicBool::new(false));

    // RSS sampler thread.
    let stop_clone = stop.clone();
    let label_owned = label.to_string();
    let sampler = if sample_secs > 0 {
        Some(std::thread::spawn(move || {
            let start = std::time::Instant::now();
            while !stop_clone.load(Ordering::Relaxed) {
                if let Some(rss) = rss_kb() {
                    println!(
                        "  [{}] +{:>5.0}s   RSS {:>9} kB",
                        label_owned,
                        start.elapsed().as_secs_f64(),
                        rss
                    );
                }
                std::thread::sleep(std::time::Duration::from_secs(sample_secs));
            }
        }))
    } else {
        None
    };

    let timer = std::time::Instant::now();

    timely::execute(timely::Config::process(workers), move |worker| {
        let index = worker.index();
        let peers = worker.peers();

        let mut input = <InputHandle<u64, ValColBuilder<TestUpdate>>>::new_with_builder();
        let mut probe: ProbeHandle<u64> = ProbeHandle::new();

        worker.dataflow::<u64, _, _>(|scope| {
            let stream = scope.input_from(&mut input);
            let arranged = arrange_core::<
                _,
                SpillBatcher<u64, u64, u64, i64>,
                ValBuilder<u64, u64, u64, i64>,
                ValSpine<u64, u64, u64, i64>,
            >(stream, Pipeline, "ColumnarSpillArrange");
            arranged.stream.probe_with(&mut probe);
        });

        // Push positives and negatives at the same timely time so they land
        // in the batcher together — the merger cancels (k,v,t,+1) against
        // (k,v,t,-1) during its own merges, instead of producing two giant
        // sealed batches that cancel only at the spine.
        //
        // Keys are mixed through a deterministic bijection so the post-sort
        // columnar bytes look ~random (otherwise the workload is sequential
        // u64s plus repeated v/t/r, which macOS' page compressor crushes,
        // making the baseline-vs-spill comparison unfairly favor baseline).
        // `mix` is reversible and identical across phases, so cancellation
        // still pairs positives with negatives.
        fn mix(k: u64) -> u64 {
            let x = k.wrapping_mul(0x9E3779B97F4A7C15);
            x ^ (x >> 32)
        }

        // Step periodically so the input handle's internal buffer doesn't
        // pile up unbounded. Diffs are derived from the same `mix` so they
        // also look random per record (incompressible) but pair-wise negate
        // exactly across phases.
        const STEP_EVERY: usize = 1 << 16;
        let mut sent_since_step = 0usize;
        for sign in [1i64, -1] {
            for t in 0..times {
                let mut k = index as u64;
                while k < keys_per_time {
                    let kh = mix(k);
                    // Half-range, always odd — nonzero and `-d` never overflows.
                    let d = ((kh as i64) >> 1) | 1;
                    input.send((kh, kh & 0x3, t, sign * d));
                    k += peers as u64;
                    sent_since_step += 1;
                    if sent_since_step >= STEP_EVERY {
                        worker.step();
                        sent_since_step = 0;
                    }
                }
            }
        }
        input.advance_to(1);
        input.flush();

        while probe.less_than(input.time()) {
            worker.step();
        }
    })
    .expect("timely::execute failed");

    let elapsed = timer.elapsed();
    stop.store(true, Ordering::Relaxed);
    if let Some(s) = sampler {
        let _ = s.join();
    }
    elapsed
}

#[derive(Debug, PartialEq)]
enum Mode { Both, Spill, Baseline }

struct Config {
    times: u64,
    keys_per_time: u64,
    head: usize,
    thresh: usize,
    workers: usize,
    sample_secs: u64,
    mode: Mode,
}

fn parse_args() -> Option<Config> {
    let mut cfg = Config {
        times: 8,
        keys_per_time: 500_000,
        head: 10_000_000,
        thresh: 50_000_000,
        workers: 1,
        sample_secs: 0,
        mode: Mode::Both,
    };
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        let take = |it: &mut dyn Iterator<Item = String>, name: &str| -> String {
            it.next().unwrap_or_else(|| { print_usage(); panic!("--{} requires a value", name) })
        };
        match a.as_str() {
            "-h" | "--help" => { print_usage(); return None; }
            "--times"   => { cfg.times = take(&mut it, "times").parse().expect("times: integer"); }
            "--keys"    => { cfg.keys_per_time = take(&mut it, "keys").parse().expect("keys: integer"); }
            "--head"    => { cfg.head = take(&mut it, "head").parse().expect("head: integer"); }
            "--thresh"  => { cfg.thresh = take(&mut it, "thresh").parse().expect("thresh: integer"); }
            "--workers" => { cfg.workers = take(&mut it, "workers").parse().expect("workers: integer"); }
            "--sample-secs" => { cfg.sample_secs = take(&mut it, "sample-secs").parse().expect("sample-secs: integer"); }
            "--mode"    => {
                cfg.mode = match take(&mut it, "mode").as_str() {
                    "both" => Mode::Both,
                    "spill" => Mode::Spill,
                    "baseline" => Mode::Baseline,
                    other => { print_usage(); panic!("unknown mode: {}", other); }
                };
            }
            other => { print_usage(); panic!("unknown arg: {}", other); }
        }
    }
    Some(cfg)
}

fn print_usage() {
    eprintln!("Usage: columnar_spill [OPTIONS]");
    eprintln!();
    eprintln!("  --times N            distinct data timestamps         (default 8)");
    eprintln!("  --keys N             keys per timestamp               (default 500000)");
    eprintln!("  --head N             total head_reserve_records (split across workers)  (default 10000000)");
    eprintln!("  --thresh N           total threshold_records    (split across workers)  (default 50000000)");
    eprintln!("  --workers N          timely worker threads            (default 1)");
    eprintln!("  --sample-secs N      print RSS every N seconds        (default 0 = off)");
    eprintln!("  --mode MODE          spill | baseline | both          (default both)");
    eprintln!();
    eprintln!("Total records pushed = 2 * times * keys (positives + negatives that cancel).");
    eprintln!("Records are partitioned across workers by k % workers.");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  # default — 8M records, both runs, 1 worker");
    eprintln!("  columnar_spill");
    eprintln!();
    eprintln!("  # 100 GB spill-only on 4 workers, RSS every 30s");
    eprintln!("  columnar_spill --mode spill --workers 4 --times 64 --keys 24000000 \\");
    eprintln!("                 --head 10000000 --thresh 50000000 --sample-secs 30");
    eprintln!();
    eprintln!("  # baseline only (no spill installed)");
    eprintln!("  columnar_spill --mode baseline");
}

/// Return current process RSS in kB by shelling out to `ps`. Portable across
/// macOS and Linux without adding a dep. Returns `None` if `ps` isn't
/// available or output can't be parsed.
fn rss_kb() -> Option<usize> {
    let pid = std::process::id();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let s = std::str::from_utf8(&output.stdout).ok()?;
    s.trim().parse::<usize>().ok()
}
