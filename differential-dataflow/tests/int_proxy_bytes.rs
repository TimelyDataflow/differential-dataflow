//! Behavioral tests for the proxy tactics against a byte-rows backend.
//!
//! The compressing-token instantiation from the seam design, aimed at data as general
//! as Materialize's `[u8]` rows of unknown size and shape. Rows are opaque byte strings;
//! the grouping opinion extracts a key prefix (here: bytes before the first `b'|'`).
//! Storage is columnar (concatenated bytes + bounds, aligned time/diff columns) with a
//! **stored token column** `(G, I)` computed once at batch formation — presentation is
//! a pure column read, never a re-hash. `G = hash(key)`, `I = hash(whole row)` (whole
//! datum, so value confusion under a group collision needs a double collision).
//!
//! Rows are stored in token order — `((G, I), row, time)` — so equal-token rows are
//! adjacent and batch formation doubles as T1 *value*-collision detection; join's
//! `redeem` and reduce's key/row stashes re-check at redemption, the other T1 sites.
//! *Group* collisions (distinct keys, equal `G`) are not errors under compression —
//! they are **resolved** per the module contract: join's `cross` re-checks real key
//! bytes and drops false pairs; reduce partitions each granule by real key and reduces
//! the parts separately. Weak-hash tests force both collision kinds: value collisions
//! must panic at their detection site; group collisions must yield *correct output*.

use std::collections::BTreeMap;

use timely::progress::frontier::AntichainRef;
use timely::progress::Antichain;

use differential_dataflow::consolidation::{consolidate, consolidate_updates};
use differential_dataflow::operators::int_proxy::{
    JoinInstance, JoinWindow, ProxyJoinBackend, ProxyJoinTactic, ProxyReduceBackend,
    ProxyReduceTactic, ReduceInstance, ReduceWindow,
};
use differential_dataflow::operators::join::{Fresh, JoinTactic};
use differential_dataflow::operators::reduce::ReduceTactic;
use differential_dataflow::trace::{BatchReader, Description};

type Time = u64;
type Diff = isize;
type Row = Vec<u8>;
type Hasher = fn(&[u8]) -> u64;

/// The grouping opinion: a row's key is its prefix before the first `b'|'`.
fn key_of(row: &[u8]) -> &[u8] {
    match row.iter().position(|b| *b == b'|') {
        Some(p) => &row[..p],
        None => row,
    }
}

/// A real hash (SipHash via std).
fn hash(bytes: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish()
}

/// A deliberately terrible hash, to force collisions in tests.
fn weak_hash(bytes: &[u8]) -> u64 {
    hash(bytes) & 0x3
}

/// Weak for keys (which never contain `b'|'`), strong for whole rows (which always do):
/// forces *group* collisions while value tokens stay distinct, so the resolution paths
/// are exercised rather than the panic paths.
fn weak_group_hash(bytes: &[u8]) -> u64 {
    if bytes.contains(&b'|') { hash(bytes) } else { hash(bytes) & 0x3 }
}

/// A columnar batch of row updates: concatenated bytes with bounds, aligned time,
/// diff, and token columns, in `((G, I), time)` order with row bytes as tiebreak.
#[derive(Clone)]
struct RowBatch {
    data: Vec<u8>,
    /// Row `i` is `data[bounds[i]..bounds[i + 1]]`; `bounds.len() == rows + 1`.
    bounds: Vec<usize>,
    times: Vec<Time>,
    diffs: Vec<Diff>,
    /// The stored token column: `(G, I)` per row, computed at formation.
    tokens: Vec<(u64, u64)>,
    description: Description<Time>,
}

impl BatchReader for RowBatch {
    type Time = Time;
    fn len(&self) -> usize { self.times.len() }
    fn description(&self) -> &Description<Time> { &self.description }
}

impl RowBatch {
    fn row(&self, i: usize) -> &[u8] {
        &self.data[self.bounds[i]..self.bounds[i + 1]]
    }

    /// Form a batch: tokenize, sort + consolidate by `((G, I), row, time)` (the library's
    /// `consolidate_updates`, so test batches agree with real DD consolidation), and check
    /// formation-site T1 — equal tokens must carry equal bytes, which sorting makes adjacent.
    fn form(updates: Vec<(Row, Time, Diff)>, lower: Time, upper: Time, hasher: Hasher) -> Self {
        let mut tokenized: Vec<(((u64, u64), Row), Time, Diff)> = updates
            .into_iter()
            .map(|(row, t, r)| {
                let g = hasher(key_of(&row));
                let i = hasher(&row);
                (((g, i), row), t, r)
            })
            .collect();
        consolidate_updates(&mut tokenized);
        for w in tokenized.windows(2) {
            if w[0].0 .0 == w[1].0 .0 {
                assert!(w[0].0 .1 == w[1].0 .1, "token collision detected at formation");
            }
        }
        let formed: Vec<((u64, u64), Row, Time, Diff)> =
            tokenized.into_iter().map(|((tok, row), t, r)| (tok, row, t, r)).collect();

        let mut batch = RowBatch {
            data: Vec::new(),
            bounds: vec![0],
            times: Vec::new(),
            diffs: Vec::new(),
            tokens: Vec::new(),
            description: Description::new(
                Antichain::from_elem(lower),
                Antichain::from_elem(upper),
                Antichain::from_elem(0),
            ),
        };
        for (tok, row, t, r) in formed {
            batch.data.extend_from_slice(&row);
            batch.bounds.push(batch.data.len());
            batch.times.push(t);
            batch.diffs.push(r);
            batch.tokens.push(tok);
        }
        batch
    }
}

/// Advance a time to the compaction frontier (total order: max against its element).
fn advance(t: Time, lower: AntichainRef<'_, Time>) -> Time {
    lower.iter().fold(t, |t, l| std::cmp::max(t, *l))
}

/// The next group at or after the per-batch positions `pos` into `batches`.
///
/// Test-grade: rescans every batch head per group. A real backend should k-way merge
/// with a heap over batch cursors.
fn next_group(batches: &[RowBatch], pos: &[usize]) -> Option<u64> {
    batches
        .iter()
        .zip(pos.iter())
        .filter_map(|(b, &p)| b.tokens.get(p).map(|t| t.0))
        .min()
}

/// Drain every update of group `g` from `batches` (advancing `pos`) into `logic`,
/// straight off the columns — no hashing at presentation time.
fn drain_group(
    batches: &[RowBatch],
    pos: &mut [usize],
    g: u64,
    mut logic: impl FnMut(&RowBatch, usize),
) {
    for (b, p) in batches.iter().zip(pos.iter_mut()) {
        while *p < b.tokens.len() && b.tokens[*p].0 == g {
            logic(b, *p);
            *p += 1;
        }
    }
}

/// Redeem a token to its row bytes, checking redemption-site T1: every batch holding
/// the token must agree on the bytes.
///
/// Test-grade: binary-searches every batch's token column per redeemed token. A real
/// backend should redeem from a window-scoped stash harvested during presentation
/// (as the reduce backend's `minted` demonstrates) rather than search the trace.
fn redeem<'a>(batches: &'a [RowBatch], token: (u64, u64)) -> &'a [u8] {
    let mut found: Option<&[u8]> = None;
    for b in batches {
        let lo = b.tokens.partition_point(|t| *t < token);
        let hi = b.tokens.partition_point(|t| *t <= token);
        for i in lo..hi {
            match found {
                None => found = Some(b.row(i)),
                Some(prev) => assert!(prev == b.row(i), "token collision detected at redemption"),
            }
        }
    }
    found.expect("token redeemed but never presented")
}

// ---------------------------------------------------------------- join backend

#[derive(Default)]
struct RowJoinCursor {
    pos0: Vec<usize>,
    pos1: Vec<usize>,
}

struct RowJoinBackend {
    /// Groups per window; tiny in tests to force many windows.
    window: usize,
}

impl ProxyJoinBackend<RowBatch, RowBatch> for RowJoinBackend {
    type Group = u64;
    type Token0 = u64;
    type Token1 = u64;
    type R0 = Diff;
    type R1 = Diff;
    type ROut = Diff;
    /// Matched row pairs with their joined time and multiplied diff.
    type Output = Vec<(Row, Row, Time, Diff)>;
    type Cursor = RowJoinCursor;

    fn next_window(
        &mut self,
        instance: &JoinInstance<'_, RowBatch, RowBatch>,
        fresh: Fresh,
        cursor: &mut RowJoinCursor,
    ) -> Option<JoinWindow<u64, u64, u64, Time, Diff, Diff>> {
        cursor.pos0.resize(instance.batches0.len(), 0);
        cursor.pos1.resize(instance.batches1.len(), 0);
        // Drive by the fresh side's groups: the accumulated side's other groups are
        // skipped, never presented (O(fresh) presentations; empty fresh side is free).
        let (fresh_b, other_b) = match fresh {
            Fresh::Input0 => (instance.batches0, instance.batches1),
            Fresh::Input1 => (instance.batches1, instance.batches0),
        };
        loop {
            let mut fresh_run = Vec::new();
            let mut other_run = Vec::new();
            let mut groups = 0;
            while groups < self.window {
                let (fresh_pos, other_pos) = match fresh {
                    Fresh::Input0 => (&mut cursor.pos0, &mut cursor.pos1),
                    Fresh::Input1 => (&mut cursor.pos1, &mut cursor.pos0),
                };
                let Some(g) = next_group(fresh_b, fresh_pos) else { break };
                groups += 1;
                drain_group(fresh_b, fresh_pos, g, |b, i| {
                    fresh_run.push((b.tokens[i], advance(b.times[i], instance.lower), b.diffs[i]));
                });
                for (b, p) in other_b.iter().zip(other_pos.iter_mut()) {
                    while *p < b.tokens.len() && b.tokens[*p].0 < g {
                        *p += 1;
                    }
                }
                drain_group(other_b, other_pos, g, |b, i| {
                    other_run.push((b.tokens[i], advance(b.times[i], instance.lower), b.diffs[i]));
                });
            }
            if groups == 0 {
                return None;
            }
            consolidate_updates(&mut fresh_run);
            consolidate_updates(&mut other_run);
            // A fully-cancelled quota presents nothing; keep going rather than emit an
            // empty window (the harness's progress guard forbids those).
            if fresh_run.is_empty() && other_run.is_empty() {
                continue;
            }
            let (input0, input1) = match fresh {
                Fresh::Input0 => (fresh_run, other_run),
                Fresh::Input1 => (other_run, fresh_run),
            };
            return Some(JoinWindow { input0, input1 });
        }
    }

    fn cross(
        &mut self,
        instance: &JoinInstance<'_, RowBatch, RowBatch>,
        left: &[(u64, u64)],
        right: &[(u64, u64)],
        times: &[Time],
        diffs: &[Diff],
    ) -> Self::Output {
        // Not self-redeeming: tokens are redeemed against the columns, T1-checked. Group
        // collisions are resolved here, per the module contract: matching was by group
        // token, so re-check real key bytes and drop false pairs.
        left.iter()
            .zip(right)
            .zip(times)
            .zip(diffs)
            .filter_map(|(((l, r), t), d)| {
                let lrow = redeem(instance.batches0, *l);
                let rrow = redeem(instance.batches1, *r);
                if key_of(lrow) == key_of(rrow) {
                    Some((lrow.to_vec(), rrow.to_vec(), *t, *d))
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Reference join: all row pairs with equal key bytes, times joined, diffs multiplied.
fn naive_join(
    a: &[(Row, Time, Diff)],
    b: &[(Row, Time, Diff)],
) -> Vec<((Row, Row), Time, Diff)> {
    let mut out = Vec::new();
    for (r0, t0, d0) in a {
        for (r1, t1, d1) in b {
            if key_of(r0) == key_of(r1) {
                out.push(((r0.clone(), r1.clone()), std::cmp::max(*t0, *t1), d0 * d1));
            }
        }
    }
    consolidate_updates(&mut out);
    out
}

fn rows(spec: &[(&str, Time, Diff)]) -> Vec<(Row, Time, Diff)> {
    spec.iter().map(|(s, t, d)| (s.as_bytes().to_vec(), *t, *d)).collect()
}

/// Drive the proxy join at several window sizes and both `Fresh` choices, comparing
/// against the byte-level naive join (which compares *real* key bytes).
fn check_join(hasher: Hasher) {
    // Variable-length rows, keys of different sizes, a retraction, several batches.
    // Five distinct keys: under a 2-bit group hash, at least two must collide.
    let a0 = rows(&[("alice|1", 0, 1), ("alice|22", 1, 1), ("bob|x", 0, 1), ("carol|zzz", 2, 1)]);
    let a1 = rows(&[("alice|22", 2, -1), ("dave|4", 2, 1)]);
    let b0 = rows(&[("alice|red", 1, 1), ("bob|blue", 2, 1), ("erin|green", 0, 1)]);
    let b1 = rows(&[("bob|blue", 2, 1), ("carol|teal", 0, 1), ("dave|gray", 1, 1), ("erin|pink", 2, 1)]);

    let all_a: Vec<_> = a0.iter().chain(&a1).cloned().collect();
    let all_b: Vec<_> = b0.iter().chain(&b1).cloned().collect();
    let expected = naive_join(&all_a, &all_b);

    for window in [1, 2, 100] {
        for fresh in [Fresh::Input0, Fresh::Input1] {
            let mut tactic = ProxyJoinTactic::new(RowJoinBackend { window });
            let work = tactic.prep(
                vec![RowBatch::form(a0.clone(), 0, 3, hasher), RowBatch::form(a1.clone(), 0, 3, hasher)],
                vec![RowBatch::form(b0.clone(), 0, 3, hasher), RowBatch::form(b1.clone(), 0, 3, hasher)],
                fresh,
                0,
            );
            let mut got: Vec<((Row, Row), Time, Diff)> =
                work.flatten().map(|(l, r, t, d)| ((l, r), t, d)).collect();
            consolidate_updates(&mut got);
            assert_eq!(got, expected, "window={window}");
        }
    }
}

#[test]
fn join_rows_matches_naive() {
    check_join(hash);
}

#[test]
fn join_resolves_group_collisions() {
    // Forced group collisions (weak key hashes, strong row hashes): cross must re-check
    // real key bytes and drop false pairs, yielding exactly the naive output.
    check_join(weak_group_hash);
}

#[test]
#[should_panic(expected = "token collision detected at formation")]
fn formation_detects_collisions() {
    // Two distinct rows under a 2-bit hash must collide within one batch.
    let updates = rows(&[
        ("a|0", 0, 1), ("a|1", 0, 1), ("a|2", 0, 1), ("a|3", 0, 1), ("a|4", 0, 1),
    ]);
    RowBatch::form(updates, 0, 1, weak_hash);
}

#[test]
#[should_panic(expected = "token collision detected at redemption")]
fn redemption_detects_cross_batch_collisions() {
    // Find two distinct rows with the same weak token pair: same key (same G) and
    // colliding 2-bit value hashes. Placed in *separate* batches, formation cannot
    // see the collision; redemption must.
    let candidates: Vec<Row> = (0..64).map(|i| format!("k|{i}").into_bytes()).collect();
    let (r0, r1) = {
        let mut found = None;
        'outer: for (x, a) in candidates.iter().enumerate() {
            for b in &candidates[x + 1..] {
                if weak_hash(a) == weak_hash(b) {
                    found = Some((a.clone(), b.clone()));
                    break 'outer;
                }
            }
        }
        found.expect("2-bit hashes must collide among 64 rows")
    };
    let left0 = RowBatch::form(vec![(r0, 0, 1)], 0, 1, weak_hash);
    let left1 = RowBatch::form(vec![(r1, 0, 1)], 0, 1, weak_hash);
    let right = RowBatch::form(rows(&[("k|hit", 0, 1)]), 0, 1, weak_hash);

    let mut tactic = ProxyJoinTactic::new(RowJoinBackend { window: 100 });
    let work = tactic.prep(vec![left0, left1], vec![right], Fresh::Input0, 0);
    let _: Vec<_> = work.collect();
}

// -------------------------------------------------------------- reduce backend

/// Stash `row` under `token`, checking stash-site T1: a token maps to one byte string.
fn stash_checked(map: &mut BTreeMap<(u64, u64), Row>, token: (u64, u64), row: &[u8]) {
    match map.entry(token) {
        std::collections::btree_map::Entry::Occupied(e) => {
            assert!(e.get() == row, "token collision detected at stash");
        }
        std::collections::btree_map::Entry::Vacant(v) => {
            v.insert(row.to_vec());
        }
    }
}

/// Counts rows per key: the output row for key `k` with count `c` is `k|c`.
///
/// Not self-redeeming, which exposes the expected pattern for byte backends: the
/// window is the redemption scope. `next_window` walks the columns anyway, so it
/// stashes what later steps need — key bytes per input token, and row bytes per
/// presented output token; `reduce_corrections` partitions each granule by *real*
/// key (resolving group collisions per the module contract) and mints tokens for
/// desired rows it invents; `emit`/`finish` redeem from the stash. Nothing is ever
/// re-derived from a hash.
struct RowReduceBackend {
    window: usize,
    hasher: Hasher,
    // Session state, bracketed by `begin`/`finish`.
    tiles: Vec<Description<Time>>,
    emitted: Vec<Vec<(Row, Time, Diff)>>,
    pos_source: Vec<usize>,
    pos_input: Vec<usize>,
    pos_output: Vec<usize>,
    pend_idx: usize,
    /// Key bytes per input token, harvested while draining windows. (Compares key
    /// bytes only: an input *value*-token collision merges two same-key rows, which
    /// count-by-diffs tolerates; logics reading values would stash whole rows.)
    input_keys: BTreeMap<(u64, u64), Row>,
    /// Row bytes per output token: presented output rows and minted desired rows.
    minted: BTreeMap<(u64, u64), Row>,
}

impl RowReduceBackend {
    fn new(window: usize, hasher: Hasher) -> Self {
        RowReduceBackend {
            window,
            hasher,
            tiles: Vec::new(),
            emitted: Vec::new(),
            pos_source: Vec::new(),
            pos_input: Vec::new(),
            pos_output: Vec::new(),
            pend_idx: 0,
            input_keys: BTreeMap::new(),
            minted: BTreeMap::new(),
        }
    }
}

impl ProxyReduceBackend<RowBatch, RowBatch> for RowReduceBackend {
    type Group = u64;
    type Token = u64;
    type RIn = Diff;
    type ROut = Diff;

    fn begin(&mut self, tiles: &[Description<Time>]) {
        self.tiles = tiles.to_vec();
        self.emitted = vec![Vec::new(); tiles.len()];
        self.pos_source.clear();
        self.pos_input.clear();
        self.pos_output.clear();
        self.pend_idx = 0;
        self.input_keys.clear();
        self.minted.clear();
    }

    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, RowBatch, RowBatch>,
        pending: &[u64],
    ) -> Option<ReduceWindow<u64, u64, Time, Diff, Diff>> {
        self.pos_source.resize(instance.source_batches.len(), 0);
        self.pos_input.resize(instance.input_batches.len(), 0);
        self.pos_output.resize(instance.output_batches.len(), 0);
        let mut keys = Vec::new();
        let mut seeds = Vec::new();
        let mut input = Vec::new();
        let mut output = Vec::new();
        while keys.len() < self.window {
            let g = [
                next_group(instance.source_batches, &self.pos_source),
                next_group(instance.input_batches, &self.pos_input),
                next_group(instance.output_batches, &self.pos_output),
                pending.get(self.pend_idx).copied(),
            ]
            .into_iter()
            .flatten()
            .min();
            let Some(g) = g else { break };
            keys.push(g);
            if pending.get(self.pend_idx) == Some(&g) {
                self.pend_idx += 1;
            }
            let input_keys = &mut self.input_keys;
            drain_group(instance.source_batches, &mut self.pos_source, g, |b, i| {
                stash_checked(input_keys, b.tokens[i], key_of(b.row(i)));
                input.push((b.tokens[i], advance(b.times[i], instance.lower), b.diffs[i]));
            });
            drain_group(instance.input_batches, &mut self.pos_input, g, |b, i| {
                stash_checked(input_keys, b.tokens[i], key_of(b.row(i)));
                seeds.push((g, b.times[i]));
                input.push((b.tokens[i], advance(b.times[i], instance.lower), b.diffs[i]));
            });
            let minted = &mut self.minted;
            drain_group(instance.output_batches, &mut self.pos_output, g, |b, i| {
                stash_checked(minted, b.tokens[i], b.row(i));
                output.push((b.tokens[i], advance(b.times[i], instance.lower), b.diffs[i]));
            });
        }
        if keys.is_empty() {
            return None;
        }
        consolidate_updates(&mut input);
        consolidate_updates(&mut output);
        seeds.sort();
        seeds.dedup();
        Some(ReduceWindow { keys, seeds, input, output })
    }

    fn reduce_corrections(
        &mut self,
        keys: &[u64],
        in_ends: &[usize],
        input: &[(u64, Diff)],
        out_ends: &[usize],
        output: &[(u64, Diff)],
    ) -> (Vec<(u64, Diff)>, Vec<usize>) {
        let mut corr = Vec::new();
        let mut ends = Vec::new();
        let (mut i0, mut o0) = (0, 0);
        for (k, (&i1, &o1)) in keys.iter().zip(in_ends.iter().zip(out_ends)) {
            // Partition the granule by REAL key: under compression, distinct keys can
            // share a group token, and each must be reduced separately (the module
            // contract's prescription for group collisions).
            let mut per_key: BTreeMap<Row, (Diff, Vec<(u64, Diff)>)> = BTreeMap::new();
            for (tok, d) in &input[i0..i1] {
                let key = self.input_keys.get(&(*k, *tok)).expect("input token missing from key stash");
                per_key.entry(key.clone()).or_default().0 += d;
            }
            for (tok, d) in &output[o0..o1] {
                let row = self.minted.get(&(*k, *tok)).expect("output token was never presented");
                per_key.entry(key_of(row).to_vec()).or_default().1.push((*tok, -d));
            }
            let mut delta: Vec<(u64, Diff)> = Vec::new();
            for (key, (count, mut retractions)) in per_key {
                assert!(count >= 0, "row count went negative");
                if count > 0 {
                    // Mint the desired output row and its token.
                    let mut row = key;
                    row.push(b'|');
                    row.extend_from_slice(count.to_string().as_bytes());
                    let token = (self.hasher)(&row);
                    stash_checked(&mut self.minted, (*k, token), &row);
                    delta.push((token, 1));
                }
                delta.append(&mut retractions);
            }
            consolidate(&mut delta);
            corr.extend(delta);
            ends.push(corr.len());
            i0 = i1;
            o0 = o1;
        }
        (corr, ends)
    }

    fn emit(&mut self, tile: usize, records: &[((u64, u64), Time, Diff)]) {
        for ((g, i), t, r) in records {
            let row = self.minted.get(&(*g, *i)).expect("emitted token was never minted or presented");
            self.emitted[tile].push((row.clone(), *t, *r));
        }
    }

    fn finish(&mut self) -> Vec<RowBatch> {
        let hasher = self.hasher;
        self.tiles
            .drain(..)
            .zip(self.emitted.drain(..))
            .map(|(desc, updates)| {
                let lower = desc.lower().elements()[0];
                let upper = if desc.upper().elements().is_empty() { lower } else { desc.upper().elements()[0] };
                let mut b = RowBatch::form(updates, lower, upper, hasher);
                b.description = desc;
                b
            })
            .collect()
    }
}

/// Accumulate row updates at times `<= t`.
fn accumulate(updates: impl Iterator<Item = (Row, Time, Diff)>, t: Time) -> Vec<(Row, Diff)> {
    let mut acc: Vec<(Row, Diff)> =
        updates.filter(|(_, ut, _)| *ut <= t).map(|(r, _, d)| (r, d)).collect();
    consolidate(&mut acc);
    acc
}

/// Drive the proxy reduce over several rounds at several window sizes, comparing
/// accumulated outputs at every round boundary against byte-level per-key counts.
fn check_reduce(hasher: Hasher) {
    // Five distinct keys: under a 2-bit group hash, at least two must collide.
    let rounds: Vec<Vec<(Row, Time, Diff)>> = vec![
        rows(&[("alice|1", 0, 1), ("alice|22", 0, 1), ("bob|x", 0, 1), ("erin|e", 0, 1)]),
        rows(&[("alice|333", 1, 1), ("carol|c", 1, 1), ("bob|x", 1, -1)]),
        rows(&[("carol|d", 2, 1), ("alice|22", 2, -1), ("dave|9", 2, 1), ("erin|e", 2, -1)]),
    ];

    for window in [1, 2, 100] {
        let mut tactic = ProxyReduceTactic::new(RowReduceBackend::new(window, hasher));
        let mut source: Vec<RowBatch> = Vec::new();
        let mut outputs: Vec<RowBatch> = Vec::new();

        for (r, updates) in rounds.iter().enumerate() {
            let r = r as Time;
            let input = RowBatch::form(updates.clone(), r, r + 1, hasher);
            let lower = Antichain::from_elem(r);
            let upper = Antichain::from_elem(r + 1);
            let held = Antichain::from_elem(r);
            let (produced, frontier) =
                tactic.retire(source.clone(), outputs.clone(), vec![input.clone()], &lower, &upper, &held);
            assert!(frontier.is_empty(), "total order should defer nothing");
            for (time, b) in produced {
                assert!(held.elements().contains(&time));
                outputs.push(b);
            }
            source.push(input);
        }

        for t in 0..rounds.len() as Time {
            let input_acc = accumulate(source.iter().flat_map(|b| (0..b.len()).map(|i| (b.row(i).to_vec(), b.times[i], b.diffs[i]))), t);
            let mut counts: BTreeMap<Row, Diff> = BTreeMap::new();
            for (row, d) in &input_acc {
                *counts.entry(key_of(row).to_vec()).or_insert(0) += d;
            }
            let mut expected: Vec<(Row, Diff)> = Vec::new();
            for (key, c) in counts {
                assert!(c >= 0);
                if c > 0 {
                    let mut row = key.clone();
                    row.push(b'|');
                    row.extend_from_slice(c.to_string().as_bytes());
                    expected.push((row, 1));
                }
            }
            expected.sort();
            let got = accumulate(outputs.iter().flat_map(|b| (0..b.len()).map(|i| (b.row(i).to_vec(), b.times[i], b.diffs[i]))), t);
            assert_eq!(got, expected, "window={window} at time={t}");
        }
    }
}

#[test]
fn reduce_row_counts_match_naive() {
    check_reduce(hash);
}

#[test]
fn reduce_resolves_group_collisions() {
    // Forced group collisions: the granule partition by real key must reduce each
    // colliding key separately, yielding exactly the naive per-key counts.
    check_reduce(weak_group_hash);
}
