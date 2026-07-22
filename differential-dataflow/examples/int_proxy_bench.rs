//! Measures the indirection tax: proxy tactics vs hand-written native implementations
//! over identical data, for the value-token (exact, zero-translation) instantiation.
//!
//! Data is graph edges `(src: u32, dst: u32)` grouped by source — `G = src`, token =
//! the edge itself — so every observed gap is protocol overhead (windowing, bridge
//! materialization, harness crossings), not tokenization.
//!
//! Run: `cargo run --release --example int_proxy_bench [-- <keys> <fanout>]`

use std::hint::black_box;
use std::time::Instant;

use timely::progress::frontier::AntichainRef;
use timely::progress::Antichain;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::operators::int_proxy::{
    JoinInstance, JoinWindow, ProxyJoinBackend, ProxyJoinTactic, ProxyReduceBackend,
    ProxyReduceTactic, ReduceInstance, ReduceWindow,
};
use differential_dataflow::operators::join::{Fresh, JoinTactic};
use differential_dataflow::operators::reduce::ReduceTactic;
use differential_dataflow::trace::{BatchReader, Description};

type Edge = (u32, u32);
type Time = u64;
type Diff = isize;

#[derive(Clone)]
struct GraphBatch {
    updates: Vec<(Edge, Time, Diff)>,
    description: Description<Time>,
}

impl BatchReader for GraphBatch {
    type Time = Time;
    fn len(&self) -> usize { self.updates.len() }
    fn description(&self) -> &Description<Time> { &self.description }
}

fn batch(mut updates: Vec<(Edge, Time, Diff)>, lower: Time, upper: Time) -> GraphBatch {
    consolidate_updates(&mut updates);
    GraphBatch {
        updates,
        description: Description::new(
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
            Antichain::from_elem(0),
        ),
    }
}

fn advance(t: Time, lower: AntichainRef<'_, Time>) -> Time {
    lower.iter().fold(t, |t, l| std::cmp::max(t, *l))
}

fn next_group(batches: &[GraphBatch], pos: &[usize]) -> Option<u32> {
    batches
        .iter()
        .zip(pos.iter())
        .filter_map(|(b, &p)| b.updates.get(p).map(|u| u.0 .0))
        .min()
}

fn drain_group(
    batches: &[GraphBatch],
    pos: &mut [usize],
    g: u32,
    mut logic: impl FnMut(Edge, Time, Diff),
) {
    for (b, p) in batches.iter().zip(pos.iter_mut()) {
        while *p < b.updates.len() && b.updates[*p].0 .0 == g {
            let (edge, t, r) = b.updates[*p];
            logic(edge, t, r);
            *p += 1;
        }
    }
}

// ------------------------------------------------------------------ proxy join

#[derive(Default)]
struct GraphJoinCursor {
    pos0: Vec<usize>,
    pos1: Vec<usize>,
}

struct GraphJoinBackend {
    window: usize,
}

impl ProxyJoinBackend<GraphBatch, GraphBatch> for GraphJoinBackend {
    type Group = u32;
    type Token0 = Edge;
    type Token1 = Edge;
    type R0 = Diff;
    type R1 = Diff;
    type ROut = Diff;
    type Output = Vec<(Edge, Edge, Time, Diff)>;
    type Cursor = GraphJoinCursor;

    fn next_window(
        &mut self,
        instance: &JoinInstance<'_, GraphBatch, GraphBatch>,
        fresh: Fresh,
        cursor: &mut GraphJoinCursor,
    ) -> Option<JoinWindow<u32, Edge, Edge, Time, Diff, Diff>> {
        cursor.pos0.resize(instance.batches0.len(), 0);
        cursor.pos1.resize(instance.batches1.len(), 0);
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
                drain_group(fresh_b, fresh_pos, g, |e, t, r| {
                    fresh_run.push(((g, e), advance(t, instance.lower), r));
                });
                for (b, p) in other_b.iter().zip(other_pos.iter_mut()) {
                    while *p < b.updates.len() && b.updates[*p].0 .0 < g {
                        *p += 1;
                    }
                }
                drain_group(other_b, other_pos, g, |e, t, r| {
                    other_run.push(((g, e), advance(t, instance.lower), r));
                });
            }
            if groups == 0 {
                return None;
            }
            consolidate_updates(&mut fresh_run);
            consolidate_updates(&mut other_run);
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
        _instance: &JoinInstance<'_, GraphBatch, GraphBatch>,
        left: &[(u32, Edge)],
        right: &[(u32, Edge)],
        times: &[Time],
        diffs: &[Diff],
    ) -> Self::Output {
        left.iter()
            .zip(right)
            .zip(times)
            .zip(diffs)
            .map(|(((l, r), t), d)| (l.1, r.1, *t, *d))
            .collect()
    }
}

// ---------------------------------------------------------------- proxy reduce

struct GraphReduceBackend {
    window: usize,
    tiles: Vec<Description<Time>>,
    emitted: Vec<Vec<(Edge, Time, Diff)>>,
    pos_source: Vec<usize>,
    pos_input: Vec<usize>,
    pos_output: Vec<usize>,
    pend_idx: usize,
}

impl GraphReduceBackend {
    fn new(window: usize) -> Self {
        GraphReduceBackend {
            window,
            tiles: Vec::new(),
            emitted: Vec::new(),
            pos_source: Vec::new(),
            pos_input: Vec::new(),
            pos_output: Vec::new(),
            pend_idx: 0,
        }
    }
}

impl ProxyReduceBackend<GraphBatch, GraphBatch> for GraphReduceBackend {
    type Group = u32;
    type Token = Edge;
    type RIn = Diff;
    type ROut = Diff;

    fn begin(&mut self, tiles: &[Description<Time>]) {
        self.tiles = tiles.to_vec();
        self.emitted = vec![Vec::new(); tiles.len()];
        self.pos_source.clear();
        self.pos_input.clear();
        self.pos_output.clear();
        self.pend_idx = 0;
    }

    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, GraphBatch, GraphBatch>,
        pending: &[u32],
    ) -> Option<ReduceWindow<u32, Edge, Time, Diff, Diff>> {
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
            drain_group(instance.source_batches, &mut self.pos_source, g, |e, t, r| {
                input.push(((g, e), advance(t, instance.lower), r));
            });
            drain_group(instance.input_batches, &mut self.pos_input, g, |e, t, r| {
                seeds.push((g, t));
                input.push(((g, e), advance(t, instance.lower), r));
            });
            drain_group(instance.output_batches, &mut self.pos_output, g, |e, t, r| {
                output.push(((g, e), advance(t, instance.lower), r));
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
        keys: &[u32],
        in_ends: &[usize],
        input: &[(Edge, Diff)],
        out_ends: &[usize],
        output: &[(Edge, Diff)],
    ) -> (Vec<(Edge, Diff)>, Vec<usize>) {
        let mut corr = Vec::new();
        let mut ends = Vec::new();
        let (mut i0, mut o0) = (0, 0);
        for (k, (&i1, &o1)) in keys.iter().zip(in_ends.iter().zip(out_ends)) {
            let count: Diff = input[i0..i1].iter().map(|(_, d)| d).sum();
            let mut delta: Vec<(Edge, Diff)> = Vec::new();
            if count > 0 {
                delta.push(((*k, count as u32), 1));
            }
            for (v, d) in &output[o0..o1] {
                delta.push((*v, -d));
            }
            differential_dataflow::consolidation::consolidate(&mut delta);
            corr.extend(delta);
            ends.push(corr.len());
            i0 = i1;
            o0 = o1;
        }
        (corr, ends)
    }

    fn emit(&mut self, tile: usize, records: &[((u32, Edge), Time, Diff)]) {
        self.emitted[tile].extend(records.iter().map(|((_g, e), t, r)| (*e, *t, *r)));
    }

    fn finish(&mut self) -> Vec<GraphBatch> {
        self.tiles
            .drain(..)
            .zip(self.emitted.drain(..))
            .map(|(desc, mut updates)| {
                consolidate_updates(&mut updates);
                GraphBatch { updates, description: desc }
            })
            .collect()
    }
}

// --------------------------------------------------------------------- natives

/// Hand-written merge-join over the same batch lists: the native baseline. Produces
/// the same containers (chunks of matched pairs) the proxy path produces.
fn native_join(a: &[GraphBatch], b: &[GraphBatch], chunk: usize) -> Vec<Vec<(Edge, Edge, Time, Diff)>> {
    let mut out = Vec::new();
    let mut cur = Vec::new();
    let mut pos0 = vec![0; a.len()];
    let mut pos1 = vec![0; b.len()];
    let (mut e0, mut e1) = (Vec::new(), Vec::new());
    loop {
        let (g0, g1) = (next_group(a, &pos0), next_group(b, &pos1));
        let g = match (g0, g1) {
            (None, None) => break,
            (Some(x), None) => x,
            (None, Some(y)) => y,
            (Some(x), Some(y)) => x.min(y),
        };
        e0.clear();
        e1.clear();
        drain_group(a, &mut pos0, g, |e, t, r| e0.push((e, t, r)));
        drain_group(b, &mut pos1, g, |e, t, r| e1.push((e, t, r)));
        for (x, t0, r0) in &e0 {
            for (y, t1, r1) in &e1 {
                cur.push((*x, *y, std::cmp::max(*t0, *t1), r0 * r1));
                if cur.len() >= chunk {
                    out.push(std::mem::replace(&mut cur, Vec::with_capacity(chunk)));
                }
            }
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Hand-written per-source count over the same batch list: the native baseline.
fn native_count(a: &[GraphBatch]) -> Vec<(Edge, Time, Diff)> {
    let mut out = Vec::new();
    let mut pos = vec![0; a.len()];
    while let Some(g) = next_group(a, &pos) {
        let mut count: Diff = 0;
        let mut time: Time = 0;
        drain_group(a, &mut pos, g, |_e, t, r| {
            count += r;
            time = time.max(t);
        });
        if count > 0 {
            out.push(((g, count as u32), time, 1));
        }
    }
    out
}

// ----------------------------------------------------------------------- bench

fn time<T>(label: &str, reps: usize, mut f: impl FnMut() -> T) -> f64 {
    let mut best = f64::INFINITY;
    for _ in 0..reps {
        let start = Instant::now();
        black_box(f());
        best = best.min(start.elapsed().as_secs_f64());
    }
    println!("    {label:<28} {:>10.3} ms", best * 1e3);
    best
}

fn main() {
    let mut args = std::env::args().skip(1);
    let keys: u32 = args.next().and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let fanout: u32 = args.next().and_then(|s| s.parse().ok()).unwrap_or(4);
    let window: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(1024);
    let reps = 5;

    // Two edge sets over the same key space, `fanout` edges per key each.
    let edges = |salt: u32| {
        let mut v = Vec::with_capacity((keys * fanout) as usize);
        for k in 0..keys {
            for f in 0..fanout {
                v.push(((k, salt.wrapping_mul(1_000_003).wrapping_add(k * fanout + f)), 0u64, 1isize));
            }
        }
        v
    };
    let a = vec![batch(edges(1), 0, 1)];
    let b = vec![batch(edges(2), 0, 1)];

    println!(
        "join: {} x {} edges over {} keys (fanout {}, ~{} matches)",
        keys * fanout, keys * fanout, keys, fanout, (keys as u64) * (fanout as u64) * (fanout as u64),
    );
    let t_native = time("native merge-join", reps, || native_join(&a, &b, 1 << 20));
    let t_proxy = time("proxy join (value tokens)", reps, || {
        let mut tactic = ProxyJoinTactic::new(GraphJoinBackend { window });
        let work = tactic.prep(a.clone(), b.clone(), Fresh::Input0, 0);
        work.collect::<Vec<_>>()
    });
    println!("    ratio: {:.2}x\n", t_proxy / t_native);

    let input = vec![batch(edges(1), 0, 1)];
    println!("reduce (count per key): {} edges over {} keys", keys * fanout, keys);
    let t_native = time("native count", reps, || native_count(&input));
    let t_proxy = time("proxy reduce (value tokens)", reps, || {
        let mut tactic = ProxyReduceTactic::new(GraphReduceBackend::new(window));
        let lower = Antichain::from_elem(0u64);
        let upper = Antichain::from_elem(1u64);
        let held = Antichain::from_elem(0u64);
        tactic.retire(Vec::new(), Vec::new(), input.clone(), &lower, &upper, &held)
    });
    println!("    ratio: {:.2}x", t_proxy / t_native);
}
