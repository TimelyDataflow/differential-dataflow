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
    target: usize,
    staged: Vec<(Edge, Edge, Time, Diff)>,
}

impl GraphJoinBackend {
    fn new(window: usize) -> Self {
        GraphJoinBackend { window, target: 1 << 20, staged: Vec::new() }
    }
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
        reuse: Option<JoinWindow<u32, Edge, Edge, Time, Diff, Diff>>,
    ) -> Option<JoinWindow<u32, Edge, Edge, Time, Diff, Diff>> {
        cursor.pos0.resize(instance.batches0.len(), 0);
        cursor.pos1.resize(instance.batches1.len(), 0);
        let (fresh_b, other_b) = match fresh {
            Fresh::Input0 => (instance.batches0, instance.batches1),
            Fresh::Input1 => (instance.batches1, instance.batches0),
        };
        let (mut fresh_run, mut other_run) = match reuse {
            Some(JoinWindow { input0, input1 }) => (input0, input1),
            None => (Vec::new(), Vec::new()),
        };
        loop {
            fresh_run.clear();
            other_run.clear();
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

    fn absorb(
        &mut self,
        _instance: &JoinInstance<'_, GraphBatch, GraphBatch>,
        left: (u32, Edge),
        right: (u32, Edge),
        time: Time,
        diff: Diff,
    ) -> Option<Self::Output> {
        self.staged.push((left.1, right.1, time, diff));
        if self.staged.len() >= self.target {
            Some(std::mem::take(&mut self.staged))
        } else {
            None
        }
    }

    fn flush(&mut self, _instance: &JoinInstance<'_, GraphBatch, GraphBatch>) -> Option<Self::Output> {
        if self.staged.is_empty() { None } else { Some(std::mem::take(&mut self.staged)) }
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
        reuse: Option<ReduceWindow<u32, Edge, Time, Diff, Diff>>,
    ) -> Option<ReduceWindow<u32, Edge, Time, Diff, Diff>> {
        self.pos_source.resize(instance.source_batches.len(), 0);
        self.pos_input.resize(instance.input_batches.len(), 0);
        self.pos_output.resize(instance.output_batches.len(), 0);
        let (mut keys, mut seeds, mut input, mut output) = match reuse {
            Some(ReduceWindow { mut keys, mut seeds, mut input, mut output }) => {
                keys.clear(); seeds.clear(); input.clear(); output.clear();
                (keys, seeds, input, output)
            }
            None => (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
        };
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

// ------------------------------------------- cursor incumbents, same storage

/// The incumbent comparison: real `OrdValBatch` storage, consumed two ways — the
/// cursor tactics (the shipping implementations), and the proxy tactics through
/// backends that present from the same batches via the same cursors. Both pay
/// identical storage access; the difference is protocol.
mod ordval {
    use super::{advance, Diff, Edge, Time};

    use timely::container::CapacityContainerBuilder;
    use timely::progress::Antichain;

    use differential_dataflow::consolidation::consolidate_updates;
    use differential_dataflow::operators::int_proxy::{
        JoinInstance, JoinWindow, ProxyJoinBackend, ProxyReduceBackend, ReduceInstance,
        ReduceWindow,
    };
    use differential_dataflow::operators::join::Fresh;
    use differential_dataflow::trace::implementations::{ValBuilder, ValSpine};
    use differential_dataflow::trace::{Builder, Cursor, Description, Navigable, TraceReader};

    pub type Bt = <ValSpine<u32, u32, Time, Diff> as TraceReader>::Batch;
    pub type Bu = ValBuilder<u32, u32, Time, Diff>;
    type Cur = <Bt as Navigable>::Cursor;
    pub type JoinCb = CapacityContainerBuilder<Vec<(Edge, Edge, Time, Diff)>>;

    /// Seal sorted, consolidated updates into one `OrdValBatch`.
    pub fn seal(mut updates: Vec<((u32, u32), Time, Diff)>, lower: Time, upper: Time) -> Bt {
        consolidate_updates(&mut updates);
        let description = Description::new(
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
            Antichain::from_elem(0),
        );
        <Bu as Builder>::seal(&mut vec![updates], description)
    }

    /// The cursor-tactic closures for per-key counting, matching the proxy semantics.
    pub fn count_logic(
        _key: &u32,
        input: &[(&u32, Diff)],
        output: &mut Vec<(u32, Diff)>,
        change: &mut Vec<(u32, Diff)>,
    ) {
        let count: Diff = input.iter().map(|(_, d)| *d).sum();
        if count > 0 {
            change.push((count as u32, 1));
        }
        change.extend(output.drain(..).map(|(x, d)| (x, -d)));
        differential_dataflow::consolidation::consolidate(change);
    }

    /// Shared group-order scanning state over a set of batch cursors.
    #[derive(Default)]
    pub struct Cursors {
        cursors: Vec<Cur>,
        started: bool,
    }

    impl Cursors {
        fn start(&mut self, batches: &[Bt]) {
            if !self.started {
                self.started = true;
                self.cursors = batches.iter().map(|b| b.cursor()).collect();
            }
        }
        fn next_group(&mut self, batches: &[Bt]) -> Option<u32> {
            self.cursors
                .iter()
                .zip(batches)
                .filter_map(|(c, b)| c.get_key(b).copied())
                .min()
        }
        /// Visit every (val, time, diff) of group `g`, advancing past it.
        fn drain_group(&mut self, batches: &[Bt], g: u32, mut logic: impl FnMut(u32, Time, Diff)) {
            for (c, b) in self.cursors.iter_mut().zip(batches) {
                if c.get_key(b) == Some(&g) {
                    while let Some(v) = c.get_val(b) {
                        let v = *v;
                        c.map_times(b, |t, d| logic(v, *t, *d));
                        c.step_val(b);
                    }
                    c.step_key(b);
                }
            }
        }
        /// Skip groups below `g`.
        fn seek(&mut self, batches: &[Bt], g: u32) {
            for (c, b) in self.cursors.iter_mut().zip(batches) {
                c.seek_key(b, &g);
            }
        }
    }

    // ------------------------------------------------------------- proxy join

    #[derive(Default)]
    pub struct OrdJoinCursor {
        c0: Cursors,
        c1: Cursors,
    }

    pub struct OrdJoinBackend {
        pub window: usize,
        pub target: usize,
        staged: Vec<(Edge, Edge, Time, Diff)>,
    }

    impl OrdJoinBackend {
        pub fn new(window: usize) -> Self {
            OrdJoinBackend { window, target: 1 << 20, staged: Vec::new() }
        }
    }

    impl ProxyJoinBackend<Bt, Bt> for OrdJoinBackend {
        type Group = u32;
        type Token0 = Edge;
        type Token1 = Edge;
        type R0 = Diff;
        type R1 = Diff;
        type ROut = Diff;
        type Output = Vec<(Edge, Edge, Time, Diff)>;
        type Cursor = OrdJoinCursor;

        fn next_window(
            &mut self,
            instance: &JoinInstance<'_, Bt, Bt>,
            fresh: Fresh,
            cursor: &mut OrdJoinCursor,
            reuse: Option<JoinWindow<u32, Edge, Edge, Time, Diff, Diff>>,
        ) -> Option<JoinWindow<u32, Edge, Edge, Time, Diff, Diff>> {
            cursor.c0.start(instance.batches0);
            cursor.c1.start(instance.batches1);
            let (mut fresh_run, mut other_run) = match reuse {
                Some(JoinWindow { input0, input1 }) => (input0, input1),
                None => (Vec::new(), Vec::new()),
            };
            loop {
                fresh_run.clear();
                other_run.clear();
                let mut groups = 0;
                while groups < self.window {
                    let (fc, ob, oc, fb) = match fresh {
                        Fresh::Input0 => (&mut cursor.c0, instance.batches1, &mut cursor.c1, instance.batches0),
                        Fresh::Input1 => (&mut cursor.c1, instance.batches0, &mut cursor.c0, instance.batches1),
                    };
                    let Some(g) = fc.next_group(fb) else { break };
                    groups += 1;
                    fc.drain_group(fb, g, |v, t, r| {
                        fresh_run.push(((g, (g, v)), advance(t, instance.lower), r));
                    });
                    oc.seek(ob, g);
                    oc.drain_group(ob, g, |v, t, r| {
                        other_run.push(((g, (g, v)), advance(t, instance.lower), r));
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

        fn absorb(
            &mut self,
            _instance: &JoinInstance<'_, Bt, Bt>,
            left: (u32, Edge),
            right: (u32, Edge),
            time: Time,
            diff: Diff,
        ) -> Option<Self::Output> {
            self.staged.push((left.1, right.1, time, diff));
            if self.staged.len() >= self.target {
                Some(std::mem::take(&mut self.staged))
            } else {
                None
            }
        }

        fn flush(&mut self, _instance: &JoinInstance<'_, Bt, Bt>) -> Option<Self::Output> {
            if self.staged.is_empty() { None } else { Some(std::mem::take(&mut self.staged)) }
        }
    }

    // ----------------------------------------------------------- proxy reduce

    pub struct OrdReduceBackend {
        pub window: usize,
        tiles: Vec<Description<Time>>,
        emitted: Vec<Vec<((u32, u32), Time, Diff)>>,
        source: Cursors,
        input: Cursors,
        output: Cursors,
        pend_idx: usize,
    }

    impl OrdReduceBackend {
        pub fn new(window: usize) -> Self {
            OrdReduceBackend {
                window,
                tiles: Vec::new(),
                emitted: Vec::new(),
                source: Cursors::default(),
                input: Cursors::default(),
                output: Cursors::default(),
                pend_idx: 0,
            }
        }
    }

    impl ProxyReduceBackend<Bt, Bt> for OrdReduceBackend {
        type Group = u32;
        type Token = Edge;
        type RIn = Diff;
        type ROut = Diff;

        fn begin(&mut self, tiles: &[Description<Time>]) {
            self.tiles = tiles.to_vec();
            self.emitted = vec![Vec::new(); tiles.len()];
            self.source = Cursors::default();
            self.input = Cursors::default();
            self.output = Cursors::default();
            self.pend_idx = 0;
        }

        fn next_window(
            &mut self,
            instance: &ReduceInstance<'_, Bt, Bt>,
            pending: &[u32],
            reuse: Option<ReduceWindow<u32, Edge, Time, Diff, Diff>>,
        ) -> Option<ReduceWindow<u32, Edge, Time, Diff, Diff>> {
            self.source.start(instance.source_batches);
            self.input.start(instance.input_batches);
            self.output.start(instance.output_batches);
            let (mut keys, mut seeds, mut input, mut output) = match reuse {
                Some(ReduceWindow { mut keys, mut seeds, mut input, mut output }) => {
                    keys.clear(); seeds.clear(); input.clear(); output.clear();
                    (keys, seeds, input, output)
                }
                None => (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
            };
            while keys.len() < self.window {
                let g = [
                    self.source.next_group(instance.source_batches),
                    self.input.next_group(instance.input_batches),
                    self.output.next_group(instance.output_batches),
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
                self.source.drain_group(instance.source_batches, g, |v, t, r| {
                    input.push(((g, (g, v)), advance(t, instance.lower), r));
                });
                self.input.drain_group(instance.input_batches, g, |v, t, r| {
                    seeds.push((g, t));
                    input.push(((g, (g, v)), advance(t, instance.lower), r));
                });
                self.output.drain_group(instance.output_batches, g, |v, t, r| {
                    output.push(((g, (g, v)), advance(t, instance.lower), r));
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
            self.emitted[tile].extend(records.iter().map(|((g, e), t, r)| ((*g, e.1), *t, *r)));
        }

        fn finish(&mut self) -> Vec<Bt> {
            self.tiles
                .drain(..)
                .zip(self.emitted.drain(..))
                .map(|(desc, mut updates)| {
                    consolidate_updates(&mut updates);
                    let mut chain = vec![updates];
                    <Bu as Builder>::seal(&mut chain, desc)
                })
                .collect()
        }
    }

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
        let mut tactic = ProxyJoinTactic::new(GraphJoinBackend::new(window));
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

    // ---- incumbent comparison: identical OrdValBatch storage, cursor vs proxy ----

    use differential_dataflow::operators::reduce::cursors::CursorTactic as ReduceCursorTactic;
    use differential_dataflow::operators::join::cursors::CursorTactic as JoinCursorTactic;

    let a_ord = vec![ordval::seal(edges(1), 0, 1)];
    let b_ord = vec![ordval::seal(edges(2), 0, 1)];

    println!("\njoin on OrdValBatch storage (cursor incumbent vs proxy protocol):");
    let t_cursor = time("cursor JoinTactic", reps, || {
        let mut tactic = JoinCursorTactic::<ordval::Bt, ordval::Bt, _, ordval::JoinCb>::new(
            |k: &u32, v0: &u32, v1: &u32, t: Time, d0: &Diff, d1: &Diff, cb: &mut ordval::JoinCb| {
                use timely::container::PushInto;
                cb.push_into(((*k, *v0), (*k, *v1), t, d0 * d1));
            },
        );
        let work = tactic.prep(a_ord.clone(), b_ord.clone(), Fresh::Input0, 0);
        work.collect::<Vec<_>>()
    });
    let t_proxy = time("proxy join (cursor backend)", reps, || {
        let mut tactic = ProxyJoinTactic::new(ordval::OrdJoinBackend::new(window));
        let work = tactic.prep(a_ord.clone(), b_ord.clone(), Fresh::Input0, 0);
        work.collect::<Vec<_>>()
    });
    println!("    ratio: {:.2}x", t_proxy / t_cursor);

    println!("\nreduce on OrdValBatch storage (cursor incumbent vs proxy protocol):");
    let input_ord = vec![ordval::seal(edges(1), 0, 1)];
    let lower = Antichain::from_elem(0u64);
    let upper = Antichain::from_elem(1u64);
    let held = Antichain::from_elem(0u64);
    let t_cursor = time("cursor ReduceTactic", reps, || {
        let mut tactic = ReduceCursorTactic::<ordval::Bt, ordval::Bt, ordval::Bu, _, _>::new(
            ordval::count_logic,
            |vec: &mut Vec<((u32, u32), Time, Diff)>, key: &u32, upds: &mut Vec<(u32, Time, Diff)>| {
                vec.clear();
                vec.extend(upds.drain(..).map(|(v, t, r)| ((*key, v), t, r)));
            },
        );
        tactic.retire(Vec::new(), Vec::new(), input_ord.clone(), &lower, &upper, &held)
    });
    let t_proxy = time("proxy reduce (cursor backend)", reps, || {
        let mut tactic = ProxyReduceTactic::new(ordval::OrdReduceBackend::new(window));
        tactic.retire(Vec::new(), Vec::new(), input_ord.clone(), &lower, &upper, &held)
    });
    println!("    ratio: {:.2}x", t_proxy / t_cursor);
}
