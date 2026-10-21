//! A backend reading an input as of a frontier, as [`BatchFrontier`] reads it.

use std::marker::PhantomData;

use timely::progress::Timestamp;

use crate::consolidation::consolidate_updates;
use crate::lattice::Lattice;
use crate::trace::Description;
use crate::trace::wrappers::frontier::BatchFrontier;

use super::super::join::{JoinInstance, JoinMatches, ProxyJoinBackend};
use super::super::reduce::{ProxyReduceBackend, ReduceInstance, ReduceWindow};
use super::super::{KeyPosition, ProxyBridge};
use super::{Left, Right};

/// A backend whose input on side `S` is read as of a frontier: the inner backend reads the batches
/// inside each `BatchFrontier`, and every time it draws is advanced by `since`, or dropped at or
/// beyond `until`, and what then coincides is consolidated.
pub struct BackendFrontier<B, S = Left> {
    inner: B,
    marker: PhantomData<S>,
}

impl<B> BackendFrontier<B, Left> {
    /// Read the first input of a join, or the input of a reduce, through `BatchFrontier`.
    pub fn new(inner: B) -> Self { BackendFrontier { inner, marker: PhantomData } }
}

impl<B> BackendFrontier<B, Right> {
    /// Read the second input of a join through `BatchFrontier`.
    pub fn right(inner: B) -> Self { BackendFrontier { inner, marker: PhantomData } }
}

/// The batches inside `BatchFrontier`s.
fn frontiered<B: Clone, T>(batches: &[BatchFrontier<B, T>]) -> Vec<B> {
    batches.iter().map(|batch| batch.inner().clone()).collect()
}

/// `bridge`'s records as of `frontier`: times advanced, those at or beyond `until` dropped, and
/// the rest consolidated. Returns whether any record changed; if none did, the bridge is as it
/// was, still sorted and consolidated.
fn advance_bridge<B, T, R, K, V>(bridge: &mut ProxyBridge<T, R, K, V>, frontier: &BatchFrontier<B, T>) -> bool
where
    T: Timestamp + Lattice,
    R: crate::difference::Semigroup,
    K: Ord,
    V: Ord,
{
    let mut changed = false;
    bridge.retain_mut(|(_, time, _)| {
        let stored = time.clone();
        let kept = frontier.advance_time(time);
        changed |= !kept || *time != stored;
        kept
    });
    if changed { consolidate_updates(bridge); }
    changed
}

/// Keep only `bridge`'s keys that `other` holds: a join's bridges present common keys, and
/// consolidating one side can empty some.
fn common_keys<T, R, K: Copy + Ord, V, T2, R2, V2>(bridge: &mut ProxyBridge<T, R, K, V>, other: &ProxyBridge<T2, R2, K, V2>) {
    let mut at = 0;
    bridge.retain(|((key, _), _, _)| {
        while at < other.len() && other[at].0.0 < *key { at += 1; }
        at < other.len() && other[at].0.0 == *key
    });
}

// ===== Join =====

impl<T, T0, W0, B1, T1, B> ProxyJoinBackend<T, BatchFrontier<W0, T0>, B1, T0, T1> for BackendFrontier<B, Left>
where
    T0: Timestamp + Lattice,
    W0: Clone,
    B1: Clone,
    T1: Clone,
    B: ProxyJoinBackend<T, W0, B1, T0, T1>,
{
    type Key = B::Key;
    type V0 = B::V0;
    type V1 = B::V1;
    type R0 = B::R0;
    type R1 = B::R1;
    type ROut = B::ROut;
    type Output = B::Output;

    fn advance(
        &mut self,
        instance: &JoinInstance<T0, BatchFrontier<W0, T0>, B1, T1>,
        from: &mut KeyPosition<Self::Key>,
        bridge0: &mut ProxyBridge<T0, Self::R0, Self::Key, Self::V0>,
        bridge1: &mut ProxyBridge<T1, Self::R1, Self::Key, Self::V1>,
    ) {
        self.inner.advance(&frontier_left(instance), from, bridge0, bridge1);
        if let Some(frontier) = instance.batches0.first() {
            if advance_bridge(bridge0, frontier) { common_keys(bridge1, bridge0); }
        }
    }

    fn cross(
        &mut self,
        instance: &JoinInstance<T0, BatchFrontier<W0, T0>, B1, T1>,
        matches: &mut JoinMatches<T, Self::ROut, Self::Key, Self::V0, Self::V1>,
        output: &mut Vec<Self::Output>,
    ) {
        self.inner.cross(&frontier_left(instance), matches, output);
    }
}

/// The instance a `BackendFrontier` on the first input hands its inner backend.
fn frontier_left<T0: Clone, W0: Clone, B1: Clone, T1: Clone>(instance: &JoinInstance<T0, BatchFrontier<W0, T0>, B1, T1>) -> JoinInstance<T0, W0, B1, T1> {
    JoinInstance {
        batches0: frontiered(&instance.batches0),
        batches1: instance.batches1.clone(),
        lower0: instance.lower0.clone(),
        lower1: instance.lower1.clone(),
    }
}

impl<T, T0, B0, W1, T1, B> ProxyJoinBackend<T, B0, BatchFrontier<W1, T1>, T0, T1> for BackendFrontier<B, Right>
where
    T1: Timestamp + Lattice,
    B0: Clone,
    W1: Clone,
    T0: Clone,
    B: ProxyJoinBackend<T, B0, W1, T0, T1>,
{
    type Key = B::Key;
    type V0 = B::V0;
    type V1 = B::V1;
    type R0 = B::R0;
    type R1 = B::R1;
    type ROut = B::ROut;
    type Output = B::Output;

    fn advance(
        &mut self,
        instance: &JoinInstance<T0, B0, BatchFrontier<W1, T1>, T1>,
        from: &mut KeyPosition<Self::Key>,
        bridge0: &mut ProxyBridge<T0, Self::R0, Self::Key, Self::V0>,
        bridge1: &mut ProxyBridge<T1, Self::R1, Self::Key, Self::V1>,
    ) {
        self.inner.advance(&frontier_right(instance), from, bridge0, bridge1);
        if let Some(frontier) = instance.batches1.first() {
            if advance_bridge(bridge1, frontier) { common_keys(bridge0, bridge1); }
        }
    }

    fn cross(
        &mut self,
        instance: &JoinInstance<T0, B0, BatchFrontier<W1, T1>, T1>,
        matches: &mut JoinMatches<T, Self::ROut, Self::Key, Self::V0, Self::V1>,
        output: &mut Vec<Self::Output>,
    ) {
        self.inner.cross(&frontier_right(instance), matches, output);
    }
}

/// The instance a `BackendFrontier` on the second input hands its inner backend.
fn frontier_right<T0: Clone, B0: Clone, W1: Clone, T1: Clone>(instance: &JoinInstance<T0, B0, BatchFrontier<W1, T1>, T1>) -> JoinInstance<T0, B0, W1, T1> {
    JoinInstance {
        batches0: instance.batches0.clone(),
        batches1: frontiered(&instance.batches1),
        lower0: instance.lower0.clone(),
        lower1: instance.lower1.clone(),
    }
}

// ===== Reduce =====

impl<T, TIn, W, B2, B> ProxyReduceBackend<T, BatchFrontier<W, TIn>, B2, TIn> for BackendFrontier<B, Left>
where
    TIn: Timestamp + Lattice,
    W: Clone,
    B: ProxyReduceBackend<T, W, B2, TIn>,
{
    type Key = B::Key;
    type VIn = B::VIn;
    type VOut = B::VOut;
    type RIn = B::RIn;
    type ROut = B::ROut;

    fn begin(&mut self, description: Description<T>) { self.inner.begin(description) }

    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, T, BatchFrontier<W, TIn>, B2, TIn>,
        changed: &[Self::Key],
        from: &mut KeyPosition<Self::Key>,
        window: &mut ReduceWindow<T, Self::RIn, Self::ROut, Self::Key, Self::VIn, Self::VOut, TIn>,
    ) {
        let (source, input) = (frontiered(instance.source_batches), frontiered(instance.input_batches));
        let inner = ReduceInstance {
            source_batches: &source,
            input_batches: &input,
            output_batches: instance.output_batches,
            lower: instance.lower,
            input_lower: instance.input_lower,
        };
        self.inner.next_window(&inner, changed, from, window);
        // The prior and novel batches are one trace's, read as of one frontier.
        if let Some(frontier) = instance.source_batches.first().or(instance.input_batches.first()) {
            advance_bridge(&mut window.input, frontier);
            let mut changed = false;
            window.seeds.retain_mut(|(_, time)| {
                let stored = time.clone();
                let kept = frontier.advance_time(time);
                changed |= !kept || *time != stored;
                kept
            });
            if changed {
                window.seeds.sort();
                window.seeds.dedup();
            }
        }
    }

    fn reduce_corrections(
        &mut self,
        keys: &[Self::Key],
        in_ends: &[usize],
        input: &[(Self::VIn, Self::RIn)],
        out_ends: &[usize],
        output: &[(Self::VOut, Self::ROut)],
    ) -> (Vec<(Self::VOut, Self::ROut)>, Vec<usize>) {
        self.inner.reduce_corrections(keys, in_ends, input, out_ends, output)
    }

    fn emit(&mut self, records: &[((Self::Key, Self::VOut), T, Self::ROut)]) { self.inner.emit(records) }

    fn finish(&mut self) -> Option<B2> { self.inner.finish() }
}
