//! A backend reading an input entered into a nested scope, as [`BatchEnter`] reads it.

use std::marker::PhantomData;

use timely::progress::{Antichain, Timestamp};
use timely::progress::timestamp::Refines;

use crate::trace::Description;
use crate::trace::wrappers::enter::BatchEnter;

use super::super::join::{JoinInstance, JoinMatches, ProxyJoinBackend};
use super::super::reduce::{ProxyReduceBackend, ReduceInstance, ReduceWindow};
use super::super::{KeyPosition, ProxyBridge};
use super::{Left, Right};

/// A backend whose input on side `S` is entered into a nested scope from times `TOuter`: the inner
/// backend reads the batches inside each `BatchEnter`, and every time it draws enters the scope.
pub struct BackendEnter<B, TOuter, S = Left> {
    inner: B,
    marker: PhantomData<(TOuter, S)>,
}

impl<B, TOuter> BackendEnter<B, TOuter, Left> {
    /// Read the first input of a join, or the input of a reduce, through `BatchEnter`.
    pub fn new(inner: B) -> Self { BackendEnter { inner, marker: PhantomData } }
}

impl<B, TOuter> BackendEnter<B, TOuter, Right> {
    /// Read the second input of a join through `BatchEnter`.
    pub fn right(inner: B) -> Self { BackendEnter { inner, marker: PhantomData } }
}

/// The batches inside `BatchEnter`s.
fn entered<B: Clone, T>(batches: &[BatchEnter<B, T>]) -> Vec<B> {
    batches.iter().map(|batch| batch.inner().clone()).collect()
}

/// `drawn`'s records with their times entered, into `bridge`. Entering is injective and keeps
/// order, so the records stay sorted and consolidated.
fn enter_bridge<T: Refines<TOuter>, TOuter: Timestamp, R, K, V>(drawn: ProxyBridge<TOuter, R, K, V>, bridge: &mut ProxyBridge<T, R, K, V>) {
    bridge.extend(drawn.into_iter().map(|(kv, time, diff)| (kv, T::to_inner(time), diff)));
}

// ===== Join =====

impl<T, TOuter, W0, B1, T1, B> ProxyJoinBackend<T, BatchEnter<W0, T>, B1, T, T1> for BackendEnter<B, TOuter, Left>
where
    T: Refines<TOuter>,
    TOuter: Timestamp,
    W0: Clone,
    B1: Clone,
    T1: Clone,
    B: ProxyJoinBackend<T, W0, B1, TOuter, T1>,
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
        instance: &JoinInstance<T, BatchEnter<W0, T>, B1, T1>,
        from: &mut KeyPosition<Self::Key>,
        bridge0: &mut ProxyBridge<T, Self::R0, Self::Key, Self::V0>,
        bridge1: &mut ProxyBridge<T1, Self::R1, Self::Key, Self::V1>,
    ) {
        let mut drawn = Vec::with_capacity(bridge0.capacity());
        self.inner.advance(&enter_left(instance), from, &mut drawn, bridge1);
        enter_bridge(drawn, bridge0);
    }

    fn cross(
        &mut self,
        instance: &JoinInstance<T, BatchEnter<W0, T>, B1, T1>,
        matches: &mut JoinMatches<T, Self::ROut, Self::Key, Self::V0, Self::V1>,
        output: &mut Vec<Self::Output>,
    ) {
        self.inner.cross(&enter_left(instance), matches, output);
    }
}

/// The instance an `BackendEnter` on the first input hands its inner backend.
fn enter_left<T: Refines<TOuter>, TOuter: Timestamp, W0: Clone, B1: Clone, T1: Clone>(instance: &JoinInstance<T, BatchEnter<W0, T>, B1, T1>) -> JoinInstance<TOuter, W0, B1, T1> {
    JoinInstance {
        batches0: entered(&instance.batches0),
        batches1: instance.batches1.clone(),
        lower0: instance.lower0.clone().to_outer(),
        lower1: instance.lower1.clone(),
    }
}

impl<T, TOuter, B0, W1, T0, B> ProxyJoinBackend<T, B0, BatchEnter<W1, T>, T0, T> for BackendEnter<B, TOuter, Right>
where
    T: Refines<TOuter>,
    TOuter: Timestamp,
    B0: Clone,
    W1: Clone,
    T0: Clone,
    B: ProxyJoinBackend<T, B0, W1, T0, TOuter>,
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
        instance: &JoinInstance<T0, B0, BatchEnter<W1, T>, T>,
        from: &mut KeyPosition<Self::Key>,
        bridge0: &mut ProxyBridge<T0, Self::R0, Self::Key, Self::V0>,
        bridge1: &mut ProxyBridge<T, Self::R1, Self::Key, Self::V1>,
    ) {
        let mut drawn = Vec::with_capacity(bridge1.capacity());
        self.inner.advance(&enter_right(instance), from, bridge0, &mut drawn);
        enter_bridge(drawn, bridge1);
    }

    fn cross(
        &mut self,
        instance: &JoinInstance<T0, B0, BatchEnter<W1, T>, T>,
        matches: &mut JoinMatches<T, Self::ROut, Self::Key, Self::V0, Self::V1>,
        output: &mut Vec<Self::Output>,
    ) {
        self.inner.cross(&enter_right(instance), matches, output);
    }
}

/// The instance an `BackendEnter` on the second input hands its inner backend.
fn enter_right<T: Refines<TOuter>, TOuter: Timestamp, B0: Clone, W1: Clone, T0: Clone>(instance: &JoinInstance<T0, B0, BatchEnter<W1, T>, T>) -> JoinInstance<T0, B0, W1, TOuter> {
    JoinInstance {
        batches0: instance.batches0.clone(),
        batches1: entered(&instance.batches1),
        lower0: instance.lower0.clone(),
        lower1: instance.lower1.clone().to_outer(),
    }
}

// ===== Reduce =====

impl<T, TOuter, W, B2, B> ProxyReduceBackend<T, BatchEnter<W, T>, B2> for BackendEnter<B, TOuter, Left>
where
    T: Refines<TOuter>,
    TOuter: Timestamp,
    W: Clone,
    B: ProxyReduceBackend<T, W, B2, TOuter>,
{
    type Key = B::Key;
    type VIn = B::VIn;
    type VOut = B::VOut;
    type RIn = B::RIn;
    type ROut = B::ROut;

    fn begin(&mut self, description: Description<T>) { self.inner.begin(description) }

    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, T, BatchEnter<W, T>, B2>,
        changed: &[Self::Key],
        from: &mut KeyPosition<Self::Key>,
        window: &mut ReduceWindow<T, Self::RIn, Self::ROut, Self::Key, Self::VIn, Self::VOut>,
    ) {
        let (source, input) = (entered(instance.source_batches), entered(instance.input_batches));
        // The input's loading frontier, as outer times: no later than the inner one, so sound.
        let input_lower: Antichain<TOuter> = instance.input_lower.iter().map(|time| time.clone().to_outer()).collect();
        let inner = ReduceInstance {
            source_batches: &source,
            input_batches: &input,
            output_batches: instance.output_batches,
            lower: instance.lower,
            input_lower: input_lower.borrow(),
        };
        let mut drawn = ReduceWindow {
            input: Vec::with_capacity(window.input.capacity()),
            seeds: Vec::with_capacity(window.seeds.capacity()),
            output: std::mem::take(&mut window.output),
        };
        self.inner.next_window(&inner, changed, from, &mut drawn);
        enter_bridge(drawn.input, &mut window.input);
        window.seeds.extend(drawn.seeds.into_iter().map(|(key, time)| (key, T::to_inner(time))));
        window.output = drawn.output;
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
