//! A backend that reads its inputs as of a frontier.
//!
//! A trace's accumulations are correct only at or beyond its compaction frontier: a batch may hold
//! a retraction advanced to the frontier beside the insertion it retracts, so an earlier time can
//! show a count no time had. A reader that starts after the trace has compacted must read every
//! update's time advanced to a frontier `since`, as [`TraceFrontier`](crate::trace::wrappers::frontier::TraceFrontier)
//! does for a cursor. The proxy tactics hand batches to a backend, which reads their times as
//! stored, so [`BackendFrontier`] reads what its inner backend draws as of `since`.
//!
//! Advancing is not injective: updates that coincide once advanced are consolidated, since the
//! tactics require consolidated bridges, and a join's bridges must then still hold keys common to
//! both inputs. Both are done only where a time actually moved.
//!
//! # What `since` must be
//!
//! `since` must be beyond every frontier the trace has compacted by: each of its elements at or
//! beyond some element of each such frontier. Then advancing a stored time, already advanced by a
//! compaction frontier `F`, by `since` is advancing the original time by `since`: with
//! `x = advance_F(t)`, `t ≤ x ≤ t ∨ f` for some `f ≤ s` for each `s` in `since`, so
//! `t ∨ s ≤ x ∨ s ≤ t ∨ f ∨ s = t ∨ s`, and the meets over `s` agree. So merged and unmerged
//! copies of one update land on one time, in any lattice. A reader checks this against the
//! trace's logical compaction frontier when it imports, as `TraceAgent::import_frontier` does by
//! taking that frontier as `since`. In particular, reading as of the *meet* of the trace's
//! compaction frontier is not safe in a non-distributive lattice: it advances too little, and
//! leaves times between the meet and the frontier where the trace promises nothing.
//!
//! # Where the work is, and what is exact
//!
//! Everything below uses only the defining property of [`Lattice::advance_by`]: for every `t'`
//! beyond a frontier `F`, `t ≤ t' ⟺ advance_F(t) ≤ t'`; together with `t ≤ advance_F(t)`, that
//! `advance_F(t) = t` when `t` is beyond `F`, and the squeeze `t ∨ t' = advance_F(t) ∨ t'` for
//! `t'` beyond `F`. None of it needs the lattice to be distributive.
//!
//! Each unit of work carries a bound on the times it is about: the join's meet of the fresh
//! input's times (`JoinInstance::lower`), the reduce's retire interval (`ReduceInstance::lower`).
//! When that bound is beyond `since`, the wrapper changes nothing, for any inner backend:
//!
//! - A join's output times are `t0 ∨ t1` with the fresh input's time beyond `since`, so by the
//!   squeeze advancing either input by `since` leaves every output time as it is: the output is
//!   identical to the as-of reading's.
//! - A reduce evaluates only at times `at` at or beyond its lower bound, hence beyond `since`.
//!   Whether a record is in the accumulation there (`t ≤ at`) and which times it synthesizes
//!   (`t ∨ at`) are the same for `t` and `advance(t)`, so the retire's output is identical.
//!
//! That is the steady state of an operator reading a trace it imported: every unit after the
//! first has its bound beyond the import time. The first unit reads the trace's history, and only
//! it pays for the read. There the wrapper advances what the inner backend presents, and:
//!
//! - **It is exact** when the inner backend presents stored times, or presents them advanced by a
//!   bound that `since` is beyond (every first unit, whose bound is the minimum): the same squeeze
//!   as for compaction. It is also exact when `since` has one element, whatever the backend does.
//! - **In general** the inner backend may consolidate on load by its bound `lower`, and in a
//!   non-distributive lattice `advance_since(t ∨ lower)` need not equal `advance_since(t)` when
//!   `since` and `lower` cross. Then the output agrees with the as-of reading at every time beyond
//!   `since`, which is all a trace read as of `since` promises its readers anyway: for `t'` beyond
//!   `since`, `advance(t ∨ c) ≤ t' ⟺ t ≤ t' ∧ c ≤ t'`, and `c ≤ lower ≤` the fresh time `≤ t'`.
//!
//! # The join reads both inputs
//!
//! Reading one input of a join as of `since` and reading both agree at every time beyond `since`,
//! and are identical when the other input's times are beyond `since` (the squeeze again). So the
//! join wrapper reads both inputs as of `since`; a join whose inputs are read as of two frontiers
//! nests two wrappers.

use timely::progress::{Antichain, Timestamp};

use crate::consolidation::consolidate_updates;
use crate::difference::Semigroup;
use crate::lattice::Lattice;
use crate::trace::Description;

use super::join::{JoinInstance, JoinMatches, ProxyJoinBackend};
use super::reduce::{ProxyReduceBackend, ReduceInstance, ReduceWindow};
use super::{KeyPosition, ProxyBridge};

/// A backend whose inputs are read as of `since`: every time the inner backend draws is advanced
/// by it, and what then coincides is consolidated.
///
/// Only reads are translated. Outputs are the inner backend's, at the operator's times, which are
/// beyond `since` once its inputs are. An empty `since` reads the inputs as they are.
pub struct BackendFrontier<B, T> {
    inner: B,
    since: Antichain<T>,
}

impl<B, T> BackendFrontier<B, T> {
    /// Read `inner`'s inputs as of `since`, which must be beyond every frontier the inputs' traces
    /// have compacted by (see the module documentation).
    pub fn new(inner: B, since: Antichain<T>) -> Self { BackendFrontier { inner, since } }
    /// The frontier the inputs are read as of.
    pub fn since(&self) -> &Antichain<T> { &self.since }
    /// The wrapped backend.
    pub fn inner(&self) -> &B { &self.inner }
    /// The wrapped backend, mutably: a wrapper implementing a backend trait defined outside this
    /// crate, such as dogs3's half-join backend, drives the inner backend through this.
    pub fn inner_mut(&mut self) -> &mut B { &mut self.inner }
}

/// Read `bridge` as of `since`: times advanced by it, and the bridge consolidated if any moved,
/// as advancing can make records coincide. Returns whether any did; if none, the bridge is as
/// presented, still sorted and consolidated.
///
/// Public so that a wrapper for a backend trait defined outside this crate reads its bridges the
/// one way: dogs3's half-join wrapper reads the arranged side with it.
pub fn read_as_of<T, R, K, V>(bridge: &mut ProxyBridge<T, R, K, V>, since: &Antichain<T>) -> bool
where
    T: Timestamp + Lattice,
    R: Semigroup,
    K: Ord,
    V: Ord,
{
    let mut moved = false;
    for (_, time, _) in bridge.iter_mut() {
        // A time beyond `since` is its own advance; the test is cheaper than the advance.
        if !since.less_equal(time) {
            let stored = time.clone();
            time.advance_by(since.borrow());
            moved |= *time != stored;
        }
    }
    if moved { consolidate_updates(bridge); }
    moved
}

/// Keep only `bridge`'s keys that `other` holds. Both are in key order.
fn common_keys<T, R, K: Copy + Ord, V, T2, R2, V2>(bridge: &mut ProxyBridge<T, R, K, V>, other: &ProxyBridge<T2, R2, K, V2>) {
    let mut at = 0;
    bridge.retain(|((key, _), _, _)| {
        while at < other.len() && other[at].0.0 < *key { at += 1; }
        at < other.len() && other[at].0.0 == *key
    });
}

// ===== Join =====

impl<T, B0, B1, B> ProxyJoinBackend<T, B0, B1> for BackendFrontier<B, T>
where
    T: Timestamp + Lattice,
    B: ProxyJoinBackend<T, B0, B1>,
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
        instance: &JoinInstance<T, B0, B1>,
        from: &mut KeyPosition<Self::Key>,
        bridge0: &mut ProxyBridge<T, Self::R0, Self::Key, Self::V0>,
        bridge1: &mut ProxyBridge<T, Self::R1, Self::Key, Self::V1>,
    ) {
        self.inner.advance(instance, from, bridge0, bridge1);
        // Every output time is at or beyond the fresh input's times, hence `lower`: beyond `since`.
        if self.since.is_empty() || self.since.less_equal(&instance.lower) { return; }
        // A key whose updates net away on one side has nothing to match, and the harness requires
        // the bridges' keys to agree.
        let moved0 = read_as_of(bridge0, &self.since);
        let moved1 = read_as_of(bridge1, &self.since);
        if moved0 || moved1 {
            common_keys(bridge1, bridge0);
            common_keys(bridge0, bridge1);
        }
    }

    fn cross(
        &mut self,
        instance: &JoinInstance<T, B0, B1>,
        matches: &mut JoinMatches<T, Self::ROut, Self::Key, Self::V0, Self::V1>,
        output: &mut Vec<Self::Output>,
    ) {
        self.inner.cross(instance, matches, output)
    }
}

// ===== Reduce =====

impl<T, B1, B2, B> ProxyReduceBackend<T, B1, B2> for BackendFrontier<B, T>
where
    T: Timestamp + Lattice,
    B: ProxyReduceBackend<T, B1, B2>,
{
    type Key = B::Key;
    type VIn = B::VIn;
    type VOut = B::VOut;
    type RIn = B::RIn;
    type ROut = B::ROut;

    fn begin(&mut self, description: Description<T>) { self.inner.begin(description) }

    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, T, B1, B2>,
        changed: &[Self::Key],
        from: &mut KeyPosition<Self::Key>,
        window: &mut ReduceWindow<T, Self::RIn, Self::ROut, Self::Key, Self::VIn, Self::VOut>,
    ) {
        self.inner.next_window(instance, changed, from, window);
        // The retire evaluates only at times at or beyond `lower`; beyond `since`, a record and
        // its advanced self accumulate and synthesize alike.
        if self.since.is_empty() || instance.lower.iter().all(|time| self.since.less_equal(time)) { return; }
        // The input, novel and prior merged, is read as of `since`; so is the novel time support,
        // which seeds the retire's interesting times. The output is the operator's own, at times
        // beyond `since` already.
        read_as_of(&mut window.input, &self.since);
        let mut moved = false;
        for (_, time) in window.seeds.iter_mut() {
            if !self.since.less_equal(time) {
                let stored = time.clone();
                time.advance_by(self.since.borrow());
                moved |= *time != stored;
            }
        }
        if moved {
            window.seeds.sort();
            window.seeds.dedup();
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
