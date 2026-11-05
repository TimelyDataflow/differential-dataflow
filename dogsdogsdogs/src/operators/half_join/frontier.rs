//! Reading a half join's arrangement as of a frontier: differential's [`BackendFrontier`] as a
//! [`ProxyHalfJoinBackend`].
//!
//! The wrapper reads the arranged side as of `since`: every arranged time the inner backend
//! presents that is not already beyond `since` is advanced by it, and the bridge is consolidated
//! if any moved ([`read_as_of`]). The stream side is the operator's own input and is left alone.
//! `stage` and `cross` pass through, and `cross` sees the advanced times, since the tactic hands
//! it the presented sides back: the data time `data ∨ time2` is taken over the advanced `time2`,
//! which for a data time beyond `since` equals the stored one's (`s ∨ t2 = s ∨ advance(t2)` for
//! `s` beyond `since`, in any lattice).
//!
//! # Why every unit reads as of `since`
//!
//! The join and reduce wrappers skip a unit whose lower bound is beyond `since`, because their
//! logic is in the partial order, where `t ≤ t'` and `advance(t) ≤ t'` agree for `t'` beyond
//! `since`. The half join compares in the *total* order, and there advancing changes the answer
//! even against stream times beyond `since`: with product times, `since = {(1, 0)}`, a stream
//! time `(1, 0)` and an arranged time `(0, 5)`, the stored `(0, 5)` precedes `(1, 0)` while its
//! advance `(1, 5)` does not. That is the point: an import's rows appear once arranged, here, and
//! once as a stream, read as of `since` by the operator that lifts them, and a delta query meets
//! each pair exactly once only if both paths compare the same times. So there is no unit-level
//! shortcut; the shortcut is per row, one `since.less_equal` per presented arranged row, which
//! moves nothing at a program time. Only an import's history, at times before `since`, is
//! advanced and consolidated.
//!
//! No key intersection is needed, unlike the join: the tactic skips keys found on one side alone.

use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::int_proxy::{read_as_of, BackendFrontier, KeyPosition, ProxyBridge};

use super::proxy::{HalfJoinInstance, HalfJoinMatches, HalfJoinStream, ProxyHalfJoinBackend};

impl<T, B, C0, Bk> ProxyHalfJoinBackend<T, B, C0> for BackendFrontier<Bk, T>
where
    T: Timestamp + Lattice,
    Bk: ProxyHalfJoinBackend<T, B, C0>,
{
    type Key = Bk::Key;
    type V0 = Bk::V0;
    type V1 = Bk::V1;
    type R0 = Bk::R0;
    type R1 = Bk::R1;
    type ROut = Bk::ROut;
    type Output = Bk::Output;
    type Stream = Bk::Stream;

    fn stage(&mut self, chunks: Vec<C0>) -> Bk::Stream { self.inner_mut().stage(chunks) }

    fn advance(
        &mut self,
        instance: &HalfJoinInstance<B, Bk::Stream>,
        from: &mut KeyPosition<Bk::Key>,
        stream: &mut HalfJoinStream<T, Bk::R0, Bk::Key, Bk::V0>,
        arranged: &mut ProxyBridge<T, Bk::R1, Bk::Key, Bk::V1>,
    ) {
        self.inner_mut().advance(instance, from, stream, arranged);
        if self.since().is_empty() { return; }
        read_as_of(arranged, self.since());
    }

    fn cross(
        &mut self,
        instance: &HalfJoinInstance<B, Bk::Stream>,
        stream: &HalfJoinStream<T, Bk::R0, Bk::Key, Bk::V0>,
        arranged: &ProxyBridge<T, Bk::R1, Bk::Key, Bk::V1>,
        matches: &mut HalfJoinMatches<Bk::ROut>,
        output: &mut Vec<Bk::Output>,
    ) {
        self.inner_mut().cross(instance, stream, arranged, matches, output)
    }
}
