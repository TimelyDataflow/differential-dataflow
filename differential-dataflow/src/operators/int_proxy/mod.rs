//! Backend-agnostic operator tactics using ordered, copyable proxies.
//!
//! The tactics support custom operator implementations without rebuilding the non-trivial and often non-obvious time-based logic that supports them.
//! They run DD's operator logic over consolidated `[((key, val), time, diff)]` lists.
//! Keys identify independent groups; values identify data within a group.
//! Both require only `Copy + Ord` and need not be the types presented to user logic.
//! The tactics elicit proxies, perform time and difference computations, and return proxies to the backend for interpretation.
//! The backend is oblivious to the navigation of time, and the operator to the backend's implementation.
//!
//! The default proxy types remain `u64`, with the interpretation described below.
//! Exact key proxies do not require collision handling.
//!
//! Input and output value proxies may have different types.
//! Standalone reduce calls can use borrowed strings whose storage outlives the call and any pending keys.
//! Deferred join work still requires `'static` proxies.
//! Borrowing directly from backend-owned presentation storage needs a further lifetime or container abstraction.
//!
//! # The two integers
//!
//! The two integers play the role of key and value to the operator, but their connection
//! to the key and value of the intended computation is nuanced.
//!
//! *   `key_hash: u64` — a content hash of the intended key.
//!     This value should be identical for each instance of identical keys.
//!     The value is not assumed to be a well-distributed quality hash, only distinct.
//!     There may be collisions, and the next identifier should assist with this.
//!
//! *   `value_id: u64` — an ephemeral intra-hash value identifier.
//!     These values should be distinct for each distinct datum with the same key_hash.
//!     In particular, they should account for the conventional `(key, val)` data,
//!     rather than only the value component, to avoid errors due to colliding keys.
//!
//! The key hash acts as an independence marker: keys with different hashes will be isolated,
//! and their incremental updates performed independently. By forcing the keyspace into `u64`,
//! there is the risk of hash collision. This means that per-hash logic should be prepared for
//! this, and should not discard key information that remains semantically important.
//!
//! Informally, one should mentally rewrite one's `(key, val)` into `(hash(key), (key, val))`.
//! Retaining the `key` as data provides access to it, and allows one to certainly respond to
//! hash collisions that can occur. Although the operator will treat `hash(key)` as the "key",
//! the backend implementation can apply its own subseqent logic to resolve unwanted matches.
//! As examples, join logic can follow its hash-wise cross product with key-based filtering,
//! and reduce logic can group by keys and apply logic to slices within the per-hash list.
//! Both are welcome to efficiently notice that there have been no collisions and optimize,
//! or to ignore the risk entirely and live dangerously.

use std::ops::Range;

use crate::difference::Multiply;
use crate::lattice::Lattice;

mod history;

pub mod join;
pub mod reduce;
mod pending;
pub mod frontier;
pub mod vec_backend;

/// A consolidated collection of `[((key, val), time, diff)]` proxies.
///
/// The [`debug_assert_sorted_bridge`] method is (and can be) used to validate this property.
pub type ProxyBridge<T, R, K = u64, V = u64> = Vec<((K, V), T, R)>;

/// Debug check that a presented [`ProxyBridge`] is consolidated.
///
/// Operator harnesses use the test to flag backend implementations that do not uphold it. It is
/// public so that a tactic built outside this crate over the same bridges, such as dogs3's proxy
/// half join, checks the contract its backends owe with the same test.
pub fn debug_assert_sorted_bridge<T: Ord, R, K: Copy + Ord, V: Copy + Ord>(bridge: &ProxyBridge<T, R, K, V>, who: &str) {
    debug_assert!(
        bridge.windows(2).all(|w| (w[0].0, &w[0].1) < (w[1].0, &w[1].1)),
        "{}: a presented bridge must be sorted & consolidated by ((key, val), time)",
        who,
    );
}

/// A column of times, as a backend stores them, with the operations the proxy tactics need.
///
/// Operations take and produce columns, never owned times, except the two scaffolding exits,
/// which exist for paths not yet expressed as column operations and go once nothing calls them.
pub trait TimeColumn<T>: Default {
    /// The number of times in the column.
    fn len(&self) -> usize;
    /// Remove every time, keeping the allocation.
    fn clear(&mut self);
    /// For each block `(r0, r1)`, append `a[i] ∨ b[j]` for `i` in `r0`, `j` in `r1`, `i` outer.
    fn extend_cross_join(&mut self, a: &Self, b: &Self, blocks: &[(Range<usize>, Range<usize>)]);

    // Scaffolding: the owned exits, to delete once nothing calls them.
    /// The time at `i`, owned.
    fn owned_get(&self, i: usize) -> T;
    /// Append an owned time.
    fn owned_push(&mut self, t: &T);
}

impl<T: Lattice + Clone> TimeColumn<T> for Vec<T> {
    fn len(&self) -> usize { Vec::len(self) }
    fn clear(&mut self) { Vec::clear(self) }
    fn extend_cross_join(&mut self, a: &Self, b: &Self, blocks: &[(Range<usize>, Range<usize>)]) {
        for (r0, r1) in blocks {
            for i in r0.clone() {
                self.extend(b[r1.clone()].iter().map(|t| a[i].join(t)));
            }
        }
    }
    fn owned_get(&self, i: usize) -> T { self[i].clone() }
    fn owned_push(&mut self, t: &T) { self.push(t.clone()) }
}

/// For each block `(r0, r1)`, append `a[i] · b[j]` for `i` in `r0`, `j` in `r1`, `i` outer.
pub fn extend_cross_product<R0, R1, RO>(out: &mut Vec<RO>, a: &[R0], b: &[R1], blocks: &[(Range<usize>, Range<usize>)])
where
    R0: Multiply<R1, Output = RO> + Clone,
{
    for (r0, r1) in blocks {
        for i in r0.clone() {
            out.extend(b[r1.clone()].iter().map(|d| a[i].clone().multiply(d)));
        }
    }
}

pub use join::{JoinInstance, ProxyJoinBackend, ProxyJoinTactic};
pub use reduce::{ProxyReduceBackend, ProxyReduceTactic, ReduceInstance, ReduceWindow};
pub use frontier::{read_as_of, BackendFrontier};

/// A position between windows of an ordered key space.
/// No minimum key or successor operation is required.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum KeyPosition<K> {
    /// No keys have been visited.
    Start,
    /// The inclusive lower bound on keys still to visit.
    At(K),
    /// All keys have been visited.
    End,
}
