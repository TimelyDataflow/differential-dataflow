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

mod history;

pub mod join;
pub mod reduce;
mod pending;
pub mod vec_backend;

/// A consolidated collection of `[((key, val), time, diff)]` proxies.
///
/// The [`debug_assert_sorted_bridge`] method is (and can be) used to validate this property.
pub type ProxyBridge<T, R, K = u64, V = u64> = Vec<((K, V), T, R)>;

/// Debug check that a presented [`ProxyBridge`] is consolidated.
///
/// Operator harnesses use the test to flag backend implementations that do not uphold it.
pub(crate) fn debug_assert_sorted_bridge<T: Ord, R, K: Copy + Ord, V: Copy + Ord>(bridge: &ProxyBridge<T, R, K, V>, who: &str) {
    debug_assert!(
        bridge.windows(2).all(|w| (w[0].0, &w[0].1) < (w[1].0, &w[1].1)),
        "{}: a presented bridge must be sorted & consolidated by ((key, val), time)",
        who,
    );
}

pub use join::{JoinInstance, ProxyJoinBackend, ProxyJoinTactic};
pub use reduce::{ProxyReduceBackend, ProxyReduceTactic, ReduceInstance, ReduceWindow};

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
