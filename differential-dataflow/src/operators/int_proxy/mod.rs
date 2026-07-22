//! Backend-agnostic operator tactics using integer proxies.
//!
//! The tactics are intended to support custom operator implementations without rebuilding
//! the non-trivial and often non-obvious time-based logic that supports them.
//!
//! The tactics here run DD's operator logic over consolidated `[((group, token), time, diff)]`
//! lists: the first coordinate names the granule of independence, the second is an ephemeral
//! value token understood by the backend but opaque to the operator harness. Both are
//! backend-chosen `Copy + Ord` types — commonly `u64` hashes, but exact values for small
//! data (`G = u32` node ids, tokens `(u32, u32)` edges), wider hashes for insurance, or
//! dense ids where exactness is required.
//! The tactics first elicit proxy identifiers from the backends, perform their necessary time
//! and difference based computations to stage integer collections, and then re-invoke the
//! backends with those same identifiers to produce the necessary output.
//!
//! The backend is oblivious to the navigation of time, and the operator to the backend's
//! implementation.
//!
//! # The two tokens
//!
//! The two tokens play the role of key and value to the operator, but their connection
//! to the key and value of the intended computation is nuanced.
//!
//! *   `Group: Copy + Ord + 'static` — the group token, naming the granule of independence.
//!     This value should be identical for each instance of identical keys. It is the one
//!     token that crosses invocations (pending work is remembered per group), hence
//!     `'static`. When it is a content hash of the intended key, it need not be a
//!     well-distributed quality hash, only distinct; there may be collisions, and the
//!     next token should assist with this. When it is the key itself (small `Copy` keys),
//!     collisions are impossible and the cautions below are vacuous.
//!
//! *   `Token: Copy + Ord` — an ephemeral intra-group value token, scoped to one
//!     invocation. These should be distinct for each distinct datum with the same group
//!     token. When derived by hashing, they should account for the conventional
//!     `(key, val)` data, rather than only the value component, to avoid errors due to
//!     colliding groups.
//!
//! The group token acts as an independence marker: keys with different tokens will be isolated,
//! and their incremental updates performed independently. When the keyspace is compressed
//! (hashed) into the token, there is the risk of collision. This means that per-group logic
//! should be prepared for this, and should not discard key information that remains
//! semantically important.
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

/// Token-only exchange medium: a consolidated collection of `[((group, token), time, diff)]`.
///
/// `G` is the group token (commonly `u64`, a key hash) and `I` the value token (commonly
/// `u64`, an id or content hash) — see the backend traits for the obligations each carries.
///
/// The [`debug_assert_sorted_bridge`] method is (and can be) used to validate this property.
pub type ProxyBridge<G, I, T, R> = Vec<((G, I), T, R)>;

/// Debug check that a presented [`ProxyBridge`] is consolidated.
///
/// Operator harnesses use the test to flag backend implementations that do not uphold it.
pub(crate) fn debug_assert_sorted_bridge<G: Ord, I: Ord, T: Ord, R>(bridge: &ProxyBridge<G, I, T, R>, who: &str) {
    debug_assert!(
        bridge.windows(2).all(|w| ((&w[0].0, &w[0].1)) < ((&w[1].0, &w[1].1))),
        "{}: a presented bridge must be sorted & consolidated by ((group, token), time)",
        who,
    );
}

pub use join::{JoinInstance, JoinWindow, ProxyJoinBackend, ProxyJoinTactic};
pub use reduce::{ProxyReduceBackend, ProxyReduceTactic, ReduceInstance, ReduceWindow};
