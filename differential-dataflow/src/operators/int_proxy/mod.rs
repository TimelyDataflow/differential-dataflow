//! Backend-agnostic operator tactics using integer proxies.
//!
//! The tactics here run DD's operator logic over consolidated `[((u64, u64), time, diff)]`
//! lists, the first integer a hash of the "key" and granule of independence, the second an
//! ephemeral data identifier understood by the backend but opaque to the harness.
//!
//! The tactics first elicit proxy identifiers from the backends, perform their necessary time
//! and difference based computations to stage integer collections, and then re-invoke the
//! backends with those same identifiers to produce the necessary output (still opaque to the operator).
//!
//! A worked in-memory backend over the [`VecChunk`](crate::trace::chunk::vec::VecChunk)
//! layout lives in the integration tests (`tests/support/vec_backend.rs`).
//!
//! # The two integers
//!
//! The two integers play the role of key and value, but their connection to the key and value
//! of the intended computation is slightly different.
//!
//! *   `key_hash: u64` — a content hash of the key.
//!     This value should be the same for each instance of the same key.
//!     The operators will partition and order work by these values.
//!     There may be collisions, and the next identifier should assist with this.
//!
//! *   `value_id: u64` — an ephemeral intra-hash value identifier.
//!     These values should be distinct for each distinct datum with the same hash.
//!     In particular, they should account for the conventional `(key, val)` data,
//!     rather than only the value component, to avoid errors due to colliding keys.
//!
//! The key hash acts as an independence marker: keys with different hashes will be isolated,
//! and their incremental updates performed independently. By forcing the keyspace into `u64`,
//! there is the risk of hash collision. This means that per-hash logic should be prepared for
//! this, and should not discard key information that remains semantically important.
//!
//! As examples, join logic can follow its hash-wise cross product with key-based filtering,
//! and reduce logic can group by keys and apply logic to slices within the larger value list.
//! Both are welcome to efficiently notice that there have been no collisions and optimize,
//! or to ignore the risk entirely and live dangerously.
//!
//! # What DD owns vs what the backend owns
//!
//! DD (the tactics, written once): interesting-time discovery (joins of input times),
//! wave navigation, per-`(key, time)` desired-vs-current deltas, `pending` keyed by the
//! stable `key_hash` across retires, held-capability bucket routing, output batch
//! descriptions, and all sorting/merging/consolidation of proxy runs. The backend:
//! hashing, the value function, and building real columns from ids. The backend never
//! sees a timestamp except as an opaque payload.

mod history;

pub mod join;
pub mod reduce;

/// Integer-only exchange medium: a run of `((hash, id), time, diff)` that a `present*` method
/// returns, **sorted and consolidated** by `((key_hash, value_id), time)`. That ordering is not
/// optional — the tactics merge-walk the run, so an unsorted or unconsolidated presentation
/// silently drops records (see [`debug_assert_sorted_bridge`]).
pub type ProxyBridge<T, R> = Vec<((u64, u64), T, R)>;

/// Debug check that a presented [`ProxyBridge`] is sorted and consolidated by
/// `((key_hash, value_id), time)`. **Mandatory**, unlike the optional advance-by-`lower`: the
/// tactics merge-walk the run, so a violation silently drops records. Mirrors the `seed_times`
/// sortedness assert; compiles to a no-op in release.
pub(crate) fn debug_assert_sorted_bridge<T: Ord, R>(bridge: &ProxyBridge<T, R>, who: &str) {
    debug_assert!(
        bridge.windows(2).all(|w| (w[0].0, &w[0].1) < (w[1].0, &w[1].1)),
        "{}: a presented bridge must be sorted & consolidated by ((key_hash, value_id), time)",
        who,
    );
}

pub use join::{JoinInstance, ProxyJoinBackend, ProxyJoinTactic};
pub use reduce::{ProxyReduceBackend, ProxyReduceTactic, ReduceInstance, ReduceWindow};
