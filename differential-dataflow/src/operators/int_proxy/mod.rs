//! Backend-agnostic join and reduce over integer proxies.
//!
//! The tactics here run DD's operator logic over [`ProxyChunk`]s — sorted, consolidated
//! runs of `((key_hash, value_id), time, diff)` — while a backend keeps the real keys and
//! values in whatever (typically columnar) layout it likes. Only integers cross the
//! boundary, in both directions:
//!
//! * **Read side.** The backend *presents* a batch list as one proxy run
//!   ([`ProxyJoinBackend::present0`], [`ProxyReduceBackend::present_input`], ...). DD
//!   navigates the run — grouping by `key_hash`, consolidating diffs by `value_id`, and
//!   running all lattice/time logic — and refers back to backend records only by *index
//!   into the presented run*.
//! * **Write side.** Reduce produces `((key_hash, value_id), time, diff)` deltas. The
//!   `key_hash` is carried from the input key; output `value_id`s are *minted by hashing
//!   the produced value* ([`ProxyReduceBackend::reduce`]), and the backend's
//!   [`materialize`](ProxyReduceBackend::materialize) seals the proxy records into a real
//!   output batch — a real arrangement, fit to be a downstream operator's input, where
//!   the same value presents with the same id because the id *is* its content hash.
//!
//! The tactics implement the seams in [`join`](crate::operators::join) and
//! [`reduce`](crate::operators::reduce): [`ProxyJoinTactic`] is a [`JoinTactic`] for
//! [`join_with_tactic`](crate::operators::join::join_with_tactic), [`ProxyReduceTactic`] a
//! [`ReduceTactic`] for [`reduce_with_tactic`](crate::operators::reduce::reduce_with_tactic).
//! Neither driver requires `Navigable` of its input, so a cursor-less backend arrangement
//! (a [`Chunk`](crate::trace::chunk::Chunk) that skips the navigation capability) suffices.
//! The [`reference`](mod@reference) module supplies an in-memory backend over the
//! [`VecChunk`](crate::trace::chunk::vec::VecChunk) layout, so the framework is testable
//! without any columnar engine.
//!
//! [`JoinTactic`]: crate::operators::join::JoinTactic
//! [`ReduceTactic`]: crate::operators::reduce::ReduceTactic
//!
//! # The two integers
//!
//! * `key_hash: u64` — a content hash of the key, stable across the whole system with no
//!   registry: the same key hashes identically in every operator, including across the
//!   output→input boundary. The hash function is a property of the *backend* (all of its
//!   operators must share it); DD never hashes anything.
//! * `value_id: u64` — an intra-key value identifier, consistent within one operator
//!   computation: equal values ⇔ equal ids there (both directions — a value-id collision
//!   merges two values' diffs, a correctness bug of the same character as a key
//!   collision). No id outlives a computation: a join work unit's presentations, or one
//!   reduce retire — within which the output presentation and minted ids must agree on
//!   equal values (see [`ProxyReduceBackend`]); `materialize` resolves ids to real data
//!   before anything leaves the retire. Content hashing (`id = hash(value)`) is the
//!   stateless scheme that discharges all of this at once — the reference backend's
//!   choice, and it makes min "reusing an input value's id" automatic — but *exact*
//!   schemes are equally valid and collision-free: dense ordinals from grouping the
//!   presented values, plus a per-retire value→id map on the output side. Only a future
//!   design that persists ids into an output arrangement itself would force stable
//!   (hashed) value ids.
//!
//! # Design note: `value_id` is *not* order-preserving
//!
//! We considered assigning `value_id`s in value order within each key, which would let
//! the reduce tactic compute order-sensitive reductions (min/max) directly over the
//! integers. We decided against it; min/max fall to the value callback. Why:
//!
//! 1. **It would reimport the cost the framework exists to avoid.** Order-preserving ids
//!    require the backend to rank values by their *semantic* order per key per
//!    presentation. Columnar backends deliberately order by their own structural
//!    comparison (any consistent total order is fine for arrangement maintenance);
//!    imposing the semantic `Ord` is exactly the row-wise value-comparison cost being
//!    removed.
//! 2. **The write side scrambles order anyway.** Minted ids must be stable content
//!    hashes, and hashes are order-blind. An order-preserving read side would be a second
//!    id regime living alongside the hashed one; a single hash-based scheme serves both.
//! 3. **Division of labor stays clean.** The tactic's competence is time/lattice logic,
//!    for which id *equality* suffices. Order is value semantics, and all value semantics
//!    live behind the callback — which sees one key's few present values, the same work
//!    a row backend does for min today, without the value materialization elsewhere.
//!
//! The cost: min/max cannot be answered purely in proxy space. The callback does one pass
//! over the key's present values — acceptable, and recoverable later backend-side with a
//! columnar min primitive applied across many keys per wave.
//!
//! # Collision risk
//!
//! Key ids are 64-bit content hashes, and colliding *keys* merge two groups (a join
//! would cross-match them; a reduce would reduce their union). By the birthday bound the
//! probability of any collision among `n` distinct keys is ≈ `n²/2⁶⁵`: about `5·10⁻⁸` at
//! a million, `5·10⁻²` at a billion. This is the accepted risk of the design — it buys
//! key-id stability across the operator's lifetime with no registry and no coordination,
//! which the cross-retire `pending` set and the changed-key restriction rely on. If a
//! deployment's key cardinality makes the bound uncomfortable, the upgrade path is
//! widening the id to 128 bits (a second id column), not a registry.
//!
//! Value ids carry the same exposure only if the backend hashes them (as the reference
//! backend does). Because they are scoped to one computation, a backend can instead
//! assign them exactly (see above) and eliminate value-side collisions outright; the key
//! side is the irreducible exposure.
//!
//! # What DD owns vs what the backend owns
//!
//! DD (the tactics, written once): interesting-time discovery (joins of input times),
//! wave navigation, per-`(key, time)` desired-vs-current deltas, `pending` keyed by the
//! stable `key_hash` across retires, held-capability bucket routing, output batch
//! descriptions, and all sorting/merging/consolidation of proxy runs. The backend:
//! hashing, the value function, and building real columns from ids. The backend never
//! sees a timestamp except as an opaque payload.

pub mod join;
pub mod reduce;
pub mod reference;

pub use crate::trace::chunk::int_proxy::ProxyChunk;
pub use join::{ProxyJoinBackend, ProxyJoinTactic};
pub use reduce::{ProxyReduceBackend, ProxyReduceTactic};
