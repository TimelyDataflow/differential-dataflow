//! Trie-structured update storage.
//!
//! `UpdatesTyped<U>` is the core trie: four nested `Lists` (keys, vals, times, diffs).
//! `Consolidating` is a streaming consolidator over sorted `(k,v,t,d)` data.
//! `UpdatesBuilder` melds sorted, consolidated chunks into a single trie.
//!
//! NOTE: `UpdatesTyped::iter` / `form` / `form_unsorted` / `consolidate` / `filter_zero`
//! are escape hatches that flatten the trie. Prefer trie-native operations where
//! possible — flattening + rebuilding is a significant cost on hot paths.

use columnar::{Columnar, Container, ContainerOf, Vecs, Borrow, Index, IndexAs, Len, Push};
use columnar::primitive::offsets::Strides;
use crate::difference::{Semigroup, IsZero};

use super::layout::ColumnarUpdate as Update;

/// A `Vecs` using strided offsets.
pub type Lists<C> = Vecs<C, Strides>;

/// Returns the non-empty lists once values are filtered by `keep`, and the bitmap of lists to keep.
pub fn retain_items<'a, C: Container>(lists: <Lists<C> as Borrow>::Borrowed<'a>, keep: &[bool]) -> (Lists<C>, Vec<bool>) {

    // In principle we can copy runs described in `bools` for bulk copying.
    let mut output = <Lists::<C> as Container>::with_capacity_for([lists].into_iter());
    let mut bitmap = Vec::with_capacity(lists.len());
    assert_eq!(keep.len(), lists.values.len());
    for list_index in 0 .. lists.len() {
        let (lower, upper) = lists.bounds.bounds(list_index);
        for item_index in lower .. upper {
            if keep[item_index] {
                output.values.push(lists.values.get(item_index));
            }
        }
        if output.values.len() > columnar::Index::last(&output.bounds.borrow()).unwrap_or(0) as usize {
            output.bounds.push(output.values.len() as u64);
            bitmap.push(true);
        }
        else { bitmap.push(false); }
    }

    assert_eq!(bitmap.len(), lists.len());
    (output, bitmap)
}


/// Trie-structured update storage using columnar containers.
///
/// Four nested layers of `Lists`:
/// - `keys`: lists of keys (outer lists are independent groups)
/// - `vals`: per-key, lists of vals
/// - `times`: per-val, lists of times
/// - `diffs`: per-time, lists of diffs (singletons when consolidated)
///
/// A flat unsorted input has stride 1 at every level (one key per entry,
/// one val per key, one time per val, one diff per time).
/// A fully consolidated trie has a single outer key list, all lists sorted
/// and deduplicated, and singleton diff lists.
pub struct UpdatesTyped<U: Update> {
    /// Outer key list (one entry per group of keys at the trie root).
    pub keys:  Lists<ContainerOf<U::Key>>,
    /// Per-key list of vals.
    pub vals:  Lists<ContainerOf<U::Val>>,
    /// Per-val list of times.
    pub times: Lists<ContainerOf<U::Time>>,
    /// Per-time list of diffs (one diff per time after consolidation).
    pub diffs: Lists<ContainerOf<U::Diff>>,
}

impl<U: Update> Default for UpdatesTyped<U> {
    fn default() -> Self {
        Self {
            keys: Default::default(),
            vals: Default::default(),
            times: Default::default(),
            diffs: Default::default(),
        }
    }
}

impl<U: Update> std::fmt::Debug for UpdatesTyped<U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdatesTyped").finish()
    }
}

impl<U: Update> Clone for UpdatesTyped<U> {
    fn clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            vals: self.vals.clone(),
            times: self.times.clone(),
            diffs: self.diffs.clone(),
        }
    }
}

/// Borrowed view of an [`UpdatesTyped<U>`] with the same four-field shape.
///
/// Reader code should consume an `UpdatesTyped` through this view rather than reading
/// fields directly. This decouples readers from the storage representation: the
/// view's shape stays the same whether the underlying `UpdatesTyped` holds owned
/// `Lists` or (later) `Stash`-backed columns that may be borrowed from wire bytes.
pub struct UpdatesView<'a, U: Update> {
    /// Outer key list (one entry per group of keys at the trie root).
    pub keys:  <Lists<ContainerOf<U::Key>>  as Borrow>::Borrowed<'a>,
    /// Per-key list of vals.
    pub vals:  <Lists<ContainerOf<U::Val>>  as Borrow>::Borrowed<'a>,
    /// Per-val list of times.
    pub times: <Lists<ContainerOf<U::Time>> as Borrow>::Borrowed<'a>,
    /// Per-time list of diffs.
    pub diffs: <Lists<ContainerOf<U::Diff>> as Borrow>::Borrowed<'a>,
}

impl<'a, U: Update> Copy for UpdatesView<'a, U> {}
impl<'a, U: Update> Clone for UpdatesView<'a, U> { fn clone(&self) -> Self { *self } }

impl<'a, U: Update> UpdatesView<'a, U> {
    /// Iterate all `(key, val, time, diff)` entries as refs.
    pub fn iter(self) -> impl Iterator<Item = (
        columnar::Ref<'a, U::Key>,
        columnar::Ref<'a, U::Val>,
        columnar::Ref<'a, U::Time>,
        columnar::Ref<'a, U::Diff>,
    )> {
        let UpdatesView { keys, vals, times, diffs } = self;
        (0..Len::len(&keys))
            .flat_map(move |outer| child_range(keys.bounds, outer))
            .flat_map(move |k| {
                let key = keys.values.get(k);
                child_range(vals.bounds, k).map(move |v| (key, v))
            })
            .flat_map(move |(key, v)| {
                let val = vals.values.get(v);
                child_range(times.bounds, v).map(move |t| (key, val, t))
            })
            .flat_map(move |(key, val, t)| {
                let time = times.values.get(t);
                child_range(diffs.bounds, t).map(move |d| (key, val, time, diffs.values.get(d)))
            })
    }

    /// Translate a key-range into the corresponding val-range via `vals.bounds`.
    pub fn vals_bounds(self, key_range: std::ops::Range<usize>) -> std::ops::Range<usize> {
        if !key_range.is_empty() {
            let bounds = self.vals.bounds;
            let lower = if key_range.start == 0 { 0 } else { bounds.index_as(key_range.start - 1) as usize };
            let upper = bounds.index_as(key_range.end - 1) as usize;
            lower..upper
        } else { key_range }
    }
    /// Translate a val-range into the corresponding time-range via `times.bounds`.
    pub fn times_bounds(self, val_range: std::ops::Range<usize>) -> std::ops::Range<usize> {
        if !val_range.is_empty() {
            let bounds = self.times.bounds;
            let lower = if val_range.start == 0 { 0 } else { bounds.index_as(val_range.start - 1) as usize };
            let upper = bounds.index_as(val_range.end - 1) as usize;
            lower..upper
        } else { val_range }
    }
}

impl<U: Update> UpdatesTyped<U> {
    /// Borrow the four columns as a single `UpdatesView`.
    pub fn view(&self) -> UpdatesView<'_, U> {
        UpdatesView {
            keys:  self.keys.borrow(),
            vals:  self.vals.borrow(),
            times: self.times.borrow(),
            diffs: self.diffs.borrow(),
        }
    }
}

/// `Stash`-backed update storage: each column may be typed (writable) or
/// borrowed from wire bytes (read-only, zero-copy).
///
/// Construction sites work in [`UpdatesTyped`]; convert via `From` at the
/// boundary. Reader code uses [`UpdatesView`] via [`Updates::view`], which
/// produces the same shape regardless of whether the columns are typed or
/// borrowed.
pub struct Updates<U: Update, B = timely::bytes::arc::Bytes> {
    /// Outer key list (one entry per group of keys at the trie root).
    pub keys:  columnar::bytes::stash::Stash<Lists<ContainerOf<U::Key>>,  B>,
    /// Per-key list of vals.
    pub vals:  columnar::bytes::stash::Stash<Lists<ContainerOf<U::Val>>,  B>,
    /// Per-val list of times.
    pub times: columnar::bytes::stash::Stash<Lists<ContainerOf<U::Time>>, B>,
    /// Per-time list of diffs.
    pub diffs: columnar::bytes::stash::Stash<Lists<ContainerOf<U::Diff>>, B>,
}

impl<U: Update, B> Default for Updates<U, B> {
    fn default() -> Self {
        Self {
            keys:  Default::default(),
            vals:  Default::default(),
            times: Default::default(),
            diffs: Default::default(),
        }
    }
}

impl<U: Update, B: Clone> Clone for Updates<U, B> {
    fn clone(&self) -> Self {
        Self {
            keys:  self.keys.clone(),
            vals:  self.vals.clone(),
            times: self.times.clone(),
            diffs: self.diffs.clone(),
        }
    }
}

impl<U: Update, B> From<UpdatesTyped<U>> for Updates<U, B> {
    fn from(owned: UpdatesTyped<U>) -> Self {
        use columnar::bytes::stash::Stash;
        Self {
            keys:  Stash::Typed(owned.keys),
            vals:  Stash::Typed(owned.vals),
            times: Stash::Typed(owned.times),
            diffs: Stash::Typed(owned.diffs),
        }
    }
}

impl<U: Update, B: std::ops::Deref<Target = [u8]> + Clone + 'static> Updates<U, B> {
    /// Borrow the four columns as a single `UpdatesView`.
    pub fn view(&self) -> UpdatesView<'_, U> {
        UpdatesView {
            keys:  self.keys.borrow(),
            vals:  self.vals.borrow(),
            times: self.times.borrow(),
            diffs: self.diffs.borrow(),
        }
    }

    /// Total number of updates (records) in the trie.
    pub fn len(&self) -> usize {
        self.view().diffs.values.len()
    }

    /// Whether the trie is empty.
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    /// Convert to fully owned form, copying any `Stash::Bytes` columns into
    /// typed `Lists`. Already-typed columns pass through with no copy.
    ///
    /// This method should be avoided unless typed containers are truly needed.
    pub fn into_typed(mut self) -> UpdatesTyped<U> {
        use columnar::bytes::stash::Stash;
        self.keys.make_typed();
        self.vals.make_typed();
        self.times.make_typed();
        self.diffs.make_typed();
        let Stash::Typed(keys)  = self.keys  else { unreachable!() };
        let Stash::Typed(vals)  = self.vals  else { unreachable!() };
        let Stash::Typed(times) = self.times else { unreachable!() };
        let Stash::Typed(diffs) = self.diffs else { unreachable!() };
        UpdatesTyped { keys, vals, times, diffs }
    }
}

/// The flat `(key, val, time, diff)` tuple for an [`Update`].
pub type Tuple<U> = (<U as Update>::Key, <U as Update>::Val, <U as Update>::Time, <U as Update>::Diff);

/// Wrap a flat `Tuple<U>` columnar container as a stride-1 [`UpdatesTyped<U>`].
///
/// Each layer gets `Strides::new(1, n)` bounds, where `n` is the record count.
/// The result is unsorted and unconsolidated; pass it through `consolidate()` to
/// canonicalize. Allocation-free: the four column containers move in place.
impl<U: Update> From<(ContainerOf<U::Key>, ContainerOf<U::Val>, ContainerOf<U::Time>, ContainerOf<U::Diff>)> for UpdatesTyped<U> {
    fn from(container: (ContainerOf<U::Key>, ContainerOf<U::Val>, ContainerOf<U::Time>, ContainerOf<U::Diff>)) -> Self {
        let (k_col, v_col, t_col, d_col) = container;
        let n = k_col.len() as u64;
        Self {
            keys:  Lists { values: k_col, bounds: Strides::new(1, n) },
            vals:  Lists { values: v_col, bounds: Strides::new(1, n) },
            times: Lists { values: t_col, bounds: Strides::new(1, n) },
            diffs: Lists { values: d_col, bounds: Strides::new(1, n) },
        }
    }
}

/// Returns the value-index range for list `i` given cumulative bounds.
#[inline]
pub fn child_range<B: IndexAs<u64>>(bounds: B, i: usize) -> std::ops::Range<usize> {
    let lower = if i == 0 { 0 } else { bounds.index_as(i - 1) as usize };
    let upper = bounds.index_as(i) as usize;
    lower..upper
}

/// A streaming consolidation iterator for sorted `(key, val, time, diff)` data.
///
/// Accumulates diffs for equal `(key, val, time)` triples, yielding at most
/// one output per distinct triple, with a non-zero accumulated diff.
/// Input must be sorted by `(key, val, time)`.
pub struct Consolidating<I: Iterator, D> {
    iter: std::iter::Peekable<I>,
    diff: D,
}

impl<K, V, T, D, I> Consolidating<I, D>
where
    K: Copy + Eq,
    V: Copy + Eq,
    T: Copy + Eq,
    D: Semigroup + IsZero + Default,
    I: Iterator<Item = (K, V, T, D)>,
{
    /// Wrap a sorted `(K, V, T, D)` iterator so adjacent equal `(K, V, T)`
    /// runs accumulate into a single output with the summed diff.
    pub fn new(iter: I) -> Self {
        Self { iter: iter.peekable(), diff: D::default() }
    }
}

impl<K, V, T, D, I> Iterator for Consolidating<I, D>
where
    K: Copy + Eq,
    V: Copy + Eq,
    T: Copy + Eq,
    D: Semigroup + IsZero + Default + Clone,
    I: Iterator<Item = (K, V, T, D)>,
{
    type Item = (K, V, T, D);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (k, v, t, d) = self.iter.next()?;
            self.diff = d;
            while let Some(&(k2, v2, t2, _)) = self.iter.peek() {
                if k2 == k && v2 == v && t2 == t {
                    let (_, _, _, d2) = self.iter.next().unwrap();
                    self.diff.plus_equals(&d2);
                } else {
                    break;
                }
            }
            if !self.diff.is_zero() {
                return Some((k, v, t, self.diff.clone()));
            }
        }
    }
}

impl<U: Update> UpdatesTyped<U> {

    /// Copies `other[key_range]` into self, keys and all.
    pub fn extend_from_keys(&mut self, other: UpdatesView<'_, U>, key_range: std::ops::Range<usize>) {
        self.keys.values.extend_from_self(other.keys.values, key_range.clone());
        self.vals.extend_from_self(other.vals, key_range.clone());
        let val_range = other.vals_bounds(key_range);
        self.times.extend_from_self(other.times, val_range.clone());
        let time_range = other.times_bounds(val_range);
        self.diffs.extend_from_self(other.diffs, time_range);
    }

    /// Forms a consolidated `UpdatesTyped` trie from unsorted `(key, val, time, diff)` refs.
    pub fn form_unsorted<'a>(unsorted: impl Iterator<Item = columnar::Ref<'a, Tuple<U>>>) -> Self {
        let mut data = unsorted.collect::<Vec<_>>();
        data.sort_by(|(ak, av, at, _), (bk, bv, bt, _)| (ak, av, at).cmp(&(bk, bv, bt)));
        Self::form(data.into_iter())
    }

    /// Forms a consolidated `UpdatesTyped` trie from sorted `(key, val, time, diff)` refs.
    pub fn form<'a>(sorted: impl Iterator<Item = columnar::Ref<'a, Tuple<U>>>) -> Self {

        // Step 1: Streaming consolidation — accumulate diffs, drop zeros.
        let consolidated = Consolidating::new(
            sorted.map(|(k, v, t, d)| (k, v, t, <U::Diff as Columnar>::into_owned(d)))
        );

        // Step 2: Build the trie from consolidated, sorted, non-zero data.
        let mut output = Self::default();
        let mut updates = consolidated;
        if let Some((key, val, time, diff)) = updates.next() {
            let mut prev = (key, val, time);
            output.keys.values.push(key);
            output.vals.values.push(val);
            output.times.values.push(time);
            output.diffs.values.push(&diff);
            output.diffs.bounds.push(output.diffs.values.len() as u64);

            // As we proceed, seal up known complete runs.
            for (key, val, time, diff) in updates {

                // If keys differ, record key and seal vals and times.
                if key != prev.0 {
                    output.vals.bounds.push(output.vals.values.len() as u64);
                    output.times.bounds.push(output.times.values.len() as u64);
                    output.keys.values.push(key);
                    output.vals.values.push(val);
                }
                // If vals differ, record val and seal times.
                else if val != prev.1 {
                    output.times.bounds.push(output.times.values.len() as u64);
                    output.vals.values.push(val);
                }
                else {
                    // We better not find a duplicate time.
                    assert!(time != prev.2);
                }

                // Always record (time, diff).
                output.times.values.push(time);
                output.diffs.values.push(&diff);
                output.diffs.bounds.push(output.diffs.values.len() as u64);

                prev = (key, val, time);
            }

            // Seal up open lists.
            output.keys.bounds.push(output.keys.values.len() as u64);
            output.vals.bounds.push(output.vals.values.len() as u64);
            output.times.bounds.push(output.times.values.len() as u64);
        }

        output
    }

    /// Consolidates into canonical trie form:
    /// single outer key list, all lists sorted and deduplicated,
    /// diff lists are singletons (or absent if cancelled).
    pub fn consolidate(&self) -> Self {
        // Fast path: stride-1 at every layer (flat tuple list, e.g. produced by
        // `From<TupleContainer>`). Skip the four-level trie iter and walk the
        // underlying value columns directly.
        if self.keys.bounds.strided()  == Some(1)
            && self.vals.bounds.strided()  == Some(1)
            && self.times.bounds.strided() == Some(1)
            && self.diffs.bounds.strided() == Some(1)
        {
            let n = Len::len(&self.keys.values);
            // Tuple up borrows of the four columns — `Index::get(i)` on the
            // tuple dispatches to each component, yielding a `Ref<Tuple<U>>`
            // in a single call.
            let view = (
                self.keys.values.borrow(),
                self.vals.values.borrow(),
                self.times.values.borrow(),
                self.diffs.values.borrow(),
            );
            return Self::form_unsorted((0..n).map(|i| view.get(i)));
        }
        Self::form_unsorted(self.iter())
    }
    /// Drop entries whose diff list is empty (cancelled), rebuilding the trie.
    pub fn filter_zero(self) -> Self {
        if self.diffs.bounds.strided() == Some(1) { self }
        // TODO: rework to move from trie structure to trie structure.
        else {
            let mut keep = Vec::with_capacity(self.times.values.len());
            for index in 0 .. self.times.values.len() {
                keep.push({
                    let (lower, upper) = self.diffs.bounds.bounds(index);
                    lower < upper
                });
            }
            let (times, keep) = retain_items(self.times.borrow(), &keep[..]);
            let (vals, keep) = retain_items(self.vals.borrow(), &keep[..]);
            let (keys, _keep) = retain_items(self.keys.borrow(), &keep[..]);
            UpdatesTyped {
                keys,
                vals,
                times,
                diffs: Lists {
                    bounds: Strides::new(1, self.diffs.values.len() as u64),
                    values: self.diffs.values,
                },
            }
        }
        // else { Self::form(self.iter()) }
    }

    /// The number of leaf-level diff entries (total updates).
    pub fn len(&self) -> usize { self.diffs.values.len() }
}

/// Push a single flat update as a stride-1 entry.
///
/// Each field is independently typed — columnar refs, `&Owned`, owned values,
/// or any other type the column container accepts via its `Push` impl.
impl<KP, VP, TP, DP, U: Update> Push<(KP, VP, TP, DP)> for UpdatesTyped<U>
where
    ContainerOf<U::Key>: Push<KP>,
    ContainerOf<U::Val>: Push<VP>,
    ContainerOf<U::Time>: Push<TP>,
    ContainerOf<U::Diff>: Push<DP>,
{
    fn push(&mut self, (key, val, time, diff): (KP, VP, TP, DP)) {
        self.keys.values.push(key);
        self.keys.bounds.push(self.keys.values.len() as u64);
        self.vals.values.push(val);
        self.vals.bounds.push(self.vals.values.len() as u64);
        self.times.values.push(time);
        self.times.bounds.push(self.times.values.len() as u64);
        self.diffs.values.push(diff);
        self.diffs.bounds.push(self.diffs.values.len() as u64);
    }
}

/// PushInto for the `((K, V), T, R)` shape that reduce_trace uses.
impl<U: Update> timely::container::PushInto<((U::Key, U::Val), U::Time, U::Diff)> for UpdatesTyped<U> {
    fn push_into(&mut self, ((key, val), time, diff): ((U::Key, U::Val), U::Time, U::Diff)) {
        self.push((&key, &val, &time, &diff));
    }
}

impl<U: Update> UpdatesTyped<U> {

    /// Iterate all `(key, val, time, diff)` entries as refs.
    pub fn iter(&self) -> impl Iterator<Item = (
        columnar::Ref<'_, U::Key>,
        columnar::Ref<'_, U::Val>,
        columnar::Ref<'_, U::Time>,
        columnar::Ref<'_, U::Diff>,
    )> {
        self.view().iter()
    }
}

impl<U: Update> timely::Accountable for UpdatesTyped<U> {
    #[inline] fn record_count(&self) -> i64 { Len::len(&self.diffs.values) as i64 }
}

impl<U: Update> timely::dataflow::channels::ContainerBytes for UpdatesTyped<U> {
    fn from_bytes(_bytes: timely::bytes::arc::Bytes) -> Self { unimplemented!() }
    fn length_in_bytes(&self) -> usize { unimplemented!() }
    fn into_bytes<W: std::io::Write>(&self, _writer: &mut W) { unimplemented!() }
}

/// An incremental trie builder that accepts sorted, consolidated `UpdatesTyped` chunks
/// and melds them into a single `UpdatesTyped` trie.
///
/// The internal `UpdatesTyped` has open (unsealed) bounds at the keys, vals, and times
/// levels — the last group at each level has its values pushed but no corresponding
/// bounds entry. `diffs.bounds` is always 1:1 with `times.values`.
///
/// `meld` accepts a consolidated `UpdatesTyped` whose first `(key, val, time)` is
/// strictly greater than the builder's last `(key, val, time)`. The key and val
/// may equal the builder's current open key/val, as long as the time is greater.
///
/// `done` seals all open bounds and returns the completed `UpdatesTyped`.
pub struct UpdatesBuilder<U: Update> {
    /// Non-empty, consolidated updates.
    updates: UpdatesTyped<U>,
}

impl<U: Update> UpdatesBuilder<U> {
    /// Construct a new builder from consolidated, sealed updates.
    ///
    /// Unseals the last group at keys, vals, and times levels so that
    /// subsequent `meld` calls can extend the open groups.
    /// If the updates are not consolidated none of this works.
    pub fn new_from(mut updates: UpdatesTyped<U>) -> Self {
        use columnar::Len;
        if Len::len(&updates.keys.values) > 0 {
            updates.keys.bounds.pop();
            updates.vals.bounds.pop();
            updates.times.bounds.pop();
        }
        Self { updates }
    }

    /// Meld a sorted, consolidated `UpdatesTyped` chunk into this builder.
    ///
    /// The chunk's first `(key, val, time)` must be strictly greater than
    /// the builder's last `(key, val, time)`. Keys and vals may overlap
    /// (continue the current group), but times must be strictly increasing
    /// within the same `(key, val)`.
    pub fn meld(&mut self, chunk: &UpdatesTyped<U>) {
        use columnar::{Borrow, Index, Len};

        if chunk.len() == 0 { return; }

        // Empty builder: clone the chunk and unseal it.
        if Len::len(&self.updates.keys.values) == 0 {
            self.updates = chunk.clone();
            self.updates.keys.bounds.pop();
            self.updates.vals.bounds.pop();
            self.updates.times.bounds.pop();
            return;
        }

        // Pre-compute boundary comparisons before mutating.
        let keys_match = {
            let skb = self.updates.keys.values.borrow();
            let ckb = chunk.keys.values.borrow();
            skb.get(Len::len(&skb) - 1) == ckb.get(0)
        };
        let vals_match = keys_match && {
            let svb = self.updates.vals.values.borrow();
            let cvb = chunk.vals.values.borrow();
            svb.get(Len::len(&svb) - 1) == cvb.get(0)
        };

        let chunk_num_keys = Len::len(&chunk.keys.values);
        let chunk_num_vals = Len::len(&chunk.vals.values);
        let chunk_num_times = Len::len(&chunk.times.values);

        // Child ranges for the first element at each level of the chunk.
        let first_key_vals = child_range(chunk.vals.borrow().bounds, 0);
        let first_val_times = child_range(chunk.times.borrow().bounds, 0);

        // There is a first position where coordinates disagree.
        // Strictly beyond that position: seal bounds, extend lists, re-open the last bound.
        // At that position: meld the first list, extend subsequent lists, re-open.
        let mut differ = false;

        // --- Keys ---
        if keys_match {
            // Skip the duplicate first key; add remaining keys.
            if chunk_num_keys > 1 {
                self.updates.keys.values.extend_from_self(chunk.keys.values.borrow(), 1..chunk_num_keys);
            }
        } else {
            // All keys are new.
            self.updates.keys.values.extend_from_self(chunk.keys.values.borrow(), 0..chunk_num_keys);
            differ = true;
        }

        // --- Vals ---
        if differ {
            // Keys differed: seal open val group, extend all val lists, unseal last.
            self.updates.vals.bounds.push(Len::len(&self.updates.vals.values) as u64);
            self.updates.vals.extend_from_self(chunk.vals.borrow(), 0..chunk_num_keys);
            self.updates.vals.bounds.pop();
        } else {
            // Keys matched: meld vals for the shared key.
            if vals_match {
                // Skip the duplicate first val; add remaining vals from the first key's list.
                if first_key_vals.len() > 1 {
                    self.updates.vals.values.extend_from_self(
                        chunk.vals.values.borrow(),
                        (first_key_vals.start + 1)..first_key_vals.end,
                    );
                }
            } else {
                // First val differs: add all vals from the first key's list.
                self.updates.vals.values.extend_from_self(
                    chunk.vals.values.borrow(),
                    first_key_vals.clone(),
                );
                differ = true;
            }
            // Seal the matched key's val group, extend remaining keys' val lists, unseal.
            if chunk_num_keys > 1 {
                self.updates.vals.bounds.push(Len::len(&self.updates.vals.values) as u64);
                self.updates.vals.extend_from_self(chunk.vals.borrow(), 1..chunk_num_keys);
                self.updates.vals.bounds.pop();
            }
        }

        // --- Times ---
        if differ {
            // Seal open time group, extend all time lists, unseal last.
            self.updates.times.bounds.push(Len::len(&self.updates.times.values) as u64);
            self.updates.times.extend_from_self(chunk.times.borrow(), 0..chunk_num_vals);
            self.updates.times.bounds.pop();
        } else {
            // Keys and vals matched. Times must be strictly greater (precondition),
            // so we always set differ = true here.
            debug_assert!({
                let stb = self.updates.times.values.borrow();
                let ctb = chunk.times.values.borrow();
                stb.get(Len::len(&stb) - 1) != ctb.get(0)
            }, "meld: duplicate time within same (key, val)");
            // Add times from the first val's time list into the open group.
            self.updates.times.values.extend_from_self(
                chunk.times.values.borrow(),
                first_val_times.clone(),
            );
            differ = true;
            // Seal the matched val's time group, extend remaining vals' time lists, unseal.
            if chunk_num_vals > 1 {
                self.updates.times.bounds.push(Len::len(&self.updates.times.values) as u64);
                self.updates.times.extend_from_self(chunk.times.borrow(), 1..chunk_num_vals);
                self.updates.times.bounds.pop();
            }
        }

        // --- Diffs ---
        // Diffs are always sealed (1:1 with times). By the precondition that
        // times are strictly increasing for the same (key, val), differ is
        // always true by this point — just extend all diff lists.
        debug_assert!(differ);
        self.updates.diffs.extend_from_self(chunk.diffs.borrow(), 0..chunk_num_times);
    }

    /// Seal all open bounds and return the completed `UpdatesTyped`.
    pub fn done(mut self) -> UpdatesTyped<U> {
        use columnar::Len;
        if Len::len(&self.updates.keys.values) > 0 {
            // Seal the open time group.
            self.updates.times.bounds.push(Len::len(&self.updates.times.values) as u64);
            // Seal the open val group.
            self.updates.vals.bounds.push(Len::len(&self.updates.vals.values) as u64);
            // Seal the outer key group.
            self.updates.keys.bounds.push(Len::len(&self.updates.keys.values) as u64);
        }
        self.updates
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use columnar::Push;

    type TestUpdate = (u64, u64, u64, i64);

    fn collect(updates: &UpdatesTyped<TestUpdate>) -> Vec<(u64, u64, u64, i64)> {
        updates.iter().map(|(k, v, t, d)| (*k, *v, *t, *d)).collect()
    }

    #[test]
    fn test_push_and_consolidate_basic() {
        let mut updates = UpdatesTyped::<TestUpdate>::default();
        updates.push((&1, &10, &100, &1));
        updates.push((&1, &10, &100, &2));
        updates.push((&2, &20, &200, &5));
        assert_eq!(updates.len(), 3);
        assert_eq!(collect(&updates.consolidate()), vec![(1, 10, 100, 3), (2, 20, 200, 5)]);
    }

    #[test]
    fn test_cancellation() {
        let mut updates = UpdatesTyped::<TestUpdate>::default();
        updates.push((&1, &10, &100, &3));
        updates.push((&1, &10, &100, &-3));
        updates.push((&2, &20, &200, &1));
        assert_eq!(collect(&updates.consolidate()), vec![(2, 20, 200, 1)]);
    }

    #[test]
    fn test_multiple_vals_and_times() {
        let mut updates = UpdatesTyped::<TestUpdate>::default();
        updates.push((&1, &10, &100, &1));
        updates.push((&1, &10, &200, &2));
        updates.push((&1, &20, &100, &3));
        updates.push((&1, &20, &100, &4));
        assert_eq!(collect(&updates.consolidate()), vec![(1, 10, 100, 1), (1, 10, 200, 2), (1, 20, 100, 7)]);
    }

    #[test]
    fn test_val_cancellation_propagates() {
        let mut updates = UpdatesTyped::<TestUpdate>::default();
        updates.push((&1, &10, &100, &5));
        updates.push((&1, &10, &100, &-5));
        updates.push((&1, &20, &100, &1));
        assert_eq!(collect(&updates.consolidate()), vec![(1, 20, 100, 1)]);
    }

    #[test]
    fn test_empty() {
        let updates = UpdatesTyped::<TestUpdate>::default();
        assert_eq!(collect(&updates.consolidate()), vec![]);
    }

    #[test]
    fn test_total_cancellation() {
        let mut updates = UpdatesTyped::<TestUpdate>::default();
        updates.push((&1, &10, &100, &1));
        updates.push((&1, &10, &100, &-1));
        assert_eq!(collect(&updates.consolidate()), vec![]);
    }

    #[test]
    fn test_unsorted_input() {
        let mut updates = UpdatesTyped::<TestUpdate>::default();
        updates.push((&3, &30, &300, &1));
        updates.push((&1, &10, &100, &2));
        updates.push((&2, &20, &200, &3));
        assert_eq!(collect(&updates.consolidate()), vec![(1, 10, 100, 2), (2, 20, 200, 3), (3, 30, 300, 1)]);
    }

    #[test]
    fn test_first_key_cancels() {
        let mut updates = UpdatesTyped::<TestUpdate>::default();
        updates.push((&1, &10, &100, &5));
        updates.push((&1, &10, &100, &-5));
        updates.push((&2, &20, &200, &3));
        assert_eq!(collect(&updates.consolidate()), vec![(2, 20, 200, 3)]);
    }

    #[test]
    fn test_middle_time_cancels() {
        let mut updates = UpdatesTyped::<TestUpdate>::default();
        updates.push((&1, &10, &100, &1));
        updates.push((&1, &10, &200, &2));
        updates.push((&1, &10, &200, &-2));
        updates.push((&1, &10, &300, &3));
        assert_eq!(collect(&updates.consolidate()), vec![(1, 10, 100, 1), (1, 10, 300, 3)]);
    }

    #[test]
    fn test_first_val_cancels() {
        let mut updates = UpdatesTyped::<TestUpdate>::default();
        updates.push((&1, &10, &100, &1));
        updates.push((&1, &10, &100, &-1));
        updates.push((&1, &20, &100, &5));
        assert_eq!(collect(&updates.consolidate()), vec![(1, 20, 100, 5)]);
    }

    #[test]
    fn test_interleaved_cancellations() {
        let mut updates = UpdatesTyped::<TestUpdate>::default();
        updates.push((&1, &10, &100, &1));
        updates.push((&1, &10, &100, &-1));
        updates.push((&2, &20, &200, &7));
        updates.push((&3, &30, &300, &4));
        updates.push((&3, &30, &300, &-4));
        assert_eq!(collect(&updates.consolidate()), vec![(2, 20, 200, 7)]);
    }
}
