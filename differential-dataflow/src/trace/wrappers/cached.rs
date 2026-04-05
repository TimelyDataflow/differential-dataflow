//! A spine wrapper that caches keys on demand.
//!
//! `CachedSpine` wraps a `Spine` and maintains a hot cache of batches
//! for keys that have been explicitly requested. Cache misses are
//! loaded from the spine using the provided builder and push closure.
//!
//! Since it wraps `Spine` directly, `Storage = Vec<B>` and
//! `Cursor = CursorList<B::Cursor>` are known concretely, avoiding
//! opaque associated type issues.

use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

use timely::progress::{Antichain, frontier::AntichainRef};

use crate::trace::{Batch, Builder, Description, TraceReader};
use crate::trace::cursor::{Cursor, CursorList};
use crate::trace::implementations::WithLayout;
use crate::trace::implementations::containers::BatchContainer;
use crate::trace::implementations::spine_fueled::Spine;
use crate::trace::implementations::Layout;

/// Key container type for a batch's layout.
type KeyCon<B> = <<B as WithLayout>::Layout as Layout>::KeyContainer;

/// A spine with a hot cache for frequently-accessed keys.
///
/// - `B`: batch type
/// - `Bu`: builder type for constructing cache batches
/// - `P`: closure that loads a key's data from a cursor into `Bu::Input`
pub struct CachedSpine<B, Bu, P>
where
    B: Batch + Clone + 'static,
{
    /// The backing spine, containing all data.
    spine: Spine<B>,
    /// Cached batches for loaded keys.
    hot: Vec<B>,
    /// The set of keys currently in the cache, sorted.
    cached_keys: KeyCon<B>,
    /// The frontier through which cached data is complete.
    /// Cached keys have data for `[0, hot_physical)`.
    hot_physical: Antichain<<B as crate::trace::implementations::LayoutExt>::Time>,
    /// Closure to push cursor data into a builder's input container.
    /// Behind Rc<RefCell> so clones share it.
    push: Rc<RefCell<P>>,
    _marker: PhantomData<Bu>,
}

impl<B, Bu, P> CachedSpine<B, Bu, P>
where
    B: Batch + Clone + 'static,
{
    /// Wraps a spine with caching support.
    pub fn new(spine: Spine<B>, push: P) -> Self {
        CachedSpine {
            spine,
            hot: Vec::new(),
            cached_keys: KeyCon::<B>::with_capacity(0),
            hot_physical: Antichain::from_elem(<<B as crate::trace::implementations::LayoutExt>::Time as timely::progress::Timestamp>::minimum()),
            push: Rc::new(RefCell::new(push)),
            _marker: PhantomData,
        }
    }

    /// Returns true if the given key is in the cache.
    pub fn contains_key(&self, key: <KeyCon<B> as BatchContainer>::ReadItem<'_>) -> bool {
        (0..self.cached_keys.len()).any(|i| self.cached_keys.index(i) == <KeyCon<B> as BatchContainer>::reborrow(key))
    }

    /// Installs a batch of cached key data.
    pub fn insert_hot(&mut self, batch: B) {
        if !batch.is_empty() {
            self.hot.push(batch);
        }
    }

    /// Clears all cached data and the key set.
    pub fn clear_hot(&mut self) {
        self.hot.clear();
        self.cached_keys.clear();
        self.hot_physical = Antichain::from_elem(<<B as crate::trace::implementations::LayoutExt>::Time as timely::progress::Timestamp>::minimum());
    }

    /// The number of cached keys.
    pub fn cached_key_count(&self) -> usize {
        self.cached_keys.len()
    }

    /// The number of hot batches.
    pub fn hot_batch_count(&self) -> usize {
        self.hot.len()
    }

    /// Returns a reference to the inner spine.
    pub fn spine(&self) -> &Spine<B> { &self.spine }

    /// Returns a mutable reference to the inner spine.
    pub fn spine_mut(&mut self) -> &mut Spine<B> { &mut self.spine }
}

// CachedSpine is not Clone — it wraps a mutable Spine.
// Use TraceAgent<CachedSpine<...>> for shared access.

impl<B, Bu, P> WithLayout for CachedSpine<B, Bu, P>
where
    B: Batch + Clone + 'static,
{
    type Layout = B::Layout;
}

impl<B, Bu, P> TraceReader for CachedSpine<B, Bu, P>
where
    B: Batch + Clone + 'static,
    Bu: Builder<Time = Self::Time, Output = B, Input: Default> + 'static,
    P: FnMut(&mut Bu::Input, &mut CursorList<B::Cursor>, &Vec<B>) + 'static,
{
    type Batch = B;
    type Storage = Vec<B>;
    type Cursor = CursorList<B::Cursor>;

    fn cursor_through(&mut self, upper: AntichainRef<Self::Time>) -> Option<(Self::Cursor, Self::Storage)> {
        self.spine.cursor_through(upper)
    }

    fn cursor_through_keyed(&mut self, upper: AntichainRef<Self::Time>, keys: &Self::KeyContainer) -> Option<(Self::Cursor, Self::Storage)> {
        // Check for missing keys.
        let mut any_missing = false;
        for i in 0..keys.len() {
            if !self.contains_key(keys.index(i)) {
                any_missing = true;
                break;
            }
        }

        // Load missing keys from the spine.
        if any_missing {
            if let Some((mut cursor, storage)) = self.spine.cursor_through(upper) {
                let mut builder = Bu::new();
                let mut buffer = Bu::Input::default();

                for i in 0..keys.len() {
                    let key = keys.index(i);
                    if !self.contains_key(<KeyCon<B> as BatchContainer>::reborrow(key)) {
                        cursor.seek_key(&storage, <KeyCon<B> as BatchContainer>::reborrow(key));
                        if cursor.get_key(&storage) == Some(<KeyCon<B> as BatchContainer>::reborrow(key)) {
                            (self.push.borrow_mut())(&mut buffer, &mut cursor, &storage);
                        }
                        builder.push(&mut buffer);
                    }
                }

                let description = Description::new(
                    Antichain::from_elem(<<B as crate::trace::implementations::LayoutExt>::Time as timely::progress::Timestamp>::minimum()),
                    upper.to_owned(),
                    self.spine.get_logical_compaction().to_owned(),
                );
                let batch = builder.done(description);
                self.insert_hot(batch);
            }

            // TODO: Maintain sorted invariant for cached_keys.
            // Currently new keys are appended unsorted relative to existing ones.
            // Need to merge the two sorted sequences, or rebuild from scratch.
            // Can't just replace with `keys` because the caller may not pass all
            // previously cached keys each time.
            for i in 0..keys.len() {
                if !self.contains_key(keys.index(i)) {
                    self.cached_keys.push_ref(keys.index(i));
                }
            }
        }

        // Build storage from hot batches plus any cold batches beyond hot_physical.
        let mut storage: Vec<B> = self.hot.clone();

        // Include cold batches in [hot_physical, upper) — recent, uncompacted data.
        self.spine.map_batches(|batch| {
            if timely::order::PartialOrder::less_than(&self.hot_physical.borrow(), &batch.upper().borrow())
            && !batch.is_empty()
            {
                storage.push(batch.clone());
            }
        });

        let cursors: Vec<_> = storage.iter().map(|b| b.cursor()).collect();
        let cursor = CursorList::new(cursors, &storage);
        Some((cursor, storage))
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Self::Time>) {
        self.spine.set_logical_compaction(frontier);
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Self::Time> {
        self.spine.get_logical_compaction()
    }
    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Self::Time>) {
        // Load cached keys' data from batches in [hot_physical, frontier)
        // before advancing, so the cache stays current.
        if self.cached_keys.len() > 0 && self.hot_physical.borrow() != frontier {
            // Collect batches overlapping [hot_physical, frontier).
            let mut recent_batches = Vec::new();
            self.spine.map_batches(|batch| {
                if timely::order::PartialOrder::less_than(&self.hot_physical.borrow(), &batch.upper().borrow())
                && !timely::order::PartialOrder::less_equal(&frontier, &batch.lower().borrow())
                {
                    recent_batches.push(batch.clone());
                }
            });

            if !recent_batches.is_empty() {
                let cursors: Vec<_> = recent_batches.iter().map(|b| b.cursor()).collect();
                let mut cursor = CursorList::new(cursors, &recent_batches);

                let mut builder = Bu::new();
                let mut buffer = Bu::Input::default();
                let mut any_loaded = false;

                for i in 0..self.cached_keys.len() {
                    let key = self.cached_keys.index(i);
                    cursor.seek_key(&recent_batches, key);
                    if cursor.get_key(&recent_batches) == Some(key) {
                        (self.push.borrow_mut())(&mut buffer, &mut cursor, &recent_batches);
                        builder.push(&mut buffer);
                        any_loaded = true;
                    }
                }

                if any_loaded {
                    let description = Description::new(
                        self.hot_physical.clone(),
                        frontier.to_owned(),
                        self.spine.get_logical_compaction().to_owned(),
                    );
                    let batch = builder.done(description);
                    self.insert_hot(batch);
                }
            }

            self.hot_physical = frontier.to_owned();
        }
        self.spine.set_physical_compaction(frontier);
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Self::Time> {
        self.spine.get_physical_compaction()
    }
    fn map_batches<F: FnMut(&Self::Batch)>(&self, f: F) {
        self.spine.map_batches(f);
    }
}
