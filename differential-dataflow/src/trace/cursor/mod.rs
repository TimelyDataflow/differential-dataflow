//! Traits and types for navigating order sequences of update tuples.
//!
//! The `Cursor` trait contains several methods for efficiently navigating ordered collections
//! of tuples of the form `(key, val, time, diff)`. The cursor is different from an iterator
//! both because it allows navigation on multiple levels (key and val), but also because it
//! supports efficient seeking (via the `seek_key` and `seek_val` methods).

use timely::progress::Timestamp;

use crate::difference::Semigroup;
use crate::lattice::Lattice;

pub mod cursor_list;

pub use self::cursor_list::CursorList;

use crate::trace::implementations::LayoutExt;

/// A cursor for navigating ordered `(key, val, time, diff)` updates.
pub trait Cursor : LayoutExt {

    /// Key by which updates are indexed.
    type Key<'a>: Copy + Clone + Ord;
    /// Values associated with keys.
    type Val<'a>: Copy + Clone + Ord;
    /// Timestamps associated with updates
    type Time: Timestamp + Lattice + Ord + Clone;
    /// Borrowed form of timestamp.
    type TimeGat<'a>: Copy;
    /// Owned form of update difference.
    type Diff: Semigroup + 'static;
    /// Borrowed form of update difference.
    type DiffGat<'a> : Copy;

    /// An owned copy of a reference to a time.
    fn owned_time(time: Self::TimeGat<'_>) -> Self::Time;
    /// Clones a reference time onto an owned time.
    fn clone_time_onto(time: Self::TimeGat<'_>, onto: &mut Self::Time);
    /// An owned copy of a reference to a diff.
    fn owned_diff(diff: Self::DiffGat<'_>) -> Self::Diff;

    /// Storage required by the cursor.
    type Storage;

    /// Indicates if the current key is valid.
    ///
    /// A value of `false` indicates that the cursor has exhausted all keys.
    fn key_valid(&self, storage: &Self::Storage) -> bool;
    /// Indicates if the current value is valid.
    ///
    /// A value of `false` indicates that the cursor has exhausted all values for this key.
    fn val_valid(&self, storage: &Self::Storage) -> bool;

    /// A reference to the current key. Asserts if invalid.
    fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a>;
    /// A reference to the current value. Asserts if invalid.
    fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a>;

    /// Returns a reference to the current key, if valid.
    fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Key<'a>>;
    /// Returns a reference to the current value, if valid.
    fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Val<'a>>;

    /// Applies `logic` to each pair of time and difference. Intended for mutation of the
    /// closure's scope.
    fn map_times<L: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &Self::Storage, logic: L);

    /// Advances the cursor to the next key.
    fn step_key(&mut self, storage: &Self::Storage);
    /// Advances the cursor to the specified key.
    fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>);

    /// Advances the cursor to the next value.
    fn step_val(&mut self, storage: &Self::Storage);
    /// Advances the cursor to the specified value.
    fn seek_val(&mut self, storage: &Self::Storage, val: Self::Val<'_>);

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self, storage: &Self::Storage);
    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self, storage: &Self::Storage);

    /// Rewinds the cursor and outputs its contents to a Vec
    fn to_vec<K, IK, V, IV>(&mut self, storage: &Self::Storage, into_key: IK, into_val: IV) -> Vec<((K, V), Vec<(Self::Time, Self::Diff)>)>
    where
        IK: for<'a> Fn(Self::Key<'a>) -> K,
        IV: for<'a> Fn(Self::Val<'a>) -> V,
    {
        let mut out = Vec::new();
        self.rewind_keys(storage);
        while let Some(key) = self.get_key(storage) {
            self.rewind_vals(storage);
            while let Some(val) = self.get_val(storage) {
                let mut kv_out = Vec::new();
                self.map_times(storage, |ts, r| {
                    kv_out.push((Self::owned_time(ts), Self::owned_diff(r)));
                });
                out.push(((into_key(key), into_val(val)), kv_out));
                self.step_val(storage);
            }
            self.step_key(storage);
        }
        out
    }
}
