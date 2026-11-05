//! Loading adapters between cursors and value histories.

use crate::lattice::Lattice;
use crate::operators::history::{EditList, HistoryReplay, ValueHistory};
use crate::trace::Cursor;

/// Walks the cursor's values at the current key into `target`, advancing times by `meet` if supplied.
fn load_values<'a, V, T, D, C>(
    target: &mut EditList<V, T, D>,
    cursor: &mut C,
    storage: &'a C::Storage,
    meet: Option<&T>,
)
where
    V: Copy + Ord,
    T: Ord + Clone + Lattice,
    D: crate::difference::Semigroup,
    C: Cursor<Val<'a> = V, Time = T, Diff = D>,
{
    while let Some(val) = cursor.get_val(storage) {
        cursor.map_times(storage, |time, diff| {
            let mut time = C::owned_time(time);
            if let Some(meet) = meet { time.join_assign(meet); }
            target.push(time, C::owned_diff(diff));
        });
        target.seal(val);
        cursor.step_val(storage);
    }
}

/// Loads the cursor's values at its current key into `history`.
///
/// This avoids a redundant seek in the merge-join inner loop, where the cursor is positioned by the upstream merge step.
pub(super) fn load_current<'a, V, T, D, C>(
    history: &mut ValueHistory<V, T, D>,
    cursor: &mut C,
    storage: &'a C::Storage,
    meet: Option<&T>,
)
where
    V: Copy + Ord,
    T: Ord + Clone + Lattice,
    D: crate::difference::Semigroup,
    C: Cursor<Val<'a> = V, Time = T, Diff = D>,
{
    history.clear();
    load_values(history.edits_mut(), cursor, storage, meet);
}

/// Loads and replays a specified key.
///
/// If the key is absent, the replayed history will be empty.
pub(super) fn replay_key<'a, 'history, V, T, D, C>(
    history: &'history mut ValueHistory<V, T, D>,
    cursor: &mut C,
    storage: &'a C::Storage,
    key: C::Key<'a>,
    meet: Option<&T>,
) -> HistoryReplay<'history, V, T, D>
where
    V: Copy + Ord,
    T: Ord + Clone + Lattice,
    D: crate::difference::Semigroup,
    C: Cursor<Val<'a> = V, Time = T, Diff = D>,
{
    history.clear();
    cursor.seek_key(storage, key);
    if cursor.get_key(storage) == Some(key) {
        cursor.rewind_vals(storage);
        load_values(history.edits_mut(), cursor, storage, meet);
    }
    history.replay()
}
