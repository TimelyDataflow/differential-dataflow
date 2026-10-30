//! Differences stored as a constant, byte-indexed dictionary, or dense values.
use super::row_map::RowMap;

#[derive(Clone, Debug)]
pub(crate) struct ColDiffs<R> {
    values: Vec<R>,
    rows: RowMap,
    len: usize,
}
impl<R> Default for ColDiffs<R> { fn default() -> Self { Self::new() } }
impl<R> ColDiffs<R> {
    pub(crate) fn new() -> Self { Self { values: Vec::new(), rows: RowMap::Constant, len: 0 } }
    pub(crate) fn len(&self) -> usize { self.len }
    pub(crate) fn is_empty(&self) -> bool { self.len == 0 }
    #[cfg(test)]
    pub(crate) fn storage_bytes(&self) -> (usize, usize) {
        let size = std::mem::size_of::<R>();
        (self.values.len() * size + self.rows.bytes(), self.values.capacity() * size + self.rows.capacity_bytes())
    }
}
impl<R> std::ops::Index<usize> for ColDiffs<R> {
    type Output = R;
    #[inline] fn index(&self, row: usize) -> &R {
        debug_assert!(row < self.len);
        &self.values[self.rows.at(row)]
    }
}
impl<R: Clone + PartialEq> ColDiffs<R> {
    /// Classify an owned, completed buffer once. Intern equal runs, not individual rows.
    pub(crate) fn from_vec(mut values: Vec<R>) -> Self {
        let len = values.len();
        if values.windows(2).all(|w| w[0] == w[1]) {
            values.truncate(usize::from(len != 0));
            values.shrink_to_fit();
            return Self { values, rows: RowMap::Constant, len };
        }
        let mut dictionary = Vec::new();
        let mut ids = Vec::with_capacity(len);
        for run in values.chunk_by(|a, b| a == b) {
            let entry = if let Some(entry) = dictionary.iter().position(|value| value == &run[0]) { entry } else {
                if dictionary.len() == 256 { return Self { values, rows: RowMap::Dense, len }; }
                dictionary.push(run[0].clone());
                dictionary.len() - 1
            };
            ids.resize(ids.len() + run.len(), entry as u8);
        }
        if dictionary.len() * std::mem::size_of::<R>() + len >= len * std::mem::size_of::<R>() {
            Self { values, rows: RowMap::Dense, len }
        } else {
            Self { values: dictionary, rows: RowMap::Indexed(ids), len }
        }
    }
    fn dense(&mut self) {
        if !self.rows.is_dense() {
            self.values = (0..self.len).map(|i| self[i].clone()).collect();
            self.rows = RowMap::Dense;
        }
    }
    /// Used only to reconcile dictionaries or append a whole constant range.
    fn intern(&mut self, value: &R) -> Option<usize> {
        if let Some(i) = self.values.iter().position(|x| x == value) { return Some(i); }
        if self.values.len() == 256 { self.dense(); return None; }
        self.values.push(value.clone());
        Some(self.values.len() - 1)
    }
    /// Append a range to an unsealed dense builder without interning its elements.
    pub(crate) fn extend_dense(&self, start: usize, end: usize, out: &mut Vec<R>) {
        match &self.rows {
            RowMap::Constant if start != end => out.resize(out.len() + end - start, self.values[0].clone()),
            RowMap::Constant => (),
            RowMap::Indexed(ids) => out.extend(ids[start..end].iter().map(|&i| self.values[i as usize].clone())),
            RowMap::Dense => out.extend_from_slice(&self.values[start..end]),
        }
    }
    pub(crate) fn push_range(&mut self, other: &Self, start: usize, end: usize) {
        if start == end { return; }
        assert!(start < end && end <= other.len);
        if self.len == 0 { *self = other.slice(start, end); return; }
        if other.rows.is_dense() && !self.rows.is_dense() && end - start <= 256 {
            // A tiny source can be dense by byte cost without having many distinct values.
            let mut ids = [0u8; 256];
            let mut offset = 0;
            for run in other.values[start..end].chunk_by(|a, b| a == b) {
                let Some(entry) = self.intern(&run[0]) else { self.push_range(other, start, end); return };
                ids[offset..offset + run.len()].fill(entry as u8);
                offset += run.len();
            }
            self.rows.extend(&ids[..offset], self.len);
            self.len += end - start;
            return;
        }
        if self.rows.is_dense() || other.rows.is_dense() {
            self.dense();
            other.extend_dense(start, end, &mut self.values);
            self.len += end - start;
            return;
        }
        if other.rows.is_constant() {
            if let Some(entry) = self.intern(&other.values[0]) { self.rows.push(entry, self.len, end - start); }
            else { self.values.resize(self.len + end - start, other.values[0].clone()); }
        } else if let RowMap::Indexed(ids) = &other.rows {
            let mut map = [0u8; 256];
            let mut identity = true;
            for (i, value) in other.values.iter().enumerate() {
                let Some(entry) = self.intern(value) else { self.push_range(other, start, end); return };
                map[i] = entry as u8;
                identity &= entry == i;
            }
            self.rows.extend_mapped(&ids[start..end], &map, identity, self.len);
        }
        self.len += end - start;
        // A constant may have interned unused entries from the source dictionary.
        if self.rows.is_constant() { self.values.truncate(1); }
    }
    pub(crate) fn slice(&self, start: usize, end: usize) -> Self {
        if start == end { return Self::new(); }
        match &self.rows {
            RowMap::Constant => Self { values: self.values.clone(), rows: RowMap::Constant, len: end-start },
            RowMap::Dense => Self::from_vec(self.values[start..end].to_vec()),
            RowMap::Indexed(ids) => {
                if ids[start..end].iter().all(|&i| i == ids[start]) {
                    Self { values: vec![self[start].clone()], rows: RowMap::Constant, len: end-start }
                } else { Self { values: self.values.clone(), rows: RowMap::Indexed(ids[start..end].to_vec()), len: end-start } }
            }
        }
    }
    pub(crate) fn gather(&self, selected: &[usize]) -> Self {
        if selected.is_empty() { return Self::new(); }
        let first = self.rows.at(selected[0]);
        if self.rows.is_constant() || (!self.rows.is_dense() && selected.iter().all(|&r| self.rows.at(r) == first)) {
            return Self { values: vec![self.values[first].clone()], rows: RowMap::Constant, len: selected.len() };
        }
        if self.rows.is_dense() { return Self::from_vec(selected.iter().map(|&r| self.values[r].clone()).collect()); }
        Self { values: self.values.clone(), rows: self.rows.gather(selected), len: selected.len() }
    }
    pub(crate) fn to_vec(&self) -> Vec<R> { (0..self.len).map(|i| self[i].clone()).collect() }
    /// Seal by actual byte cost. Already-dense values have no dictionary bookkeeping.
    pub(crate) fn compact(&mut self) {
        if matches!(self.rows, RowMap::Indexed(_)) && self.values.len() * std::mem::size_of::<R>() + self.rows.bytes() >= self.len * std::mem::size_of::<R>() {
            self.dense();
        }
        self.values.shrink_to_fit();
        self.rows.shrink_to_fit();
    }
}
impl<R: Clone + PartialEq> FromIterator<R> for ColDiffs<R> {
    fn from_iter<I: IntoIterator<Item = R>>(iter: I) -> Self { Self::from_vec(iter.into_iter().collect()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn constant_dictionary_ranges_and_dense_fallback() {
        let mut col: ColDiffs<i64> = std::iter::repeat_n(1, 1000).collect();
        assert!(col.rows.is_constant());
        let source = ColDiffs::from_vec([vec![1; 10], vec![-1; 10]].concat());
        let mut constant = col.clone();
        constant.push_range(&source, 0, 10);
        constant.compact();
        assert!(constant.rows.is_constant());
        assert_eq!(constant.values, vec![1]);
        assert_eq!(constant.to_vec(), vec![1; 1010]);
        col.push_range(&ColDiffs::from_vec(vec![-1, 1].repeat(50)), 0, 2);
        assert_eq!(col.slice(999, 1002).to_vec(), vec![1, -1, 1]);
        assert_eq!(col.gather(&[1000, 1000, 0]).to_vec(), vec![-1, -1, 1]);
        assert_eq!(col.values.len(), 2);
        let mut dense: ColDiffs<i64> = (0..1000).collect();
        assert!(dense.rows.is_dense());
        dense.push_range(&col, 0, 1000);
        assert_eq!(dense.slice(999, 1002).to_vec(), vec![999, 1, 1]);
        assert!(dense.slice(1000, 2000).rows.is_constant());
        let mut copied = ColDiffs::new();
        copied.push_range(&col, 0, col.len());
        copied.compact();
        assert_eq!(copied.to_vec(), col.to_vec());
        let mut small_dense: ColDiffs<i64> = std::iter::repeat_n(1, 1000).collect();
        small_dense.push_range(&ColDiffs::from_vec(vec![-1, 1]), 0, 2);
        assert!(!small_dense.rows.is_dense());
        assert_eq!(small_dense.to_vec(), col.to_vec());
        // A bulk dictionary append can exceed the byte-address limit midway through remapping.
        let mut joined: ColDiffs<i64> = (0..200).cycle().take(2000).collect();
        let tail: ColDiffs<i64> = (100..300).cycle().take(2000).collect();
        let expected: Vec<_> = joined.to_vec().into_iter().chain(tail.to_vec()).collect();
        joined.push_range(&tail, 0, tail.len());
        assert!(joined.rows.is_dense());
        assert_eq!(joined.to_vec(), expected);
        let mut buffer = Vec::new();
        for source in [&col, &dense, &joined] {
            source.extend_dense(1, source.len() - 1, &mut buffer);
        }
        let expected: Vec<_> = [&col, &dense, &joined].into_iter()
            .flat_map(|source| source.to_vec().into_iter().skip(1).take(source.len() - 2)).collect();
        assert_eq!(buffer, expected);
    }
}
