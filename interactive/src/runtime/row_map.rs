//! Private physical row addressing, shared by the time and difference containers.
//! At most 256 dictionary entries: the census has at most twelve distinct times.
#[derive(Clone, Debug, Default)]
pub(super) enum RowMap {
    #[default]
    Constant,
    Indexed(Vec<u8>),
    Dense,
}
impl RowMap {
    #[inline]
    pub(super) fn at(&self, row: usize) -> usize {
        match self { Self::Constant => 0, Self::Indexed(ids) => ids[row] as usize, Self::Dense => row }
    }
    pub(super) fn is_constant(&self) -> bool { matches!(self, Self::Constant) }
    pub(super) fn is_dense(&self) -> bool { matches!(self, Self::Dense) }
    pub(super) fn bytes(&self) -> usize { match self { Self::Indexed(ids) => ids.len(), _ => 0 } }
    #[cfg(test)]
    pub(super) fn capacity_bytes(&self) -> usize { match self { Self::Indexed(ids) => ids.capacity(), _ => 0 } }
    pub(super) fn shrink_to_fit(&mut self) { if let Self::Indexed(ids) = self { ids.shrink_to_fit(); } }
    pub(super) fn truncate(&mut self, len: usize) { if let Self::Indexed(ids) = self { ids.truncate(len); } }
    /// Append one dictionary entry, repeated. The constant case allocates nothing.
    pub(super) fn push(&mut self, entry: usize, old_len: usize, count: usize) {
        if count == 0 { return; }
        match self {
            Self::Constant if entry == 0 => (),
            Self::Constant => {
                let mut ids = Vec::with_capacity(old_len + count);
                ids.resize(old_len, 0);
                ids.resize(old_len + count, entry as u8);
                *self = Self::Indexed(ids);
            }
            Self::Indexed(ids) => ids.resize(old_len + count, entry as u8),
            Self::Dense => unreachable!("dense rows do not have dictionary entries"),
        }
    }
    /// Copy a range of row addresses after reconciling the source dictionary once.
    pub(super) fn extend_mapped(&mut self, source: &[u8], map: &[u8; 256], identity: bool, old_len: usize) {
        if source.is_empty() { return; }
        if identity { self.extend(source, old_len); return; }
        if self.is_constant() && source.iter().all(|&i| map[i as usize] == 0) { return; }
        if self.is_constant() { *self = Self::Indexed(vec![0; old_len]); }
        let Self::Indexed(out) = self else { unreachable!("dense rows do not have dictionary entries") };
        out.extend(source.iter().map(|&i| map[i as usize]));
    }
    /// Append already reconciled addresses as one contiguous range.
    pub(super) fn extend(&mut self, source: &[u8], old_len: usize) {
        if source.is_empty() || (self.is_constant() && source.iter().all(|&i| i == 0)) { return; }
        if self.is_constant() { *self = Self::Indexed(vec![0; old_len]); }
        let Self::Indexed(out) = self else { unreachable!("dense rows do not have dictionary entries") };
        out.extend_from_slice(source);
    }
    /// Select rows without dictionary lookups or rebuilding dictionary values.
    pub(super) fn gather(&self, rows: &[usize]) -> Self {
        match self {
            Self::Constant => Self::Constant,
            Self::Indexed(ids) => Self::Indexed(rows.iter().map(|&r| ids[r]).collect()),
            Self::Dense => Self::Dense,
        }
    }
}
