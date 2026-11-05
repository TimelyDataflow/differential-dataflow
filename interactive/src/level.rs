//! Coordinate positions, independent of PointStamp's trimmed storage.

#[derive(Clone, Copy, Debug)]
pub(crate) struct Level {
    pub(crate) root: usize,
    pub(crate) depth: usize,
}

impl Level {
    pub(crate) fn root(root: usize) -> Self { Self { root, depth: 0 } }
    pub(crate) fn width(self) -> usize { self.root + self.depth }
    pub(crate) fn own_lane(self) -> Option<usize> { (self.depth > 0).then(|| self.width() - 1) }
    pub(crate) fn child(self) -> Self { Self { depth: self.depth + 1, ..self } }
    pub(crate) fn parent(self) -> Self { Self { depth: self.depth.saturating_sub(1), ..self } }
}
