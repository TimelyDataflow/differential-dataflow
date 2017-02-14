//! Descriptions of intervals of partially ordered times.

/// Describes an interval of partially ordered times.
///
/// A `Description` indicates a set of partially ordered times, and a moment at which they are
/// observed. The `lower` and `upper` frontiers bound the times contained within, and the `since`
/// frontier indicates a moment at which the times were observed. If `since` is strictly in 
/// advance of `lower`, the contained times may be "advanced" to times which appear equivalent to
/// any time after `since`.
pub struct Description<Time> {
	/// lower frontier of contained updates.
	lower: Vec<Time>,
	/// upper frontier of contained updates.
	upper: Vec<Time>,
	/// frontier used for update compaction.
	since: Vec<Time>,
}

impl<Time: Clone> Description<Time> {
	/// Returns a new description from its component parts.
	pub fn new(lower: &[Time], upper: &[Time], since: &[Time]) -> Self {
		Description {
			lower: lower.to_vec(),
			upper: upper.to_vec(),
			since: since.to_vec(),
		}
	}
	/// The lower envelope for times in the interval.
	pub fn lower(&self) -> &[Time] { &self.lower[..] }
	/// The upper envelope for times in the interval.
	pub fn upper(&self) -> &[Time] { &self.upper[..] }
	/// Times from whose future the interval may be observed.
	pub fn since(&self) -> &[Time] { &self.since[..] }
}