//! Datastructures representing a collection trace.

pub mod trace;
pub mod layer;
pub mod trace_trait;
pub mod cursor;

pub use self::layer::{Layer, LayerCursor};
pub use self::trace::LayerTrace;
pub use self::cursor::Cursor;

/// Scans `vec[off..]` and consolidates differences of adjacent equivalent elements.
pub fn consolidate<T: Ord+Clone>(vec: &mut Vec<(T, isize)>, off: usize) {
	vec[off..].sort_by(|x,y| x.0.cmp(&y.0));
	for index in (off + 1) .. vec.len() {
		if vec[index].0 == vec[index - 1].0 {
			vec[index].1 += vec[index - 1].1;
			vec[index - 1].1 = 0;
		}
	}
	let mut cursor = off;
	for index in off .. vec.len() {
		if vec[index].1 != 0 {
			vec[cursor] = vec[index].clone();
			cursor += 1;
		}
	}
	vec.truncate(cursor);
}