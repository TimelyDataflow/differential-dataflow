//! Input session to simplify high resolution input.

use ::{Data, Delta};
use timely::progress::Timestamp;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;

/// An input session wrapping a single timely dataflow timestamp.
pub struct InputSession<'a, T: Timestamp+Ord+Clone, D: Data> {
	time: Product<RootTimestamp, T>,
	buffer: Vec<(D, Product<RootTimestamp, T>, Delta)>,
	handle: &'a mut ::timely::dataflow::operators::input::Handle<T,(D,Product<RootTimestamp, T>,Delta)>,
}

impl<'a, T: Timestamp+Ord+Clone, D: Data> InputSession<'a, T, D> {

	/// Creates a new session from a reference to an input handle.
	pub fn from(handle: &'a mut ::timely::dataflow::operators::input::Handle<T,(D,Product<RootTimestamp, T>,Delta)>) -> Self {
		InputSession {
			time: handle.time().clone(),
			buffer: Vec::new(),
			handle: handle,
		}
	}

	/// Adds an element to the collection.
	pub fn insert(&mut self, element: D) { self.update(element, 1); }
	/// Removes an element from the collection.
	pub fn remove(&mut self, element: D) { self.update(element,-1); }
	/// Adds to the weight of an element in the collection.
	pub fn update(&mut self, element: D, change: isize) { self.buffer.push((element, self.time.clone(), change)); }

	/// Forces buffered data into the timely input, and advances its time to match that of the sesson.
	pub fn flush(&mut self) {
		for (data, time, diff) in self.buffer.drain(..) {
			self.handle.send((data, time, diff));
		}
		self.handle.advance_to(self.time.inner);		
	}

	/// Advances the logical time for future records.
	///
	/// Importantly, this method does **not** advance the time on the underlying handle. This happens only when the
	/// session is dropped. It is not correct to use this time as a basis for a computations `step_while` method.
	pub fn advance_to(&mut self, time: T) {
		assert!(self.handle.epoch() < &time);
		assert!(&self.time.inner < &time);
		self.time = Product::new(RootTimestamp, time);
	}

	/// Reveals the current time of the session.
	pub fn epoch(&self) -> &T { &self.time.inner }
	/// Reveals the current time of the session.
	pub fn time(&self) -> &Product<RootTimestamp, T> { &self.time }
}

impl<'a, T: Timestamp+Ord+Clone, D: Data> Drop for InputSession<'a, T, D> {
	fn drop(&mut self) {
		self.flush();
	}
}
