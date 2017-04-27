//! Input session to simplify high resolution input.

use ::{Data, Diff};
use timely::order::PartialOrder;
use timely::progress::Timestamp;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;

/// An input session wrapping a single timely dataflow timestamp.
pub struct InputSession<'a, T: Timestamp+Clone, D: Data, R: Diff> {
	time: Product<RootTimestamp, T>,
	buffer: Vec<(D, Product<RootTimestamp, T>, R)>,
	handle: &'a mut ::timely::dataflow::operators::input::Handle<T,(D,Product<RootTimestamp, T>,R)>,
}

impl<'a, T: Timestamp+Clone, D: Data> InputSession<'a, T, D, isize> {
	/// Adds an element to the collection.
	pub fn insert(&mut self, element: D) { self.update(element, 1); }
	/// Removes an element from the collection.
	pub fn remove(&mut self, element: D) { self.update(element,-1); }
}

impl<'a, T: Timestamp+Clone, D: Data, R: Diff> InputSession<'a, T, D, R> {

	/// Creates a new session from a reference to an input handle.
	pub fn from(handle: &'a mut ::timely::dataflow::operators::input::Handle<T,(D,Product<RootTimestamp, T>,R)>) -> Self {
		InputSession {
			time: handle.time().clone(),
			buffer: Vec::new(),
			handle: handle,
		}
	}

	/// Adds to the weight of an element in the collection.
	pub fn update(&mut self, element: D, change: R) { 
		self.buffer.push((element, self.time.clone(), change)); 
	}

	/// Forces buffered data into the timely input, and advances its time to match that of the session.
	pub fn flush(&mut self) {
		self.handle.send_batch(&mut self.buffer);
		if self.handle.epoch().less_than(&self.time.inner) {
			self.handle.advance_to(self.time.inner);		
		}
	}

	/// Advances the logical time for future records.
	///
	/// Importantly, this method does **not** advance the time on the underlying handle. This happens only when the
	/// session is dropped or flushed. It is not correct to use this time as a basis for a computation's `step_while`
	/// method unless it has just been flushed.
	pub fn advance_to(&mut self, time: T) {
		assert!(self.handle.epoch().less_equal(&time));
		assert!(&self.time.inner.less_equal(&time));
		self.time = Product::new(RootTimestamp, time);
	}

	/// Reveals the current time of the session.
	pub fn epoch(&self) -> &T { &self.time.inner }
	/// Reveals the current time of the session.
	pub fn time(&self) -> &Product<RootTimestamp, T> { &self.time }
}

impl<'a, T: Timestamp+Clone, D: Data, R: Diff> Drop for InputSession<'a, T, D, R> {
	fn drop(&mut self) {
		self.flush();
	}
}
