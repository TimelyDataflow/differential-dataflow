//! Input sessions for simplified collection updates.
//!
//! Although users can directly manipulate timely dataflow streams as collection inputs,
//! the `InputSession` type can make this more efficient and less error-prone. Specifically,
//! the type batches up updates with their logical times and ships them with coarsened
//! timely dataflow capabilities, exposing more concurrency to the operator implementations
//! than are evident from the logical times, which appear to execute in sequence.

use timely::communication::Allocate;

use timely::progress::Timestamp;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;
use timely::dataflow::operators::Input as TimelyInput;
use timely::dataflow::operators::input::Handle;
use timely::dataflow::scopes::{Child, Root};

use ::{Data, Diff};
use collection::{Collection, AsCollection};

/// Create a new collection and input handle to control the collection.
pub trait Input<'a, A: Allocate, T: Timestamp+Ord> {
    /// Create a new collection and input handle to subsequently control the collection.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use timely::Configuration;
    /// use differential_dataflow::input::Input;
    ///
    /// fn main() {
    ///     ::timely::execute(Configuration::Thread, |worker| {
    ///
    ///			let (mut handle, probe) = worker.dataflow::<(),_,_>(|scope| {
    ///				// create input handle and collection.
    ///				let (handle, data) = scope.new_collection();
    ///         	let probe = data.map(|x| x * 2)
    ///				            	.inspect(|x| println!("{:?}", x))
    ///				            	.probe();
    ///				(handle, probe)
    ///     	});
    ///
    ///			handle.insert(1);
    ///			handle.insert(5);
    ///
    ///		}).unwrap();
    /// }
    /// ```
    fn new_collection<D, R>(&mut self) -> (InputSession<T, D, R>, Collection<Child<'a, Root<A>, T>, D, R>)
    where D: Data, R: Diff;
    /// Create a new collection and input handle from initial data.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use timely::Configuration;
    /// use differential_dataflow::input::Input;
    ///
    /// fn main() {
    ///     ::timely::execute(Configuration::Thread, |worker| {
    ///
    ///			let (mut handle, probe) = worker.dataflow::<(),_,_>(|scope| {
    ///				// create input handle and collection.
    ///				let (handle, data) = scope.new_collection_from(0 .. 10);
    ///         	let probe = data.map(|x| x * 2)
    ///				            	.inspect(|x| println!("{:?}", x))
    ///				            	.probe();
    ///				(handle, probe)
    ///     	});
    ///
    ///			handle.insert(1);
    ///			handle.insert(5);
    ///
    ///		}).unwrap();
    /// }
    /// ```
    fn new_collection_from<I>(&mut self, data: I) -> (InputSession<T, I::Item, isize>, Collection<Child<'a, Root<A>, T>, I::Item, isize>)
    where I: IntoIterator+'static, I::Item: Data;
}

impl<'a, A: Allocate, T: Timestamp+Ord> Input<'a, A, T> for Child<'a, Root<A>, T> {
    fn new_collection<D, R>(&mut self) -> (InputSession<T, D, R>, Collection<Child<'a, Root<A>, T>, D, R>)
    where D: Data, R: Diff{
		let (handle, stream) = self.new_input();
		(InputSession::from(handle), stream.as_collection())
    }
    fn new_collection_from<I>(&mut self, data: I) -> (InputSession<T, I::Item, isize>, Collection<Child<'a, Root<A>, T>, I::Item, isize>)
    where I: IntoIterator+'static, I::Item: Data {

    	use timely::dataflow::operators::ToStream;

		let (handle, stream) = self.new_input();
		let source = data.into_iter().map(|d| (d, Default::default(), 1)).to_stream(self).as_collection();

		(InputSession::from(handle), stream.as_collection().concat(&source))
    }
}

/// An input session wrapping a single timely dataflow capability.
///
/// Each timely dataflow message has a corresponding capability, which is a logical time in the
/// timely dataflow system. Differential dataflow updates can happen at a much higher rate than
/// timely dataflow's progress tracking infrastructure supports, because the logical times are
/// promoted to data and updates are batched together. The `InputSession` type does this batching.
///
/// # Examples
///
/// ```
/// extern crate timely;
/// extern crate differential_dataflow;
///
/// use timely::Configuration;
/// use differential_dataflow::input::Input;
///
/// fn main() {
///     ::timely::execute(Configuration::Thread, |worker| {
///
///			let (mut handle, probe) = worker.dataflow(|scope| {
///				// create input handle and collection.
///				let (handle, data) = scope.new_collection_from(0 .. 10);
///         	let probe = data.map(|x| x * 2)
///				            	.inspect(|x| println!("{:?}", x))
///				            	.probe();
///				(handle, probe)
///     	});
///
///			handle.insert(3);
///			handle.advance_to(1);
///			handle.insert(5);
///			handle.advance_to(2);
///			handle.flush();
///
///			while probe.less_than(handle.time()) {
///				worker.step();
///			}
///
///			handle.remove(5);
///			handle.advance_to(3);
///			handle.flush();
///
///			while probe.less_than(handle.time()) {
///				worker.step();
///			}
///
///		}).unwrap();
/// }
/// ```
pub struct InputSession<T: Timestamp+Clone, D: Data, R: Diff> {
	time: Product<RootTimestamp, T>,
	buffer: Vec<(D, Product<RootTimestamp, T>, R)>,
	handle: Handle<T,(D,Product<RootTimestamp, T>,R)>,
}

impl<T: Timestamp+Clone, D: Data> InputSession<T, D, isize> {
	/// Adds an element to the collection.
	pub fn insert(&mut self, element: D) { self.update(element, 1); }
	/// Removes an element from the collection.
	pub fn remove(&mut self, element: D) { self.update(element,-1); }
}

// impl<T: Timestamp+Clone, D: Data> InputSession<T, D, i64> {
//     /// Adds an element to the collection.
//     pub fn insert(&mut self, element: D) { self.update(element, 1); }
//     /// Removes an element from the collection.
//     pub fn remove(&mut self, element: D) { self.update(element,-1); }
// }

// impl<T: Timestamp+Clone, D: Data> InputSession<T, D, i32> {
//     /// Adds an element to the collection.
//     pub fn insert(&mut self, element: D) { self.update(element, 1); }
//     /// Removes an element from the collection.
//     pub fn remove(&mut self, element: D) { self.update(element,-1); }
// }

impl<T: Timestamp+Clone, D: Data, R: Diff> InputSession<T, D, R> {

    /// Introduces a handle as collection.
    pub fn to_collection<'a, A: Allocate>(&mut self, scope: &mut Child<'a, Root<A>, T>) -> Collection<Child<'a, Root<A>, T>, D, R> {
        scope
            .input_from(&mut self.handle)
            .as_collection()
    }

    /// Allocates a new input handle.
    pub fn new() -> Self {
        let handle = Handle::new();
        InputSession {
            time: handle.time().clone(),
            buffer: Vec::new(),
            handle,
        }
    }

	/// Creates a new session from a reference to an input handle.
	pub fn from(handle: Handle<T,(D,Product<RootTimestamp, T>,R)>) -> Self {
		InputSession {
			time: handle.time().clone(),
			buffer: Vec::new(),
			handle,
		}
	}

	/// Adds to the weight of an element in the collection.
	pub fn update(&mut self, element: D, change: R) {
        if self.buffer.len() == self.buffer.capacity() {
            if self.buffer.len() > 0 {
                self.handle.send_batch(&mut self.buffer);
            }
            // TODO : This is a fairly arbitrary choice; should probably use `Context::default_size()` or such.
            self.buffer.reserve(1024);
        }
		self.buffer.push((element, self.time.clone(), change));
	}

    /// Adds to the weight of an element in the collection at a future time.
    pub fn update_at(&mut self, element: D, time: T, change: R) {
        assert!(self.time.inner.less_equal(&time));
        if self.buffer.len() == self.buffer.capacity() {
            if self.buffer.len() > 0 {
                self.handle.send_batch(&mut self.buffer);
            }
            // TODO : This is a fairly arbitrary choice; should probably use `Context::default_size()` or such.
            self.buffer.reserve(1024);
        }
        self.buffer.push((element, RootTimestamp::new(time), change));
    }

	/// Forces buffered data into the timely dataflow input, and advances its time to match that of the session.
	///
	/// It is important to call `flush` before expecting timely dataflow to report progress. Until this method is
	/// called, all updates may still be in internal buffers and not exposed to timely dataflow. Once the method is
	/// called, all buffers are flushed and timely dataflow is advised that some logical times are no longer possible.
	pub fn flush(&mut self) {
		self.handle.send_batch(&mut self.buffer);
		if self.handle.epoch().less_than(&self.time.inner) {
			self.handle.advance_to(self.time.inner.clone());
		}
	}

	/// Advances the logical time for future records.
	///
	/// Importantly, this method does **not** immediately inform timely dataflow of the change. This happens only when
	/// the session is dropped or flushed. It is not correct to use this time as a basis for a computation's `step_while`
	/// method unless the session has just been flushed.
	pub fn advance_to(&mut self, time: T) {
		assert!(self.handle.epoch().less_equal(&time));
		assert!(&self.time.inner.less_equal(&time));
		self.time = Product::new(RootTimestamp, time);
	}

	/// Reveals the current time of the session.
	pub fn epoch(&self) -> &T { &self.time.inner }
	/// Reveals the current time of the session.
	pub fn time(&self) -> &Product<RootTimestamp, T> { &self.time }

	/// Closes the input, flushing and sealing the wrapped timely input.
	pub fn close(self) { }
}

impl<T: Timestamp+Clone, D: Data, R: Diff> Drop for InputSession<T, D, R> {
	fn drop(&mut self) {
		self.flush();
	}
}
