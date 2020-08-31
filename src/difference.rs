//! A type that can be treated as a difference.
//!
//! Differential dataflow most commonly tracks the counts associated with records in a multiset, but it
//! generalizes to tracking any map from the records to an Abelian group. The most common generalization
//! is when we maintain both a count and another accumulation, for example height. The differential
//! dataflow collections would then track for each record the total of counts and heights, which allows
//! us to track something like the average.

use std::ops::{AddAssign, Neg};

use ::Data;

#[deprecated]
pub use self::Abelian as Diff;

/// A type with addition and a test for zero.
///
/// These traits are currently the minimal requirements for a type to be a "difference" in differential
/// dataflow. Addition allows differential dataflow to compact multiple updates to the same data, and
/// the test for zero allows differential dataflow to retire updates that have no effect. There is no
/// requirement that the test for zero ever return true, and the zero value does not need to inhabit the
/// type.
///
/// There is a light presumption of commutativity here, in that while we will largely perform addition
/// in order of timestamps, for many types of timestamps there is no total order and consequently no
/// obvious order to respect. Non-commutative semigroups should be used with care.
pub trait Semigroup : for<'a> AddAssign<&'a Self> + ::std::marker::Sized + Data + Clone {
	/// Returns true if the element is the additive identity.
	///
	/// This is primarily used by differential dataflow to know when it is safe to delete an update.
	/// When a difference accumulates to zero, the difference has no effect on any accumulation and can
	/// be removed.
	///
	/// A semigroup is not obligated to have a zero element, and this method could always return
	/// false in such a setting.
	fn is_zero(&self) -> bool;
}

impl Semigroup for isize {
	#[inline] fn is_zero(&self) -> bool { self == &0 }
}

impl Semigroup for i128 {
	#[inline] fn is_zero(&self) -> bool { self == &0 }
}

impl Semigroup for i64 {
	#[inline] fn is_zero(&self) -> bool { self == &0 }
}

impl Semigroup for i32 {
	#[inline] fn is_zero(&self) -> bool { self == &0 }
}

impl Semigroup for i16 {
	#[inline] fn is_zero(&self) -> bool { self == &0 }
}

impl Semigroup for i8 {
	#[inline] fn is_zero(&self) -> bool { self == &0 }
}


/// A semigroup with an explicit zero element.
pub trait Monoid : Semigroup {
	/// A zero element under the semigroup addition operator.
	fn zero() -> Self;
}

impl Monoid for isize {
	#[inline] fn zero() -> Self { 0 }
}

impl Monoid for i128 {
	#[inline] fn zero() -> Self { 0 }
}

impl Monoid for i64 {
	#[inline] fn zero() -> Self { 0 }
}

impl Monoid for i32 {
	#[inline] fn zero() -> Self { 0 }
}

impl Monoid for i16 {
	#[inline] fn zero() -> Self { 0 }
}

impl Monoid for i8 {
	#[inline] fn zero() -> Self { 0 }
}


/// A `Monoid` with negation.
///
/// This trait extends the requirements of `Semigroup` to include a negation operator.
/// Several differential dataflow operators require negation in order to retract prior outputs, but
/// not quite as many as you might imagine.
pub trait Abelian : Monoid + Neg<Output=Self> { }
impl<T: Monoid + Neg<Output=Self>> Abelian for T { }


pub use self::present::Present;
mod present {

	/// A zero-sized difference that indicates the presence of a record.
	///
	/// This difference type has no negation, and present records cannot be retracted.
	/// Addition and multiplication maintain presence, and zero does not inhabit the type.
	///
	/// The primary feature of this type is that it has zero size, which reduces the overhead
	/// of differential dataflow's representations for settings where collections either do
	/// not change, or for which records are only added (for example, derived facts in Datalog).
	#[derive(Abomonation, Copy, Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Serialize, Deserialize, Hash)]
	pub struct Present;

	impl<'a> std::ops::AddAssign<&'a Self> for Present {
		fn add_assign(&mut self, _rhs: &'a Self) { }
	}

	impl<T> std::ops::Mul<T> for Present {
		type Output = T;
		fn mul(self, rhs: T) -> T {
			rhs
		}
	}

	impl super::Semigroup for Present {
		fn is_zero(&self) -> bool { false }
	}
}

pub use self::pair::DiffPair;
mod pair {

	use std::ops::{AddAssign, Neg, Mul};
	use super::{Semigroup, Monoid};

	/// The difference defined by a pair of difference elements.
	///
	/// This type is essentially a "pair", though in Rust the tuple types do not derive the numeric
	/// traits we require, and so we need to emulate the types ourselves. In the interest of ergonomics,
	/// we may eventually replace the numeric traits with our own, so that we can implement them for
	/// tuples and allow users to ignore details like these.
	#[derive(Abomonation, Copy, Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
	pub struct DiffPair<R1, R2> {
		/// The first element in the pair.
		pub element1: R1,
		/// The second element in the pair.
		pub element2: R2,
	}

	impl<R1, R2> DiffPair<R1, R2> {
		/// Creates a new Diff pair from two elements.
		#[inline] pub fn new(elt1: R1, elt2: R2) -> Self {
			DiffPair {
				element1: elt1,
				element2: elt2,
			}
		}
	}

	impl<R1: Semigroup, R2: Semigroup> Semigroup for DiffPair<R1, R2> {
		#[inline] fn is_zero(&self) -> bool {
			self.element1.is_zero() && self.element2.is_zero()
		}
	}

	impl<'a, R1: AddAssign<&'a R1>, R2: AddAssign<&'a R2>> AddAssign<&'a DiffPair<R1, R2>> for DiffPair<R1, R2> {
		#[inline] fn add_assign(&mut self, rhs: &'a Self) {
			self.element1 += &rhs.element1;
			self.element2 += &rhs.element2;
		}
	}

	impl<R1: Neg, R2: Neg> Neg for DiffPair<R1, R2> {
		type Output = DiffPair<<R1 as Neg>::Output, <R2 as Neg>::Output>;
		#[inline] fn neg(self) -> Self::Output {
			DiffPair {
				element1: -self.element1,
				element2: -self.element2,
			}
		}
	}

	impl<T: Copy, R1: Mul<T>, R2: Mul<T>> Mul<T> for DiffPair<R1,R2> {
		type Output = DiffPair<<R1 as Mul<T>>::Output, <R2 as Mul<T>>::Output>;
		fn mul(self, other: T) -> Self::Output {
			DiffPair::new(
				self.element1 * other,
				self.element2 * other,
			)
		}
	}

	impl<R1: Monoid, R2: Monoid> Monoid for DiffPair<R1, R2> {
		fn zero() -> Self {
			Self {
				element1: R1::zero(),
				element2: R2::zero(),
			}
		}
	}

	// // TODO: This currently causes rustc to trip a recursion limit, because who knows why.
	// impl<R1: Diff, R2: Diff> Mul<DiffPair<R1,R2>> for isize
	// where isize: Mul<R1>, isize: Mul<R2>, <isize as Mul<R1>>::Output: Diff, <isize as Mul<R2>>::Output: Diff {
	// 	type Output = DiffPair<<isize as Mul<R1>>::Output, <isize as Mul<R2>>::Output>;
	// 	fn mul(self, other: DiffPair<R1,R2>) -> Self::Output {
	// 		DiffPair::new(
	// 			self * other.element1,
	// 			self * other.element2,
	// 		)
	// 	}
	// }
}

pub use self::vector::DiffVector;
mod vector {

	use std::ops::{AddAssign, Neg, Mul};
	use super::{Semigroup, Monoid};

	/// A variable number of accumulable updates.
	#[derive(Abomonation, Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
	pub struct DiffVector<R> {
		buffer: Vec<R>,
	}

	impl<R> DiffVector<R> {
		/// Create new DiffVector from Vec
		#[inline(always)]
		pub fn new(vec: Vec<R>) -> DiffVector<R> {
			DiffVector { buffer: vec }
		}
	}

	impl<R> IntoIterator for DiffVector<R> {
		type Item = R;
		type IntoIter = ::std::vec::IntoIter<R>;
		fn into_iter(self) -> Self::IntoIter {
			self.buffer.into_iter()
		}
	}

	impl<R> std::ops::Deref for DiffVector<R> {
		type Target = [R];
		fn deref(&self) -> &Self::Target {
			&self.buffer[..]
		}
	}

	impl<R> std::ops::DerefMut for DiffVector<R> {
		fn deref_mut(&mut self) -> &mut Self::Target {
			&mut self.buffer[..]
		}
	}

	impl<R: Semigroup> Semigroup for DiffVector<R> {
		#[inline] fn is_zero(&self) -> bool {
			self.buffer.iter().all(|x| x.is_zero())
		}
	}

	impl<'a, R: AddAssign<&'a R>+Clone> AddAssign<&'a DiffVector<R>> for DiffVector<R> {
		#[inline]
		fn add_assign(&mut self, rhs: &'a Self) {

			// Ensure sufficient length to receive addition.
			while self.buffer.len() < rhs.buffer.len() {
				let element = &rhs.buffer[self.buffer.len()];
				self.buffer.push(element.clone());
			}

			// As other is not longer, apply updates without tests.
			for (index, update) in rhs.buffer.iter().enumerate() {
				self.buffer[index] += update;
			}
		}
	}

	impl<R: Neg<Output=R>+Clone> Neg for DiffVector<R> {
		type Output = DiffVector<<R as Neg>::Output>;
		#[inline]
		fn neg(mut self) -> Self::Output {
			for update in self.buffer.iter_mut() {
				*update = -update.clone();
			}
			self
		}
	}

	impl<T: Copy, R: Mul<T>> Mul<T> for DiffVector<R> {
		type Output = DiffVector<<R as Mul<T>>::Output>;
		fn mul(self, other: T) -> Self::Output {
			let buffer =
			self.buffer
				.into_iter()
				.map(|x| x * other)
				.collect();

			DiffVector { buffer }
		}
	}

	impl<R: Semigroup> Monoid for DiffVector<R> {
		fn zero() -> Self {
			Self { buffer: Vec::new() }
		}
	}
}