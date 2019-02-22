//! A type that can be treated as a difference.
//!
//! Differential dataflow most commonly tracks the counts associated with records in a multiset, but it
//! generalizes to tracking any map from the records to an Abelian group. The most common generalization
//! is when we maintain both a count and another accumulation, for example height. The differential
//! dataflow collections would then track for each record the total of counts and heights, which allows
//! us to track something like the average.

use std::ops::{AddAssign, Neg, Mul};

use ::Data;

pub use self::Abelian as Diff;

/// A type that can be treated as a difference.
///
/// The mathematical requirements are, I believe, an Abelian group, in that we require addition, inverses,
/// and almost certainly use commutativity somewhere (it isn't clear if it is a requirement, as it isn't
/// clear that there are semantics other than "we accumulate your differences"; I suspect we don't always
/// accumulate them in the right order, so commutativity is important until we conclude otherwise).
pub trait Monoid : AddAssign + ::std::marker::Sized + Data + Clone {
	/// Returns true if the element is the additive identity.
	///
	/// This is primarily used by differential dataflow to know when it is safe to delete and update.
	/// When a difference accumulates to zero, the difference has no effect on any accumulation and can
	/// be removed.
	fn is_zero(&self) -> bool { self.eq(&Self::zero()) }
	/// The additive identity.
	///
	/// This method is primarily used by differential dataflow internals as part of consolidation, when
	/// one value is accumulated elsewhere and must be replaced by valid but harmless value.
	fn zero() -> Self;
}

/// A commutative monoid with negation.
///
/// This trait represents a commutative group, here a commutative monoid with
/// the additional support for subtraction and negation. An identity subtracted
/// from itself or added to its negation should be the zero element from the
/// underlying monoid.
pub trait Abelian : Monoid + Neg<Output=Self> { }
impl<T: Monoid + Neg<Output=Self>> Abelian for T { }

impl Monoid for isize {
	#[inline(always)] fn is_zero(&self) -> bool { *self == 0 }
	#[inline(always)] fn zero() -> Self { 0 }
}

impl Monoid for i64 {
	#[inline(always)] fn is_zero(&self) -> bool { *self == 0 }
	#[inline(always)] fn zero() -> Self { 0 }
}

impl Monoid for i32 {
	#[inline(always)] fn is_zero(&self) -> bool { *self == 0 }
	#[inline(always)] fn zero() -> Self { 0 }
}

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
	#[inline(always)] pub fn new(elt1: R1, elt2: R2) -> Self {
		DiffPair {
			element1: elt1,
			element2: elt2,
		}
	}
}

impl<R1: Monoid, R2: Monoid> Monoid for DiffPair<R1, R2> {
	#[inline(always)] fn is_zero(&self) -> bool { self.element1.is_zero() && self.element2.is_zero() }
	#[inline(always)] fn zero() -> Self { DiffPair { element1: R1::zero(), element2: R2::zero() } }
}

impl<R1: AddAssign, R2: AddAssign> AddAssign<DiffPair<R1, R2>> for DiffPair<R1, R2> {
	#[inline(always)] fn add_assign(&mut self, rhs: Self) {
		self.element1 += rhs.element1;
		self.element2 += rhs.element2;
	}
}

impl<R1: Neg, R2: Neg> Neg for DiffPair<R1, R2> {
	type Output = DiffPair<<R1 as Neg>::Output, <R2 as Neg>::Output>;
	#[inline(always)] fn neg(self) -> Self::Output {
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

/// A variable number of accumulable updates.
#[derive(Abomonation, Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct DiffVector<R> {
	buffer: Vec<R>,
}

impl<R: Monoid> Monoid for DiffVector<R> {
	#[inline(always)] fn is_zero(&self) -> bool {
		self.buffer.iter().all(|x| x.is_zero())
	}
	#[inline(always)] fn zero() -> Self {
		Self { buffer: Vec::new() }
	}
}

impl<R: AddAssign> AddAssign<DiffVector<R>> for DiffVector<R> {
	#[inline(always)]
	fn add_assign(&mut self, mut rhs: Self) {

		// Swap arguments if other is longer.
		if self.buffer.len() < rhs.buffer.len() {
			::std::mem::swap(self, &mut rhs);
		}

		// As other is not longer, apply updates without tests.
		for (index, update) in rhs.buffer.into_iter().enumerate() {
			self.buffer[index] += update;
		}
	}
}

impl<R: Neg<Output=R>+Clone> Neg for DiffVector<R> {
	type Output = DiffVector<<R as Neg>::Output>;
	#[inline(always)]
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