//! A type that can be treated as a mathematical ring.

use std::ops::{Add, Sub, Neg, Mul};

use abomonation::Abomonation;
use ::Data;

/// A type that can be treated as a mathematical ring.
pub trait Ring : Add<Self, Output=Self> + Sub<Self, Output=Self> + Neg<Output=Self> + Mul<Self, Output=Self> + ::std::marker::Sized + Data + Copy {
	/// Returns true if the element is the additive identity.
	fn is_zero(&self) -> bool;
	/// The additive identity.
	fn zero() -> Self;
}

impl Ring for isize { 
	#[inline(always)] fn is_zero(&self) -> bool { *self == 0 }
	#[inline(always)] fn zero() -> Self { 0 }
}


/// The ring defined by a pair of ring elements.
#[derive(Copy, Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct RingPair<R1: Ring, R2: Ring> {
	element1: R1,
	element2: R2,
}

impl<R1: Ring, R2: Ring> RingPair<R1, R2> {
	/// Creates a new ring pair from two elements.
	#[inline(always)] pub fn new(elt1: R1, elt2: R2) -> Self {
		RingPair {
			element1: elt1,
			element2: elt2,
		}
	}
}

impl<R1: Ring, R2: Ring> Ring for RingPair<R1, R2> {
	#[inline(always)] fn is_zero(&self) -> bool { self.element1.is_zero() && self.element2.is_zero() }
	#[inline(always)] fn zero() -> Self { RingPair { element1: R1::zero(), element2: R2::zero() } }
}

impl<R1: Ring, R2: Ring> Add<RingPair<R1, R2>> for RingPair<R1, R2> {
	type Output = Self;
	#[inline(always)] fn add(self, rhs: Self) -> Self {
		RingPair { 
			element1: self.element1 + rhs.element1, 
			element2: self.element2 + rhs.element2,
		}
	}
}

impl<R1: Ring, R2: Ring> Sub<RingPair<R1, R2>> for RingPair<R1, R2> {
	type Output = RingPair<R1, R2>;
	#[inline(always)] fn sub(self, rhs: Self) -> Self {
		RingPair { 
			element1: self.element1 - rhs.element1, 
			element2: self.element2 - rhs.element2,
		}
	}
}

impl<R1: Ring, R2: Ring> Neg for RingPair<R1, R2> {
	type Output = Self;
	#[inline(always)] fn neg(self) -> Self {
		RingPair {
			element1: -self.element1,
			element2: -self.element2,
		}
	}
}

impl<R1: Ring, R2: Ring> Mul<RingPair<R1, R2>> for RingPair<R1, R2> {
	type Output = Self;
	#[inline(always)] fn mul(self, rhs: Self) -> Self {
		RingPair { 
			element1: self.element1 * rhs.element1, 
			element2: self.element2 * rhs.element2,
		}
	}
}

impl<R1: Ring, R2: Ring> Abomonation for RingPair<R1, R2> { }
