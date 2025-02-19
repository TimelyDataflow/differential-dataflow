//! Traits for converting between owned and borrowed types.

/// A reference type corresponding to an owned type, supporting conversion in each direction.
///
/// This trait can be implemented by a GAT, and enables owned types to be borrowed as a GAT.
/// This trait is analogous to `ToOwned`, but not as prescriptive. Specifically, it avoids the
/// requirement that the other trait implement `Borrow`, for which a borrow must result in a
/// `&'self Borrowed`, which cannot move the lifetime into a GAT borrowed type.
pub trait IntoOwned<'a> {
    /// Owned type into which this type can be converted.
    type Owned;
    /// Conversion from an instance of this type to the owned type.
    #[must_use]
    fn into_owned(self) -> Self::Owned;
    /// Clones `self` onto an existing instance of the owned type.
    fn clone_onto(self, other: &mut Self::Owned);
    /// Borrows an owned instance as oneself.
    #[must_use]
    fn borrow_as(owned: &'a Self::Owned) -> Self;
}

impl<'a, T: ToOwned + ?Sized> IntoOwned<'a> for &'a T {
    type Owned = T::Owned;
    #[inline]
    fn into_owned(self) -> Self::Owned {
        self.to_owned()
    }
    #[inline]
    fn clone_onto(self, other: &mut Self::Owned) {
        <T as ToOwned>::clone_into(self, other)
    }
    #[inline]
    fn borrow_as(owned: &'a Self::Owned) -> Self {
        std::borrow::Borrow::borrow(owned)
    }
}

impl<'a, T, E> IntoOwned<'a> for Result<T, E>
where
    T: IntoOwned<'a>,
    E: IntoOwned<'a>,
{
    type Owned = Result<T::Owned, E::Owned>;

    #[inline]
    fn into_owned(self) -> Self::Owned {
        self.map(T::into_owned).map_err(E::into_owned)
    }

    #[inline]
    fn clone_onto(self, other: &mut Self::Owned) {
        match (self, other) {
            (Ok(item), Ok(target)) => T::clone_onto(item, target),
            (Err(item), Err(target)) => E::clone_onto(item, target),
            (Ok(item), target) => *target = Ok(T::into_owned(item)),
            (Err(item), target) => *target = Err(E::into_owned(item)),
        }
    }

    #[inline]
    fn borrow_as(owned: &'a Self::Owned) -> Self {
        owned.as_ref().map(T::borrow_as).map_err(E::borrow_as)
    }
}

impl<'a, T> IntoOwned<'a> for Option<T>
where
    T: IntoOwned<'a>,
{
    type Owned = Option<T::Owned>;

    #[inline]
    fn into_owned(self) -> Self::Owned {
        self.map(IntoOwned::into_owned)
    }

    #[inline]
    fn clone_onto(self, other: &mut Self::Owned) {
        match (self, other) {
            (Some(item), Some(target)) => T::clone_onto(item, target),
            (Some(item), target) => *target = Some(T::into_owned(item)),
            (None, target) => *target = None,
        }
    }

    #[inline]
    fn borrow_as(owned: &'a Self::Owned) -> Self {
        owned.as_ref().map(T::borrow_as)
    }
}

mod tuple {
    use paste::paste;

    macro_rules! tuple {
        ($($name:ident)+) => (paste! {

            #[allow(non_camel_case_types)]
            #[allow(non_snake_case)]
            impl<'a, $($name),*> crate::IntoOwned<'a> for ($($name,)*)
            where
                $($name: crate::IntoOwned<'a>),*
            {
                type Owned = ($($name::Owned,)*);

                #[inline]
                fn into_owned(self) -> Self::Owned {
                    let ($($name,)*) = self;
                    (
                        $($name.into_owned(),)*
                    )
                }

                #[inline]
                fn clone_onto(self, other: &mut Self::Owned) {
                    let ($($name,)*) = self;
                    let ($([<$name _other>],)*) = other;
                    $($name.clone_onto([<$name _other>]);)*
                }

                #[inline]
                fn borrow_as(owned: &'a Self::Owned) -> Self {
                    let ($($name,)*) = owned;
                    (
                        $($name::borrow_as($name),)*
                    )
                }
            }
        })
    }

    tuple!(A);
    tuple!(A B);
    tuple!(A B C);
    tuple!(A B C D);
    tuple!(A B C D E);
    tuple!(A B C D E F);
    tuple!(A B C D E F G);
    tuple!(A B C D E F G H);
    tuple!(A B C D E F G H I);
    tuple!(A B C D E F G H I J);
    tuple!(A B C D E F G H I J K);
    tuple!(A B C D E F G H I J K L);
    tuple!(A B C D E F G H I J K L M);
    tuple!(A B C D E F G H I J K L M N);
    tuple!(A B C D E F G H I J K L M N O);
    tuple!(A B C D E F G H I J K L M N O P);
}

mod primitive {
    macro_rules! implement_for {
    ($index_type:ty) => {
        impl<'a> crate::IntoOwned<'a> for $index_type {
            type Owned = $index_type;

            #[inline]
            fn into_owned(self) -> Self::Owned {
                self
            }

            #[inline]
            fn clone_onto(self, other: &mut Self::Owned) {
                *other = self;
            }

            #[inline]
            fn borrow_as(owned: &'a Self::Owned) -> Self {
                *owned
            }
        }
    };
}

    implement_for!(());
    implement_for!(bool);
    implement_for!(char);

    implement_for!(u8);
    implement_for!(u16);
    implement_for!(u32);
    implement_for!(u64);
    implement_for!(u128);
    implement_for!(usize);

    implement_for!(i8);
    implement_for!(i16);
    implement_for!(i32);
    implement_for!(i64);
    implement_for!(i128);
    implement_for!(isize);

    implement_for!(f32);
    implement_for!(f64);

    implement_for!(std::num::Wrapping<i8>);
    implement_for!(std::num::Wrapping<i16>);
    implement_for!(std::num::Wrapping<i32>);
    implement_for!(std::num::Wrapping<i64>);
    implement_for!(std::num::Wrapping<i128>);
    implement_for!(std::num::Wrapping<isize>);

    implement_for!(std::time::Duration);

}