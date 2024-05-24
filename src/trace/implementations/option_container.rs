//! A container optimized for identical contents.

use crate::trace::cursor::IntoOwned;
use crate::trace::implementations::BatchContainer;

/// A container that effectively represents default values.
///
/// This container is meant to be a minimal non-trivial container,
/// and may be useful in unifying `OrdVal` and `OrdKey` spines.
pub struct OptionContainer<C> {
    /// Number of default items pushed.
    defaults: usize,
    /// Spill-over for non-empty rows.
    container: C,
}

use crate::trace::implementations::containers::Push;
impl<C: BatchContainer> Push<C::OwnedItem> for OptionContainer<C> 
where 
    C: BatchContainer + Push<C::OwnedItem>,
    C::OwnedItem: Default + Ord,
{
    fn push(&mut self, item: C::OwnedItem) {
        if item == Default::default() && self.container.is_empty() {
            self.defaults += 1;
        }
        else {
            self.container.push(item)
        }
    }
}

impl<C> BatchContainer for OptionContainer<C>
where
    C: BatchContainer ,
    C::OwnedItem: Default + Ord,
{
    type OwnedItem = C::OwnedItem;
    type ReadItem<'a> = OptionWrapper<'a, C>;

    fn copy<'a>(&mut self, item: Self::ReadItem<'a>) {
        if item.eq(&IntoOwned::borrow_as(&Default::default())) && self.container.is_empty() {
            self.defaults += 1;
        }
        else {
            if let Some(item) = item.inner {
                self.container.copy(item);
            }
            else {
                self.container.copy(IntoOwned::borrow_as(&Default::default()));
            }
        }
    }
    fn with_capacity(size: usize) -> Self {
        Self {
            defaults: 0,
            container: C::with_capacity(size),
        }
    }
    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        Self {
            defaults: 0,
            container: C::merge_capacity(&cont1.container, &cont2.container),
        }
    }
    fn index(&self, index: usize) -> Self::ReadItem<'_> {
        if index < self.defaults {
            OptionWrapper { inner: None }
        }
        else {
            OptionWrapper { inner: Some(self.container.index(index - self.defaults))}
        }
    }
    fn len(&self) -> usize {
        self.container.len() + self.defaults
    }
}

/// A read wrapper capable of cheaply representing a default value.
pub struct OptionWrapper<'a, C: BatchContainer> {
    inner: Option<C::ReadItem<'a>>,
}

impl<'a, C: BatchContainer> Copy for OptionWrapper<'a, C> { }
impl<'a, C: BatchContainer> Clone for OptionWrapper<'a, C> {
    fn clone(&self) -> Self { *self }
}


use std::cmp::Ordering;
impl<'a, 'b, C: BatchContainer> PartialEq<OptionWrapper<'a, C>> for OptionWrapper<'b, C> 
where 
    C::OwnedItem: Default + Ord,
{
    fn eq(&self, other: &OptionWrapper<'a, C>) -> bool {
        match (&self.inner, &other.inner) {
            (None, None) => true,
            (None, Some(item2)) => item2.eq(&<C::ReadItem<'_> as IntoOwned>::borrow_as(&Default::default())),
            (Some(item1), None) => item1.eq(&<C::ReadItem<'_> as IntoOwned>::borrow_as(&Default::default())),
            (Some(item1), Some(item2)) => item1.eq(item2)
        }
    }
}

impl<'a, C: BatchContainer> Eq for OptionWrapper<'a, C> where C::OwnedItem: Default + Ord { }

impl<'a, 'b, C: BatchContainer> PartialOrd<OptionWrapper<'a, C>> for OptionWrapper<'b, C> where 
C::OwnedItem: Default + Ord,
{
    fn partial_cmp(&self, other: &OptionWrapper<'a, C>) -> Option<Ordering> {
        let default = Default::default();
        match (&self.inner, &other.inner) {
            (None, None) => Some(Ordering::Equal),
            (None, Some(item2)) => item2.partial_cmp(&C::ReadItem::<'_>::borrow_as(&default)).map(|x| x.reverse()),
            (Some(item1), None) => item1.partial_cmp(&C::ReadItem::<'_>::borrow_as(&default)),
            (Some(item1), Some(item2)) => item1.partial_cmp(item2)
        }
    }
}
impl<'a, C: BatchContainer> Ord for OptionWrapper<'a, C> where 
C::OwnedItem: Default + Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}


impl<'a, C: BatchContainer> IntoOwned<'a> for OptionWrapper<'a, C>
where
    C::OwnedItem : Default + Ord,
{
    type Owned = C::OwnedItem;

    fn into_owned(self) -> Self::Owned {
        self.inner.map(|r| r.into_owned()).unwrap_or_else(Default::default)
    }
    fn clone_onto(&self, other: &mut Self::Owned) {
        if let Some(item) = &self.inner {
            item.clone_onto(other)
        } 
        else {
            *other = Default::default();
        }
    }
    fn borrow_as(owned: &'a Self::Owned) -> Self {
        Self {
            inner: Some(IntoOwned::borrow_as(owned))
        }
    }
} 
