//! A container optimized for identical contents.

use crate::trace::cursor::MyTrait;
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

impl<C> BatchContainer for OptionContainer<C> 
where 
    C: BatchContainer,
    C::PushItem: Default + Ord,
{
    type PushItem = C::PushItem;
    type ReadItem<'a> = OptionWrapper<'a, C>;

    fn push(&mut self, item: Self::PushItem) {
        if item == Default::default() && self.container.is_empty() {
            self.defaults += 1;
        }
        else {
            self.container.push(item)
        }
    }
    fn copy<'a>(&mut self, item: Self::ReadItem<'a>) {
        if item.equals(&Default::default()) && self.container.is_empty() {
            self.defaults += 1;
        }
        else {
            if let Some(item) = item.inner {
                self.container.copy(item);
            }
            else {
                self.container.push(Default::default());
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
    C::PushItem: Default + Ord,
{
    fn eq(&self, other: &OptionWrapper<'a, C>) -> bool {
        match (&self.inner, &other.inner) {
            (None, None) => true,
            (None, Some(item2)) => item2.equals(&Default::default()),
            (Some(item1), None) => item1.equals(&Default::default()),
            (Some(item1), Some(item2)) => item1.eq(item2)
        }
    }
}
impl<'a, C: BatchContainer> Eq for OptionWrapper<'a, C> where 
C::PushItem: Default + Ord
{ }
impl<'a, 'b, C: BatchContainer> PartialOrd<OptionWrapper<'a, C>> for OptionWrapper<'b, C> where 
C::PushItem: Default + Ord,
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
C::PushItem: Default + Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}


impl<'a, C: BatchContainer> MyTrait<'a> for OptionWrapper<'a, C> 
where
    C::PushItem : Default + Ord,
{
    type Owned = C::PushItem;

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
    fn compare(&self, other: &Self::Owned) -> std::cmp::Ordering {
        if let Some(item) = &self.inner {
            item.compare(other)
        } 
        else {
            <C::PushItem>::default().cmp(other)
        }
    }
    fn borrow_as(other: &'a Self::Owned) -> Self {
        Self {
            inner: Some(<_>::borrow_as(other))
        }
    }
} 
