//! Organize streams of data into sorted chunks.

use std::marker::PhantomData;
use timely::communication::message::RefOrMut;
use timely::Container;
use timely::container::columnation::{Columnation, TimelyStack};
use timely::container::{PushInto, SizableContainer};
use crate::consolidation::{consolidate_updates, ConsolidateContainer, ContainerSorter};
use crate::difference::Semigroup;

/// Behavior to transform streams of data into sorted chunks of regular size.
pub trait Chunker {
    /// Input container type.
    type Input;
    /// Output container type.
    type Output;

    /// Accept a container and absorb its contents. The caller must
    /// call [`extract`] or [`finish`] soon after pushing a container.
    fn push_container(&mut self, container: RefOrMut<Self::Input>);

    /// Extract all read data, leaving unfinished data behind.
    fn extract(&mut self) -> Option<Self::Output>;

    /// Unconditionally extract all data, leaving no unfinished data behind.
    fn finish(&mut self) -> Option<Self::Output>;
}

/// Chunk a stream of vectors into chains of vectors.
pub struct VecChunker<T> {
    pending: Vec<T>,
    ready: Vec<Vec<T>>,
}

impl<T> Default for VecChunker<T> {
    fn default() -> Self {
        Self {
            pending: Vec::default(),
            ready: Vec::default(),
        }
    }
}

impl<T> VecChunker<T> {
    const BUFFER_SIZE_BYTES: usize = 8 << 10;
    fn chunk_capacity(&self) -> usize {
        let size = ::std::mem::size_of::<T>();
        if size == 0 {
            Self::BUFFER_SIZE_BYTES
        } else if size <= Self::BUFFER_SIZE_BYTES {
            Self::BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }

    fn pending_capacity(&self) -> usize {
        self.chunk_capacity() * 2
    }
}

impl<K, V, T, R> Chunker for VecChunker<((K, V), T, R)>
where
    K: Ord + Clone,
    V: Ord + Clone,
    T: Ord + Clone,
    R: Semigroup + Clone,
{
    type Input = Vec<((K, V), T, R)>;
    type Output = Self::Input;

    fn push_container(&mut self, container: RefOrMut<Self::Input>) {
        // Ensure `self.pending` has the desired capacity. We should never have a larger capacity
        // because we don't write more than capacity elements into the buffer.
        if self.pending.capacity() < self.pending_capacity() {
            self.pending.reserve(self.pending_capacity() - self.pending.len());
        }

        // Form chunks from what's in pending.
        // This closure does the following:
        // * If pending is full, consolidate.
        // * If after consolidation it's more than half full, peel off chunks,
        //   leaving behind any partial chunk in pending.
        let form_chunk = |this: &mut Self| {
            if this.pending.len() == this.pending.capacity() {
                consolidate_updates(&mut this.pending);
                if this.pending.len() >= this.chunk_capacity() {
                    while this.pending.len() > this.chunk_capacity() {
                        let mut chunk = Vec::with_capacity(this.chunk_capacity());
                        chunk.extend(this.pending.drain(..chunk.capacity()));
                        this.ready.push(chunk);
                    }
                }
            }
        };

        // `container` is either a shared reference or an owned allocations.
        match container {
            RefOrMut::Ref(vec) => {
                let mut slice = &vec[..];
                while !slice.is_empty() {
                    let (head, tail) = slice.split_at(std::cmp::min(self.pending.capacity() - self.pending.len(), slice.len()));
                    slice = tail;
                    self.pending.extend_from_slice(head);
                    form_chunk(self);
                }
            }
            RefOrMut::Mut(vec) => {
                let mut drain = vec.drain(..).peekable();
                while drain.peek().is_some() {
                    self.pending.extend((&mut drain).take(self.pending.capacity() - self.pending.len()));
                    form_chunk(self);
                }
            }
        }
    }

    fn extract(&mut self) -> Option<Self::Output> {
        self.ready.pop()
    }

    fn finish(&mut self) -> Option<Self::Output> {
        if !self.pending.is_empty() {
            consolidate_updates(&mut self.pending);
            while !self.pending.is_empty() {
                let mut chunk = Vec::with_capacity(self.chunk_capacity());
                chunk.extend(self.pending.drain(..std::cmp::min(self.pending.len(), chunk.capacity())));
                self.ready.push(chunk);
            }
        }
        self.ready.pop()
    }
}

/// Chunk a stream of vectors into chains of vectors.
pub struct ColumnationChunker<T: Columnation> {
    pending: Vec<T>,
    ready: Vec<TimelyStack<T>>,
}

impl<T: Columnation> Default for ColumnationChunker<T> {
    fn default() -> Self {
        Self {
            pending: Vec::default(),
            ready: Vec::default(),
        }
    }
}

impl<T> ColumnationChunker<T>
where
    T: Columnation,
{
    const BUFFER_SIZE_BYTES: usize = 64 << 10;
    fn chunk_capacity(&self) -> usize {
        let size = ::std::mem::size_of::<T>();
        if size == 0 {
            Self::BUFFER_SIZE_BYTES
        } else if size <= Self::BUFFER_SIZE_BYTES {
            Self::BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }

    /// Buffer size for pending updates, currently 2 * [`Self::chunk_capacity`].
    fn pending_capacity(&self) -> usize {
        self.chunk_capacity() * 2
    }
}

impl<K, V, T, R> Chunker for ColumnationChunker<((K, V), T, R)>
where
    K: Columnation + Ord + Clone,
    V: Columnation + Ord + Clone,
    T: Columnation + Ord + Clone,
    R: Columnation + Semigroup + Clone,
{
    type Input = Vec<((K, V), T, R)>;
    type Output = TimelyStack<((K,V),T,R)>;

    fn push_container(&mut self, container: RefOrMut<Self::Input>) {
        // Ensure `self.pending` has the desired capacity. We should never have a larger capacity
        // because we don't write more than capacity elements into the buffer.
        if self.pending.capacity() < self.pending_capacity() {
            self.pending.reserve(self.pending_capacity() - self.pending.len());
        }

        // Form chunks from what's in pending.
        // This closure does the following:
        // * If pending is full, consolidate.
        // * If after consolidation it's more than half full, peel off chunks,
        //   leaving behind any partial chunk in pending.
        let form_chunk = |this: &mut Self| {
            if this.pending.len() == this.pending.capacity() {
                consolidate_updates(&mut this.pending);
                if this.pending.len() >= this.chunk_capacity() {
                    while this.pending.len() > this.chunk_capacity() {
                        let mut chunk = TimelyStack::with_capacity(this.chunk_capacity());
                        for item in this.pending.drain(..chunk.capacity()) {
                            chunk.copy(&item);
                        }
                        this.ready.push(chunk);
                    }
                }
            }
        };

        // `container` is either a shared reference or an owned allocations.
        match container {
            RefOrMut::Ref(vec) => {
                let mut slice = &vec[..];
                while !slice.is_empty() {
                    let (head, tail) = slice.split_at(std::cmp::min(self.pending.capacity() - self.pending.len(), slice.len()));
                    slice = tail;
                    self.pending.extend_from_slice(head);
                    form_chunk(self);
                }
            }
            RefOrMut::Mut(vec) => {
                let mut drain = vec.drain(..).peekable();
                while drain.peek().is_some() {
                    self.pending.extend((&mut drain).take(self.pending.capacity() - self.pending.len()));
                    form_chunk(self);
                }
            }
        }
    }

    fn extract(&mut self) -> Option<Self::Output> {
        self.ready.pop()
    }

    fn finish(&mut self) -> Option<Self::Output> {
        consolidate_updates(&mut self.pending);
        while !self.pending.is_empty() {
            let mut chunk = TimelyStack::with_capacity(self.chunk_capacity());
            for item in self.pending.drain(..std::cmp::min(self.pending.len(), chunk.capacity())) {
                chunk.copy(&item);
            }
            self.ready.push(chunk);
        }
        self.ready.pop()
    }
}

/// Chunk a stream of vectors into chains of vectors.
pub struct ContainerChunker<I, O, Sorter, Consolidator>
where
    I: Container,
    for<'a> O: SizableContainer + PushInto<I::ItemRef<'a>>,
    Sorter: ContainerSorter<O>,
    Consolidator: ConsolidateContainer<O> + ?Sized,
{
    pending: O,
    empty: O,
    ready: Vec<O>,
    sorter: Sorter,
    _marker: PhantomData<(I, Consolidator)>,
}

impl<I, O, Sorter, Consolidator> Default for ContainerChunker<I, O, Sorter, Consolidator>
where
    I: Container,
    for<'a> O: SizableContainer + PushInto<I::ItemRef<'a>>,
    Sorter: ContainerSorter<O> + Default,
    Consolidator: ConsolidateContainer<O> + ?Sized,
{
    fn default() -> Self {
        Self {
            pending: O::default(),
            empty: O::default(),
            ready: Vec::default(),
            sorter: Sorter::default(),
            _marker: PhantomData,
        }
    }
}

impl<I, O, Sorter, Consolidator> Chunker for ContainerChunker<I, O, Sorter, Consolidator>
where
    I: Container,
    for<'a> O: SizableContainer + PushInto<I::ItemRef<'a>>,
    Sorter: ContainerSorter<O>,
    Consolidator: ConsolidateContainer<O>,
{
    type Input = I;
    type Output = O;

    fn push_container(&mut self, container: RefOrMut<Self::Input>) {
        if self.pending.capacity() < O::preferred_capacity() {
            self.pending.reserve(O::preferred_capacity() - self.pending.len());
        }
        // TODO: This uses `IterRef`, which isn't optimal for containers that can move.
        for item in container.iter() {
            self.pending.push(item);
            if self.pending.len() == self.pending.capacity() {
                self.sorter.sort(&mut self.pending);
                Consolidator::consolidate_container(&mut self.pending, &mut self.empty);
                std::mem::swap(&mut self.pending, &mut self.empty);
                self.empty.clear();
                if self.pending.len() > self.pending.capacity() / 2 {
                    self.ready.push(std::mem::take(&mut self.pending));
                }
            }
        }
    }

    fn extract(&mut self) -> Option<Self::Output> {
        self.ready.pop()
    }

    fn finish(&mut self) -> Option<Self::Output> {
        if !self.pending.is_empty() {
            self.sorter.sort(&mut self.pending);
            Consolidator::consolidate_container(&mut self.pending, &mut self.empty);
            std::mem::swap(&mut self.pending, &mut self.empty);
            self.empty.clear();
            if !self.pending.is_empty() {
                self.ready.push(std::mem::take(&mut self.pending));
            }
        }
        self.ready.pop()
    }
}
