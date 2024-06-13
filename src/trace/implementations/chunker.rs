//! Organize streams of data into sorted chunks.

use std::collections::VecDeque;
use timely::communication::message::RefOrMut;
use timely::Container;
use timely::container::columnation::{Columnation, TimelyStack};
use timely::container::{ContainerBuilder, PushInto, SizableContainer};
use crate::consolidation::{consolidate_updates, consolidate_container, ConsolidateLayout};
use crate::difference::Semigroup;

/// Chunk a stream of vectors into chains of vectors.
pub struct VecChunker<T> {
    pending: Vec<T>,
    ready: VecDeque<Vec<T>>,
    empty: Option<Vec<T>>,
}

impl<T> Default for VecChunker<T> {
    fn default() -> Self {
        Self {
            pending: Vec::default(),
            ready: VecDeque::default(),
            empty: None,
        }
    }
}

impl<K, V, T, R> VecChunker<((K, V), T, R)>
where
    K: Ord,
    V: Ord,
    T: Ord,
    R: Semigroup,
{
    const BUFFER_SIZE_BYTES: usize = 8 << 10;
    fn chunk_capacity() -> usize {
        let size = ::std::mem::size_of::<((K, V), T, R)>();
        if size == 0 {
            Self::BUFFER_SIZE_BYTES
        } else if size <= Self::BUFFER_SIZE_BYTES {
            Self::BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }

    /// Form chunks out of pending data, if needed. This function is meant to be applied to
    /// potentially full buffers, and ensures that if the buffer was full when called it is at most
    /// half full when the function returns.
    ///
    /// `form_chunk` does the following:
    /// * If pending is full, consolidate.
    /// * If after consolidation it's more than half full, peel off chunks,
    ///   leaving behind any partial chunk in pending.
    fn form_chunk(&mut self) {
        consolidate_updates(&mut self.pending);
        if self.pending.len() >= Self::chunk_capacity() {
            while self.pending.len() > Self::chunk_capacity() {
                let mut chunk = Vec::with_capacity(Self::chunk_capacity());
                chunk.extend(self.pending.drain(..chunk.capacity()));
                self.ready.push_back(chunk);
            }
        }
    }
}

impl<'a, K, V, T, R> PushInto<RefOrMut<'a, Vec<((K, V), T, R)>>> for VecChunker<((K, V), T, R)>
where
    K: Ord + Clone,
    V: Ord + Clone,
    T: Ord + Clone,
    R: Semigroup + Clone,
{
    fn push_into(&mut self, container: RefOrMut<'a, Vec<((K, V), T, R)>>) {
        // Ensure `self.pending` has the desired capacity. We should never have a larger capacity
        // because we don't write more than capacity elements into the buffer.
        // Important: Consolidation requires `pending` to have twice the chunk capacity to
        // amortize its cost. Otherwise, it risks to do quadratic work.
        if self.pending.capacity() < Self::chunk_capacity() * 2 {
            self.pending.reserve(Self::chunk_capacity() * 2 - self.pending.len());
        }

        // `container` is either a shared reference or an owned allocations.
        match container {
            RefOrMut::Ref(vec) => {
                let mut slice = &vec[..];
                while !slice.is_empty() {
                    let (head, tail) = slice.split_at(std::cmp::min(self.pending.capacity() - self.pending.len(), slice.len()));
                    slice = tail;
                    self.pending.extend_from_slice(head);
                    if self.pending.len() == self.pending.capacity() {
                        self.form_chunk();
                    }
                }
            }
            RefOrMut::Mut(vec) => {
                let mut drain = vec.drain(..).peekable();
                while drain.peek().is_some() {
                    self.pending.extend((&mut drain).take(self.pending.capacity() - self.pending.len()));
                    if self.pending.len() == self.pending.capacity() {
                        self.form_chunk();
                    }
                }
            }
        }
    }
}

impl<K, V, T, R> ContainerBuilder for VecChunker<((K, V), T, R)>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    T: Ord + Clone + 'static,
    R: Semigroup + Clone + 'static,
{
    type Container = Vec<((K, V), T, R)>;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(ready) = self.ready.pop_front() {
            self.empty = Some(ready);
            self.empty.as_mut()
        } else {
            None
        }
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        if !self.pending.is_empty() {
            consolidate_updates(&mut self.pending);
            while !self.pending.is_empty() {
                let mut chunk = Vec::with_capacity(Self::chunk_capacity());
                chunk.extend(self.pending.drain(..std::cmp::min(self.pending.len(), chunk.capacity())));
                self.ready.push_back(chunk);
            }
        }
        self.empty = self.ready.pop_front();
        self.empty.as_mut()
    }
}

/// Chunk a stream of vectors into chains of vectors.
pub struct ColumnationChunker<T: Columnation> {
    pending: Vec<T>,
    ready: VecDeque<TimelyStack<T>>,
    empty: Option<TimelyStack<T>>,
}

impl<T: Columnation> Default for ColumnationChunker<T> {
    fn default() -> Self {
        Self {
            pending: Vec::default(),
            ready: VecDeque::default(),
            empty: None,
        }
    }
}

impl<K,V,T,R> ColumnationChunker<((K, V), T, R)>
where
    K: Columnation + Ord,
    V: Columnation + Ord,
    T: Columnation + Ord,
    R: Columnation + Semigroup,
{
    const BUFFER_SIZE_BYTES: usize = 64 << 10;
    fn chunk_capacity() -> usize {
        let size = ::std::mem::size_of::<((K, V), T, R)>();
        if size == 0 {
            Self::BUFFER_SIZE_BYTES
        } else if size <= Self::BUFFER_SIZE_BYTES {
            Self::BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }

    /// Form chunks out of pending data, if needed. This function is meant to be applied to
    /// potentially full buffers, and ensures that if the buffer was full when called it is at most
    /// half full when the function returns.
    ///
    /// `form_chunk` does the following:
    /// * If pending is full, consolidate.
    /// * If after consolidation it's more than half full, peel off chunks,
    ///   leaving behind any partial chunk in pending.
    fn form_chunk(&mut self) {
        consolidate_updates(&mut self.pending);
        if self.pending.len() >= Self::chunk_capacity() {
            while self.pending.len() > Self::chunk_capacity() {
                let mut chunk = TimelyStack::with_capacity(Self::chunk_capacity());
                for item in self.pending.drain(..chunk.capacity()) {
                    chunk.copy(&item);
                }
                self.ready.push_back(chunk);
            }
        }
    }
}

impl<'a, K, V, T, R> PushInto<RefOrMut<'a, Vec<((K, V), T, R)>>> for ColumnationChunker<((K, V), T, R)>
where
    K: Columnation + Ord + Clone,
    V: Columnation + Ord + Clone,
    T: Columnation + Ord + Clone,
    R: Columnation + Semigroup + Clone,
{
    fn push_into(&mut self, container: RefOrMut<'a, Vec<((K, V), T, R)>>) {
        // Ensure `self.pending` has the desired capacity. We should never have a larger capacity
        // because we don't write more than capacity elements into the buffer.
        if self.pending.capacity() < Self::chunk_capacity() * 2 {
            self.pending.reserve(Self::chunk_capacity() * 2 - self.pending.len());
        }

        // `container` is either a shared reference or an owned allocations.
        match container {
            RefOrMut::Ref(vec) => {
                let mut slice = &vec[..];
                while !slice.is_empty() {
                    let (head, tail) = slice.split_at(std::cmp::min(self.pending.capacity() - self.pending.len(), slice.len()));
                    slice = tail;
                    self.pending.extend_from_slice(head);
                    if self.pending.len() == self.pending.capacity() {
                        self.form_chunk();
                    }
                }
            }
            RefOrMut::Mut(vec) => {
                let mut drain = vec.drain(..).peekable();
                while drain.peek().is_some() {
                    self.pending.extend((&mut drain).take(self.pending.capacity() - self.pending.len()));
                    if self.pending.len() == self.pending.capacity() {
                        self.form_chunk();
                    }
                }
            }
        }
    }
}

impl<K, V, T, R> ContainerBuilder for ColumnationChunker<((K, V), T, R)>
where
    K: Columnation + Ord + Clone + 'static,
    V: Columnation + Ord + Clone + 'static,
    T: Columnation + Ord + Clone + 'static,
    R: Columnation + Semigroup + Clone + 'static,
{
    type Container = TimelyStack<((K,V),T,R)>;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(ready) = self.ready.pop_front() {
            self.empty = Some(ready);
            self.empty.as_mut()
        } else {
            None
        }
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        consolidate_updates(&mut self.pending);
        while !self.pending.is_empty() {
            let mut chunk = TimelyStack::with_capacity(Self::chunk_capacity());
            for item in self.pending.drain(..std::cmp::min(self.pending.len(), chunk.capacity())) {
                chunk.copy(&item);
            }
            self.ready.push_back(chunk);
        }
        self.empty = self.ready.pop_front();
        self.empty.as_mut()
    }
}

/// Chunk a stream of containers into chains of vectors.
pub struct ContainerChunker<Output> {
    pending: Output,
    ready: VecDeque<Output>,
    empty: Output,
}

impl<Output> Default for ContainerChunker<Output>
where
    Output: Default,
{
    fn default() -> Self {
        Self {
            pending: Output::default(),
            ready: VecDeque::default(),
            empty: Output::default(),
        }
    }
}

impl<'a, Input, Output> PushInto<RefOrMut<'a, Input>> for ContainerChunker<Output>
where
    Input: Container,
    Output: SizableContainer
        + ConsolidateLayout
        + PushInto<Input::Item<'a>>
        + PushInto<Input::ItemRef<'a>>,
{
    fn push_into(&mut self, container: RefOrMut<'a, Input>) {
        if self.pending.capacity() < Output::preferred_capacity() {
            self.pending.reserve(Output::preferred_capacity() - self.pending.len());
        }
        let form_batch = |this: &mut Self| {
            if this.pending.len() == this.pending.capacity() {
                consolidate_container(&mut this.pending, &mut this.empty);
                std::mem::swap(&mut this.pending, &mut this.empty);
                this.empty.clear();
                if this.pending.len() > this.pending.capacity() / 2 {
                    // Note that we're pushing non-full containers, which is a deviation from
                    // other implementation. The reason for this is that we cannot extract
                    // partial data from `this.pending`. We should revisit this in the future.
                    this.ready.push_back(std::mem::take(&mut this.pending));
                }
            }
        };
        match container {
            RefOrMut::Ref(container) => {
                for item in container.iter() {
                    self.pending.push(item);
                    form_batch(self);
                }
            }
            RefOrMut::Mut(container) => {
                for item in container.drain() {
                    self.pending.push(item);
                    form_batch(self);
                }
            }
        }
    }
}

impl<Output> ContainerBuilder for ContainerChunker<Output>
where
    Output: SizableContainer + ConsolidateLayout,
{
    type Container = Output;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(ready) = self.ready.pop_front() {
            self.empty = ready;
            Some(&mut self.empty)
        } else {
            None
        }
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        if !self.pending.is_empty() {
            consolidate_container(&mut self.pending, &mut self.empty);
            std::mem::swap(&mut self.pending, &mut self.empty);
            self.empty.clear();
            if !self.pending.is_empty() {
                self.ready.push_back(std::mem::take(&mut self.pending));
            }
        }
        if let Some(ready) = self.ready.pop_front() {
            self.empty = ready;
            Some(&mut self.empty)
        } else {
            None
        }
    }
}
