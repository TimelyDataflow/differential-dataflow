//! Organize streams of data into sorted chunks.

use std::collections::VecDeque;
use std::marker::PhantomData;
use timely::communication::message::RefOrMut;
use timely::Container;
use timely::container::columnation::{Columnation, TimelyStack};
use timely::container::{PushInto, SizableContainer};
use crate::consolidation::{consolidate_updates, ConsolidateContainer};
use crate::difference::Semigroup;

/// Behavior to transform streams of data into sorted chunks of regular size.
pub trait Chunker {
    /// Input type.
    type Input;
    /// Output type.
    type Output;

    /// Accept a container and absorb its contents. The caller must
    /// call [`extract`] or [`finish`] soon after pushing a container.
    fn push_container(&mut self, container: RefOrMut<Self::Input>);

    /// Extract ready data, leaving unfinished data behind.
    ///
    /// Should be called repeatedly until it returns `None`, which indicates that there is no
    /// more ready data.
    fn extract(&mut self) -> Option<Self::Output>;

    /// Unconditionally extract all data, leaving no unfinished data behind.
    ///
    /// Should be called repeatedly until it returns `None`, which indicates that there is no
    /// more data.
    fn finish(&mut self) -> Option<Self::Output>;
}

/// Chunk a stream of vectors into chains of vectors.
pub struct VecChunker<T> {
    pending: Vec<T>,
    ready: VecDeque<Vec<T>>,
}

impl<T> Default for VecChunker<T> {
    fn default() -> Self {
        Self {
            pending: Vec::default(),
            ready: VecDeque::default(),
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

    fn extract(&mut self) -> Option<Self::Output> {
        self.ready.pop_front()
    }

    fn finish(&mut self) -> Option<Self::Output> {
        if !self.pending.is_empty() {
            consolidate_updates(&mut self.pending);
            while !self.pending.is_empty() {
                let mut chunk = Vec::with_capacity(Self::chunk_capacity());
                chunk.extend(self.pending.drain(..std::cmp::min(self.pending.len(), chunk.capacity())));
                self.ready.push_back(chunk);
            }
        }
        self.ready.pop_front()
    }
}

/// Chunk a stream of vectors into chains of vectors.
pub struct ColumnationChunker<T: Columnation> {
    pending: Vec<T>,
    ready: VecDeque<TimelyStack<T>>,
}

impl<T: Columnation> Default for ColumnationChunker<T> {
    fn default() -> Self {
        Self {
            pending: Vec::default(),
            ready: VecDeque::default(),
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

    fn extract(&mut self) -> Option<Self::Output> {
        self.ready.pop_front()
    }

    fn finish(&mut self) -> Option<Self::Output> {
        consolidate_updates(&mut self.pending);
        while !self.pending.is_empty() {
            let mut chunk = TimelyStack::with_capacity(Self::chunk_capacity());
            for item in self.pending.drain(..std::cmp::min(self.pending.len(), chunk.capacity())) {
                chunk.copy(&item);
            }
            self.ready.push_back(chunk);
        }
        self.ready.pop_front()
    }
}

/// Chunk a stream of vectors into chains of vectors.
pub struct ContainerChunker<Input, Output, Consolidator>
where
    Input: Container,
    for<'a> Output: SizableContainer + PushInto<Input::ItemRef<'a>>,
    Consolidator: ConsolidateContainer<Output>,
{
    pending: Output,
    empty: Output,
    ready: Vec<Output>,
    consolidator: Consolidator,
    _marker: PhantomData<(Input, Consolidator)>,
}

impl<Input, Output, Consolidator> Default for ContainerChunker<Input, Output, Consolidator>
where
    Input: Container,
    for<'a> Output: SizableContainer + PushInto<Input::ItemRef<'a>>,
    Consolidator: ConsolidateContainer<Output> + Default,
{
    fn default() -> Self {
        Self {
            pending: Output::default(),
            empty: Output::default(),
            ready: Vec::default(),
            consolidator: Consolidator::default(),
            _marker: PhantomData,
        }
    }
}

impl<Input, Output, Consolidator> Chunker for ContainerChunker<Input, Output, Consolidator>
where
    Input: Container,
    for<'a> Output: SizableContainer + PushInto<Input::ItemRef<'a>>,
    Consolidator: ConsolidateContainer<Output>,
{
    type Input = Input;
    type Output = Output;

    fn push_container(&mut self, container: RefOrMut<Self::Input>) {
        if self.pending.capacity() < Output::preferred_capacity() {
            self.pending.reserve(Output::preferred_capacity() - self.pending.len());
        }
        // TODO: This uses `IterRef`, which isn't optimal for containers that can move.
        for item in container.iter() {
            self.pending.push(item);
            if self.pending.len() == self.pending.capacity() {
                self.consolidator.consolidate(&mut self.pending, &mut self.empty);
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
            self.consolidator.consolidate(&mut self.pending, &mut self.empty);
            std::mem::swap(&mut self.pending, &mut self.empty);
            self.empty.clear();
            if !self.pending.is_empty() {
                self.ready.push(std::mem::take(&mut self.pending));
            }
        }
        self.ready.pop()
    }
}
