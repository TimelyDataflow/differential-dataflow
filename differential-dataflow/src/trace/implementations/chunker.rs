//! Organize streams of data into sorted chunks.

use std::collections::VecDeque;

use columnation::Columnation;
use timely::Container;
use timely::container::{ContainerBuilder, DrainContainer, PushInto, SizableContainer};

use crate::containers::TimelyStack;
use crate::consolidation::{consolidate_updates, ConsolidateLayout};
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

impl<'a, K, V, T, R> PushInto<&'a mut Vec<((K, V), T, R)>> for VecChunker<((K, V), T, R)>
where
    K: Ord + Clone,
    V: Ord + Clone,
    T: Ord + Clone,
    R: Semigroup + Clone,
{
    fn push_into(&mut self, container: &'a mut Vec<((K, V), T, R)>) {
        // Ensure `self.pending` has the desired capacity. We should never have a larger capacity
        // because we don't write more than capacity elements into the buffer.
        // Important: Consolidation requires `pending` to have twice the chunk capacity to
        // amortize its cost. Otherwise, it risks to do quadratic work.
        if self.pending.capacity() < Self::chunk_capacity() * 2 {
            self.pending.reserve(Self::chunk_capacity() * 2 - self.pending.len());
        }

        let mut drain = container.drain(..).peekable();
        while drain.peek().is_some() {
            self.pending.extend((&mut drain).take(self.pending.capacity() - self.pending.len()));
            if self.pending.len() == self.pending.capacity() {
                self.form_chunk();
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

impl<D,T,R> ColumnationChunker<(D, T, R)>
where
    D: Columnation + Ord,
    T: Columnation + Ord,
    R: Columnation + Semigroup,
{
    const BUFFER_SIZE_BYTES: usize = 64 << 10;
    fn chunk_capacity() -> usize {
        let size = ::std::mem::size_of::<(D, T, R)>();
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

impl<'a, D, T, R> PushInto<&'a mut Vec<(D, T, R)>> for ColumnationChunker<(D, T, R)>
where
    D: Columnation + Ord + Clone,
    T: Columnation + Ord + Clone,
    R: Columnation + Semigroup + Clone,
{
    fn push_into(&mut self, container: &'a mut Vec<(D, T, R)>) {
        // Ensure `self.pending` has the desired capacity. We should never have a larger capacity
        // because we don't write more than capacity elements into the buffer.
        if self.pending.capacity() < Self::chunk_capacity() * 2 {
            self.pending.reserve(Self::chunk_capacity() * 2 - self.pending.len());
        }

        let mut drain = container.drain(..).peekable();
        while drain.peek().is_some() {
            self.pending.extend((&mut drain).take(self.pending.capacity() - self.pending.len()));
            if self.pending.len() == self.pending.capacity() {
                self.form_chunk();
            }
        }
    }
}

impl<D, T, R> ContainerBuilder for ColumnationChunker<(D, T, R)>
where
    D: Columnation + Ord + Clone + 'static,
    T: Columnation + Ord + Clone + 'static,
    R: Columnation + Semigroup + Clone + 'static,
{
    type Container = TimelyStack<(D,T,R)>;

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

/// Chunk a stream of `ColContainer` into chains of vectors.
///
/// Accepts `ColContainer<(D, T, R)>` as input, reads elements into an
/// internal `Vec`, sorts, consolidates, and produces `Vec<(D, T, R)>` chunks.
pub struct ColumnarChunker<T> {
    pending: Vec<T>,
    ready: VecDeque<Vec<T>>,
    empty: Option<Vec<T>>,
}

impl<T> Default for ColumnarChunker<T> {
    fn default() -> Self {
        Self {
            pending: Vec::default(),
            ready: VecDeque::default(),
            empty: None,
        }
    }
}

impl<D, T, R> ColumnarChunker<(D, T, R)>
where
    D: Ord,
    T: Ord,
    R: Semigroup,
{
    const BUFFER_SIZE_BYTES: usize = 8 << 10;
    fn chunk_capacity() -> usize {
        let size = ::std::mem::size_of::<(D, T, R)>();
        if size == 0 {
            Self::BUFFER_SIZE_BYTES
        } else if size <= Self::BUFFER_SIZE_BYTES {
            Self::BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }

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

impl<'a, D, T, R> PushInto<&'a mut crate::containers::ColContainer<(D, T, R)>> for ColumnarChunker<(D, T, R)>
where
    D: columnar::Columnar + Ord + Clone,
    T: columnar::Columnar + Ord + Clone,
    R: columnar::Columnar + Semigroup + Clone,
{
    fn push_into(&mut self, container: &'a mut crate::containers::ColContainer<(D, T, R)>) {
        use columnar::{Borrow, Index, Len};
        // Ensure capacity.
        if self.pending.capacity() < Self::chunk_capacity() * 2 {
            self.pending.reserve(Self::chunk_capacity() * 2 - self.pending.len());
        }

        // Read elements from the columnar container into the pending Vec.
        let borrowed = container.container.borrow();
        for i in 0..borrowed.len() {
            let (d, t, r) = borrowed.get(i);
            self.pending.push((D::into_owned(d), T::into_owned(t), R::into_owned(r)));
            if self.pending.len() == self.pending.capacity() {
                self.form_chunk();
            }
        }
        container.clear();
    }
}

impl<D, T, R> ContainerBuilder for ColumnarChunker<(D, T, R)>
where
    D: Ord + Clone + 'static,
    T: Ord + Clone + 'static,
    R: Semigroup + Clone + 'static,
{
    type Container = Vec<(D, T, R)>;

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

/// Chunk a stream of `ColContainer` into chains of `ColContainer`.
///
/// Accumulates input into a staging `ColContainer`, then sorts columnar refs
/// and consolidates into output `ColContainer` chunks — staying columnar throughout.
pub struct ColumnarColChunker<D: columnar::Columnar, T: columnar::Columnar, R: columnar::Columnar> {
    /// Staging area: input gets appended here.
    pending: crate::containers::ColContainer<(D, T, R)>,
    /// Completed, sorted, consolidated chunks.
    ready: VecDeque<crate::containers::ColContainer<(D, T, R)>>,
    /// Scratch space for the empty output.
    empty: Option<crate::containers::ColContainer<(D, T, R)>>,
    /// Reusable index buffer for sorting.
    indices: Vec<usize>,
}

impl<D: columnar::Columnar, T: columnar::Columnar, R: columnar::Columnar> Default for ColumnarColChunker<D, T, R> {
    fn default() -> Self {
        Self {
            pending: Default::default(),
            ready: Default::default(),
            empty: None,
            indices: Vec::new(),
        }
    }
}

impl<D: columnar::Columnar, T: columnar::Columnar, R: columnar::Columnar> ColumnarColChunker<D, T, R> {
    const BUFFER_SIZE_BYTES: usize = 8 << 10;
    fn chunk_capacity() -> usize {
        let size = ::std::mem::size_of::<(D, T, R)>();
        if size == 0 {
            Self::BUFFER_SIZE_BYTES
        } else if size <= Self::BUFFER_SIZE_BYTES {
            Self::BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }
}

impl<D, T, R> ColumnarColChunker<D, T, R>
where
    D: columnar::Columnar,
    T: columnar::Columnar,
    R: columnar::Columnar + Default + Semigroup,
    for<'a> columnar::Ref<'a, D>: Ord,
    for<'a> columnar::Ref<'a, T>: Ord,
    for<'a> (D::Container, T::Container, R::Container): columnar::Push<(columnar::Ref<'a, D>, columnar::Ref<'a, T>, &'a R)>,
{
    /// Sort refs from `pending`, consolidate, push sorted results into `ColContainer` chunks.
    fn form_chunks(&mut self) {
        use columnar::{Borrow, Index, Len, Push};

        // Swap pending out so we can borrow it immutably while pushing to ready.
        let mut source = std::mem::take(&mut self.pending);
        let borrowed = source.container.borrow();
        let len = borrowed.len();
        if len == 0 {
            // Swap the (empty) allocation back.
            self.pending = source;
            return;
        }

        // Reuse the indices buffer: fill with 0..len, sort by (data, time).
        self.indices.clear();
        Extend::extend(&mut self.indices, 0..len);
        self.indices.sort_unstable_by(|&i, &j| {
            let (d1, t1, _) = borrowed.get(i);
            let (d2, t2, _) = borrowed.get(j);
            (d1, t1).cmp(&(d2, t2))
        });

        // Consolidate: merge adjacent equal (d, t) pairs by summing diffs.
        let mut chunk = crate::containers::ColContainer::<(D, T, R)>::default();
        let mut idx = 0;
        while idx < self.indices.len() {
            let (d, t, r) = borrowed.get(self.indices[idx]);
            let mut r_owned = R::into_owned(r);
            idx += 1;
            while idx < self.indices.len() {
                let (d2, t2, r2) = borrowed.get(self.indices[idx]);
                if d == d2 && t == t2 {
                    let r2_owned = R::into_owned(r2);
                    r_owned.plus_equals(&r2_owned);
                    idx += 1;
                } else {
                    break;
                }
            }
            if !r_owned.is_zero() {
                chunk.container.0.push(d);
                chunk.container.1.push(t);
                chunk.container.2.push(&r_owned);
                if chunk.len() >= Self::chunk_capacity() {
                    self.ready.push_back(std::mem::take(&mut chunk));
                }
            }
        }
        if !chunk.is_empty() {
            self.ready.push_back(chunk);
        }

        // Clear and reclaim the source's allocation for next time.
        let _ = borrowed;
        source.clear();
        self.pending = source;
    }
}

impl<'a, D, T, R> PushInto<&'a mut crate::containers::ColContainer<(D, T, R)>> for ColumnarColChunker<D, T, R>
where
    D: columnar::Columnar,
    T: columnar::Columnar,
    R: columnar::Columnar + Default + Semigroup,
    for<'b> columnar::Ref<'b, D>: Ord,
    for<'b> columnar::Ref<'b, T>: Ord,
    for<'b> (D::Container, T::Container, R::Container): columnar::Push<(columnar::Ref<'b, D>, columnar::Ref<'b, T>, &'b R)>,
{
    fn push_into(&mut self, container: &'a mut crate::containers::ColContainer<(D, T, R)>) {
        use columnar::{Borrow, Container, Len};
        // Append input into pending by extending from the source container.
        let borrowed = container.container.borrow();
        let len = borrowed.len();
        self.pending.container.extend_from_self(borrowed, 0..len);
        container.clear();
        // If pending is large enough, sort and chunk.
        if self.pending.len() >= Self::chunk_capacity() * 2 {
            self.form_chunks();
        }
    }
}

impl<D, T, R> ContainerBuilder for ColumnarColChunker<D, T, R>
where
    D: columnar::Columnar + 'static,
    T: columnar::Columnar + 'static,
    R: columnar::Columnar + Default + Semigroup + 'static,
    for<'a> columnar::Ref<'a, D>: Ord,
    for<'a> columnar::Ref<'a, T>: Ord,
    for<'a> (D::Container, T::Container, R::Container): columnar::Push<(columnar::Ref<'a, D>, columnar::Ref<'a, T>, &'a R)>,
{
    type Container = crate::containers::ColContainer<(D, T, R)>;

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
            self.form_chunks();
        }
        if let Some(ready) = self.ready.pop_front() {
            self.empty = Some(ready);
            self.empty.as_mut()
        } else {
            None
        }
    }
}

/// Chunk a stream of containers into chains of vectors.
pub struct ContainerChunker<Output> {
    pending: Output,
    ready: VecDeque<Output>,
    empty: Output,
}

impl<Output: Default> Default for ContainerChunker<Output> {
    fn default() -> Self {
        Self {
            pending: Output::default(),
            ready: VecDeque::default(),
            empty: Output::default(),
        }
    }
}

impl<'a, Input, Output> PushInto<&'a mut Input> for ContainerChunker<Output>
where
    Input: DrainContainer,
    Output: Default
        + SizableContainer
        + ConsolidateLayout
        + PushInto<Input::Item<'a>>,
{
    fn push_into(&mut self, container: &'a mut Input) {
        self.pending.ensure_capacity(&mut None);

        for item in container.drain() {
            self.pending.push_into(item);
            if self.pending.at_capacity() {
                let starting_len = self.pending.len();
                self.pending.consolidate_into(&mut self.empty);
                std::mem::swap(&mut self.pending, &mut self.empty);
                self.empty.clear();
                if self.pending.len() > starting_len / 2 {
                    // Note that we're pushing non-full containers, which is a deviation from
                    // other implementation. The reason for this is that we cannot extract
                    // partial data from `this.pending`. We should revisit this in the future.
                    self.ready.push_back(std::mem::take(&mut self.pending));
                }
            }
        }
    }
}

impl<Output> ContainerBuilder for ContainerChunker<Output>
where
    Output: SizableContainer + ConsolidateLayout + Container,
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
            self.pending.consolidate_into(&mut self.empty);
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
