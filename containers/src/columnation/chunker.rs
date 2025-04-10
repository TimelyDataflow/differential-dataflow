use std::collections::VecDeque;
use columnation::Columnation;
use timely::container::{ContainerBuilder, PushInto};
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;

use crate::columnation::TimelyStack;

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

