use std::iter::Peekable;
use std::marker::PhantomData;

pub trait Coalesce<V, I> {
    fn coalesce(self) -> CoalesceIterator<V, I>;
}

impl<V: Ord, I: Iterator<Item=(V, i32)>> Coalesce<V, I> for I {
    fn coalesce(self) -> CoalesceIterator<V, I> {
        CoalesceIterator::new(self)
    }
}

pub struct CoalesceIterator<V: Ord, I: Iterator<Item=(V, i32)>> {
    iter: Peekable<I>,
    phant: PhantomData<V>,
}

impl<V: Ord, I: Iterator<Item=(V, i32)>> CoalesceIterator<V, I> {
    pub fn new(iter: I) -> CoalesceIterator<V, I> {
        CoalesceIterator {
            iter: iter.peekable(),
            phant: PhantomData,
        }
    }
}

impl<V: Ord, I: Iterator<Item=(V, i32)>> Iterator for CoalesceIterator<V, I> {
    type Item = (V, i32);
    fn next(&mut self) -> Option<(V, i32)> {
        loop {
            if let Some((val, mut wgt)) = self.iter.next() {
                while self.iter.peek().map(|&(ref v, _)| v == &val) == Some(true) {
                    wgt += self.iter.next().unwrap().1;
                }
                if wgt != 0 { return Some((val, wgt)); }
            }
            else { return None; }
        }
    }
}
