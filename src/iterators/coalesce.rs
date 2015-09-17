use std::iter::Peekable;

pub trait Coalesce<I> {
    fn coalesce(self) -> CoalesceIterator<I>;
}

impl<I: Iterator> Coalesce<I> for I {
    fn coalesce(self) -> CoalesceIterator<I> {
        CoalesceIterator::new(self)
    }
}

pub struct CoalesceIterator<I: Iterator> {
    iter: Peekable<I>,
}

impl<I: Iterator> CoalesceIterator<I> {
    pub fn new(iter: I) -> CoalesceIterator<I> {
        CoalesceIterator {
            iter: iter.peekable(),
        }
    }
}

impl<V: Ord, I: Iterator<Item=(V, i32)>> Iterator for CoalesceIterator<I> {
    type Item = (V, i32);
    #[inline]
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
