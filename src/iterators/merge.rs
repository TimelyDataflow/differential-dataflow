use std::mem;

// TODO : Version with ordered values and don't-care payloads.

pub trait Merge<I: Iterator> {
    fn merge(self) -> MergeIterator<I>;
}

impl<I: Iterator, I2: Iterator<Item=I>> Merge<I> for I2 where I::Item: Ord {
    fn merge(self) -> MergeIterator<I> {
        MergeIterator::new(self)
    }
}

pub struct MergeIterator<I: Iterator> {
    heap: Vec<(I::Item, I)>,
}

impl<I: Iterator> MergeIterator<I> where I::Item: Ord {

    pub fn new<I2: Iterator<Item=I>>(iters: I2) -> MergeIterator<I> {
        let mut merge = MergeIterator { heap: Vec::new() };
        merge.renew(iters);
        merge
    }

    pub fn renew<I2: Iterator<Item=I>>(&mut self, iters: I2) {
        self.heap.clear();
        for mut iter in iters {
            if let Some(next) = iter.next() {
                self.heap.push((next, iter));
            }
        }

        let len = self.heap.len();
        for i in 0..self.heap.len() {
            self.sift_down(len - i - 1);
        }
    }

    fn sift_down(&mut self, mut index: usize) {
        let mut child = 2 * index + 1;
        while child < self.heap.len() {

            // maybe use other child
            let other = child + 1;
            if other < self.heap.len() && self.heap[child].0 > self.heap[other].0 {
                child = other;
            }

            // compare against the smaller child, continue if it is smaller
            if self.heap[child].0 < self.heap[index].0 {
                self.heap.swap(child, index);
                index = child;
                child = 2 * index + 1;
            }
            else { return; }
        }
    }
}

// the iterator implementation uses the assumed "fact" that each time
// it pops an element from the lead iterator, the next value can only
// be larger. therefore, we only ever need to do sift_down from the root.
impl<I: Iterator> Iterator for MergeIterator<I> where I::Item: Ord {
    type Item = I::Item;
    fn next(&mut self) -> Option<I::Item> {
        if self.heap.len() > 0 {
            let result = if let Some(mut next) = self.heap[0].1.next() {
                mem::swap(&mut next, &mut self.heap[0].0);
                next
            }
            else {
                self.heap.swap_remove(0).0
            };
            self.sift_down(0);
            Some(result)
        }
        else { None }
    }
}

#[test]
fn merge_iterator() {
    let a = vec![3, 4, 6, 9];
    let b = vec![2, 14, 26, 29];
    let c = vec![1, 5, 6, 29];

    let merge = MergeIterator::new(vec![a.into_iter(), b.into_iter(), c.into_iter()].into_iter());

    let result = merge.collect::<Vec<_>>();
    assert_eq!(result, vec![1,2,3,4,5,6,6,9,14,26,29,29]);
}
