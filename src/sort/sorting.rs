use std::mem;
use timely::drain::DrainExt;

pub trait Unsigned {
    fn bytes() -> usize;
    fn as_u64(&self) -> u64;
}

impl Unsigned for  u8 { fn bytes() -> usize { 1 } fn as_u64(&self) -> u64 { *self as u64 } }
impl Unsigned for u16 { fn bytes() -> usize { 2 } fn as_u64(&self) -> u64 { *self as u64 } }
impl Unsigned for u32 { fn bytes() -> usize { 4 } fn as_u64(&self) -> u64 { *self as u64 } }
impl Unsigned for u64 { fn bytes() -> usize { 8 } fn as_u64(&self) -> u64 { *self as u64 } }

pub struct RadixSorter<T> {
    shuffler: RadixShuffler<T>,
}

impl<T> RadixSorter<T> {
    pub fn new() -> RadixSorter<T> {
        RadixSorter {
            shuffler: RadixShuffler::new(),
        }
    }
    #[inline]
    pub fn push<U: Unsigned, F: Fn(&T)->U>(&mut self, element: T, function: &F) {
        self.shuffler.push(element, |x| (function(x).as_u64() % 256) as u8);
    }
    pub fn finish<U: Unsigned, F: Fn(&T)->U>(&mut self, function: &F) -> Vec<Vec<T>> {
        let mut sorted = self.shuffler.finish(|x| (function(x).as_u64() % 256) as u8);
        for byte in 1..<U as Unsigned>::bytes() {
            sorted = self.reshuffle(sorted, |x| ((function(x).as_u64() >> byte) % 256) as u8);
        }
        sorted
    }
    fn reshuffle<U: Unsigned, F: Fn(&T)->U>(&mut self, buffers: Vec<Vec<T>>, function: &F) -> Vec<Vec<T>> {
        for buffer in buffers.into_iter() {
            self.shuffler.push_batch(buffer, function);
        }
        self.shuffler.finish(function)
    }
}

pub struct RadixShuffler<T> {
    staged: Vec<T>,     // ideally: 256 * number of T element per cache line. using 2048 for now.
    counts: [u8; 256],

    fronts: Vec<Vec<T>>,
    buffers: Vec<Vec<Vec<T>>>, // for each byte, a list of segments
    stashed: Vec<Vec<T>>,      // spare segments
}

impl<T> RadixShuffler<T> {
    fn new() -> RadixShuffler {
        RadixShuffler {
            staged: Vec::with_capacity(256 * 8),    // really want this 64 byte aligned.
            counts: [0; 256],
            buffers: vec![],
            stashed: vec![],
        }
    }
    fn push_batch<F: Fn(&T)->u8>(&mut self, elements: Vec<T>, function: &F) {
        for element in elements.drain_temp() {
            self.push(element);
        }

        self.stashed.push(elements);
    }
    #[inline]
    fn push<F: Fn(&T)->u8>(&mut self, element: T, function: &F) {

        let byte = function(&element) as usize;

        // write the element to our scratch buffer space and consider it taken care of.
        ::std::ptr::write(self.staged.as_mut_ptr().offset(8 * byte + self.counts[byte]), element);
        ::std::mem::forget(element);

        self.counts[byte] += 1;
        if self.counts[byte] == 8 {
            for i in 0..8 {
                self.fronts[byte].push(::std::ptr::read(self.staged.as_mut_ptr().offset(8 * byte + i)));
            }

            if self.fronts[byte].len() == self.fronts[byte].capacity() {
                let replacement = self.stashed.pop().unwrap_or_else(|| Vec::with_capacity(1024));
                let complete = ::std::mem::replace(&mut self.fronts[byte], replacement);
                self.buffers[byte].push(complete);
            }
            self.counts[byte] = 0;
        }
    }
    fn finish(&mut self) -> Vec<Vec<T>> {

        for byte in 0..256 {
            for i in 0..self.counts[byte] {
                self.fronts[byte].push(::std::ptr::read(self.staged.as_mut_ptr().offset(8 * byte + i)));
            }

            if self.fronts[byte].len() > 0 {
                let replacement = self.stashed.pop().unwrap_or_else(|| Vec::with_capacity(1024));
                let complete = ::std::mem::replace(&mut self.fronts[byte], Vec::new());
                self.buffers[byte].push(complete);
            }
        }

        let mut result = vec![];
        for byte in 0..256 {
            result.extend(self.buffers.drain_temp());
        }
        result
    }
}
