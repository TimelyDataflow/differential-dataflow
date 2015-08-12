
/// A run-length decoder, given an iterator over items and an iterator over counts, notices repeats
/// in the items and yields (item, usize) pairs indicating the intended multiplicity.
pub struct Decoder<I1: Iterator, I2: Iterator<Item=usize>> where I1::Item: Eq {
    items: Peekable<I1>,
    counts: I2,
}

impl<I1: Iterator, I2: Iterator<Item=usize>> Iterator for Decoder<I1, I2> where I1::Item: Eq {
    type Item=(I1::Item, usize);
    fn next(&mut self) -> Option<(I1::Item, usize)> {
        self.items.next().map(|item| {
            if &item == self.items.peek() {
                // drop next item
                self.items.next();
                Some((item, self.counts.next().unwrap()))
            }
            else {
                Some((item, 1))
            }
        })
    }
}

// repetitions in items indicate a meaningful number of repetitions in counts.
pub struct Encoder<T> {
    items: Vec<T>,
    counts: Vec<usize>,
}

impl<T: Eq> Encoder {
    pub fn new() -> Encoder<T> { Encoder { items: vec![], counts: vec![] } }
    pub fn push(&mut self, item: T) {
        // if there is a previous equivalent item, just increment the last count.
        if self.items.len() > 0 && item == self.items[self.items.len() - 1] {
            let counts_len = self.counts.len();
            // if this is the first repetition, we need to signal that by repeating the element.
            if self.counts[counts_len - 1] == 1 {
                self.items.push(item);
            }
            self.counts[counts_len - 1] += 1;
        }
        else {
            self.items.push(item);
            // if the previous count is > 1, push a new one to work with.
            // otherwise, just keep that previous one there and hope someone increments it.
            if self.counts.len() > 0 && self.counts[self.counts.len() - 1] > 1 {
                self.counts.push(1);
            }
        }
    }
    pub fn done(self) -> (Vec<T>, Vec<usize>) {
        // pretty harmless to leave the one there, but might as well clean up.
        if self.counts.len() > 0 && self.counts[self.counts.len() - 1] == 1 {
            self.counts.pop();
        }
        (self.items, self.counts)
    }
    pub fn decode(self) -> Decoder
}

#[cfg(test)]
mod tests {

    #[test] fn distinct() { encode_decode(vec![0, 1, 2, 3, 4]); }
    #[test] fn sequence() { encode_decode(vec![0,0,0,0, 1, 2, 2, 2, 0, 0, 3, 4]); }
    #[test] fn repeats() { encode_decode(vec![0,0,0,0]); }
    #[test] fn empty() { encode_decode(vec![]); }

    fn encode_decode<T:Eq+Clone>(items: Vec<T>) {
        let mut encoder = Encoder::new();
        for item in &items {
            encoder.push(item.clone());
        }

        let (i, c) = encode.done();
        let mut decoder = Decoder { items: i.into_iter(), counts: c.into_iter() };
        let results = decoder.flat_map(|i,c| ::std::iter::repeat(i).take(c)).collect::<Vec<_>>();
        assert!(items == results);
    }
}
