//! A slice container that Huffman encodes its contents.

use std::collections::BTreeMap;

use crate::trace::implementations::{BatchContainer, OffsetList};

use self::wrapper::Wrapped;
use self::encoded::Encoded;
use self::huffman::Huffman;

/// A container that contains slices `[B]` as items.
pub struct HuffmanContainer<B: Ord+Clone> {
    /// Either encoded data or raw data.
    inner: Result<(Huffman<B>, Vec<u8>), Vec<B>>,
    /// Offsets that bound each contained slice.
    ///
    /// The length will be one greater than the number of contained items.
    offsets: OffsetList,
    /// Counts of the number of each pattern we've seen.
    stats: BTreeMap<B, i64>
}

impl<B> HuffmanContainer<B>
where
    B: Ord + Clone,
{
    /// Prints statistics about encoded containers.
    pub fn print(&self) {
        if let Ok((_huff, bytes)) = &self.inner {
            println!("Bytes: {:?}, Symbols: {:?}", bytes.len(), self.stats.values().sum::<i64>());
        }
    }
}

use crate::trace::implementations::containers::Push;
impl<B: Ord + Clone + 'static> Push<Vec<B>> for HuffmanContainer<B> {
    fn push(&mut self, item: Vec<B>) {
        for x in item.iter() { *self.stats.entry(x.clone()).or_insert(0) += 1; }
        match &mut self.inner {
            Ok((huffman, bytes)) => {
                bytes.extend(huffman.encode(item.iter()));
                self.offsets.push(bytes.len());
            },
            Err(raw) => { 
                raw.extend(item); 
                self.offsets.push(raw.len());
            }
        }
    }
}

impl<B: Ord + Clone + 'static> BatchContainer for HuffmanContainer<B> {
    type OwnedItem = Vec<B>;
    type ReadItem<'a> = Wrapped<'a, B>;

    fn copy(&mut self, item: Self::ReadItem<'_>) {
        match item.decode() {
            Ok(decoded) => {
                for x in decoded { *self.stats.entry(x.clone()).or_insert(0) += 1; }

            },
            Err(symbols) => {
                for x in symbols.iter() { *self.stats.entry(x.clone()).or_insert(0) += 1; }
            }
        }
        match (item.decode(), &mut self.inner) {
            (Ok(decoded), Ok((huffman, bytes))) => {
                bytes.extend(huffman.encode(decoded));
                self.offsets.push(bytes.len());
            }
            (Ok(decoded), Err(raw)) => {
                raw.extend(decoded.cloned());
                self.offsets.push(raw.len());
            }
            (Err(symbols), Ok((huffman, bytes))) => { 
                bytes.extend(huffman.encode(symbols.iter()));
                self.offsets.push(bytes.len());
            }
            (Err(symbols), Err(raw)) => { 
                raw.extend(symbols.iter().cloned());
                self.offsets.push(raw.len());
            }
        }
    }
    fn copy_range(&mut self, other: &Self, start: usize, end: usize) {
        for index in start .. end {
            self.copy(other.index(index));
        }
    }
    fn with_capacity(size: usize) -> Self {
        let mut offsets = OffsetList::with_capacity(size + 1);
        offsets.push(0);
        Self {
            inner: Err(Vec::with_capacity(size)),
            offsets,
            stats: Default::default(),
        }
    }
    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {

        if cont1.len() > 0 { cont1.print(); }
        if cont2.len() > 0 { cont2.print(); }

        let mut counts = BTreeMap::default();
        for (symbol, count) in cont1.stats.iter() {
            *counts.entry(symbol.clone()).or_insert(0) += count;
        }
        for (symbol, count) in cont2.stats.iter() {
            *counts.entry(symbol.clone()).or_insert(0) += count;
        }

        let bytes = Vec::with_capacity(counts.values().cloned().sum::<i64>() as usize);
        let huffman = Huffman::create_from(counts);
        let inner = Ok((huffman, bytes));
        // : Err(Vec::with_capacity(length))

        let length = cont1.offsets.len() + cont2.offsets.len() - 2;
        let mut offsets = OffsetList::with_capacity(length + 1);
        offsets.push(0);
        Self {
            inner,
            offsets,
            stats: Default::default(),
        }
    }
    fn index(&self, index: usize) -> Self::ReadItem<'_> {
        let lower = self.offsets.index(index);
        let upper = self.offsets.index(index+1);
        match &self.inner {
            Ok((huffman, bytes)) => Wrapped::encoded(Encoded::new(huffman, &bytes[lower .. upper])),
            Err(raw) => Wrapped::decoded(&raw[lower .. upper]),
        }
    }
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }
}
/// Default implementation introduces a first offset.
impl<B: Ord+Clone> Default for HuffmanContainer<B> {
    fn default() -> Self {
        let mut offsets = OffsetList::with_capacity(1);
        offsets.push(0);
        Self {
            inner: Err(Vec::new()),
            offsets,
            stats: Default::default(),
        }
    }
}

mod wrapper {

    use crate::trace::IntoOwned;
    use super::Encoded;

    pub struct Wrapped<'a, B: Ord> {
        inner: Result<Encoded<'a, B>, &'a [B]>,
    }

    impl<'a, B: Ord> Wrapped<'a, B> {
        /// Returns either a decoding iterator, or just the bytes themselves.
        pub fn decode(&'a self) -> Result<impl Iterator<Item=&'a B> + 'a, &'a [B]> {
            match &self.inner {
                Ok(encoded) => Ok(encoded.decode()),
                Err(symbols) => Err(symbols),
            }
        }
        /// A wrapper around an encoded sequence.
        pub fn encoded(e: Encoded<'a, B>) -> Self { Self { inner: Ok(e) } }
        /// A wrapper around a decoded sequence.
        pub fn decoded(d: &'a [B]) -> Self { Self { inner: Err(d) } }
    }

    impl<'a, B: Ord> Copy for Wrapped<'a, B> { }
    impl<'a, B: Ord> Clone for Wrapped<'a, B> {
        fn clone(&self) -> Self { *self }
    }

    use std::cmp::Ordering;
    impl<'a, 'b, B: Ord> PartialEq<Wrapped<'a, B>> for Wrapped<'b, B> {
        fn eq(&self, other: &Wrapped<'a, B>) -> bool {
            match (self.decode(), other.decode()) {
                (Ok(decode1), Ok(decode2)) => decode1.eq(decode2),
                (Ok(decode1), Err(bytes2)) => decode1.eq(bytes2.iter()),
                (Err(bytes1), Ok(decode2)) => bytes1.iter().eq(decode2),
                (Err(bytes1), Err(bytes2)) => bytes1.eq(bytes2),
            }
        }
    }
    impl<'a, B: Ord> Eq for Wrapped<'a, B> { }
    impl<'a, 'b, B: Ord> PartialOrd<Wrapped<'a, B>> for Wrapped<'b, B> {
        fn partial_cmp(&self, other: &Wrapped<'a, B>) -> Option<Ordering> {
            match (self.decode(), other.decode()) {
                (Ok(decode1), Ok(decode2)) => decode1.partial_cmp(decode2),
                (Ok(decode1), Err(bytes2)) => decode1.partial_cmp(bytes2.iter()),
                (Err(bytes1), Ok(decode2)) => bytes1.iter().partial_cmp(decode2),
                (Err(bytes1), Err(bytes2)) => bytes1.partial_cmp(bytes2),
            }
        }
    }
    impl<'a, B: Ord> Ord for Wrapped<'a, B> {
        fn cmp(&self, other: &Self) -> Ordering {
            self.partial_cmp(other).unwrap()
        }
    }
    impl<'a, B: Ord+Clone> IntoOwned<'a> for Wrapped<'a, B> {
        type Owned = Vec<B>;
        fn into_owned(self) -> Self::Owned {
            match self.decode() {
                Ok(decode) => decode.cloned().collect(),
                Err(bytes) => bytes.to_vec(),
            }
        }
        fn clone_onto(&self, other: &mut Self::Owned) {
            other.clear();
            match self.decode() {
                Ok(decode) => other.extend(decode.cloned()),
                Err(bytes) => other.extend_from_slice(bytes),
            }
        }
        fn borrow_as(owned: &'a Self::Owned) -> Self {
            Self { inner: Err(&owned[..]) }
        }
    }
}

/// Wrapper around a Huffman decoder and byte slices, decodeable to a byte sequence.
mod encoded {

    use super::Huffman;

    /// Welcome to GATs!
    pub struct Encoded<'a, B: Ord> {
        /// Text that decorates the data.
        huffman: &'a Huffman<B>,
        /// The data itself.
        bytes: &'a [u8],
    }

    impl<'a, B: Ord> Encoded<'a, B> {
        /// Returns either a decoding iterator, or just the bytes themselves.
        pub fn decode(&'a self) -> impl Iterator<Item=&'a B> + 'a {
            self.huffman.decode(self.bytes.iter().cloned())
        }
        pub fn new(huffman: &'a Huffman<B>, bytes: &'a [u8]) -> Self {
            Self { huffman, bytes }
        }
    }

    impl<'a, B: Ord> Copy for Encoded<'a, B> { }
    impl<'a, B: Ord> Clone for Encoded<'a, B> {
        fn clone(&self) -> Self { *self }
    }
}

mod huffman {

    use std::collections::BTreeMap;
    use std::convert::TryInto;
    
    use self::decoder::Decoder;
    use self::encoder::Encoder;

    /// Encoding and decoding state for Huffman codes.
    pub struct Huffman<T: Ord> {
        /// byte indexed description of what to blat down for encoding.
        /// An entry `(bits, code)` indicates that the low `bits` of `code` should be blatted down.
        /// Probably every `code` fits in a `u64`, unless there are crazy frequencies?
        encode: BTreeMap<T, (usize, u64)>,
        /// Byte-by-byte decoder.
        decode: [Decode<T>; 256],
    }
    impl<T: Ord> Huffman<T> {

        /// Encodes the provided symbols as a sequence of bytes.
        ///
        /// The last byte may only contain partial information, but it should be recorded as presented,
        /// as we haven't a way to distinguish (e.g. a `Result` return type).
        pub fn encode<'a, I>(&'a self, symbols: I) -> Encoder<'a, T, I::IntoIter>
        where
            I: IntoIterator<Item = &'a T>,
        {
            Encoder::new(&self.encode, symbols.into_iter())
        }

        /// Decodes the provided bytes as a sequence of symbols.
        pub fn decode<I>(&self, bytes: I) -> Decoder<'_, T, I::IntoIter> 
        where
            I: IntoIterator<Item=u8>
        {
            Decoder::new(&self.decode, bytes.into_iter())
        }

        pub fn create_from(counts: BTreeMap<T, i64>) -> Self where T: Clone {

            if counts.is_empty() {
                return Self {
                    encode: Default::default(),
                    decode: Decode::map(),
                };
            }

            let mut heap = std::collections::BinaryHeap::new();
            for (item, count) in counts {
                heap.push((-count, Node::Leaf(item)));
            }
            let mut tree = Vec::with_capacity(2 * heap.len() - 1);
            while heap.len() > 1 {
                let (count1, least1) = heap.pop().unwrap();
                let (count2, least2) = heap.pop().unwrap();
                let fork = Node::Fork(tree.len(), tree.len()+1);
                tree.push(least1);
                tree.push(least2);
                heap.push((count1 + count2, fork));
            }
            tree.push(heap.pop().unwrap().1);

            let mut levels = Vec::with_capacity(1 + tree.len()/2);
            let mut todo = vec![(tree.last().unwrap(), 0)];
            while let Some((node, level)) = todo.pop() {
                match node {
                    Node::Leaf(sym) => { levels.push((level, sym)); },
                    Node::Fork(l,r) => { 
                        todo.push((&tree[*l], level + 1));
                        todo.push((&tree[*r], level + 1));
                    },
                }
            }
            levels.sort_by(|x,y| x.0.cmp(&y.0));
            let mut code: u64 = 0;
            let mut prev_level = 0;
            let mut encode = BTreeMap::new();
            let mut decode = Decode::map();
            for (level, sym) in levels {
                if prev_level != level {
                    code <<= level - prev_level;
                    prev_level = level;
                }
                encode.insert(sym.clone(), (level, code));
                Self::insert_decode(&mut decode, sym, level, code << (64-level));

                code += 1;
            }

            for (index, entry) in decode.iter().enumerate() {
                if entry.any_void() {
                    panic!("VOID FOUND: {:?}", index);
                }
            }

            Huffman { 
                encode,
                decode,
            }
        }

        /// Inserts a symbol, and 
        fn insert_decode(map: &mut [Decode<T>; 256], symbol: &T, bits: usize, code: u64) where T: Clone {
            let byte: u8 = (code >> 56).try_into().unwrap();
            if bits <= 8 {
                for off in 0 .. (1 << (8 - bits)) {
                    map[(byte as usize) + off] = Decode::Symbol(symbol.clone(), bits);
                }
            }
            else {
                if let Decode::Void = &map[byte as usize] {
                    map[byte as usize] = Decode::Further(Box::new(Decode::map()));
                }
                if let Decode::Further(next_map) = &mut map[byte as usize] {
                    Self::insert_decode(next_map, symbol, bits - 8, code << 8);
                }
            }
        }
    }
    /// Tree structure for Huffman bit length determination.
    #[derive(Eq, PartialEq, Ord, PartialOrd, Debug)]
    enum Node<T> {
        Leaf(T),
        Fork(usize, usize),
    }

    /// Decoder 
    #[derive(Eq, PartialEq, Ord, PartialOrd, Debug, Default)]
    pub enum Decode<T> {
        /// An as-yet unfilled slot.
        #[default]
        Void,
        /// The symbol, and the number of bits consumed.
        Symbol(T, usize),
        /// An additional map to push subsequent bytes at.
        Further(Box<[Decode<T>; 256]>),
    }

    impl<T> Decode<T> {
        /// Tests to see if the map contains any invalid values.
        ///
        /// A correctly initialized map will have no invalid values.
        /// A map with invalid values will be unable to decode some 
        /// input byte sequences.
        fn any_void(&self) -> bool {
            match self {
                Decode::Void => true,
                Decode::Symbol(_,_) => false,
                Decode::Further(map) => map.iter().any(|m| m.any_void()),
            }
        }
        /// Creates a new map containing invalid values.
        fn map() -> [Decode<T>; 256] {
            let mut vec = Vec::with_capacity(256);
            for _ in 0 .. 256 {
                vec.push(Decode::Void);
            }
            vec.try_into().ok().unwrap()
        }
    }


    /// A tabled Huffman decoder, written as an iterator.
    mod decoder {

        use super::Decode;

        #[derive(Copy, Clone)]
        pub struct Decoder<'a, T, I> {
            decode: &'a [Decode<T>; 256],
            bytes: I,
            pending_byte: u16,
            pending_bits: usize,
        }

        impl<'a, T, I> Decoder<'a, T, I> {
            pub fn new(decode: &'a [Decode<T>; 256], bytes: I) -> Self {
                Self {
                    decode,
                    bytes,
                    pending_byte: 0,
                    pending_bits: 0,
                }
            }
        }

        impl<'a, T, I> Iterator for Decoder<'a, T, I>
        where
            I: Iterator<Item=u8>,
        {
            type Item = &'a T;
            fn next(&mut self) -> Option<&'a T> {
                // We must navigate `self.decode`, restocking bits whenever possible.
                // We stop if ever there are not enough bits remaining.
                let mut map = self.decode;
                loop {
                    if self.pending_bits < 8 {
                        if let Some(next_byte) = self.bytes.next() {
                            self.pending_byte = (self.pending_byte << 8) + next_byte as u16;
                            self.pending_bits += 8;
                        }
                        else {
                            return None;
                        }
                    }
                    let byte = (self.pending_byte >> (self.pending_bits - 8)) as usize;
                    match &map[byte] {
                        Decode::Void => { panic!("invalid decoding map"); }
                        Decode::Symbol(s, bits) => {
                            self.pending_bits -= bits;
                            self.pending_byte &= (1 << self.pending_bits) - 1;
                            return Some(s);
                        }
                        Decode::Further(next_map) => {
                            self.pending_bits -= 8;
                            self.pending_byte &= (1 << self.pending_bits) - 1;
                            map = next_map;
                        }
                    }
                }
            }
        }
    }

    /// A tabled Huffman encoder, written as an iterator.
    mod encoder {

        use std::collections::BTreeMap;

        #[derive(Copy, Clone)]
        pub struct Encoder<'a, T, I> {
            encode: &'a BTreeMap<T, (usize, u64)>,
            symbols: I,
            pending_byte: u64,
            pending_bits: usize,
        }

        impl<'a, T, I> Encoder<'a, T, I> {
            pub fn new(encode: &'a BTreeMap<T, (usize, u64)>, symbols: I) -> Self {
                Self {
                    encode,
                    symbols,
                    pending_byte: 0,
                    pending_bits: 0,
                }
            }
        }

        impl<'a, T: Ord, I> Iterator for Encoder<'a, T, I>
        where
            I: Iterator<Item=&'a T>,
        {
            type Item = u8;
            fn next(&mut self) -> Option<u8> {
                // We repeatedly ship bytes out of `self.pending_byte`, restocking from `self.symbols`.
                while self.pending_bits < 8 {
                    if let Some(symbol) = self.symbols.next() {
                        let (bits, code) = self.encode.get(symbol).unwrap();
                        self.pending_byte <<= bits;
                        self.pending_byte += code;
                        self.pending_bits += bits;
                    }
                    else {
                        // We have run out of symbols. Perhaps there is a final fractional byte to ship?
                        if self.pending_bits > 0 {
                            let byte = self.pending_byte << (8 - self.pending_bits);
                            self.pending_bits = 0;
                            self.pending_byte = 0;
                            return Some(byte as u8);
                        }
                        else {
                            return None;
                        }
                    }
                }

                let byte = self.pending_byte >> (self.pending_bits - 8);
                self.pending_bits -= 8;
                self.pending_byte &= (1 << self.pending_bits) - 1;
                Some(byte as u8)
            }
        }
    }

}