use std::mem;
use timely::drain::DrainExt;

pub struct RadixSorter<T, F: Fn(&T)->u64> {
    

}

pub struct SegmentList<T> {
    size:     usize,
    segments: Vec<Vec<T>>,
    current:  Vec<T>,
}

impl<T> SegmentList<T> {
    pub fn push<I: Iterator<Item=T>>(&mut self, iterator: I) {
        for item in iterator {
            if self.current.len() == self.size {
                self.segments.push(mem::replace(&mut self.current, Vec::with_capacity(self.size)));
            }
            self.current.push(item);
        }
    }
    pub fn finalize(&mut self) -> Vec<Vec<T>> {
        if self.current.len() > 0 {
            self.segments.push(mem::replace(&mut self.current, Vec::with_capacity(self.size)));
        }
        mem::replace(&mut self.segments, Vec::new())
    }
    pub fn new(size: usize) -> SegmentList<T> {
        SegmentList {
            size:     size,
            segments: Vec::new(),
            current:  Vec::with_capacity(size),
        }
    }
}


pub fn radix_sort_32<V: Copy+Default, F: Fn(&V)->u32>(data: &mut Vec<Vec<V>>, free: &mut Vec<Vec<V>>, func: &F) {
    radix_shuf(data, free, &|x| ((func(x) >>  0) & 0xFF) as u8);
    radix_shuf(data, free, &|x| ((func(x) >>  8) & 0xFF) as u8);
    radix_shuf(data, free, &|x| ((func(x) >> 16) & 0xFF) as u8);
    radix_shuf(data, free, &|x| ((func(x) >> 24) & 0xFF) as u8);
}

pub fn radix_shuf<V: Copy+Default, F: Fn(&V)->u8>(data: &mut Vec<Vec<V>>, free: &mut Vec<Vec<V>>, func: &F) {

    let mut part = vec![]; for _ in 0..256 { part.push(free.pop().unwrap_or(Vec::with_capacity(1024))); }
    let mut full = vec![]; for _ in 0..256 { full.push(vec![]); }

    let buflen = 8;

    let mut temp = vec![Default::default(); buflen * 256];
    let mut counts = vec![0u8; 256];

    // loop through each buffer
    for mut vs in data.drain_temp() {
        for v in vs.drain_temp() {
            let key = func(&v) as usize;

            temp[buflen * key + counts[key] as usize] = v;
            counts[key] += 1;

            if counts[key] == buflen as u8 {
                for v in &temp[(buflen * key) .. ((buflen * key) + buflen)] { part[key].push(*v); }

                if part[key].len() == 1024 {
                    full[key].push(mem::replace(&mut part[key], free.pop().unwrap_or(Vec::new())));
                }

                counts[key] = 0;
            }
        }

        free.push(vs);
    }

    // check each partially filled buffer
    for (key, mut p) in part.drain_temp().enumerate() {
        for v in &temp[(buflen * key) .. ((buflen * key) + counts[key] as usize)] { p.push(*v); }

        if p.len() > 0 { full[key].push(p); }
        else           { free.push(p); }
    }

    // re-order buffers
    for mut cs in full.drain_temp() {
        for c in cs.drain_temp() { data.push(c); }
    }
}
