use std::rc::Rc;
use timely::drain::DrainExt;

pub struct MergeMerge<K, V, F: Fn(&K)->u64> {
    keys_src: Vec<Vec<(K, u32)>>,
    vals_src: Vec<Vec<V>>,
    wgts_src: Vec<Vec<(i32, u32)>>,
    keys_tmp: Vec<(K, u32)>,
    vals_tmp: Vec<V>,
    wgts_tmp: Vec<(i32, u32)>,
    func: Rc<F>,
}


pub struct RadixMerge<K, V, F: Fn(&K)->u64> {
    keys_src: Vec<Vec<(K, u32)>>,
    vals_src: Vec<Vec<V>>,
    wgts_src: Vec<Vec<(i32, u32)>>,
    keys_tmp: Vec<Vec<(K, u32)>>,
    vals_tmp: Vec<Vec<V>>,
    wgts_tmp: Vec<Vec<(i32, u32)>>,
    func: Rc<F>,
}

impl<K: Ord, V: Ord, F: Fn(&K)->u64> RadixMerge<K, V, F> {
    pub fn new(func: Rc<F>) -> RadixMerge<K, V, F> {
        let mut ks = vec![]; for _ in 0..256 { ks.push(Vec::with_capacity(256)); }
        let mut vs = vec![]; for _ in 0..256 { vs.push(Vec::with_capacity(256)); }
        let mut ws = vec![]; for _ in 0..256 { ws.push(Vec::with_capacity(256)); }
        let mut kt = vec![]; for _ in 0..256 { kt.push(Vec::with_capacity(256)); }
        let mut vt = vec![]; for _ in 0..256 { vt.push(Vec::with_capacity(256)); }
        let mut wt = vec![]; for _ in 0..256 { wt.push(Vec::with_capacity(256)); }
        RadixMerge {
            keys_src: ks,
            vals_src: vs,
            wgts_src: ws,
            keys_tmp: kt,
            vals_tmp: vt,
            wgts_tmp: wt,
            func: func,
        }
    }
    pub fn push(&mut self, key: K, val: V, wgt: i32) {
        let byte = ((self.func)(&key) % 256) as usize;
        self.keys_src[byte].push((key, 1));
        self.vals_src[byte].push(val);
        self.wgts_src[byte].push((wgt, 1));

        if self.vals_src[byte].len() == self.vals_src[byte].capacity() {
            let prev = self.keys_src[byte].len();
            self.sort(byte);
            let post = self.keys_src[byte].len();

            if byte == 0 {
                println!("went from {} to {} vs {}", prev, post, self.vals_src[byte].len());
            }
        }
    }
    pub fn seal(mut self) -> (Vec<Vec<(K, u32)>>, Vec<Vec<V>>, Vec<Vec<(i32, u32)>>) {
        for byte in 0..256 {
            self.sort(byte)
        }
        (self.keys_src, self.vals_src, self.wgts_src)
    }

    fn sort(&mut self, index: usize) {
        self.radix_shuffle(index, 8);
        self.radix_shuffle(index, 16);
        self.radix_shuffle(index, 24);
        // self.radix_shuffle(byte, 32);
        // self.radix_shuffle(byte, 40);
        // self.radix_shuffle(byte, 48);
        // self.radix_shuffle(byte, 56);

        // compact those keys!
        let keys = &mut self.keys_src[index];
        let mut cursor = 0;
        for i in 1..keys.len() {
            if keys[cursor].0 == keys[i].0 {
                keys[cursor].1 += keys[i].1;
            }
            else {
                cursor += 1;
                keys.swap(cursor, i);
            }
        }
        keys.truncate(cursor+1);

        // compact those wgts!
        for i in 0..keys.len() {
            self.wgts_src[index][i] = (1, keys[i].1);
        }
        self.wgts_src[index].truncate(keys.len());
    }

    #[inline(never)]
    fn radix_shuffle(&mut self, index: usize, shift: usize) {

        // push keys, vals, and wgts based on func(&key)
        {
            let mut v_drain = self.vals_src[index].drain_temp();
            let mut w_drain = self.wgts_src[index].drain_temp();

            for (key, len) in self.keys_src[index].drain_temp() {
                let byte = (((self.func)(&key) >> shift) % 256) as usize;

                // could / should just be a memcpy
                self.vals_tmp[byte].extend(v_drain.by_ref().take(len as usize));

                let mut w_read = 0;
                while w_read < len {
                    let (wgt, cnt) = w_drain.next().unwrap();
                    self.wgts_tmp[byte].push((wgt, cnt));
                    w_read += cnt;
                }

                self.keys_tmp[byte].push((key, len));
            }

            assert!(v_drain.next().is_none());
            assert!(w_drain.next().is_none());
        }

        // copy the values back
        for byte in 0..256 {
            // should all be memcpys (probably aren't)
            self.keys_src[index].extend(self.keys_tmp[byte].drain_temp());
            self.vals_src[index].extend(self.vals_tmp[byte].drain_temp());
            self.wgts_src[index].extend(self.wgts_tmp[byte].drain_temp());
        }
    }
}
