
pub struct HashIndex<K: Eq, V, F: Fn(&K)->u64> {
    bucket: F,
    buffer: Vec<Option<(K,V)>>,
    count:  usize,
    shift:  usize,
    slop:   usize,
}

impl<K: Eq, V, F: Fn(&K)->u64> HashIndex<K, V, F> {
    pub fn new(function: F) -> HashIndex<K, V, F> {
        let slop = 16;
        let mut buffer = Vec::with_capacity(2 + slop);
        for _ in 0..buffer.capacity() { buffer.push(None); }
        HashIndex {
            bucket: function,
            buffer: buffer,
            count: 0,
            shift: 63,
            slop: slop,
        }
    }
    pub fn capacity(&self) -> usize { self.buffer.capacity() }

    pub fn get_ref<'a>(&'a self, query: &K) -> Option<&'a V> {

        let target = (self.bucket)(query) >> self.shift;
        let mut iterator = self.buffer[target as usize ..].iter().map(|x| x.as_ref());
        while let Some(Some(&(ref key, ref val))) = iterator.next() {
            let found = (self.bucket)(key) >> self.shift;
            if found >= target {
                if key == query {
                    return Some(val);
                }
                else {
                    return None;
                }
            }
        }

        return None;
    }


    pub fn get_mut<'a>(&'a mut self, query: &K) -> Option<&'a V> {

        let target = (self.bucket)(query) >> self.shift;
        let mut iterator = self.buffer[target as usize ..].iter_mut().map(|x| x.as_ref());
        while let Some(Some(&(ref key, ref val))) = iterator.next() {
            let found = (self.bucket)(key) >> self.shift;
            if found == target && key == query {
                return Some(val);
            }
            if found > target {
                return None;
            }
        }

        return None;
    }

    pub fn entry_or_insert<G: FnMut()->V>(&mut self, key: K, mut func: G) -> &mut V {

        let target = (self.bucket)(&key) >> self.shift;

        let mut success = false;
        let mut position = target as usize;
        while position < self.buffer.len() {
            if let Some(ref mut kv) = self.buffer[position].as_mut() {
                let found = (self.bucket)(&kv.0) >> self.shift;
                if found == target && key == kv.0 {
                    success = true;
                    break;
                }
                if found > target { break; }
            }
            else { break; }

            position += 1;
        }

        if success { return &mut self.buffer[position].as_mut().unwrap().1; }
        else {
            // position now points at the place where the value should go.
            // we may need to slide everyone from there forward until a None.
            let begin = position;
            while position < self.buffer.len() && self.buffer[position].is_some() {
                position += 1;
            }

            if position < self.buffer.len() {

                for i in 0..(position - begin) {
                    self.buffer.swap(position - i - 1, position - i);
                }

                assert!(self.buffer[begin].is_none());
                self.buffer[begin] = Some((key, func()));
                self.count += 1;
                &mut self.buffer[begin].as_mut().unwrap().1
            }
            else {
                // println!("resizing: load at {}", self.count as f64 / self.buffer.capacity() as f64);

                let old_length = self.buffer.len() - self.slop;
                let mut new_buffer = Vec::with_capacity(2 * old_length + self.slop);
                for _ in 0..new_buffer.capacity() {
                    new_buffer.push(None);
                }
                let old_buffer = ::std::mem::replace(&mut self.buffer, new_buffer);
                self.shift -= 1;

                let mut cursor = 0;
                for oldkeyval in old_buffer.into_iter() {
                    if let Some((oldkey, oldval)) = oldkeyval {
                        let target = (self.bucket)(&oldkey) >> self.shift;
                        cursor = ::std::cmp::max(cursor, target);
                        self.buffer[cursor as usize] = Some((oldkey, oldval));
                        cursor += 1;
                    }
                }

                self.entry_or_insert(key, func)
            }
        }
    }

    pub fn remove_key(&mut self, key: &K) -> Option<V> {

        let target = (self.bucket)(&key) >> self.shift;

        let mut success = false;
        let mut position = target as usize;
        while position < self.buffer.len() {
            if let Some(ref mut kv) = self.buffer[position].as_mut() {
                let found = (self.bucket)(&kv.0) >> self.shift;
                if found == target && key == &kv.0 {
                    success = true;
                    break;
                }
                if found > target { break; }
            }
            else { break; }

            position += 1;
        }

        if success {
            let result = self.buffer[position].take();

            // now propagate the None forward as long as records are past their preferred location
            while position + 1 < self.buffer.len()
               && self.buffer[position].is_some()
               && (self.bucket)(&self.buffer[position].as_ref().unwrap().0) >> self.shift <= position as u64 {
                self.buffer.swap(position, position + 1);
            }

            result.map(|(_,v)| v)
        }
        else {
            None
        }
    }
}
