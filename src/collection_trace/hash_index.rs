
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


    pub fn entry_or_insert<'a, G: FnMut()->V>(&'a mut self, key: K, func: &G) -> &'a mut V {
        get_or_insert(&mut self.buffer, key, &|k| (self.bucket(k) >> self.shift), func);
        if let Ok
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


pub fn get_ref<'a, K: Eq, V, F: Fn(&K)->usize>(slice: &'a [Option<(K,V)>], query: &K, function: &F) -> Option<&'a V> {

    let target = function(query);
    let mut iterator = slice[target..].iter().map(|x| x.as_ref());
    while let Some(Some(&(ref key, ref val))) = iterator.next() {
        let found = function(key);
        if found >= target && key == query { return Some(val); }
        if found > target { return None; }
    }

    return None;
}


pub fn get_mut<'a, K: Eq, V, F: Fn(&K)->usize>(slice: &'a mut [Option<(K,V>)], query: &K, function: &F) -> Option<&'a mut V> {

    let target = function(query);
    let mut iterator = slice[target..].iter_mut().map(|x| x.as_ref());
    while let Some(Some(&(ref key, ref mut val))) = iterator.next() {
        let found = function(key);
        if found >= target && key == query { return Some(val); }
        if found > target { return None; }
    }

    return None;
}

pub fn get_or_insert<'a, K: Eq, V, F: Fn(&K)->usize, G: FnMut()->V>(slice: &'a mut [Option<(K,V>)], key: K, function: &F, mut func: G) -> Result<&'a mut V,()> {

    let target = function(&key);

    let mut success = false;
    let mut position = target as usize;
    while position < self.buffer.len() {
        if let Some(ref mut kv) = slice[position].as_mut() {
            let found = function(&kv.0);
            if found == target && key == kv.0 {
                success = true;
                break;
            }
            if found > target { break; }
        }
        else { break; }

        position += 1;
    }

    if success { return Ok(&mut slice[position].as_mut().unwrap().1); }
    else {
        // position now points at the place where the value should go.
        // we may need to slide everyone from there forward until a None.
        let begin = position;
        while position < slice.len() && slice[position].is_some() {
            position += 1;
        }

        if position < slice.len() {

            for i in 0..(position - begin) {
                slice.swap(position - i - 1, position - i);
            }

            assert!(slice[begin].is_none());
            slice[begin] = Some((key, func()));
            self.count += 1;
            return Ok(&mut slice[begin].as_mut().unwrap().1);
        }
        else { return Err(()); }
    }
}

pub fn remove_key<'a, K: Eq, V, F: Fn(&K)->usize>(slice: &mut [Option<(K,V)>], key: &K, function: &F) -> Option<V> {

    let target = function(&key);

    let mut success = false;
    let mut position = target as usize;
    while position < slice.len() {
        if let Some(ref mut kv) = slice[position].as_mut() {
            let found = function(&kv.0);
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
        let result = slice[position].take();

        // now propagate the None forward as long as records are past their preferred location
        while position + 1 < slice.len()
           && slice[position].is_some()
           && function(&self.buffer[position].as_ref().unwrap().0) <= position as u64 {
            slice.swap(position, position + 1);
        }

        result.map(|(_,v)| v)
    }
    else {
        None
    }
}
