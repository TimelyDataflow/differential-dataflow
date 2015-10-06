pub struct RHHMap<K: Eq, V, F: Fn(&K)->usize> {
    bucket: F,
    buffer: Vec<Option<(K,V)>>,
    shift:  usize,
    slop:   usize,
}

impl<K: Eq, V, F: Fn(&K)->usize> RHHMap<K, V, F> {
    pub fn new(function: F) -> RHHMap<K, V, F> {
        let slop = 4;
        let mut buffer = Vec::with_capacity(2 + slop);
        for _ in 0..buffer.capacity() { buffer.push(None); }
        RHHMap {
            bucket: function,
            buffer: buffer,
            shift: 63,
            slop: slop,
        }
    }
    pub fn capacity(&self) -> usize { self.buffer.capacity() }

    #[inline]
    pub fn get_ref<'a>(&'a self, query: &K) -> Option<&'a V> {
        let shift = self.shift;
        let bucket = &self.bucket;
        get_ref(&self.buffer[..], query, &|x| bucket(x), bucket(query) >> shift)
    }
    #[inline]
    pub fn get_mut<'a>(&'a mut self, query: &K) -> Option<&'a mut V> {
        let shift = self.shift;
        let bucket = &self.bucket;
        get_mut(&mut self.buffer[..], query, &|x| bucket(x), bucket(query) >> shift)
    }
    #[inline]
    pub fn insert(&mut self, key: K, val: V) -> Option<(K,V)> {

        let result = {
            let shift = self.shift;
            let bucket = &self.bucket;
            let location = bucket(&key) >> shift;
            insert(&mut self.buffer[..], key, val, &|x| bucket(x), location)
        };
        match result {
            Ok(result)      => result,
            Err((key, val)) => {

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

                self.insert(key, val)
            }
        }
    }
    pub fn remove(&mut self, key: &K) -> Option<(K,V)> {
        let shift = self.shift;
        let bucket = &self.bucket;
        remove(&mut self.buffer[..], key, &|x| bucket(x), bucket(key) >> shift)
    }
}

/// Returns either a shared reference to the value associated with `query`, or `None` if no value exists.
#[inline]
pub fn get_ref<'a, K: Eq, V, F: Fn(&K)->usize>(slice: &'a [Option<(K,V)>], query: &K, function: &F, location: usize) -> Option<&'a V> {

    let target = function(query);
    let mut iterator = slice[location..].iter().map(|x| x.as_ref());
    while let Some(Some(&(ref key, ref val))) = iterator.next() {
        let found = function(key);
        if found >= target && key == query { return Some(val); }
        if found > target { return None; }
    }

    return None;
}

/// Returns either an exclusive reference to the value associated with `query`, or `None` if no value exists.
#[inline]
pub fn get_mut<'a, K: Eq, V, F: Fn(&K)->usize>(slice: &'a mut [Option<(K,V)>], query: &K, function: &F, location: usize) -> Option<&'a mut V> {

    let target = function(query);
    let mut iterator = slice[location..].iter_mut().map(|x| x.as_mut());
    while let Some(Some(&mut (ref key, ref mut val))) = iterator.next() {
        let found = function(key);
        if found >= target && key == query { return Some(val); }
        if found > target { return None; }
    }

    return None;
}

/// Returns either the previous `(key,val)` pair for `key`, or `Err((key,val))` if `slice` does not have enough room to insert it.
#[inline]
pub fn insert<'a, K: Eq, V, F: Fn(&K)->usize>(slice: &'a mut [Option<(K,V)>], key: K, val: V, function: &F, location: usize) -> Result<Option<(K,V)>,(K,V)> {

    if location >= slice.len() {
        panic!("location {} > slice.len: {}", location, slice.len());
    }

    let target = function(&key);

    let mut success = false;
    let mut position = location;
    while position < slice.len() {
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

    if success {
        let result = slice[position].take();
        slice[position] = Some((key, val));
        return Ok(result);
    }
    else {
        // position now points at the place where the value should go.
        // we may need to slide everyone from there forward until a None.
        let begin = position;
        while position < slice.len() && slice[position].is_some() {
            position += 1;
        }

        if position > begin + 16 {
            return Err((key, val));
        }

        if position < slice.len() {

            for i in 0..(position - begin) {
                slice.swap(position - i - 1, position - i);
            }

            assert!(slice[begin].is_none());
            slice[begin] = Some((key, val));
            return Ok(None);
        }
        else { return Err((key, val)); }
    }
}

/// Returns either the `(key,val)` pair associated with `key`, or `None` if it does not exist.
///
/// The `remove` method operates over a slice of `Option<(K,V)>`, given a target key and a hash
/// function `function`. It also takes an initial `location` from which to start its search, for
/// some reason.
#[inline]
pub fn remove<'a, K: Eq, V, F: Fn(&K)->usize>(slice: &mut [Option<(K,V)>], key: &K, function: &F, location: usize) -> Option<(K,V)> {

    if true {
        panic!("rhhmap::remove is buggy; see code for details");
    }

    let target = function(&key);

    let mut success = false;
    let mut position = location;
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
        // BUG : `function` isn't the desired position, it is the hash. it should be shifted or something.
        while position + 1 < slice.len()
           && slice[position + 1].is_some()
           && function(&slice[position + 1].as_ref().unwrap().0) <= position {
            slice.swap(position, position + 1);
            position += 1;
        }

        result
    }
    else {
        None
    }
}
