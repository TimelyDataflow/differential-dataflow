//! Adaptive constant, byte-indexed, and dense time columns.
//! All time algebra stays on the container; dictionary addresses never escape.

use std::cmp::Ordering;
use std::marker::PhantomData;

use differential_dataflow::dynamic::pointstamp::{PointStamp, PointStampSummary};
use differential_dataflow::lattice::Lattice;
use timely::progress::frontier::AntichainRef;
use timely::progress::Timestamp;

/// A timestamp that is a sequence of integer coordinates, each with minimum zero.
/// Absent trailing coordinates are zero.
pub trait Lanes: Sized {
    /// The coordinates, in order.
    fn coordinates(&self) -> impl Iterator<Item = u64> + '_;
    /// The time with these coordinates; an iterator shorter than the time's own coordinates supplies zeros.
    fn from_coordinates(coords: impl Iterator<Item = u64>) -> Self;
}

impl Lanes for u64 {
    fn coordinates(&self) -> impl Iterator<Item = u64> + '_ { std::iter::once(*self) }
    fn from_coordinates(mut coords: impl Iterator<Item = u64>) -> Self { coords.next().unwrap_or(0) }
}

impl<const N: usize> Lanes for PointStamp<u64, N> {
    fn coordinates(&self) -> impl Iterator<Item = u64> + '_ { self.iter().copied() }
    // `new` strips trailing zeros, which is the canonical form.
    fn from_coordinates(coords: impl Iterator<Item = u64>) -> Self { PointStamp::new(coords.collect()) }
}

/// A timestamp usable as a lane column: DD's time algebra plus [`Lanes`].
pub trait ColTime: Timestamp + Lattice + Lanes {}
impl<T: Timestamp + Lattice + Lanes> ColTime for T {}

/// A path summary as lane operations: keep the first `retain` lanes (all if `None`), then add
/// `adds[j]` to lane `j`, where lanes beyond the column's width start at zero. A result that
/// overflows does not exist, as the owned summary's `None`.
pub trait LaneSummary {
    fn lane_steps(&self) -> (Option<usize>, Vec<u64>);
}

impl LaneSummary for u64 {
    fn lane_steps(&self) -> (Option<usize>, Vec<u64>) { (None, vec![*self]) }
}

impl LaneSummary for PointStampSummary<u64> {
    fn lane_steps(&self) -> (Option<usize>, Vec<u64>) { (self.retain, self.actions.clone()) }
}

/// A constant time, at most 256 distinct times addressed by bytes, or dense lanes.
/// Dictionary addresses stay private: consumers use lattice verbs and row positions.
pub struct ColTimes<T> {
    lanes: Vec<Vec<u64>>,
    rows: super::row_map::RowMap,
    len: usize,
    stored: usize,
    last: usize,
    _t: PhantomData<T>,
}
use super::row_map::RowMap;
impl<T> Default for ColTimes<T> {
    fn default() -> Self { Self { lanes: Vec::new(), rows: RowMap::Constant, len: 0, stored: 0, last: 0, _t: PhantomData } }
}
impl<T> Clone for ColTimes<T> {
    fn clone(&self) -> Self { Self { lanes: self.lanes.clone(), rows: self.rows.clone(), len: self.len, stored: self.stored, last: self.last, _t: PhantomData } }
}
impl<T> ColTimes<T> {
    #[inline] pub(crate) fn new() -> Self { Self::default() }
    #[inline] pub fn len(&self) -> usize { self.len }
    #[inline] pub fn is_empty(&self) -> bool { self.len == 0 }
    #[inline] pub(crate) fn is_constant(&self) -> bool { self.rows.is_constant() }
    #[inline] fn entry(&self, row: usize) -> usize { self.rows.at(row) }
    pub(crate) fn clear(&mut self) {
        for lane in &mut self.lanes { lane.clear(); }
        self.rows = RowMap::Constant;
        self.len = 0; self.stored = 0; self.last = 0;
    }
    pub(crate) fn width(&self) -> usize { self.lanes.len() }
    #[inline] fn stored_at(&self, lane: usize, entry: usize) -> u64 { self.lanes.get(lane).map_or(0, |l| l[entry]) }
    #[inline] pub(crate) fn at(&self, lane: usize, row: usize) -> u64 { self.stored_at(lane, self.entry(row)) }
    fn widen(&mut self, width: usize) {
        while self.lanes.len() < width { self.lanes.push(vec![0; self.stored]); }
    }
    pub(crate) fn reserve(&mut self, width: usize, additional: usize) {
        self.widen(width);
        if self.rows.is_dense() { for lane in &mut self.lanes { lane.reserve(additional); } }
    }
    pub(crate) fn shrink_to_fit(&mut self) {
        for lane in &mut self.lanes { lane.shrink_to_fit(); }
        self.rows.shrink_to_fit();
    }
    fn dense(&mut self) {
        if !self.rows.is_dense() {
            for lane in &mut self.lanes { *lane = (0..self.len).map(|r| lane[self.rows.at(r)]).collect(); }
            self.rows = RowMap::Dense;
            self.stored = self.len;
        }
    }
    fn coords(&self, entry: usize) -> smallvec::SmallVec<[u64; 4]> { self.lanes.iter().map(|l| l[entry]).collect() }
    fn intern(&mut self, coords: &[u64]) -> Option<usize> {
        self.widen(coords.len());
        let equal = |i| self.lanes.iter().enumerate().all(|(j,l)| l[i] == coords.get(j).copied().unwrap_or(0));
        if self.last < self.stored && equal(self.last) { return Some(self.last); }
        if let Some(i) = (0..self.stored).find(|&i| equal(i)) { self.last = i; return Some(i); }
        if self.stored == 256 { self.dense(); return None; }
        for (j, lane) in self.lanes.iter_mut().enumerate() { lane.push(coords.get(j).copied().unwrap_or(0)); }
        self.last = self.stored; self.stored += 1;
        Some(self.last)
    }
    fn push_coords(&mut self, coords: &[u64], count: usize) {
        if count == 0 { return; }
        self.widen(coords.len());
        if !self.rows.is_dense() {
            if let Some(entry) = self.intern(coords) {
                self.rows.push(entry, self.len, count);
                self.len += count;
                return;
            }
        }
        self.len += count; self.stored = self.len;
        for (j, lane) in self.lanes.iter_mut().enumerate() { lane.resize(self.len, coords.get(j).copied().unwrap_or(0)); }
    }
    pub(crate) fn push(&mut self, t: &T) where T: Lanes {
        if self.is_constant() && self.len != 0 {
            let mut coords = t.coordinates();
            if self.lanes.iter().all(|lane| lane[0] == coords.next().unwrap_or(0)) && coords.all(|x| x == 0) {
                self.len += 1;
                return;
            }
        }
        let coords: smallvec::SmallVec<[u64; 4]> = t.coordinates().collect();
        self.push_coords(&coords, 1);
    }
    #[inline] pub(crate) fn push_ref(&mut self, other: &Self, row: usize) {
        if self.len != 0 && self.is_constant() && other.is_constant() && self.cmp_cross(0, other, row) == Ordering::Equal {
            self.len += 1;
            return;
        }
        let coords = other.coords(other.entry(row));
        self.push_coords(&coords, 1);
    }
    pub(crate) fn push_range(&mut self, other: &Self, start: usize, end: usize) {
        if start == end { return; }
        assert!(start < end && end <= other.len);
        if self.len == 0 {
            *self = if start == 0 && end == other.len { other.clone() } else { other.slice(start, end) };
            return;
        }
        self.widen(other.width());
        if other.is_constant() {
            if self.len != 0 && self.is_constant() && self.cmp_cross(0, other, 0) == Ordering::Equal { self.len += end - start; }
            else { self.push_coords(&other.coords(0), end - start); }
            return;
        }
        if let RowMap::Indexed(ids) = &other.rows {
            if (start != 0 || end != other.len) && ids[start..end].iter().all(|&i| i == ids[start]) {
                self.push_coords(&other.coords(ids[start] as usize), end - start);
                return;
            }
        }
        if other.rows.is_dense() && !self.rows.is_dense() && end - start <= 256 {
            // Small dense sources failed the byte-cost test, not the distinct-time limit.
            let mut ids = [0u8; 256];
            for (offset, row) in (start..end).enumerate() {
                let Some(entry) = self.intern(&other.coords(row)) else { self.push_range(other, start, end); return };
                ids[offset] = entry as u8;
            }
            self.rows.extend(&ids[..end - start], self.len);
            self.len += end - start;
            self.last = ids[end - start - 1] as usize;
            return;
        }
        if other.rows.is_dense() { self.dense(); }
        if self.rows.is_dense() {
            for (j, lane) in self.lanes.iter_mut().enumerate() {
                if let Some(src) = other.lanes.get(j) {
                    match &other.rows {
                        RowMap::Dense => lane.extend_from_slice(&src[start..end]),
                        _ => lane.extend((start..end).map(|r| src[other.entry(r)])),
                    }
                } else { lane.resize(lane.len() + end - start, 0); }
            }
            self.len += end - start; self.stored = self.len;
            return;
        }
        if let RowMap::Indexed(ids) = &other.rows {
            let mut map = [0u8; 256];
            let mut identity = true;
            for (i, dst) in map.iter_mut().enumerate().take(other.stored) {
                let Some(entry) = self.intern(&other.coords(i)) else { self.push_range(other, start, end); return };
                *dst = entry as u8;
                identity &= entry == i;
            }
            self.rows.extend_mapped(&ids[start..end], &map, identity, self.len);
            self.len += end - start;
            self.last = map[ids[end - 1] as usize] as usize;
            // Remapping can intern unused source entries without adding any new row addresses.
            if self.is_constant() { self.collapse_constant(); }
        }
    }

    /// A constant selection allocates no row-address array. Indexed selections copy bytes.
    pub(crate) fn gather(&self, selected: &[usize]) -> Self {
        if selected.is_empty() { return Self::new(); }
        let first = self.entry(selected[0]);
        if self.is_constant() || (!self.rows.is_dense() && selected.iter().all(|&r| self.entry(r) == first)) {
            let mut out = Self::new(); out.push_coords(&self.coords(first), selected.len()); return out;
        }
        if self.rows.is_dense() {
            return Self::from_raw_lanes(self.lanes.iter().map(|l| selected.iter().map(|&r| l[r]).collect()).collect(), selected.len());
        }
        Self { lanes: self.lanes.clone(), rows: self.rows.gather(selected), len: selected.len(), stored: self.stored, last: first, _t: PhantomData }
    }
    /// Multi-source gather: reconcile dictionaries once, not for each selected row.
    pub(crate) fn gather_from(sources: &[&Self], tags: &[usize], offsets: &[usize]) -> Self {
        assert_eq!(tags.len(), offsets.len());
        if tags.is_empty() { return Self::new(); }
        if sources.iter().all(|s| s.is_constant()) && sources[1..].iter().all(|s| sources[0].cmp_cross(0, s, 0) == Ordering::Equal) {
            let mut out = Self::new(); out.push_coords(&sources[0].coords(0), tags.len()); return out;
        }
        if sources.iter().all(|s| s.is_constant()) {
            let first = tags[0];
            let mut out = Self::new();
            let map: Vec<_> = sources.iter().map(|s| out.intern(&s.coords(0)).unwrap() as u8).collect();
            if tags.iter().all(|&s| map[s] == map[first]) {
                let mut out = Self::new(); out.push_coords(&sources[first].coords(0), tags.len()); return out;
            }
            out.rows = RowMap::Indexed(tags.iter().map(|&s| map[s]).collect());
            out.len = tags.len();
            return out;
        }
        let mut out = Self::new();
        out.reserve(sources.iter().map(|s| s.width()).max().unwrap_or(0), tags.len());
        if sources.iter().all(|s| !s.rows.is_dense()) {
            let mut maps = Vec::new();
            for source in sources {
                let mut map = [0u8; 256];
                for (i, dst) in map.iter_mut().enumerate().take(source.stored) {
                    let Some(entry) = out.intern(&source.coords(i)) else { break };
                    *dst = entry as u8;
                }
                if out.rows.is_dense() { break; }
                maps.push(map);
            }
            if !out.rows.is_dense() {
                let ids: Vec<u8> = tags.iter().zip(offsets).map(|(&s,&r)| maps[s][sources[s].entry(r)]).collect();
                out.rows = RowMap::Indexed(ids); out.len = tags.len();
                out.collapse_constant();
                return out;
            }
        }
        let width = sources.iter().map(|s| s.width()).max().unwrap_or(0);
        let lanes = (0..width).map(|k| tags.iter().zip(offsets).map(|(&s,&r)| sources[s].at(k,r)).collect()).collect();
        Self::from_raw_lanes(lanes, tags.len())
    }
    pub(crate) fn slice(&self, start: usize, end: usize) -> Self {
        if start == end { return Self::new(); }
        if self.is_constant() { let mut out = self.clone(); out.len = end - start; return out; }
        match &self.rows {
            RowMap::Indexed(ids) => {
                if ids[start..end].iter().all(|&i| i == ids[start]) {
                    let mut out = Self::new(); out.push_coords(&self.coords(ids[start] as usize), end-start); return out;
                }
                Self { lanes: self.lanes.clone(), rows: RowMap::Indexed(ids[start..end].to_vec()), len: end-start, stored: self.stored, last: 0, _t: PhantomData }
            }
            _ => Self::from_raw_lanes(self.lanes.iter().map(|l| l[start..end].to_vec()).collect(), end-start),
        }
    }
    pub(crate) fn truncate_rows(&mut self, len: usize) {
        assert!(len <= self.len);
        self.rows.truncate(len);
        if self.rows.is_dense() { for lane in &mut self.lanes { lane.truncate(len); } self.stored = len; }
        self.len = len;
        if len == 0 { self.clear(); }
    }
    fn get_stored(&self, entry: usize) -> T where T: Lanes { T::from_coordinates(self.lanes.iter().map(|l| l[entry])) }
    pub fn get(&self, row: usize) -> T where T: Lanes { assert!(row < self.len); self.get_stored(self.entry(row)) }
    fn join_stored(&self, entry: usize, lower: &T) -> T where T: Lanes {
        let mut stored = self.lanes.iter(); let mut bounds = lower.coordinates();
        T::from_coordinates(std::iter::from_fn(|| match (stored.next(), bounds.next()) {
            (None, None) => None,
            (lane, bound) => Some(lane.map_or(0, |l| l[entry]).max(bound.unwrap_or(0))),
        }))
    }
    pub(crate) fn get_joined(&self, row: usize, lower: &T) -> T where T: Lanes { self.join_stored(self.entry(row), lower) }
    pub fn get_met(&self, row: usize, upper: &T) -> T where T: Lanes {
        let entry = self.entry(row);
        T::from_coordinates(self.lanes.iter().map(|l| l[entry]).zip(upper.coordinates()).map(|(x,y)| x.min(y)))
    }
    /// Walk row addresses in order, joining only at boundaries between unequal times.
    pub(crate) fn joined_range<'a>(&'a self, range: std::ops::Range<usize>, lower: &'a T, constant: Option<T>) -> impl Iterator<Item=T> + 'a where T: Lanes + Clone {
        let mut last: Option<(usize, T)> = constant.map(|time| (0, time));
        let end = range.end;
        range.map(move |row| {
            if self.rows.is_dense() { return self.get_joined(row, lower); }
            let entry = self.entry(row);
            if last.as_ref().is_none_or(|(i,_)| *i != entry) { last = Some((entry, self.get_joined(row, lower))); }
            if row + 1 == end { last.take().unwrap().1 } else { last.as_ref().unwrap().1.clone() }
        })
    }
    pub(crate) fn reader(&self) -> TimeReader<'_, T> where T: Lanes {
        match &self.rows {
            RowMap::Constant if self.len != 0 => TimeReader(ReadTimes::Constant(self.get_stored(0))),
            RowMap::Indexed(rows) => TimeReader(ReadTimes::Indexed { times: (0..self.stored).map(|i| self.get_stored(i)).collect(), rows }),
            _ => TimeReader(ReadTimes::Dense(self)),
        }
    }
    pub fn to_vec(&self) -> Vec<T> where T: Lanes { (0..self.len).map(|i| self.get(i)).collect() }
    #[inline] pub(crate) fn cmp(&self, i: usize, j: usize) -> Ordering {
        let (i,j) = (self.entry(i), self.entry(j));
        if i == j { return Ordering::Equal; }
        for lane in &self.lanes { let o = lane[i].cmp(&lane[j]); if o != Ordering::Equal { return o; } }
        Ordering::Equal
    }
    #[inline] pub(crate) fn cmp_cross(&self, i: usize, other: &Self, j: usize) -> Ordering {
        let (i,j) = (self.entry(i), other.entry(j));
        for k in 0..self.width().max(other.width()) {
            let o = self.stored_at(k,i).cmp(&other.stored_at(k,j)); if o != Ordering::Equal { return o; }
        }
        Ordering::Equal
    }
    pub(crate) fn less_equal(&self, i: usize, j: usize) -> bool {
        let (i,j) = (self.entry(i), self.entry(j));
        i == j || self.lanes.iter().all(|l| l[i] <= l[j])
    }
    /// Advance the dictionary alone. Return whether strict time order is preserved.
    /// Row addresses are rewritten only when times collapse.
    pub(crate) fn advance_by(&mut self, frontier: AntichainRef<T>, end: usize) -> bool where T: Lanes {
        assert!(end <= self.len);
        if frontier.is_empty() || end == 0 { return true; }
        if end < self.len {
            let suffix = self.slice(end, self.len);
            self.truncate_rows(end);
            self.advance_by(frontier, end);
            self.push_range(&suffix, 0, suffix.len);
            return false;
        }
        let constant = self.is_constant();
        let order = if self.rows.is_dense() || constant { None } else {
            let mut order: Vec<_> = (0..self.stored).collect();
            order.sort_unstable_by(|&a,&b| self.lanes.iter().map(|l| l[a]).cmp(self.lanes.iter().map(|l| l[b])));
            Some(order)
        };
        let mut meet: Option<Vec<u64>> = None;
        for f in frontier.iter() {
            match &mut meet {
                Some(m) => {
                    let mut len = 0;
                    for (m,x) in m.iter_mut().zip(f.coordinates()) { *m = (*m).min(x); len += 1; }
                    m.truncate(len);
                }
                None => meet = Some(f.coordinates().collect()),
            }
        }
        let meet = meet.unwrap(); self.widen(meet.len());
        let mut changed = false;
        for (lane,&m) in self.lanes.iter_mut().zip(&meet) {
            if m != 0 { for x in lane { if *x < m { *x = m; changed = true; } } }
        }
        // Strict preservation means a sorted, consolidated chunk stays so after advancement.
        let strict = constant || !changed || order.as_ref().is_some_and(|order| order.windows(2).all(|w| {
            self.lanes.iter().map(|l| l[w[0]]).cmp(self.lanes.iter().map(|l| l[w[1]])) == Ordering::Less
        }));
        if changed { self.compact(); }
        strict
    }
    pub(crate) fn beyond(&self, frontier: AntichainRef<T>) -> Vec<bool> where T: Lanes {
        let mut beyond = vec![false; self.stored]; let mut dominated = vec![true; self.stored];
        for f in frontier.iter() {
            dominated.fill(true);
            for (j,bound) in f.coordinates().enumerate() {
                if bound == 0 { continue; }
                match self.lanes.get(j) {
                    Some(lane) => for (d,&x) in dominated.iter_mut().zip(lane) { *d &= bound <= x; },
                    None => dominated.fill(false),
                }
            }
            for (b,&d) in beyond.iter_mut().zip(&dominated) { *b |= d; }
        }
        if self.rows.is_dense() { beyond } else { (0..self.len).map(|r| beyond[self.entry(r)]).collect() }
    }
    pub(crate) fn minimal(&self, rows: &[usize]) -> Vec<usize> {
        let mut minimal = Vec::new();
        let mut seen = [false; 256];
        for &i in rows {
            if !self.rows.is_dense() {
                let entry = self.entry(i);
                if seen[entry] { continue; } seen[entry] = true;
            }
            if minimal.iter().any(|&m| self.less_equal(m,i)) { continue; }
            minimal.retain(|&m| !self.less_equal(i,m)); minimal.push(i);
        }
        minimal
    }
    pub(crate) fn lane(&self, k: usize) -> Vec<u64> {
        self.lanes.get(k).map_or_else(|| vec![0; self.len], |l| (0..self.len).map(|r| l[self.entry(r)]).collect())
    }
    pub(crate) fn lane_max(&mut self, k: usize, values: &[u64]) {
        assert_eq!(values.len(), self.len);
        self.dense(); self.widen(k+1);
        for (x,&v) in self.lanes[k].iter_mut().zip(values) { *x = (*x).max(v); }
        self.compact();
    }
    pub(crate) fn truncate(&mut self, width: usize) { self.lanes.truncate(width); self.compact(); }
    pub(crate) fn results_in<S: LaneSummary>(&mut self, step: &S) -> Option<Vec<usize>> {
        let (retain, adds) = step.lane_steps();
        if let Some(retain) = retain { self.truncate(retain); }
        self.widen(adds.len());
        let mut alive: Option<Vec<bool>> = None;
        for (lane,&add) in self.lanes.iter_mut().zip(&adds) {
            if add == 0 { continue; }
            for (i,x) in lane.iter_mut().enumerate() {
                match x.checked_add(add) { Some(y) => *x=y, None => alive.get_or_insert_with(|| vec![true; self.stored])[i]=false }
            }
        }
        let keep = alive.map(|alive| {
            let keep: Vec<_> = (0..self.len).filter(|&r| alive[self.entry(r)]).collect();
            *self = self.gather(&keep); keep
        });
        self.compact(); keep
    }
    fn collapse_constant(&mut self) {
        let entry = match &self.rows {
            RowMap::Constant if self.len != 0 => 0,
            RowMap::Indexed(ids) if ids.first().is_some_and(|first| ids.iter().all(|x| x == first)) => ids[0] as usize,
            _ => return,
        };
        for lane in &mut self.lanes { lane[0] = lane[entry]; lane.truncate(1); }
        self.rows = RowMap::Constant; self.stored = 1; self.last = 0;
    }
    /// Reclassify after time mutation. Already indexed dictionaries usually leave rows alone.
    pub(crate) fn compact(&mut self) {
        self.collapse_constant();
        if self.len == 0 || self.is_constant() { return; }
        if self.rows.is_dense() {
            if self.lanes.iter().all(|l| l.iter().all(|x| *x == l[0])) {
                for lane in &mut self.lanes { lane.truncate(1); }
                self.rows = RowMap::Constant; self.stored=1; self.last=0; return;
            }
            let mut dict = std::collections::HashMap::new();
            let mut entries = Vec::new(); let mut ids = Vec::with_capacity(self.len);
            let mut last_row = 0; let mut last_id = 0;
            for row in 0..self.len {
                let id = if row > 0 && self.lanes.iter().all(|l| l[row] == l[last_row]) { last_id } else {
                    let coords = self.coords(row);
                    if let Some(&id) = dict.get(&coords) { id } else {
                        if entries.len() == 256 { return; }
                        let id = entries.len() as u8;
                        dict.insert(coords, id); entries.push(row); id
                    }
                };
                ids.push(id); last_row=row; last_id=id;
            }
            if entries.len()*self.width()*8 + self.len >= self.len*self.width()*8 { return; }
            for lane in &mut self.lanes { *lane = entries.iter().map(|&r| lane[r]).collect(); }
            self.stored=entries.len(); self.rows=RowMap::Indexed(ids); self.last=last_id as usize;
        } else {
            let mut representatives = Vec::new(); let mut map = [0u8; 256];
            for (i,dst) in map.iter_mut().enumerate().take(self.stored) {
                if let Some(j) = representatives.iter().position(|&r| self.lanes.iter().all(|l| l[i] == l[r])) { *dst=j as u8; }
                else { *dst=representatives.len() as u8; representatives.push(i); }
            }
            if representatives.len() == self.stored { return; }
            for lane in &mut self.lanes { *lane = representatives.iter().map(|&r| lane[r]).collect(); }
            self.stored=representatives.len(); self.last=map[self.last] as usize;
            if self.stored == 1 { self.rows=RowMap::Constant; self.last=0; }
            else if let RowMap::Indexed(ids) = &mut self.rows { for id in ids { *id=map[*id as usize]; } }
            self.collapse_constant();
        }
    }
    /// Byte-cost choice at a chunk boundary; never rescan dense rows here.
    pub(crate) fn seal(&mut self) {
        if matches!(self.rows, RowMap::Indexed(_)) && self.stored*self.width()*8 + self.len >= self.len*self.width()*8 { self.dense(); }
        self.shrink_to_fit();
    }
    pub fn raw_lanes(&self) -> Vec<Vec<u64>> { (0..self.width()).map(|k| self.lane(k)).collect() }
    pub(crate) fn stored_lanes(&self) -> &[Vec<u64>] { &self.lanes }
    pub(crate) fn dictionary(&self) -> Option<(usize, &[u8])> {
        match &self.rows { RowMap::Constant => Some((self.stored,&[])), RowMap::Indexed(ids) => Some((self.stored,ids)), RowMap::Dense => None }
    }
    pub(crate) fn from_raw_lanes(lanes: Vec<Vec<u64>>, len: usize) -> Self {
        assert!(lanes.iter().all(|l| l.len()==len));
        let mut out=Self { lanes, rows: RowMap::Dense, len, stored: len, last: 0, _t: PhantomData };
        out.compact(); out.seal(); out
    }
    pub(crate) fn from_dictionary(lanes: Vec<Vec<u64>>, entries: usize, ids: Vec<u8>, len: usize) -> Self {
        assert!(entries<=256 && lanes.iter().all(|l| l.len()==entries));
        assert!(entries>0 || len==0);
        let rows = if entries<=1 { assert!(ids.is_empty()); RowMap::Constant } else {
            assert_eq!(ids.len(),len); assert!(ids.iter().all(|&i| (i as usize)<entries)); RowMap::Indexed(ids)
        };
        let mut out = Self { lanes, rows, len, stored: entries, last: 0, _t: PhantomData };
        out.collapse_constant();
        out
    }
}

/// An owned constant/dictionary presentation; dense times are built only when requested.
pub(crate) struct TimeReader<'a,T>(ReadTimes<'a,T>);
enum ReadTimes<'a,T> {
    Constant(T),
    Indexed { times: Vec<T>, rows: &'a [u8] },
    Dense(&'a ColTimes<T>),
}
impl<T: Lanes + Clone> TimeReader<'_,T> {
    pub(crate) fn get(&self, row: usize) -> T {
        match &self.0 { ReadTimes::Constant(t) => t.clone(), ReadTimes::Indexed { times, rows } => times[rows[row] as usize].clone(), ReadTimes::Dense(times) => times.get(row) }
    }
}

impl<T: Lanes> FromIterator<T> for ColTimes<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut out = ColTimes::new();
        for t in iter { out.push(&t); }
        out
    }
}

impl<'a, T: Lanes> FromIterator<&'a T> for ColTimes<T> {
    fn from_iter<I: IntoIterator<Item = &'a T>>(iter: I) -> Self {
        let mut out = ColTimes::new();
        for t in iter { out.push(t); }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use timely::progress::Antichain;

    type T = crate::ir::Time;

    fn t(outer: u64, coords: &[u64]) -> T {
        T::new(std::iter::once(outer).chain(coords.iter().copied()).collect())
    }

    #[test]
    fn uniform_maps_drop_unused_dictionary_entries() {
        let mut source: ColTimes<u64> = std::iter::repeat_n(3, 10).chain(std::iter::repeat_n(5, 10)).collect();
        source.truncate_rows(10);
        let mut appended: ColTimes<u64> = std::iter::repeat_n(3, 10).collect();
        appended.push_range(&source, 0, source.len());
        assert_eq!(appended.dictionary(), Some((1, &[][..])));
        assert_eq!(appended.stored_lanes(), &[vec![3]]);
        assert_eq!(appended.to_vec(), vec![3; 20]);
        source.compact();
        assert_eq!(source.dictionary(), Some((1, &[][..])));
        let decoded = ColTimes::<u64>::from_dictionary(vec![vec![5, 3]], 2, vec![1; 10], 10);
        assert_eq!(decoded.dictionary(), Some((1, &[][..])));
        assert_eq!(decoded.stored_lanes(), &[vec![3]]);
        assert_eq!(decoded.to_vec(), vec![3; 10]);
    }

    #[test]
    fn dense_joined_range_does_not_clone() {
        use std::cell::Cell;
        thread_local! { static CLONES: Cell<usize> = const { Cell::new(0) }; }
        struct Counted(u64);
        impl Clone for Counted {
            fn clone(&self) -> Self { CLONES.set(CLONES.get() + 1); Self(self.0) }
        }
        impl Lanes for Counted {
            fn coordinates(&self) -> impl Iterator<Item = u64> { std::iter::once(self.0) }
            fn from_coordinates(mut coords: impl Iterator<Item = u64>) -> Self { Self(coords.next().unwrap_or(0)) }
        }
        let times = ColTimes::<Counted>::from_raw_lanes(vec![(0..1000).collect()], 1000);
        assert!(times.rows.is_dense());
        let joined: Vec<_> = times.joined_range(0..1000, &Counted(500), None).map(|t| t.0).collect();
        assert_eq!(joined, (0..1000).map(|t| t.max(500)).collect::<Vec<_>>());
        assert_eq!(CLONES.get(), 0);
    }

    #[test]
    fn adaptive_constant_indexed_and_dense() {
        let mut col: ColTimes<T> = std::iter::repeat_n(t(1, &[2]), 1000).collect();
        assert!(col.is_constant());
        let frontier = Antichain::from_elem(t(4, &[3]));
        col.advance_by(frontier.borrow(), 600);
        assert_eq!(col.stored, 2);
        assert_eq!(col.get(599), t(4, &[3]));
        assert_eq!(col.get(600), t(1, &[2]));
        assert_eq!(col.get_met(599, &t(2, &[5])), t(2, &[3]));
        let mut copy = ColTimes::new();
        copy.push_range(&col, 500, 700);
        assert_eq!(copy.stored, 2);
        assert_eq!(copy.gather(&[150, 0, 151]).to_vec(), vec![t(1, &[2]), t(4, &[3]), t(1, &[2])]);
        col.advance_by(frontier.borrow(), 1000);
        assert!(col.is_constant());
        let mut dense: ColTimes<u64> = (0..1000).collect();
        assert!(dense.rows.is_dense());
        dense.advance_by(Antichain::from_elem(1000).borrow(), 1000);
        assert!(dense.is_constant());
        assert_eq!(dense.to_vec(), vec![1000; 1000]);
        let mut indexed: ColTimes<u64> = (0..1000).map(|i| if i % 2 == 0 { 1 } else { 3 }).collect();
        let before = indexed.dictionary().unwrap().1.to_vec();
        let pointer = indexed.dictionary().unwrap().1.as_ptr();
        assert!(indexed.advance_by(Antichain::from_elem(2).borrow(), 1000));
        assert_eq!(indexed.dictionary().unwrap().1, before);
        assert_eq!(indexed.dictionary().unwrap().1.as_ptr(), pointer, "no address rewrite without a collapse");
        assert_eq!(indexed.get(0), 2);
        assert_eq!(indexed.get(1), 3);
        indexed.advance_by(Antichain::from_elem(4).borrow(), 1000);
        assert!(indexed.is_constant());
        assert_eq!(indexed.get(999), 4);
        let selected: ColTimes<u64> = std::iter::repeat_n(1, 500).chain(std::iter::repeat_n(2, 500)).collect();
        let mut single = ColTimes::new();
        single.push_range(&selected, 100, 400);
        assert!(single.is_constant());
        assert_eq!(single.len(), 300);
        let dense_source: ColTimes<u64> = (0..300).chain(std::iter::repeat_n(7, 500)).collect();
        let mut single = ColTimes::new();
        single.push_range(&dense_source, 300, 800);
        assert!(single.is_constant());
        assert_eq!(single.to_vec(), vec![7; 500]);
        let small = ColTimes::<u64>::from_raw_lanes(vec![vec![2, 1]], 2);
        assert!(small.rows.is_dense());
        let mut appended: ColTimes<u64> = std::iter::repeat_n(1, 1000).collect();
        appended.push_range(&small, 0, 2);
        assert!(!appended.rows.is_dense());
        assert_eq!(appended.slice(999, 1002).to_vec(), vec![1, 2, 1]);
        let mut crossing: ColTimes<T> = [t(0, &[2]), t(1, &[1])].into_iter().collect();
        assert!(!crossing.advance_by(Antichain::from_elem(t(2, &[])).borrow(), 2));
        assert_eq!(crossing.get(0), t(2, &[2]));
        assert_eq!(crossing.get(1), t(2, &[1]));
        let mut overflow: ColTimes<u64> = std::iter::repeat_n(u64::MAX, 50).chain(std::iter::repeat_n(0, 50)).collect();
        assert_eq!(overflow.results_in(&1u64), Some((50..100).collect()));
        assert_eq!(overflow.to_vec(), vec![1; 50]);
    }

    #[test]
    fn range_copy_and_gather_across_widths() {
        let values = vec![t(0, &[]), t(1, &[2]), t(2, &[3, 4]), t(3, &[]), t(4, &[5, 6, 7])];
        let source: ColTimes<T> = values.iter().cloned().collect();
        let mut copy: ColTimes<T> = std::iter::once(t(9, &[8, 7])).collect();
        copy.push_range(&source, 1, 4);
        copy.push_range(&source, 2, 2);
        copy.push_range(&source, 0, 5);
        let expected: Vec<_> = std::iter::once(t(9, &[8, 7]))
            .chain(values[1..4].iter().cloned()).chain(values.iter().cloned()).collect();
        assert_eq!(copy.to_vec(), expected);
        copy.clear();
        copy.push_range(&source, 4, 5);
        assert_eq!(copy.to_vec(), values[4..5]);
        let narrow: ColTimes<T> = values[..2].iter().cloned().collect();
        let mut wide: ColTimes<T> = std::iter::once(t(0, &[1, 2, 3])).collect();
        wide.push_range(&narrow, 0, 2);
        assert_eq!(wide.to_vec(), vec![t(0, &[1, 2, 3]), values[0].clone(), values[1].clone()]);
        assert_eq!(source.gather(&[4, 0, 4]).to_vec(), vec![values[4].clone(), values[0].clone(), values[4].clone()]);
    }

    /// The lane order must agree with the timestamp's own `Ord` for every pair: the chunk sorts and
    /// merges by the former and every other layer reasons with the latter.
    #[test]
    fn col_times_order_matches_owned_order() {
        let times: Vec<T> = vec![
            t(0, &[]), t(0, &[0]), t(0, &[1]), t(0, &[2]), t(0, &[3]),
            t(0, &[1, 1]), t(0, &[1, 2]), t(0, &[2, 1]), t(0, &[3, 1]), t(0, &[3, 2]),
            t(0, &[1, 1, 1]), t(0, &[3, 1, 2]), t(0, &[3, 2, 1]),
            t(1, &[]), t(1, &[3]), t(1, &[3, 1]),
        ];
        let store: ColTimes<T> = times.iter().cloned().collect();
        let narrow: ColTimes<T> = times[..5].iter().cloned().collect();
        for i in 0..times.len() {
            for j in 0..times.len() {
                assert_eq!(store.get_joined(i, &times[j]), times[i].join(&times[j]));
                assert_eq!(times[i].cmp(&times[j]), store.cmp(i, j), "{:?} vs {:?}", times[i], times[j]);
                if j < 5 {
                    assert_eq!(times[i].cmp(&times[j]), store.cmp_cross(i, &narrow, j));
                    assert_eq!(times[j].cmp(&times[i]), narrow.cmp_cross(j, &store, i));
                }
            }
        }
    }

    /// The edge operations on lanes must agree with the same operations on owned timestamps: a path
    /// summary on every row, raising one coordinate, reading one coordinate, and truncation.
    #[test]
    fn edge_operations_match_owned_times() {
        use differential_dataflow::dynamic::pointstamp::PointStampSummary;
        use timely::progress::PathSummary;
        let times: Vec<T> = vec![t(0, &[]), t(0, &[2]), t(1, &[1, 1]), t(2, &[0, 3]), t(3, &[4]), t(1, &[5, 0, 2])];
        let col: ColTimes<T> = times.iter().cloned().collect();
        let steps = [
            PointStampSummary { retain: None, actions: vec![0, 0, 1] },
            PointStampSummary { retain: Some(2), actions: vec![1] },
            PointStampSummary { retain: Some(1), actions: vec![0, 7] },
            PointStampSummary { retain: None, actions: vec![0, 0, 0, 0, 1] },
            PointStampSummary { retain: None, actions: vec![0, u64::MAX - 3] },
        ];
        for step in &steps {
            let mut out = col.clone();
            let keep = out.results_in(step);
            let expected: Vec<(usize, T)> = times.iter().enumerate().filter_map(|(i, x)| step.results_in(x).map(|y| (i, y))).collect();
            assert_eq!(out.to_vec(), expected.iter().map(|(_, y)| y.clone()).collect::<Vec<_>>(), "{step:?}");
            let kept: Vec<usize> = expected.iter().map(|(i, _)| *i).collect();
            assert_eq!(keep.unwrap_or_else(|| (0..times.len()).collect()), kept, "{step:?}");
        }
        let delays = vec![3u64, 0, 1, 5, 0, 0];
        let mut raised = col.clone();
        raised.lane_max(2, &delays);
        let expected: Vec<T> = times.iter().zip(&delays).map(|(x, &d)| x.join(&t(0, &[0, d]))).collect();
        assert_eq!(raised.to_vec(), expected);
        assert_eq!(col.lane(1), vec![0, 2, 1, 0, 4, 5]);
        assert_eq!(col.lane(9), vec![0; 6]);
        let mut cut = col.clone();
        cut.truncate(2);
        assert_eq!(cut.to_vec(), vec![t(0, &[]), t(0, &[2]), t(1, &[1]), t(2, &[0]), t(3, &[4]), t(1, &[5])]);
        let rebuilt = ColTimes::<T>::from_raw_lanes(col.raw_lanes().to_vec(), col.len());
        assert_eq!(rebuilt.to_vec(), times);
    }

    /// `advance_by`, `beyond` and `minimal` must agree with the lattice on owned timestamps.
    #[test]
    fn lattice_operations_match_owned_times() {
        let times: Vec<T> = vec![
            t(0, &[]), t(0, &[2]), t(1, &[1, 1]), t(2, &[0, 3]), t(3, &[4]), t(1, &[5, 0, 2]), t(5, &[]),
        ];
        let frontiers = [
            Antichain::new(),
            Antichain::from_elem(t(2, &[1])),
            Antichain::from(vec![t(1, &[3]), t(2, &[1, 2])]),
            Antichain::from(vec![t(0, &[6]), t(1, &[2, 4]), t(3, &[0, 0, 1])]),
        ];
        for frontier in &frontiers {
            let mut store: ColTimes<T> = times.iter().cloned().collect();
            let expected: Vec<_> = times.iter().map(|x| frontier.less_equal(x)).collect();
            assert_eq!(store.beyond(frontier.borrow()), expected, "beyond {frontier:?}");
            store.advance_by(frontier.borrow(), 5);
            let mut expected = times.clone();
            for x in expected[..5].iter_mut() { x.advance_by(frontier.borrow()); }
            assert_eq!(store.to_vec(), expected, "advance by {frontier:?}");
        }
        let store: ColTimes<T> = times.iter().cloned().chain(times.iter().cloned()).collect();
        let rows: Vec<usize> = (0..store.len()).rev().collect();
        let actual: Antichain<T> = store.minimal(&rows).into_iter().map(|i| store.get(i)).collect();
        assert_eq!(store.minimal(&rows).len(), actual.len(), "one row per distinct minimal time");
        assert_eq!(actual, times.iter().cloned().collect::<Antichain<T>>());
    }
}
