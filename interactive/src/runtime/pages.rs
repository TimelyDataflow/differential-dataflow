//! A chunk's rows as compressed pages: page cutting, a resident index, and lightweight codecs.
//!
//! Rows are cut into pages of about a target count of uncompressed bytes ([`TARGET`]).
//! A page never splits a row, so a row's list elements stay in its page.
//! A page ends where the identifier changes, when there is such a place in the last quarter of the target.
//! Each column of each page is compressed on its own, so a reader decodes only the pages and the columns it needs.
//! Decoding gives ordinary corgi columns, equal to the original rows.
//!
//! **Columns.**
//! A chunk's columns are those of its keys, then those of its values, then one per time lane, then the diffs.
//! A key or value has columns by its shape, in order:
//! - a leaf is one column;
//! - a product has its fields' columns;
//! - a list has a column of row lengths, then its elements' columns;
//! - a sum has a column of tags, then each variant's columns, which hold the page's rows of that variant;
//! - a reference to list rows has the columns of a list, because a page carries the rows it references;
//! - a unit has none.
//!
//! **Codecs.**
//! A column of a page is a run of integers: a leaf's values (bytes, `i64`s, or floats as their total-order keys), list lengths, sum tags, time coordinates or diffs.
//! It is coded by the smallest of:
//! - raw: each value at its width, one byte for bytes and tags and eight otherwise;
//! - frame of reference: the least value, then each value less it, bit-packed at the width of the largest, which is zero bits for a constant run;
//! - dictionary: the distinct values by frame of reference, then each value's position among them, bit-packed;
//! - split, for eight-byte values: the high and the low 32 bits as two runs, each coded by the smallest of the three above.
//!
//! A run of no values takes no bytes.
//! Signed integers are coded as `x ^ 2^63`, which orders as the integers do, so a frame of reference spans their signed range.
//! Bit-unpacking is scalar safe Rust.
//!
//! **Index.**
//! For each page, held beside the pages: its first identifier, its first row, where its bytes start, and where each of its columns starts.

use std::borrow::Cow;
use std::ops::Range;
use std::sync::Arc;

use corgi::{Bounds, Demand, Shape, Tags, Value as CValue};

use crate::runtime::chunk::ident;
use crate::runtime::col_times::ColTimes;

/// The uncompressed bytes a page aims for: the virtual memory page size of the machines measured.
pub const TARGET: usize = 16 << 10;

/// A chunk's rows as compressed pages, with the index that finds them.
pub struct Pages {
    keys: Layout,
    vals: Layout,
    time_lanes: usize,
    /// The columns of each page.
    columns: usize,
    /// Each page's first identifier.
    first_ids: Vec<i64>,
    /// Where each page's rows start, and then the number of rows.
    first_rows: Vec<usize>,
    /// Where each page's bytes start, and then the number of bytes.
    starts: Vec<usize>,
    /// Where each column of each page starts within its page, `columns` to a page.
    offsets: Vec<usize>,
    bytes: Vec<u8>,
}

/// The columns to decode: demands on the keys and on the values, which decode as corgi's [`Demand::project`] would project them, and whether to decode the times and the diffs.
pub struct Select {
    pub keys: Demand,
    pub vals: Demand,
    pub times: bool,
    pub diffs: bool,
}

impl Select {
    /// Every column.
    pub fn all() -> Select {
        Select { keys: Demand::All, vals: Demand::All, times: true, diffs: true }
    }
}

/// Decoded rows: the key and value columns as the selection projects them, and the times and diffs when selected.
pub struct Columns<T> {
    pub keys: CValue,
    pub vals: CValue,
    pub times: Option<ColTimes<T>>,
    pub diffs: Option<Vec<i64>>,
}

impl Pages {
    /// The rows of a chunk's columns as pages of about `target` uncompressed bytes.
    ///
    /// The keys are an arrangement's, whose leading integer lane is the identifier (`chunk::ident`).
    /// Pages prefer to end where the identifier changes, and [`Pages::pages_for`] expects the rows sorted by it, as a chunk's are.
    pub fn encode<T>(keys: &CValue, vals: &CValue, times: &ColTimes<T>, diffs: &[i64], target: usize) -> Pages {
        let rows = diffs.len();
        assert!(keys.len() == rows && vals.len() == rows && times.len() == rows, "pages: columns of different lengths");
        let ids = ident(keys);
        let (ks, vs) = (Source::of(keys), Source::of(vals));
        let lanes = times.raw_lanes();
        let (layout_keys, layout_vals) = (Layout::of(keys), Layout::of(vals));
        let columns = layout_keys.columns() + layout_vals.columns() + lanes.len() + 1;

        let mut row_bytes = ks.row_bytes();
        let fixed = 8 * (lanes.len() as u64 + 1);
        for (total, value) in row_bytes.iter_mut().zip(vs.row_bytes()) { *total += value + fixed; }
        let cuts = cut(&row_bytes, ids, target as u64);
        drop(row_bytes);

        let pages = cuts.len() - 1;
        let mut out = Pages {
            keys: layout_keys,
            vals: layout_vals,
            time_lanes: lanes.len(),
            columns,
            first_ids: Vec::with_capacity(pages),
            first_rows: cuts,
            starts: Vec::with_capacity(pages + 1),
            offsets: Vec::with_capacity(pages * columns),
            bytes: Vec::new(),
        };
        let mut words = Vec::new();
        for page in 0..pages {
            let (lo, hi) = (out.first_rows[page], out.first_rows[page + 1]);
            out.first_ids.push(ids[lo]);
            out.starts.push(out.bytes.len());
            let mut writer = Writer { start: out.bytes.len(), bytes: &mut out.bytes, offsets: &mut out.offsets, words: &mut words };
            let page_rows = lo..hi;
            writer.source(&ks, std::slice::from_ref(&page_rows));
            writer.source(&vs, std::slice::from_ref(&page_rows));
            for lane in lanes { writer.column(lane[lo..hi].iter().copied(), 8); }
            writer.column(diffs[lo..hi].iter().map(|&d| signed(d)), 8);
        }
        out.starts.push(out.bytes.len());
        out.bytes.shrink_to_fit();
        out
    }

    /// Decode a run of pages into one set of columns, holding their rows in order.
    pub fn decode<T>(&self, pages: Range<usize>, select: &Select) -> Columns<T> {
        let mut keys = Sink::new(&self.keys, &select.keys);
        let mut vals = Sink::new(&self.vals, &select.vals);
        let mut lanes = vec![Vec::new(); if select.times { self.time_lanes } else { 0 }];
        let mut diffs = Vec::new();
        let first = self.keys.columns() + self.vals.columns();
        for p in pages.clone() {
            let page = self.page(p);
            let n = self.page_rows(p).len();
            let mut col = 0;
            keys.read(&self.keys, &page, &mut col, n);
            vals.read(&self.vals, &page, &mut col, n);
            for (j, lane) in lanes.iter_mut().enumerate() { decode_run(page.column(first + j), n, 8, lane, same); }
            if select.diffs { decode_run(page.column(first + self.time_lanes), n, 8, &mut diffs, unsigned); }
        }
        let rows = self.first_rows[pages.end] - self.first_rows[pages.start];
        Columns {
            keys: keys.finish(),
            vals: vals.finish(),
            times: select.times.then(|| ColTimes::from_raw_lanes(lanes, rows)),
            diffs: select.diffs.then_some(diffs),
        }
    }

    /// The number of pages.
    pub fn len(&self) -> usize { self.first_ids.len() }

    /// Whether there are no pages, and so no rows.
    pub fn is_empty(&self) -> bool { self.first_ids.is_empty() }

    /// The number of rows.
    pub fn rows(&self) -> usize { *self.first_rows.last().unwrap() }

    /// The rows of page `page`.
    pub fn page_rows(&self, page: usize) -> Range<usize> { self.first_rows[page]..self.first_rows[page + 1] }

    /// The identifier of page `page`'s first row.
    pub fn first_id(&self, page: usize) -> i64 { self.first_ids[page] }

    /// The pages that may hold rows with identifier `id`, when rows are sorted by identifier: from the last page starting below it through the last page starting at it.
    pub fn pages_for(&self, id: i64) -> Range<usize> {
        let hi = self.first_ids.partition_point(|&f| f <= id);
        let lo = self.first_ids.partition_point(|&f| f < id).saturating_sub(1);
        lo.min(hi)..hi
    }

    fn page(&self, page: usize) -> PageView<'_> {
        PageView {
            bytes: &self.bytes[self.starts[page]..self.starts[page + 1]],
            offsets: &self.offsets[page * self.columns..(page + 1) * self.columns],
        }
    }
}

/// Where pages start, ending with the number of rows: page `p` is rows `cuts[p]..cuts[p + 1]`.
///
/// A page takes rows while they fit the target, and at least one.
/// If it would then end within a run of one identifier, it ends instead at the last change of identifier in its last quarter, if there is one.
fn cut(row_bytes: &[u64], ids: &[i64], target: u64) -> Vec<usize> {
    let n = row_bytes.len();
    let mut cuts = vec![0];
    let mut start = 0;
    while start < n {
        let (mut end, mut size) = (start + 1, row_bytes[start]);
        while end < n && size + row_bytes[end] <= target {
            size += row_bytes[end];
            end += 1;
        }
        if end < n && ids[end - 1] == ids[end] {
            let (mut back, mut kept) = (end - 1, size - row_bytes[end - 1]);
            while back > start && 4 * kept >= 3 * target && ids[back - 1] == ids[back] {
                kept -= row_bytes[back - 1];
                back -= 1;
            }
            if back > start && 4 * kept >= 3 * target { end = back; }
        }
        cuts.push(end);
        start = end;
    }
    cuts
}

// ---------------------------------------------------------------------------------------------
// Shapes as columns.

/// How a leaf holds its values: corgi's three storages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Storage {
    U8,
    I64,
    F64,
}

/// A key or value shape as the columns a page holds for it (see the module documentation).
#[derive(Clone, Debug, PartialEq, Eq)]
enum Layout {
    Leaf(Storage),
    Prod(Vec<Layout>),
    List(Box<Layout>),
    Sum(Vec<Layout>),
    /// References to list rows, by the layout of the lists' elements.
    Ref(Box<Layout>),
    Unit,
}

impl Layout {
    fn of(value: &CValue) -> Layout {
        match value {
            CValue::Prim(_) => Layout::Leaf(storage(value)),
            CValue::Prod(fields) => Layout::Prod(fields.iter().map(Layout::of).collect()),
            CValue::List(_, elements) => Layout::List(Box::new(Layout::of(elements))),
            CValue::Sum(_, lanes) => Layout::Sum(lanes.iter().map(Layout::of).collect()),
            CValue::Ref(arena, _) => Layout::Ref(Box::new(Layout::of(arena_parts(arena).1))),
            CValue::Unit(_) => Layout::Unit,
        }
    }

    /// The number of columns a page holds for this layout.
    fn columns(&self) -> usize {
        match self {
            Layout::Leaf(_) => 1,
            Layout::Prod(fields) => fields.iter().map(Layout::columns).sum(),
            Layout::List(elements) | Layout::Ref(elements) => 1 + elements.columns(),
            Layout::Sum(lanes) => 1 + lanes.iter().map(Layout::columns).sum::<usize>(),
            Layout::Unit => 0,
        }
    }
}

/// A leaf's storage.
/// Corgi lets a reader see only whether an integer leaf is held as `i64`s; one that is not is held as bytes, corgi's one other integer storage.
fn storage(leaf: &CValue) -> Storage {
    match corgi::shape_of_value(leaf) {
        Shape::Float => Storage::F64,
        _ if corgi::arrange::borrowed_i64(leaf).is_some() => Storage::I64,
        _ => Storage::U8,
    }
}

/// A reference column's arena: its row bounds and its elements.
fn arena_parts(arena: &CValue) -> (&Bounds, &CValue) {
    match arena {
        CValue::List(bounds, elements) => (bounds, elements),
        _ => unreachable!("a reference names rows of a list"),
    }
}

/// The number of rows of a list.
fn list_rows(bounds: &Bounds) -> usize {
    match bounds {
        Bounds::Offsets(ends) => ends.len(),
        Bounds::Stride(_, rows) => *rows,
    }
}

/// The elements of row `i` of a list.
fn span(bounds: &Bounds, i: usize) -> Range<usize> {
    match bounds {
        Bounds::Offsets(ends) => (if i == 0 { 0 } else { ends[i - 1] })..ends[i],
        Bounds::Stride(k, _) => i * k..(i + 1) * k,
    }
}

/// Add `range` to the end of `ranges`, joining it to the last when they meet.
fn push_range(ranges: &mut Vec<Range<usize>>, range: Range<usize>) {
    if range.is_empty() { return; }
    match ranges.last_mut() {
        Some(last) if last.end == range.start => last.end = range.end,
        _ => ranges.push(range),
    }
}

// ---------------------------------------------------------------------------------------------
// Encoding.

/// A key or value column, read for encoding.
enum Source<'a> {
    /// An integer leaf, as `i64`s.
    Ints(Cow<'a, [i64]>, Storage),
    /// A float leaf, as total-order keys.
    Keys(Vec<u64>),
    Prod(Vec<Source<'a>>),
    List(&'a Bounds, Box<Source<'a>>),
    Sum(&'a Tags, Vec<Source<'a>>),
    /// The referenced rows, and the arena's bounds and elements.
    Ref(&'a [usize], &'a Bounds, Box<Source<'a>>),
    Unit(usize),
}

impl<'a> Source<'a> {
    fn of(value: &'a CValue) -> Source<'a> {
        match value {
            CValue::Prim(_) => match storage(value) {
                Storage::F64 => Source::Keys(value.as_f64("pages").unwrap().into_iter().map(float_key).collect()),
                // Corgi lends bytes only within itself: a byte leaf is read widened to `i64`s.
                held => Source::Ints(value.as_i64("pages").unwrap(), held),
            },
            CValue::Prod(fields) => Source::Prod(fields.iter().map(Source::of).collect()),
            CValue::List(bounds, elements) => Source::List(bounds, Box::new(Source::of(elements))),
            CValue::Sum(tags, lanes) => Source::Sum(tags, lanes.iter().map(Source::of).collect()),
            CValue::Ref(arena, rows) => {
                let (bounds, elements) = arena_parts(arena);
                Source::Ref(rows, bounds, Box::new(Source::of(elements)))
            }
            CValue::Unit(rows) => Source::Unit(*rows),
        }
    }

    /// The uncompressed bytes of each row: a leaf's storage width per element, eight per list row with offsets, nine per sum row with tags, and eight per reference plus the row it references.
    fn row_bytes(&self) -> Vec<u64> {
        match self {
            Source::Ints(xs, Storage::U8) => vec![1; xs.len()],
            Source::Ints(xs, _) => vec![8; xs.len()],
            Source::Keys(keys) => vec![8; keys.len()],
            Source::Prod(fields) => {
                let mut fields = fields.iter().map(Source::row_bytes);
                let mut total = fields.next().unwrap_or_default();
                for field in fields { for (t, f) in total.iter_mut().zip(field) { *t += f; } }
                total
            }
            Source::List(bounds, elements) => list_row_bytes(bounds, elements),
            Source::Sum(tags, lanes) => {
                let own = if matches!(tags, Tags::Const(..)) { 0 } else { 9 };
                let lanes: Vec<Vec<u64>> = lanes.iter().map(Source::row_bytes).collect();
                (0..tags.len()).map(|i| own + lanes[tags.tag_at(i)][tags.offset_at(i)]).collect()
            }
            Source::Ref(rows, bounds, elements) => {
                let arena = list_row_bytes(bounds, elements);
                rows.iter().map(|&row| 8 + arena[row]).collect()
            }
            Source::Unit(rows) => vec![0; *rows],
        }
    }
}

fn list_row_bytes(bounds: &Bounds, elements: &Source) -> Vec<u64> {
    let own = if matches!(bounds, Bounds::Offsets(_)) { 8 } else { 0 };
    let mut before = vec![0];
    for b in elements.row_bytes() { before.push(before.last().unwrap() + b); }
    (0..list_rows(bounds)).map(|i| {
        let row = span(bounds, i);
        own + before[row.end] - before[row.start]
    }).collect()
}

/// Writes one page's columns.
struct Writer<'a> {
    bytes: &'a mut Vec<u8>,
    offsets: &'a mut Vec<usize>,
    /// Where the page starts in `bytes`.
    start: usize,
    words: &'a mut Vec<u64>,
}

impl Writer<'_> {
    /// One column, of values `width` bytes wide.
    fn column(&mut self, words: impl Iterator<Item = u64>, width: usize) {
        self.offsets.push(self.bytes.len() - self.start);
        self.words.clear();
        self.words.extend(words);
        encode_run(self.words, width, self.bytes);
    }

    /// The columns of `source` for its rows `rows`, in order.
    fn source(&mut self, source: &Source, rows: &[Range<usize>]) {
        let all = || rows.iter().flat_map(Range::clone);
        match source {
            Source::Ints(xs, Storage::U8) => self.column(all().map(|i| xs[i] as u64), 1),
            Source::Ints(xs, _) => self.column(all().map(|i| signed(xs[i])), 8),
            Source::Keys(keys) => self.column(all().map(|i| keys[i]), 8),
            Source::Prod(fields) => for field in fields { self.source(field, rows) },
            Source::List(bounds, elements) => {
                self.column(all().map(|i| span(bounds, i).len() as u64), 8);
                let mut inner = Vec::new();
                for r in rows.iter().filter(|r| !r.is_empty()) {
                    push_range(&mut inner, span(bounds, r.start).start..span(bounds, r.end - 1).end);
                }
                self.source(elements, &inner);
            }
            Source::Sum(tags, lanes) => {
                self.column(all().map(|i| tags.tag_at(i) as u64), 1);
                let mut inner = vec![Vec::new(); lanes.len()];
                for i in all() {
                    let at = tags.offset_at(i);
                    push_range(&mut inner[tags.tag_at(i)], at..at + 1);
                }
                for (lane, ranges) in lanes.iter().zip(&inner) { self.source(lane, ranges); }
            }
            Source::Ref(refs, bounds, elements) => {
                self.column(all().map(|i| span(bounds, refs[i]).len() as u64), 8);
                let mut inner = Vec::new();
                for i in all() { push_range(&mut inner, span(bounds, refs[i])); }
                self.source(elements, &inner);
            }
            Source::Unit(_) => {}
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Decoding.

/// One page's bytes, and where its columns start.
struct PageView<'a> {
    bytes: &'a [u8],
    offsets: &'a [usize],
}

impl PageView<'_> {
    fn column(&self, c: usize) -> &[u8] {
        let end = self.offsets.get(c + 1).copied().unwrap_or(self.bytes.len());
        &self.bytes[self.offsets[c]..end]
    }
}

/// A column being decoded from pages, built from its layout and a demand on it.
enum Sink {
    /// A column the demand reads none of: only its row count.
    Rows(usize),
    U8(Vec<u8>),
    I64(Vec<i64>),
    /// Float total-order keys.
    F64(Vec<u64>),
    /// Each field's column, `None` where not demanded; the row count; and whether the demand compacts the product, as `Demand::Prod` does and `Demand::All` does not.
    Prod(Vec<Option<Sink>>, usize, bool),
    /// Row ends and elements.
    List(Vec<usize>, Box<Sink>),
    Sum(Vec<usize>, Vec<Sink>),
    /// The referenced rows' ends and elements.
    Ref(Vec<usize>, Box<Sink>),
}

impl Sink {
    fn new(layout: &Layout, demand: &Demand) -> Sink {
        match (layout, demand) {
            (_, Demand::None) | (Layout::Unit, Demand::All) => Sink::Rows(0),
            (Layout::Leaf(Storage::U8), Demand::All) => Sink::U8(Vec::new()),
            (Layout::Leaf(Storage::I64), Demand::All) => Sink::I64(Vec::new()),
            (Layout::Leaf(Storage::F64), Demand::All) => Sink::F64(Vec::new()),
            (Layout::Prod(fields), Demand::All) => Sink::Prod(fields.iter().map(|f| Some(Sink::new(f, demand))).collect(), 0, false),
            (Layout::Prod(fields), Demand::Prod(needs)) if fields.len() == needs.len() => {
                let fields = fields.iter().zip(needs).map(|(f, need)| (*need != Demand::None).then(|| Sink::new(f, need)));
                Sink::Prod(fields.collect(), 0, true)
            }
            (Layout::List(elements), Demand::All) => Sink::List(Vec::new(), Box::new(Sink::new(elements, demand))),
            (Layout::List(elements), Demand::List(need)) => Sink::List(Vec::new(), Box::new(Sink::new(elements, need))),
            (Layout::Sum(lanes), Demand::All) => Sink::Sum(Vec::new(), lanes.iter().map(|l| Sink::new(l, demand)).collect()),
            (Layout::Sum(lanes), Demand::Sum(needs)) if lanes.len() == needs.len() => {
                Sink::Sum(Vec::new(), lanes.iter().zip(needs).map(|(l, need)| Sink::new(l, need)).collect())
            }
            (Layout::Ref(elements), Demand::All) => Sink::Ref(Vec::new(), Box::new(Sink::new(elements, demand))),
            (Layout::Ref(elements), Demand::Ref(need)) => match &**need {
                Demand::All => Sink::Ref(Vec::new(), Box::new(Sink::new(elements, need))),
                Demand::List(need) => Sink::Ref(Vec::new(), Box::new(Sink::new(elements, need))),
                _ => panic!("pages: a demand on references must keep their lists, not {need:?}"),
            },
            _ => panic!("pages: demand {demand:?} does not match the column's shape"),
        }
    }

    /// Append `n` rows from `page`, whose columns for `layout` start at column `*col`, and move `*col` past them.
    fn read(&mut self, layout: &Layout, page: &PageView, col: &mut usize, n: usize) {
        match (self, layout) {
            (Sink::Rows(rows), _) => {
                *rows += n;
                *col += layout.columns();
            }
            (Sink::U8(xs), _) => { decode_run(page.column(*col), n, 1, xs, |w| w as u8); *col += 1; }
            (Sink::I64(xs), _) => { decode_run(page.column(*col), n, 8, xs, unsigned); *col += 1; }
            (Sink::F64(keys), _) => { decode_run(page.column(*col), n, 8, keys, same); *col += 1; }
            (Sink::Prod(sinks, rows, _), Layout::Prod(fields)) => {
                *rows += n;
                for (sink, field) in sinks.iter_mut().zip(fields) {
                    match sink {
                        Some(sink) => sink.read(field, page, col, n),
                        None => *col += field.columns(),
                    }
                }
            }
            (Sink::List(ends, elements), Layout::List(inner)) | (Sink::Ref(ends, elements), Layout::Ref(inner)) => {
                let before = ends.last().copied().unwrap_or(0);
                let mut total = before;
                decode_run(page.column(*col), n, 8, ends, |len| { total += len as usize; total });
                *col += 1;
                elements.read(inner, page, col, total - before);
            }
            (Sink::Sum(tags, sinks), Layout::Sum(lanes)) => {
                let from = tags.len();
                decode_run(page.column(*col), n, 1, tags, |t| t as usize);
                *col += 1;
                let mut counts = vec![0; lanes.len()];
                for &t in &tags[from..] { counts[t] += 1; }
                for ((sink, lane), count) in sinks.iter_mut().zip(lanes).zip(counts) { sink.read(lane, page, col, count); }
            }
            _ => unreachable!("a sink is built from its layout"),
        }
    }

    fn finish(self) -> CValue {
        match self {
            Sink::Rows(rows) => CValue::Unit(rows),
            Sink::U8(xs) => CValue::u8(xs),
            Sink::I64(xs) => CValue::i64(xs),
            // Corgi builds a float leaf only from floats, and keys them again.
            Sink::F64(keys) => CValue::f64(keys.into_iter().map(float_of_key).collect()),
            Sink::Prod(sinks, rows, compact) => {
                let fields: Vec<CValue> = sinks.into_iter().flatten().map(Sink::finish).collect();
                if compact && fields.is_empty() { CValue::Unit(rows) } else { CValue::Prod(fields) }
            }
            Sink::List(ends, elements) => CValue::List(ends.into(), Box::new(elements.finish())),
            Sink::Sum(tags, lanes) => CValue::sum(tags, lanes.into_iter().map(Sink::finish).collect()),
            Sink::Ref(ends, elements) => {
                let rows = ends.len();
                let arena = CValue::List(ends.into(), Box::new(elements.finish()));
                CValue::Ref(Arc::new(arena), Arc::new((0..rows).collect()))
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Codecs on one run of integers.

const RAW: u8 = 0;
const FOR: u8 = 1;
const DICT: u8 = 2;
const SPLIT: u8 = 3;

const SIGN: u64 = 1 << 63;

/// A signed integer as a word in the same order.
fn signed(x: i64) -> u64 { (x as u64) ^ SIGN }

/// The signed integer of a word from [`signed`].
fn unsigned(w: u64) -> i64 { (w ^ SIGN) as i64 }

fn same(w: u64) -> u64 { w }

/// A float's total-order key, as corgi keys a float leaf: the unsigned order of keys is `f64::total_cmp`.
fn float_key(f: f64) -> u64 {
    let b = f.to_bits();
    if b >> 63 == 1 { !b } else { b ^ SIGN }
}

/// The float whose key is `k`.
fn float_of_key(k: u64) -> f64 {
    f64::from_bits(if k >> 63 == 1 { k ^ SIGN } else { !k })
}

/// The bits that hold `x`.
fn bits(x: u64) -> u32 { 64 - x.leading_zeros() }

/// The bytes of `n` values packed at `bits` bits.
fn packed(n: usize, bits: u32) -> usize { (n * bits as usize).div_ceil(8) }

/// A word from up to eight little-endian bytes.
fn word(bytes: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    b[..bytes.len()].copy_from_slice(bytes);
    u64::from_le_bytes(b)
}

/// The coding chosen for a run.
enum Plan {
    Raw,
    /// The least value and the bits of the largest difference from it.
    For(u64, u32),
    /// The bits of the entries' frame of reference, the bits of a position, and the distinct values in order.
    Dict(u32, u32, Vec<u64>),
    /// The codings of the high and low halves, and the halves.
    Split(Box<Plan>, Box<Plan>, Vec<u64>, Vec<u64>),
}

impl Plan {
    /// The smallest coding of `words`, values `width` bytes wide; `split` allows splitting eight-byte values.
    fn new(words: &[u64], width: usize, split: bool) -> Plan {
        let n = words.len();
        let (least, most) = words.iter().fold((u64::MAX, 0), |(l, m), &w| (l.min(w), m.max(w)));
        let spread = bits(most - least);
        let mut best = Plan::For(least, spread);
        if Plan::Raw.size(n, width) <= best.size(n, width) { best = Plan::Raw; }
        if spread > 0 {
            let mut entries = words.to_vec();
            entries.sort_unstable();
            entries.dedup();
            if entries.len() < n {
                let dict = Plan::Dict(bits(entries[entries.len() - 1] - entries[0]), bits(entries.len() as u64 - 1), entries);
                if dict.size(n, width) < best.size(n, width) { best = dict; }
            }
            if split && width == 8 {
                let hi: Vec<u64> = words.iter().map(|w| w >> 32).collect();
                let lo: Vec<u64> = words.iter().map(|w| w & 0xFFFF_FFFF).collect();
                let halves = Plan::Split(Box::new(Plan::new(&hi, 4, false)), Box::new(Plan::new(&lo, 4, false)), hi, lo);
                if halves.size(n, width) < best.size(n, width) { best = halves; }
            }
        }
        best
    }

    /// The bytes of `n` values coded so.
    fn size(&self, n: usize, width: usize) -> usize {
        match self {
            Plan::Raw => 1 + n * width,
            Plan::For(_, b) => 2 + width + packed(n, *b),
            Plan::Dict(b, code, entries) => 3 + varint_len(entries.len()) + width + packed(entries.len(), *b) + packed(n, *code),
            Plan::Split(hi, lo, ..) => 1 + hi.size(n, 4) + lo.size(n, 4),
        }
    }

    fn write(&self, words: &[u64], width: usize, out: &mut Vec<u8>) {
        match self {
            Plan::Raw => {
                out.push(RAW);
                for &w in words { out.extend_from_slice(&w.to_le_bytes()[..width]); }
            }
            Plan::For(least, b) => {
                out.push(FOR);
                out.extend_from_slice(&least.to_le_bytes()[..width]);
                out.push(*b as u8);
                pack(words.iter().map(|&w| w - least), *b, out);
            }
            Plan::Dict(b, code, entries) => {
                out.push(DICT);
                varint(entries.len(), out);
                out.extend_from_slice(&entries[0].to_le_bytes()[..width]);
                out.push(*b as u8);
                pack(entries.iter().map(|&e| e - entries[0]), *b, out);
                out.push(*code as u8);
                pack(words.iter().map(|w| entries.binary_search(w).unwrap() as u64), *code, out);
            }
            Plan::Split(hi, lo, his, los) => {
                out.push(SPLIT);
                hi.write(his, 4, out);
                lo.write(los, 4, out);
            }
        }
    }
}

/// Append the smallest coding of `words`, values `width` bytes wide (one, four or eight), to `out`; no values take no bytes.
fn encode_run(words: &[u64], width: usize, out: &mut Vec<u8>) {
    if words.is_empty() { return; }
    Plan::new(words, width, true).write(words, width, out);
}

/// Decode `n` values coded by [`encode_run`] at the start of `bytes`, appending each through `from` to `out`, and return the bytes read.
fn decode_run<W>(bytes: &[u8], n: usize, width: usize, out: &mut Vec<W>, mut from: impl FnMut(u64) -> W) -> usize {
    if n == 0 { return 0; }
    if bytes[0] != SPLIT { return decode_whole(bytes, n, width, out, from); }
    // The halves are coded whole; the low half joins the high half as it is decoded.
    let mut hi = Vec::with_capacity(n);
    let at = 1 + decode_whole(&bytes[1..], n, 4, &mut hi, |h| h << 32);
    let mut his = hi.into_iter();
    at + decode_whole(&bytes[at..], n, 4, out, |lo| from(his.next().unwrap() | lo))
}

/// [`decode_run`] for a run coded raw, by frame of reference, or by dictionary.
fn decode_whole<W>(bytes: &[u8], n: usize, width: usize, out: &mut Vec<W>, mut from: impl FnMut(u64) -> W) -> usize {
    match bytes[0] {
        RAW => {
            let end = 1 + n * width;
            let raw = &bytes[1..end];
            match width {
                1 => out.extend(raw.iter().map(|&b| from(b as u64))),
                8 => out.extend(raw.chunks_exact(8).map(|c| from(u64::from_le_bytes(c.try_into().unwrap())))),
                _ => out.extend(raw.chunks_exact(width).map(|c| from(word(c)))),
            }
            end
        }
        FOR => {
            let least = word(&bytes[1..1 + width]);
            let b = bytes[1 + width] as u32;
            let (start, end) = (2 + width, 2 + width + packed(n, b));
            unpack(&bytes[start..end], n, b, out, |r| from(least.wrapping_add(r)));
            end
        }
        DICT => {
            let (d, mut at) = read_varint(bytes, 1);
            let least = word(&bytes[at..at + width]);
            let b = bytes[at + width] as u32;
            at += width + 1;
            let mut entries = Vec::with_capacity(d);
            unpack(&bytes[at..at + packed(d, b)], d, b, &mut entries, |r| least.wrapping_add(r));
            at += packed(d, b);
            let code = bytes[at] as u32;
            at += 1;
            unpack(&bytes[at..at + packed(n, code)], n, code, out, |c| from(entries[c as usize]));
            at + packed(n, code)
        }
        tag => panic!("pages: unknown codec {tag}"),
    }
}

/// Append `values`, each less than `2^bits`, packed at `bits` bits from the lowest bit of the first byte up.
fn pack(values: impl Iterator<Item = u64>, bits: u32, out: &mut Vec<u8>) {
    if bits == 0 { return; }
    let (mut acc, mut held) = (0u128, 0u32);
    for v in values {
        acc |= (v as u128) << held;
        held += bits;
        if held >= 64 {
            out.extend_from_slice(&(acc as u64).to_le_bytes());
            acc >>= 64;
            held -= 64;
        }
    }
    out.extend_from_slice(&acc.to_le_bytes()[..held.div_ceil(8) as usize]);
}

/// Append `n` values packed by [`pack`] at `bits` bits, each through `f`.
fn unpack<W>(bytes: &[u8], n: usize, bits: u32, out: &mut Vec<W>, mut f: impl FnMut(u64) -> W) {
    if bits == 0 {
        out.extend((0..n).map(|_| f(0)));
        return;
    }
    let b = bits as usize;
    let mask = u64::MAX >> (64 - bits);
    // A value is in the sixteen bytes from its first; it reads from `bytes` in place while they are there.
    let at = |packed: &[u8], bit: usize| {
        let window = u128::from_le_bytes(packed[bit / 8..bit / 8 + 16].try_into().unwrap());
        (window >> (bit % 8)) as u64 & mask
    };
    let in_place = if bytes.len() < 16 { 0 } else { ((8 * (bytes.len() - 16) + 7) / b + 1).min(n) };
    out.extend((0..in_place).map(|i| f(at(bytes, i * b))));
    if in_place < n {
        let skip = in_place * b / 8;
        let mut tail = bytes[skip..].to_vec();
        tail.resize(tail.len() + 16, 0);
        out.extend((in_place..n).map(|i| f(at(&tail, i * b - 8 * skip))));
    }
}

fn varint(mut x: usize, out: &mut Vec<u8>) {
    while x >= 0x80 {
        out.push(x as u8 | 0x80);
        x >>= 7;
    }
    out.push(x as u8);
}

fn varint_len(x: usize) -> usize { (bits(x as u64).max(1) as usize).div_ceil(7) }

/// The varint at `bytes[at..]`, and where it ends.
fn read_varint(bytes: &[u8], mut at: usize) -> (usize, usize) {
    let (mut x, mut shift) = (0, 0);
    loop {
        let byte = bytes[at];
        at += 1;
        x |= ((byte & 0x7F) as usize) << shift;
        if byte < 0x80 { return (x, at); }
        shift += 7;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use corgi::arrange::gather;

    type T = crate::ir::Time;

    fn xorshift(s: &mut u64) -> u64 { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; *s }

    /// Some chunk's columns, sorted by identifier in runs of one to three rows, with values of every shape.
    ///
    /// The values: edge integers; a constant; integers whose high halves vary; random integers; bytes; text, some empty; edge floats; lists of lists, some empty; lists of one width; a sum with a variant no row carries; a sum of one variant throughout; lists of sums; units; and references to rows of text.
    /// Hashed keys add a compound key after the identifier, as an arrangement's do.
    fn columns(n: usize, hashed: bool, seed: &mut u64) -> (CValue, CValue, ColTimes<T>, Vec<i64>) {
        let mut next = || xorshift(seed);
        let mut id = -(n as i64);
        let ids: Vec<i64> = (0..n).map(|i| { if i % 3 == 0 || next() % 2 == 0 { id += 1 + (next() % 4) as i64; } id }).collect();
        let edges = [i64::MIN, i64::MAX, 0, -1, 1, i64::MIN + 1, i64::MAX - 1];
        let ints = |rand: &mut dyn FnMut() -> u64, f: &dyn Fn(u64) -> i64| CValue::i64((0..n).map(|_| f(rand())).collect());
        let ends = |rand: &mut dyn FnMut() -> u64, rows: usize, most: u64| {
            let mut end = 0;
            Bounds::offsets((0..rows).map(|_| { end += (rand() % (most + 1)) as usize; end }).collect())
        };
        let text = |rand: &mut dyn FnMut() -> u64, bounds: Bounds| {
            let total = match &bounds { Bounds::Offsets(e) => e.last().copied().unwrap_or(0), Bounds::Stride(k, r) => k * r };
            CValue::List(bounds, Box::new(CValue::u8((0..total).map(|_| b'a' + (rand() % 26) as u8).collect())))
        };
        let floats = [0.0, -0.0, f64::NAN, f64::from_bits(0xFFF8_0000_0000_0001), f64::INFINITY, f64::NEG_INFINITY, 1.5, -2.25];

        let edge = ints(&mut next, &|x| edges[x as usize % edges.len()]);
        let constant = ints(&mut next, &|_| 7);
        let halves = ints(&mut next, &|x| (((x % 3) as i64 - 1) << 32) | (x >> 40) as i64);
        let random = ints(&mut next, &|x| x as i64);
        let bytes = CValue::u8((0..n).map(|_| next() as u8).collect());
        let word_ends = ends(&mut next, n, 12);
        let words = text(&mut next, word_ends);
        let float = CValue::f64((0..n).map(|_| floats[next() as usize % floats.len()]).collect());
        let outer = ends(&mut next, n, 3);
        let inner_rows = match &outer { Bounds::Offsets(e) => e.last().copied().unwrap_or(0), _ => unreachable!() };
        let inner = ends(&mut next, inner_rows, 2);
        let inner_total = match &inner { Bounds::Offsets(e) => e.last().copied().unwrap_or(0), _ => unreachable!() };
        let nested = CValue::List(outer, Box::new(CValue::List(inner, Box::new(CValue::i64((0..inner_total).map(|_| next() as i64 >> 50).collect())))));
        let stride = CValue::List(Bounds::Stride(3, n), Box::new(CValue::i64((0..3 * n).map(|_| (next() % 9) as i64).collect())));
        let tags: Vec<usize> = (0..n).map(|_| (next() % 2) as usize).collect();
        let carried = tags.iter().filter(|&&t| t == 1).count();
        let carried_ends = ends(&mut next, carried, 4);
        let sum = CValue::sum(tags, vec![
            CValue::i64((0..n - carried).map(|_| next() as i64).collect()),
            text(&mut next, carried_ends),
            CValue::Unit(0),
        ]);
        let one_variant = CValue::sum(vec![1; n], vec![CValue::Unit(0), CValue::i64((0..n).map(|_| (next() % 3) as i64).collect())]);
        let listed = ends(&mut next, n, 3);
        let listed_total = match &listed { Bounds::Offsets(e) => e.last().copied().unwrap_or(0), _ => unreachable!() };
        let listed_tags: Vec<usize> = (0..listed_total).map(|_| (next() % 2) as usize).collect();
        let listed_ints = listed_tags.iter().filter(|&&t| t == 0).count();
        let sums = CValue::List(listed, Box::new(CValue::sum(listed_tags, vec![
            CValue::i64((0..listed_ints).map(|_| next() as i64 % 100).collect()),
            CValue::Unit(listed_total - listed_ints),
        ])));
        let arena_rows = 1 + n / 2;
        let arena_ends = ends(&mut next, arena_rows, 5);
        let arena = text(&mut next, arena_ends);
        let refs = CValue::Ref(Arc::new(arena), Arc::new((0..n).map(|_| next() as usize % arena_rows).collect()));

        let vals = CValue::Prod(vec![edge, constant, halves, random, bytes, words, float, nested, stride, sum, one_variant, sums, CValue::Unit(n), refs]);
        let keys = if hashed {
            let key_ends = ends(&mut next, n, 2);
            CValue::Prod(vec![CValue::i64(ids), CValue::Prod(vec![CValue::u8((0..n).map(|_| next() as u8 % 4).collect()), text(&mut next, key_ends)])])
        } else {
            CValue::i64(ids)
        };
        let lanes = vec![
            (0..n).map(|i| (i / 7) as u64).collect(),
            (0..n).map(|_| [0, 1, 5, u64::MAX][next() as usize % 4]).collect(),
        ];
        let times = ColTimes::from_raw_lanes(lanes, n);
        let diffs = (0..n).map(|_| [1, -1, 3, i64::MIN, i64::MAX][next() as usize % 5]).collect();
        (keys, vals, times, diffs)
    }

    /// `value` with each reference replaced by the rows it references, so that columns compare by the rows they hold.
    fn by_value(value: &CValue) -> CValue {
        match value {
            CValue::Prod(fields) => CValue::Prod(fields.iter().map(by_value).collect()),
            CValue::List(bounds, elements) => CValue::List(bounds.clone(), Box::new(by_value(elements))),
            CValue::Sum(tags, lanes) => CValue::Sum(tags.clone(), lanes.iter().map(by_value).collect()),
            CValue::Ref(arena, rows) => by_value(&gather(arena, rows)),
            other => other.clone(),
        }
    }

    /// The columns hold the same rows, at the same storages.
    fn assert_same(actual: &CValue, expected: &CValue) {
        assert_eq!(Layout::of(actual), Layout::of(expected));
        assert_eq!(by_value(actual), by_value(expected));
    }

    /// Rows `rows` of each time lane.
    fn lanes(times: &ColTimes<T>, rows: Range<usize>) -> Vec<Vec<u64>> {
        times.raw_lanes().iter().map(|lane| lane[rows.clone()].to_vec()).collect()
    }

    #[test]
    fn every_shape_round_trips() {
        let mut seed = 11;
        for hashed in [false, true] {
            for n in [0, 1, 2, 500] {
                let (keys, vals, times, diffs) = columns(n, hashed, &mut seed);
                for target in [1, 300, 4096, TARGET] {
                    let pages = Pages::encode(&keys, &vals, &times, &diffs, target);
                    assert_eq!(pages.rows(), n);
                    let all = pages.decode::<T>(0..pages.len(), &Select::all());
                    assert_same(&all.keys, &keys);
                    assert_same(&all.vals, &vals);
                    assert_eq!(all.times.unwrap().raw_lanes(), times.raw_lanes());
                    assert_eq!(all.diffs.unwrap(), diffs);
                }
            }
        }
    }

    /// Each codec, on a run it is the smallest for, and on runs at the extremes of each width.
    #[test]
    fn codecs_round_trip() {
        let mut seed = 5;
        let mut next = || xorshift(&mut seed);
        let three = [next(), next(), next()];
        let cases: Vec<(Vec<u64>, usize, Option<u8>)> = vec![
            ((0..1000).map(|_| next() % 256).collect(), 1, Some(RAW)),
            ((0..1000).map(|_| (1 << 40) | (next() % 1000)).collect(), 8, Some(FOR)),
            (vec![signed(i64::MIN); 1000], 8, Some(FOR)),
            ((0..1000).map(|_| three[next() as usize % 3]).collect(), 8, Some(DICT)),
            ((0..1000).map(|_| ([1, 7, 1000][next() as usize % 3] << 32) | (next() % (1 << 20))).collect(), 8, Some(SPLIT)),
            ((0..1000).map(|_| signed([i64::MIN, i64::MAX, 0][next() as usize % 3])).collect(), 8, None),
            ((0..1000).map(|_| next()).collect(), 8, None),
            ((0..999).map(|_| next() >> 32).collect(), 4, None),
            (vec![0, u64::MAX], 8, None),
            (vec![u64::MAX], 8, None),
        ];
        for (run, width, codec) in cases {
            let mut bytes = vec![0xAA];
            encode_run(&run, width, &mut bytes);
            if let Some(codec) = codec { assert_eq!(bytes[1], codec); }
            let mut out = Vec::new();
            assert_eq!(decode_run(&bytes[1..], run.len(), width, &mut out, same), bytes.len() - 1);
            assert_eq!(out, run);
        }
    }

    #[test]
    fn runs_of_pages_decode_their_rows() {
        let mut seed = 23;
        for hashed in [false, true] {
            let (keys, vals, times, diffs) = columns(1500, hashed, &mut seed);
            let pages = Pages::encode(&keys, &vals, &times, &diffs, 4096);
            assert!(pages.len() > 20);
            let ids = ident(&keys);
            // Runs of an identifier are short against the target, so every page starts a new identifier.
            for p in 1..pages.len() {
                let row = pages.page_rows(p).start;
                assert_ne!(ids[row - 1], ids[row]);
                assert_eq!(pages.first_id(p), ids[row]);
            }
            for (row, &id) in ids.iter().enumerate() {
                assert!(pages.pages_for(id).any(|p| pages.page_rows(p).contains(&row)));
            }
            assert!(pages.pages_for(ids[0] - 1).is_empty());
            for start in 0..=pages.len() {
                for end in (start..=pages.len()).filter(|&end| end - start <= 2 || end == pages.len()) {
                    let rows = pages.first_rows[start]..pages.first_rows[end];
                    let picked: Vec<usize> = rows.clone().collect();
                    let decoded = pages.decode::<T>(start..end, &Select::all());
                    assert_same(&decoded.keys, &gather(&keys, &picked));
                    assert_same(&decoded.vals, &gather(&vals, &picked));
                    assert_eq!(decoded.times.unwrap().raw_lanes(), lanes(&times, rows.clone()));
                    assert_eq!(decoded.diffs.unwrap(), diffs[rows].to_vec());
                }
            }
        }
    }

    #[test]
    fn selected_columns_decode_their_projection() {
        let mut seed = 31;
        let (keys, vals, times, diffs) = columns(800, true, &mut seed);
        let pages = Pages::encode(&keys, &vals, &times, &diffs, 2048);
        let fields = match &vals { CValue::Prod(fields) => fields.len(), _ => unreachable!() };
        let field = |picks: &[(usize, Demand)]| Demand::Prod((0..fields).map(|i| {
            picks.iter().find(|(j, _)| *j == i).map_or(Demand::None, |(_, d)| d.clone())
        }).collect());
        let key_demands = [Demand::None, Demand::All, Demand::Prod(vec![Demand::All, Demand::None]), Demand::Prod(vec![Demand::None, Demand::Prod(vec![Demand::None, Demand::List(Box::new(Demand::None))])])];
        let val_demands = [
            Demand::None,
            Demand::All,
            field(&[]),
            Demand::Prod((0..fields).map(|i| if i % 2 == 0 { Demand::All } else { Demand::None }).collect()),
            field(&[(5, Demand::List(Box::new(Demand::None))), (7, Demand::List(Box::new(Demand::List(Box::new(Demand::All)))))]),
            field(&[(9, Demand::Sum(vec![Demand::None, Demand::All, Demand::None])), (11, Demand::List(Box::new(Demand::Sum(vec![Demand::All, Demand::None]))))]),
            field(&[(13, Demand::Ref(Box::new(Demand::List(Box::new(Demand::None))))), (12, Demand::All)]),
            field(&[(13, Demand::Ref(Box::new(Demand::All))), (6, Demand::All), (3, Demand::All)]),
        ];
        let pick = 3..pages.len() - 2;
        let rows = pages.page_rows(pick.start).start..pages.page_rows(pick.end - 1).end;
        let picked: Vec<usize> = rows.clone().collect();
        let (some_keys, some_vals) = (gather(&keys, &picked), gather(&vals, &picked));
        for (k, key_demand) in key_demands.iter().enumerate() {
            for (v, val_demand) in val_demands.iter().enumerate() {
                let select = Select { keys: key_demand.clone(), vals: val_demand.clone(), times: k % 2 == 0, diffs: v % 2 == 0 };
                let decoded = pages.decode::<T>(pick.clone(), &select);
                assert_same(&decoded.keys, &key_demand.project(some_keys.clone()).unwrap());
                assert_same(&decoded.vals, &val_demand.project(some_vals.clone()).unwrap());
                assert_eq!(decoded.times.map(|t| t.raw_lanes().to_vec()), select.times.then(|| lanes(&times, rows.clone())));
                assert_eq!(decoded.diffs, select.diffs.then(|| diffs[rows.clone()].to_vec()));
            }
        }
    }
}
