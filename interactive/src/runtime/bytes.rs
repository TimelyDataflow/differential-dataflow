//! The wire format for a [`CorgiContainer`] — what a worker sends when a container crosses a
//! process boundary.
//!
//! Four columns, four encoders, one framing header:
//!
//! ```text
//! u64 keys_len | u64 vals_len | u64 times_len | u64 diffs_len | keys | vals | times | diffs
//! ```
//!
//! * `keys` / `vals` go through [`corgi::bytes`], which writes each leaf column as one contiguous
//!   run of its stored bytes. A batch of a million `(u64, u64)` keys is two headers and two 8 MB
//!   writes — no per-row framing, no per-row dispatch.
//! * `times` are a lane column ([`ColTimes`](crate::runtime::col_times::ColTimes)): the section is
//!   its lane count and row count, with a tagged constant/dictionary form when compressed.
//! * `diffs` go through `columnar`'s [`Stash`], the same encoder DD's own columnar updates use.
//!
//! Every section is a whole number of 64-bit words, so each begins 8-aligned in an 8-aligned
//! buffer — which is what lets `Stash::try_from_bytes` install the received bytes directly rather
//! than relocating them.
//!
//! The comparison worth keeping in view: a `Vec<((Row, Row), T, R)>` of rows reaches the
//! wire through `bincode`, which walks every `Value` of every row and emits varints. This walks
//! four columns and emits memcpys. That difference is the whole reason a columnar exchange is
//! worth building.
//!
//! **Known cost.** `length_in_bytes` and `into_bytes` are separate calls on `&self`, and the
//! diff column must be built to be measured, so it is built twice. Keys, values and times do not
//! have this problem: `corgi::bytes` sizes a `Value` by walking its shape, and the time section's
//! size follows from the lane and row counts.

use columnar::Columnar;
use columnar::bytes::stash::Stash;

use timely::bytes::arc::Bytes;
use timely::dataflow::channels::ContainerBytes;

use crate::runtime::col_times::ColTimes;
use crate::runtime::container::CorgiContainer;

/// A columnar column of `T` backed either by an owned container or by received bytes.
type ColStash<T> = Stash<<T as Columnar>::Container, Bytes>;

/// Dense sections retain the original format. Dictionary sections have:
/// `tagged_width | rows | entries | coordinate_lanes | byte_indices | padding`.
/// A constant has one entry and no row indices.
const DICT_TAG: u64 = 1 << 62;
fn times_len<T>(times: &ColTimes<T>) -> usize {
    match times.dictionary() {
        Some((entries, ids)) => 24 + 8 * entries * times.width() + (ids.len() + 7) / 8 * 8,
        None => 16 + 8 * times.width() * times.len(),
    }
}
fn write_times<T, W: std::io::Write>(times: &ColTimes<T>, writer: &mut W) {
    let dict = times.dictionary();
    writer.write_all(&(times.width() as u64 | if dict.is_some() { DICT_TAG } else { 0 }).to_le_bytes()).unwrap();
    writer.write_all(&(times.len() as u64).to_le_bytes()).unwrap();
    if let Some((entries, _)) = dict { writer.write_all(&(entries as u64).to_le_bytes()).unwrap(); }
    for lane in times.stored_lanes() { for x in lane { writer.write_all(&x.to_le_bytes()).unwrap(); } }
    if let Some((_, ids)) = dict {
        writer.write_all(ids).unwrap();
        writer.write_all(&[0; 7][..(8 - ids.len() % 8) % 8]).unwrap();
    }
}
fn read_times<T>(bytes: &[u8]) -> ColTimes<T> {
    let tagged = u64::from_le_bytes(bytes[..8].try_into().unwrap());
    let width = (tagged & !DICT_TAG) as usize;
    let rows = header_word(bytes, 1);
    let dictionary = tagged & DICT_TAG != 0;
    let (entries, offset) = if dictionary { (header_word(bytes, 2), 24) } else { (rows, 16) };
    let lane_bytes = width.checked_mul(entries).unwrap().checked_mul(8).unwrap();
    let ids_bytes = if dictionary && entries > 1 { rows } else { 0 };
    let index_start = offset + lane_bytes;
    assert_eq!(bytes.len(), index_start + (ids_bytes + 7) / 8 * 8, "time section length");
    let words: Vec<u64> = bytes[offset..index_start].chunks_exact(8).map(|c| u64::from_le_bytes(c.try_into().unwrap())).collect();
    let lanes = if entries == 0 { vec![Vec::new(); width] } else { words.chunks_exact(entries).map(<[u64]>::to_vec).collect() };
    if dictionary {
        ColTimes::from_dictionary(lanes, entries, bytes[index_start..index_start + ids_bytes].to_vec(), rows)
    } else {
        ColTimes::from_raw_lanes(lanes, rows)
    }
}

/// Read a `u64` length word out of the framing header.
fn header_word(header: &[u8], i: usize) -> usize {
    u64::from_le_bytes(header[i * 8..i * 8 + 8].try_into().unwrap()) as usize
}

/// Materialize a received column as owned values. This is the one place the decode pays per row:
/// `Vec<T>` owns its elements, so a `T` has to be reconstructed for each.
fn to_owned_vec<T: Columnar>(stash: &ColStash<T>) -> Vec<T> {
    use columnar::{Index, Len};
    let borrowed = stash.borrow();
    (0..borrowed.len()).map(|i| <T as Columnar>::into_owned(borrowed.get(i))).collect()
}

impl<T, R: Columnar> ContainerBytes for CorgiContainer<T, R> {
    fn from_bytes(mut bytes: Bytes) -> Self {
        let header = bytes.extract_to(32);
        let (kl, vl, tl, dl) = (header_word(&header, 0), header_word(&header, 1), header_word(&header, 2), header_word(&header, 3));

        let key_bytes = bytes.extract_to(kl);
        let (keys, read) = corgi::bytes::read_from(&key_bytes).expect("corgi key column decode");
        assert_eq!(read, kl, "corgi key column decode read {read} of {kl} bytes");
        let val_bytes = bytes.extract_to(vl);
        let (vals, vread) = corgi::bytes::read_from(&val_bytes).expect("corgi val column decode");
        assert_eq!(vread, vl, "corgi val column decode read {vread} of {vl} bytes");

        let times: ColTimes<T> = read_times(&bytes.extract_to(tl));
        let diffs: ColStash<R> = Stash::try_from_bytes(bytes.extract_to(dl)).expect("diff column decode");
        let container = CorgiContainer { keys, vals, times, diffs: to_owned_vec(&diffs) };

        // The four columns are one table, so they must agree on how many rows it has. Checking
        // that here is not ceremony — it is the only layer that knows the answer.
        //
        // Counts in all four sections must agree. Dictionary times can describe many rows in
        // little space; diffs are still dense here. This is a cluster-internal format.
        let rows = container.times.len();
        assert_eq!(container.diffs.len(), rows, "corgi container: {} diffs for {rows} times", container.diffs.len());
        assert_eq!(container.keys.len(), rows, "corgi container: {} keys for {rows} times", container.keys.len());
        assert_eq!(container.vals.len(), rows, "corgi container: {} vals for {rows} times", container.vals.len());
        container
    }

    fn length_in_bytes(&self) -> usize {
        let diffs: ColStash<R> = Stash::Typed(R::as_columns(self.diffs.iter()));
        32 + corgi::bytes::length_in_bytes(&self.keys)
           + corgi::bytes::length_in_bytes(&self.vals)
           + times_len(&self.times)
           + diffs.length_in_bytes()
    }

    fn into_bytes<W: std::io::Write>(&self, writer: &mut W) {
        let diffs: ColStash<R> = Stash::Typed(R::as_columns(self.diffs.iter()));
        let lens = [
            corgi::bytes::length_in_bytes(&self.keys) as u64,
            corgi::bytes::length_in_bytes(&self.vals) as u64,
            times_len(&self.times) as u64,
            diffs.length_in_bytes() as u64,
        ];
        for l in lens {
            writer.write_all(&l.to_le_bytes()).unwrap();
        }
        corgi::bytes::write_to(&self.keys, writer).unwrap();
        corgi::bytes::write_to(&self.vals, writer).unwrap();
        write_times(&self.times, writer);
        diffs.write_bytes(writer).unwrap();
    }
}

#[cfg(test)]
mod test {
    use timely::dataflow::channels::ContainerBytes;

    use crate::runtime::container::CorgiContainer;
    use crate::ir::{Diff, Time};

    type Update = ((String, String), Time, Diff);

    /// Serialize through the same two-step timely uses (size, then write) and decode the result,
    /// checking that the promised size is the size delivered.
    fn round_trip(c: &CorgiContainer<Time, Diff>) -> CorgiContainer<Time, Diff> {
        let len = c.length_in_bytes();
        let mut buf: Vec<u8> = Vec::with_capacity(len);
        c.into_bytes(&mut buf);
        assert_eq!(buf.len(), len, "length_in_bytes disagrees with into_bytes");
        assert_eq!(buf.len() % 8, 0, "encoding is not word-aligned");
        let bytes = timely::bytes::arc::BytesMut::from(buf).freeze();
        <CorgiContainer<Time, Diff> as ContainerBytes>::from_bytes(bytes)
    }

    fn time(outer: u64, coords: &[u64]) -> Time {
        crate::ir::Time::new(std::iter::once(outer).chain(coords.iter().copied()).collect())
    }

    /// One batch per key/value shape family. A corgi column is homogeneous — every row of a
    /// column shares a shape — so the constructors are exercised one batch at a time rather than
    /// mixed into a single container: scalars (a bare leaf), tuples (a `Prod`), lists (a `List`,
    /// including an empty row), variants (a `Sum`, with per-arm payload shapes), and the nesting
    /// that puts one inside another.
    fn shape_families() -> Vec<(&'static str, Vec<Update>)> {
        let rows = |rows: &[(&str, &str, Time, Diff)]| rows.iter().map(|(k, v, t, d)| ((k.to_string(), v.to_string()), t.clone(), *d)).collect();
        vec![
            ("scalars", rows(&[
                ("7", "1", time(0, &[]), 1),
                ("-3", "9", time(1, &[2]), -4),
                (&i64::MIN.to_string(), &i64::MAX.to_string(), time(0, &[1, 1]), 7),
            ])),
            ("tuples", rows(&[
                ("tuple(1,2)", "tuple(3)", time(2, &[]), 2),
                ("tuple(1,3)", "tuple(-3)", time(0, &[0]), -1),
            ])),
            ("lists", rows(&[
                ("1", "list(3,4,5)", time(0, &[]), 1),
                ("2", "list()", time(0, &[7]), -2),
                ("3", "list(6)", time(1, &[]), 3),
            ])),
            ("variants", rows(&[
                ("inject(0,11)", "0", time(3, &[7, 7]), 5),
                ("inject(1,tuple(2,3))", "1", time(0, &[]), 1),
                ("inject(0,12)", "2", time(0, &[]), -1),
            ])),
            ("nested", rows(&[
                ("tuple(1,inject(0,5))", "list(1,2)", time(0, &[]), 1),
                ("tuple(2,inject(1,tuple(6,7)))", "list(3)", time(1, &[1]), -1),
            ])),
        ]
    }

    /// The declared shapes of each family: what a program's schema would pin.
    fn shapes_of(name: &str) -> (corgi::Shape, corgi::Shape) {
        use corgi::Shape::{Int, List, Prod, Sum};
        let u = || Int;
        let pair = || Prod(vec![u(), u()]);
        match name {
            "scalars" => (u(), u()),
            "tuples" => (pair(), Prod(vec![u()])),
            "lists" => (u(), List(Box::new(u()))),
            "variants" => (Sum(vec![u(), pair()]), u()),
            "nested" => (Prod(vec![u(), Sum(vec![u(), pair()])]), List(Box::new(u()))),
            other => panic!("no shapes for family {other}"),
        }
    }

    fn container_of(name: &str, updates: Vec<Update>) -> CorgiContainer<Time, Diff> {
        let (k, v) = shapes_of(name);
        CorgiContainer::from_updates(updates, &k, &v)
    }

    #[test]
    fn dictionary_times_wire_and_legacy_dense() {
        let updates = (0..1000).map(|i| ((i.to_string(), "1".into()), time(2, &[3]), 1)).collect();
        let c = container_of("scalars", updates);
        assert_eq!(super::times_len(&c.times), 40);
        assert_eq!(round_trip(&c).times.to_vec(), c.times.to_vec());
        let mut legacy = Vec::new();
        for word in [1u64, 3, 7, 7, 9] { legacy.extend_from_slice(&word.to_le_bytes()); }
        assert_eq!(super::read_times::<u64>(&legacy).to_vec(), vec![7, 7, 9]);
        let dense: crate::runtime::col_times::ColTimes<u64> = (0..1000).collect();
        let mut bytes = Vec::new();
        super::write_times(&dense, &mut bytes);
        assert_eq!(bytes.len(), super::times_len(&dense));
        assert_eq!(super::read_times::<u64>(&bytes).to_vec(), dense.to_vec());
    }

    #[test]
    fn extract_advance_settle_round_trip() {
        use std::collections::VecDeque;
        use differential_dataflow::trace::chunk::Chunk;
        use timely::progress::Antichain;
        use crate::runtime::chunk::CorgiChunk;

        let chunk = |keys: std::ops::Range<i64>, times: Vec<u64>| {
            let len = times.len();
            CorgiChunk::from_columns(
                corgi::Value::i64(keys.collect()),
                corgi::Value::Unit(len), times.into_iter().map(|t| time(t, &[])).collect(), vec![1; len],
            )
        };
        let mut input = VecDeque::from([chunk(10..21, vec![1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 5])]);
        let frontier = Antichain::from_elem(time(3, &[]));
        let (mut keep, mut ship) = (VecDeque::new(), VecDeque::new());
        CorgiChunk::extract(&mut input, frontier.borrow(), &mut Antichain::new(), &mut keep, &mut ship);
        assert!(input.is_empty());
        assert_eq!(keep[0].times().to_vec(), vec![time(5, &[])]);
        let mut advanced = VecDeque::new();
        CorgiChunk::advance(&mut ship, frontier.borrow(), true, &mut advanced);
        assert!(ship.is_empty());
        advanced.push_front(chunk(0..10, vec![3; 10]));
        let mut settled = VecDeque::new();
        CorgiChunk::settle(&mut advanced, true, &mut settled);
        assert!(advanced.is_empty());
        assert_eq!(settled.len(), 1);
        let c = settled.pop_front().unwrap();
        let back = round_trip(&CorgiContainer {
            keys: c.keys().clone(), vals: c.vals().clone(),
            times: c.times().clone(), diffs: c.diffs().to_vec(),
        });
        let expected: Vec<_> = (0..20).map(|k| ((k.to_string(), "tuple()".into()), time(3, &[]), 1)).collect();
        assert_eq!(back.into_updates(), expected);
    }

    /// Every update survives the round trip, with its time and diff, for every shape family.
    #[test]
    fn round_trip_preserves_updates() {
        for (name, updates) in shape_families() {
            let c = container_of(name, updates.clone());
            let back = round_trip(&c);
            assert_eq!(back.into_updates(), updates, "{name} did not survive the round trip");
        }
    }

    /// An empty container is a legal message: timely can hand one to the encoder, and what comes
    /// back must be empty rather than malformed.
    #[test]
    fn round_trip_empty() {
        let c = CorgiContainer::<Time, Diff>::default();
        let back = round_trip(&c);
        assert_eq!(back.into_updates(), Vec::new());
    }

    /// The columns themselves survive, not just the rows read back out: the decoded key column
    /// hashes row-for-row like the sender's, which is what the exchange relies on to place a key
    /// on the same worker after a round trip as before it.
    #[test]
    fn round_trip_preserves_column_hashes() {
        for (name, updates) in shape_families() {
            let c = container_of(name, updates);
            let (keys, vals) = (c.keys.clone(), c.vals.clone());
            let back = round_trip(&c);
            assert_eq!(corgi::hash(&back.keys), corgi::hash(&keys), "{name} keys");
            assert_eq!(corgi::hash(&back.vals), corgi::hash(&vals), "{name} vals");
        }
    }

    /// The payload scales with the data, not with a per-row framing tax: a thousand scalar keys
    /// cost their thousand words plus one header, which is the property the whole format exists
    /// for. (Times and diffs are genuinely per-row data and are counted separately.)
    #[test]
    fn key_columns_cost_their_payload() {
        let updates = (0..1000i64).map(|i| ((i.to_string(), (i * 2).to_string()), time(0, &[]), 1)).collect();
        let c = CorgiContainer::<Time, Diff>::from_updates(updates, &corgi::Shape::Int, &corgi::Shape::Int);
        assert_eq!(corgi::bytes::length_in_bytes(&c.keys), 24 + 8 * 1000);
        assert_eq!(corgi::bytes::length_in_bytes(&c.vals), 24 + 8 * 1000);
    }
}
