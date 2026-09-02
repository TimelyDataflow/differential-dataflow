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
//! * `times` / `diffs` go through `columnar`'s [`Stash`], the same encoder DD's own columnar
//!   updates use. `T` is already `Columnar` (the arrangement stores times SoA in
//!   [`ColTimes`](crate::corgi::col_times::ColTimes)), so this is the encoder that type was chosen
//!   for.
//!
//! Every section is a whole number of 64-bit words, so each begins 8-aligned in an 8-aligned
//! buffer — which is what lets `Stash::try_from_bytes` install the received bytes directly rather
//! than relocating them.
//!
//! The comparison worth keeping in view: the row backend's `Vec<((Row, Row), T, R)>` reaches the
//! wire through `bincode`, which walks every `Value` of every row and emits varints. This walks
//! four columns and emits memcpys. That difference is the whole reason a columnar exchange is
//! worth building.
//!
//! **Known cost.** `length_in_bytes` and `into_bytes` are separate calls on `&self`, and the
//! time/diff columns must be built to be measured, so they are built twice — two linear passes
//! over `times`. The fix is the one [`container`](crate::corgi::container) already names: hold
//! times columnar in the container instead of as `Vec<T>`, at which point both calls read a
//! column that already exists. Keys and values do not have this problem: `corgi::bytes` sizes a
//! `Value` by walking its shape, without touching a payload byte.

use columnar::Columnar;
use columnar::bytes::stash::Stash;

use timely::bytes::arc::Bytes;
use timely::dataflow::channels::ContainerBytes;

use crate::corgi::container::CorgiContainer;

/// A columnar column of `T` backed either by an owned container or by received bytes.
type ColStash<T> = Stash<<T as Columnar>::Container, Bytes>;

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

impl<T: Columnar, R: Columnar> ContainerBytes for CorgiContainer<T, R> {
    fn from_bytes(mut bytes: Bytes) -> Self {
        let header = bytes.extract_to(32);
        let (kl, vl, tl, dl) = (header_word(&header, 0), header_word(&header, 1), header_word(&header, 2), header_word(&header, 3));

        let key_bytes = bytes.extract_to(kl);
        let (keys, read) = corgi::bytes::read_from(&key_bytes).expect("corgi key column decode");
        assert_eq!(read, kl, "corgi key column decode read {read} of {kl} bytes");
        let val_bytes = bytes.extract_to(vl);
        let (vals, vread) = corgi::bytes::read_from(&val_bytes).expect("corgi val column decode");
        assert_eq!(vread, vl, "corgi val column decode read {vread} of {vl} bytes");

        let times: ColStash<T> = Stash::try_from_bytes(bytes.extract_to(tl)).expect("time column decode");
        let diffs: ColStash<R> = Stash::try_from_bytes(bytes.extract_to(dl)).expect("diff column decode");
        let container = CorgiContainer { keys, vals, times: to_owned_vec(&times), diffs: to_owned_vec(&diffs) };

        // The four columns are one table, so they must agree on how many rows it has. Checking
        // that here is not ceremony — it is the only layer that knows the answer.
        //
        // `corgi::bytes` guarantees a structurally sound `Value`, but not a *small* one: the
        // payload-free constructors declare rows without spending bytes, so a `Unit` can name a
        // trillion rows in sixteen bytes and nothing inside corgi can call that wrong. The time
        // column can. It is stored per row, so its length is the row count this message actually
        // paid for, and every other column has to match it.
        //
        // What this does not reach is a claim nested *under a `List`*, where flattening
        // legitimately multiplies and the row count no longer bounds the element count. That is
        // the sender's honesty, which is the boundary a cluster-internal exchange accepts anyway;
        // a caller that wants a hard ceiling has `corgi::bytes::declared_rows`.
        let rows = container.times.len();
        assert_eq!(container.diffs.len(), rows, "corgi container: {} diffs for {rows} times", container.diffs.len());
        assert_eq!(container.keys.len(), rows, "corgi container: {} keys for {rows} times", container.keys.len());
        assert_eq!(container.vals.len(), rows, "corgi container: {} vals for {rows} times", container.vals.len());
        container
    }

    fn length_in_bytes(&self) -> usize {
        let times: ColStash<T> = Stash::Typed(T::as_columns(self.times.iter()));
        let diffs: ColStash<R> = Stash::Typed(R::as_columns(self.diffs.iter()));
        32 + corgi::bytes::length_in_bytes(&self.keys)
           + corgi::bytes::length_in_bytes(&self.vals)
           + times.length_in_bytes()
           + diffs.length_in_bytes()
    }

    fn into_bytes<W: std::io::Write>(&self, writer: &mut W) {
        let times: ColStash<T> = Stash::Typed(T::as_columns(self.times.iter()));
        let diffs: ColStash<R> = Stash::Typed(R::as_columns(self.diffs.iter()));
        let lens = [
            corgi::bytes::length_in_bytes(&self.keys) as u64,
            corgi::bytes::length_in_bytes(&self.vals) as u64,
            times.length_in_bytes() as u64,
            diffs.length_in_bytes() as u64,
        ];
        for l in lens {
            writer.write_all(&l.to_le_bytes()).unwrap();
        }
        corgi::bytes::write_to(&self.keys, writer).unwrap();
        corgi::bytes::write_to(&self.vals, writer).unwrap();
        times.write_bytes(writer).unwrap();
        diffs.write_bytes(writer).unwrap();
    }
}

#[cfg(test)]
mod test {
    use timely::dataflow::channels::ContainerBytes;

    use crate::corgi::container::CorgiContainer;
    use crate::ir::{Diff, Time, Value as DValue};

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
        use differential_dataflow::dynamic::pointstamp::PointStamp;
        timely::order::Product::new(outer, PointStamp::new(coords.iter().copied().collect()))
    }

    /// One batch per key/value shape family. A corgi column is homogeneous — every row of a
    /// column shares a shape — so the constructors are exercised one batch at a time rather than
    /// mixed into a single container: scalars (a bare leaf), tuples (a `Prod`), lists (a `List`,
    /// including an empty row), variants (a `Sum`, with per-arm payload shapes), and the nesting
    /// that puts one inside another.
    fn shape_families() -> Vec<(&'static str, Vec<((DValue, DValue), Time, Diff)>)> {
        use DValue::*;
        vec![
            ("scalars", vec![
                ((Int(7), Int(1)), time(0, &[]), 1),
                ((Int(-3), Int(9)), time(1, &[2]), -4),
                ((Int(i64::MIN), Int(i64::MAX)), time(0, &[1, 1]), 7),
            ]),
            ("tuples", vec![
                ((Tuple(vec![Int(1), Int(2)]), Tuple(vec![Int(3)])), time(2, &[]), 2),
                ((Tuple(vec![Int(1), Int(3)]), Tuple(vec![Int(-3)])), time(0, &[0]), -1),
            ]),
            ("lists", vec![
                ((Int(1), List(vec![Int(3), Int(4), Int(5)])), time(0, &[]), 1),
                ((Int(2), List(vec![])), time(0, &[7]), -2),
                ((Int(3), List(vec![Int(6)])), time(1, &[]), 3),
            ]),
            ("variants", vec![
                ((Variant(0, Box::new(Int(11))), Int(0)), time(3, &[7, 7]), 5),
                ((Variant(1, Box::new(Tuple(vec![Int(2), Int(3)]))), Int(1)), time(0, &[]), 1),
                ((Variant(0, Box::new(Int(12))), Int(2)), time(0, &[]), -1),
            ]),
            ("nested", vec![
                ((Tuple(vec![Int(1), Variant(0, Box::new(Int(5)))]), List(vec![Int(1), Int(2)])), time(0, &[]), 1),
                ((Tuple(vec![Int(2), Variant(1, Box::new(Tuple(vec![Int(6), Int(7)])))]), List(vec![Int(3)])), time(1, &[1]), -1),
            ]),
        ]
    }

    /// The declared shapes of each family — what a program's schema would pin (a variant carries
    /// only its tag, so a family with sums cannot be pinned from a row).
    fn shapes_of(name: &str) -> (corgi::Shape, corgi::Shape) {
        use corgi::Shape::{List, Prim, Prod, Sum};
        let u = || Prim(64);
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

    fn container_of(name: &str, updates: Vec<((DValue, DValue), Time, Diff)>) -> CorgiContainer<Time, Diff> {
        let (k, v) = shapes_of(name);
        CorgiContainer::from_updates(updates, &k, &v)
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
        let updates: Vec<_> = (0..1000i64)
            .map(|i| ((DValue::Int(i), DValue::Int(i * 2)), time(0, &[]), 1))
            .collect();
        let c = CorgiContainer::<Time, Diff>::from_updates_pinned(updates);
        assert_eq!(corgi::bytes::length_in_bytes(&c.keys), 24 + 8 * 1000);
        assert_eq!(corgi::bytes::length_in_bytes(&c.vals), 24 + 8 * 1000);
    }
}
