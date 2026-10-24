//! The payload of a binary `data` frame: a batch of updates as columns, for a client to decode.
//!
//! ```text
//! u64 rows | u64 lanes | keys | vals | i64 * rows diffs | u64 * rows * lanes times
//! ```
//!
//! Every quantity is little-endian. `keys` and `vals` are each one column in corgi's
//! self-describing encoding ([`corgi::bytes`]), so a decoder reads their shapes from the bytes.
//! The times are `lanes` columns of `rows` words, one after the other: a `peek`'s frame holds the
//! consolidated contents and no times, and a `tail`'s frames hold changes, each at its time, one
//! lane; a frame holds no more ([`MAX_LANES`]). Every section is a whole number of 64-bit words.

use corgi::Shape;

use crate::runtime::container::Updates;
use crate::ir::Diff;

/// The most time lanes a frame holds: a server's frames carry host times, one lane, or none.
pub const MAX_LANES: u64 = 1;

/// `updates`, each at its time in `lanes` (one column per time coordinate; none for no times), as
/// a frame's payload.
pub fn encode(updates: &Updates, lanes: &[Vec<u64>]) -> Vec<u8> {
    let rows = updates.len();
    assert!(lanes.len() as u64 <= MAX_LANES, "a frame holds at most {MAX_LANES} time lanes");
    assert!(lanes.iter().all(|lane| lane.len() == rows), "every time lane has a time per update");
    let length = 16 + corgi::bytes::length_in_bytes(&updates.keys) + corgi::bytes::length_in_bytes(&updates.vals) + 8 * rows * (1 + lanes.len());
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(&(rows as u64).to_le_bytes());
    bytes.extend_from_slice(&(lanes.len() as u64).to_le_bytes());
    corgi::bytes::write_to(&updates.keys, &mut bytes).expect("writing to a vector");
    corgi::bytes::write_to(&updates.vals, &mut bytes).expect("writing to a vector");
    for diff in &updates.diffs {
        bytes.extend_from_slice(&diff.to_le_bytes());
    }
    for time in lanes.iter().flatten() {
        bytes.extend_from_slice(&time.to_le_bytes());
    }
    bytes
}

/// A frame's updates, and their time lanes.
pub fn decode(bytes: &[u8]) -> Result<(Updates, Vec<Vec<u64>>), String> {
    let word = |at: usize| -> Result<u64, String> {
        bytes.get(at..at + 8).map(|w| u64::from_le_bytes(w.try_into().unwrap())).ok_or_else(|| format!("frame: no word at byte {at}"))
    };
    let (rows, lanes) = (word(0)?, word(8)?);
    if lanes > MAX_LANES {
        return Err(format!("frame: {lanes} time lanes, more than the {MAX_LANES} a frame holds"));
    }
    let (keys, key_bytes) = corgi::bytes::read_from(&bytes[16..])?;
    let at = 16 + key_bytes;
    let (vals, val_bytes) = corgi::bytes::read_from(&bytes[at..])?;
    let mut at = at + val_bytes;
    // The diffs and times, as bytes: no more than the frame holds, with no arithmetic that wraps.
    let tail = rows.checked_mul(8 * (1 + lanes)).and_then(|n| n.checked_add(at as u64));
    if keys.len() as u64 != rows || vals.len() as u64 != rows || tail != Some(bytes.len() as u64) {
        return Err(format!("frame: {rows} rows and {lanes} time lanes, but {} keys, {} values and {} bytes", keys.len(), vals.len(), bytes.len()));
    }
    let (rows, lanes) = (rows as usize, lanes as usize);
    let mut column = || -> Result<Vec<u64>, String> {
        let words = (0..rows).map(|i| word(at + 8 * i)).collect();
        at += 8 * rows;
        words
    };
    let diffs: Vec<Diff> = column()?.into_iter().map(|w| w as Diff).collect();
    let times = (0..lanes).map(|_| column()).collect::<Result<_, _>>()?;
    Ok((Updates { keys, vals, diffs }, times))
}

/// The updates of a frame fed to an input whose declared shapes are `key` and `val`: a frame with
/// no time lanes (a feed's rows take the time it is fed at), whose keys and values have exactly
/// those shapes, whatever their storage. A column can name more rows than its bytes hold (a `Unit`
/// says how many it has), so a frame naming more than `max_rows` rows anywhere is refused before
/// any work is done per row.
pub fn decode_feed(bytes: &[u8], (key, val): &(Shape, Shape), max_rows: u64) -> Result<Updates, String> {
    let (updates, lanes) = decode(bytes)?;
    if !lanes.is_empty() {
        return Err(format!("frame: a feed's rows take the time they are fed at, but this frame has {} time lanes", lanes.len()));
    }
    let named = corgi::bytes::declared_rows(&updates.keys).max(corgi::bytes::declared_rows(&updates.vals));
    if named > max_rows {
        return Err(format!("frame: a column names {named} rows, more than the {max_rows} a feed may"));
    }
    let shapes = (corgi::shape_of_value(&updates.keys), corgi::shape_of_value(&updates.vals));
    if shapes.0 != *key || shapes.1 != *val {
        return Err(format!("frame: rows of shape ({}, {}), but the input's are ({key}, {val})", shapes.0, shapes.1));
    }
    Ok(updates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::feed::FeedDecoder;

    /// A frame decodes to the updates and times it was made from.
    #[test]
    fn frames_round_trip() {
        let key = Shape::Prod(vec![Shape::Int, Shape::List(Box::new(Shape::Int))]);
        let val = Shape::Sum(vec![Shape::Float, Shape::Unit]);
        let mut decoder = FeedDecoder::new(&key, &val);
        decoder.push("tuple(1,list(104,105))", Some("inject(0,2.5)"), 3).unwrap();
        decoder.push("tuple(-7,list())", Some("inject(1,tuple())"), -1).unwrap();
        let updates = decoder.finish();
        assert_eq!(decode(&encode(&updates, &[])).unwrap(), (updates.clone(), vec![]));
        let lanes = vec![vec![4, 9]];
        assert_eq!(decode(&encode(&updates, &lanes)).unwrap(), (updates.clone(), lanes));
        let mut short = encode(&updates, &[]);
        short.pop();
        assert!(decode(&short).is_err(), "a frame cut short");
    }

    /// A feed frame is read only at its input's shapes, with no times, and naming no more rows than
    /// a feed may.
    #[test]
    fn feed_frames_are_checked() {
        let shape = (Shape::Prod(vec![Shape::Int, Shape::List(Box::new(Shape::Int))]), Shape::Unit);
        let mut decoder = FeedDecoder::new(&shape.0, &shape.1);
        decoder.push("tuple(1,list(104,105))", None, 3).unwrap();
        decoder.push("tuple(-7,list())", None, -1).unwrap();
        let updates = decoder.finish();
        assert_eq!(decode_feed(&encode(&updates, &[]), &shape, 1 << 20).unwrap(), updates);
        assert!(decode_feed(&encode(&updates, &[vec![4, 9]]), &shape, 1 << 20).is_err(), "a frame with times");
        assert!(decode_feed(&encode(&updates, &[]), &(Shape::Int, Shape::Unit), 1 << 20).is_err(), "another shape");
        let units = Updates { keys: corgi::Value::Unit(5), vals: corgi::Value::Unit(5), diffs: vec![1; 5] };
        let unit = (Shape::Unit, Shape::Unit);
        assert!(decode_feed(&encode(&units, &[]), &unit, 5).is_ok());
        assert!(decode_feed(&encode(&units, &[]), &unit, 4).is_err(), "more rows than a feed may");
    }

    /// Counts that do not fit the frame are refused, not overflowed or allocated.
    #[test]
    fn impossible_counts_are_errors() {
        let empty = Updates::empty(&(Shape::Int, Shape::Unit));
        let with = |rows: u64, lanes: u64| {
            let mut bytes = encode(&empty, &[]);
            bytes[..8].copy_from_slice(&rows.to_le_bytes());
            bytes[8..16].copy_from_slice(&lanes.to_le_bytes());
            decode(&bytes)
        };
        assert!(with(0, 0).is_ok());
        assert!(with(0, u64::MAX).is_err(), "lanes past the most a frame holds");
        assert!(with(0, MAX_LANES + 1).is_err());
        assert!(with(u64::MAX, 1).is_err(), "a row count whose bytes overflow");
        assert!(with(u64::MAX / 8, 0).is_err());
    }
}
