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
//! lane. Every section is a whole number of 64-bit words.

use crate::corgi::container::Updates;
use crate::ir::Diff;

/// `updates`, each at its time in `lanes` (one column per time coordinate; none for no times), as
/// a frame's payload.
pub fn encode(updates: &Updates, lanes: &[Vec<u64>]) -> Vec<u8> {
    let rows = updates.len();
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
    let (rows, lanes) = (word(0)? as usize, word(8)? as usize);
    let (keys, key_bytes) = corgi::bytes::read_from(&bytes[16..])?;
    let at = 16 + key_bytes;
    let (vals, val_bytes) = corgi::bytes::read_from(&bytes[at..])?;
    let mut at = at + val_bytes;
    if keys.len() != rows || vals.len() != rows || bytes.len() != at + 8 * rows * (1 + lanes) {
        return Err(format!("frame: {rows} rows and {lanes} time lanes, but {} keys, {} values and {} bytes", keys.len(), vals.len(), bytes.len()));
    }
    let mut column = || -> Result<Vec<u64>, String> {
        let words = (0..rows).map(|i| word(at + 8 * i)).collect();
        at += 8 * rows;
        words
    };
    let diffs: Vec<Diff> = column()?.into_iter().map(|w| w as Diff).collect();
    let times = (0..lanes).map(|_| column()).collect::<Result<_, _>>()?;
    Ok((Updates { keys, vals, diffs }, times))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corgi::feed::FeedDecoder;
    use corgi::Shape;

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
}
