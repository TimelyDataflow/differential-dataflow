use super::*;
use std::collections::BTreeMap;
#[test]
fn advance_owned_and_shared_nested_times_matches_reference() {
    use differential_dataflow::dynamic::pointstamp::PointStamp;
    use differential_dataflow::lattice::Lattice;
    use timely::order::Product;
    type T = Product<u64, PointStamp<u64>>;
    for depth in [0, 1, 2, 3, 4, 5, 8, 16] {
        let time = |outer, coords: &[u64]| {
            T::new(
                outer,
                PointStamp::new(coords.iter().copied().cycle().take(depth).collect()),
            )
        };
        let times = [
            time(0, &[]),
            time(1, &[2, 3, 1]),
            time(2, &[1, 4, 2]),
            time(2, &[3, 1, 3]),
            time(3, &[2]),
        ];
        let rows: Vec<_> = (0..5)
            .flat_map(|k| (0..2).flat_map(move |v| (0..5).map(move |t| (k, v, t))))
            .map(|(k, v, i)| ((k, v), times[i].clone(), if i % 2 == 0 { 1i64 } else { -1 }))
            .collect();
        for frontier in [
            Antichain::new(),
            Antichain::from_elem(time(3, &[2, 2, 1])),
            Antichain::from(vec![time(1, &[4, 1, 2]), time(3, &[1, 2, 3])]),
            Antichain::from_elem(T::new(
                3,
                PointStamp::new([3, 2, 1, 2].into_iter().collect()),
            )),
        ] {
            let mut expected = BTreeMap::new();
            for (kv, t, d) in &rows {
                let mut t = t.clone();
                t.advance_by(frontier.borrow());
                *expected.entry((*kv, t)).or_insert(0i64) += d;
            }
            expected.retain(|_, d| *d != 0);
            for size in [1, 3, rows.len()] {
                for shared in [false, true] {
                    let chunks: Vec<_> = rows
                        .chunks(size)
                        .map(|rows| {
                            CorgiChunk::from_columns(
                                CValue::u64(rows.iter().map(|r| r.0 .0).collect()),
                                CValue::u64(rows.iter().map(|r| r.0 .1).collect()),
                                rows.iter().map(|r| r.1.clone()).collect(),
                                rows.iter().map(|r| r.2).collect(),
                            )
                        })
                        .collect();
                    let retained = if shared { chunks.clone() } else { Vec::new() };
                    let retained_times: Vec<_> =
                        retained.iter().map(|c| c.times().to_vec()).collect();
                    let (mut input, mut output) = (VecDeque::new(), VecDeque::new());
                    for chunk in chunks {
                        input.push_back(chunk);
                        CorgiChunk::advance(&mut input, frontier.borrow(), false, &mut output);
                    }
                    CorgiChunk::advance(&mut input, frontier.borrow(), true, &mut output);
                    assert!(input.is_empty());
                    let mut actual = BTreeMap::new();
                    let mut previous = None;
                    for chunk in output {
                        let keys = corgi::arrange::leaf_slice(chunk.keys()).unwrap();
                        let vals = corgi::arrange::leaf_slice(chunk.vals()).unwrap();
                        for i in 0..chunk.len_() {
                            let key = ((keys[i], vals[i]), chunk.times().get(i));
                            assert!(previous.as_ref().is_none_or(|p| p < &key));
                            previous = Some(key.clone());
                            assert!(actual.insert(key, chunk.diffs()[i]).is_none());
                        }
                    }
                    assert_eq!(
                        actual, expected,
                        "size={size}, shared={shared}, frontier={frontier:?}"
                    );
                    for (chunk, original) in retained.iter().zip(&retained_times) {
                        assert_eq!(
                            chunk.times().to_vec(),
                            *original,
                            "shared input was modified"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn advance_carries_a_large_group_then_flushes_within_it() {
    use crate::ir::Time;
    let count = TARGET + 1;
    let initial = CorgiChunk::from_parts(
        CValue::Unit(count),
        CValue::Unit(count),
        (0..count)
            .map(|i| Time::new(i as u64, Default::default()))
            .collect(),
        vec![1i64; count],
    );
    let frontier = Antichain::from_elem(Time::new(0, Default::default()));
    let mut input = VecDeque::from([initial]);
    let mut output = VecDeque::new();
    // One key/value group is not complete until the input is done.
    CorgiChunk::advance(&mut input, frontier.borrow(), false, &mut output);
    assert_eq!(input.len(), 1);
    assert!(output.is_empty());
    CorgiChunk::advance(&mut input, frontier.borrow(), true, &mut output);
    assert!(input.is_empty());
    assert_eq!(output.len(), 2);
    let mut row = 0;
    for chunk in output {
        assert!(chunk.len() <= TARGET);
        for i in 0..chunk.len() {
            assert_eq!(chunk.times().get(i), Time::new(row, Default::default()));
            assert_eq!(chunk.diffs()[i], 1);
            row += 1;
        }
    }
    assert_eq!(row, count as u64);
}
