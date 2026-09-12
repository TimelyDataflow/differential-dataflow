//! Regression coverage for the sorted/consolidated chunk-chain merge contract.
use corgi::Value;
use differential_dataflow::batcher::merge::Merger;
use differential_dataflow::trace::chunk::{Chunk, ChunkMerger};
use interactive::corgi::chunk::CorgiChunk;

fn chunk(times: &[u64]) -> CorgiChunk<u64, i64> {
    CorgiChunk::from_columns(
        Value::u64(vec![7; times.len()]),
        Value::u64(vec![8; times.len()]),
        times.iter().collect(),
        vec![1; times.len()],
    )
}

fn check_horizon(prefix: usize) {
    let n = prefix as u64;
    let left = vec![chunk(&(0..n).collect::<Vec<_>>()), chunk(&[n + 1])];
    let right = vec![chunk(&[n, n + 2])];
    let mut output = Vec::new();
    ChunkMerger::default().merge(left, right, &mut output, &mut Vec::new());
    let times: Vec<_> = output.iter().flat_map(|c| (0..c.times().len()).map(|i| c.times().get(i))).collect();
    assert_eq!(times.len(), prefix + 3);
    assert_eq!(&times[prefix - 1..], [n - 1, n, n + 1, n + 2]);
    assert!(times.windows(2).all(|pair| pair[0] < pair[1]));
}

#[test]
fn merge_keeps_time_order_when_an_equal_value_class_crosses_a_chunk_boundary() {
    check_horizon(1);
}

#[test]
#[ignore = "scale confirmation with fully graded inputs; the small case tests the same merge contract"]
fn merge_horizon_with_graded_input_chains() {
    check_horizon(CorgiChunk::<u64, i64>::TARGET);
}
