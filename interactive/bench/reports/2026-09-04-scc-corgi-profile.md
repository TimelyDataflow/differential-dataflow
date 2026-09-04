# Where scc's time goes on corgi (medium, 1 worker)

Profile: `bench.py --profile scc --backend corgi --scale medium` at d875d6e0,
samply at 1kHz, thread `timely:work-0`. 100k nodes / 200k edges, then 50 epochs
of 100 replaced edges. Initial epoch 1.89s, churn 2.81s (56ms per epoch).

## Churn epochs (2.77s, 2770 samples)

| where | share | what it is |
|---|---|---|
| `ProxyReduceTactic::retire` | 48% | the corgi reduce (`min` in both fixpoints) |
| ├ `collect_present` | 19% | gathering each changed key's rows; `find_ranges` is 14.5% of the epoch on its own |
| ├ `merge_present` | 6% | k-way merge of the selected runs (`BinaryHeap::sift_up` 2.3%) |
| ├ `Sweep::next_crossing` | 4% | the time sweep |
| └ retire self + `SmallVec::from_iter` | 11% | per-key bookkeeping and allocation |
| spine merges via `set_physical_compaction` | 29% | `MergeVariant::work` on the corgi arrangements |
| ├ `CorgiChunk::advance` | 18% | `advance_by(frontier)` per row: `ColTimes::get` materializes a `Product<u64, PointStamp>`, `join` allocates a `SmallVec` |
| ├ `CorgiChunk::merge` | 6% | `compare_pairs`, `ColTimes::push_range` |
| └ `pack` | 3% | |
| `join_with_tactic` | 19% | the two joins per fixpoint; `ProxyJoinIter::next` 7.5% |
| allocation (`mi_malloc_aligned`, `mi_free`, `finish_grow`, `try_grow`) | ~17% self, spread over the above | |

## Initial epoch (1.9s)

| where | share |
|---|---|
| `arrange_core` (batcher `merge_by` 21%, `CorgiChunk::merge` 19%, spine merges 16%) | 48% |
| `ProxyReduceTactic::retire` | 27% |
| `join_with_tactic` | 11% |
| `compare_pairs` (self) | 6% |

## Candidates, in the order I would try them

1. **`find_ranges` seeks from scratch for every needle.** `corgi::arrange::find_ranges`
   runs one `partition_point` over the whole haystack per needle, so a sorted
   needle set costs n·log(h). The needles (`changed`) are ascending, so a
   galloping search from the previous hit costs n·log(h/n). 14.5% of a churn
   epoch is in that loop. Lives in corgi (WIP repo), not here.
2. **`CorgiChunk::advance` re-derives the same advanced time per row.** Times in
   a batch repeat heavily (one epoch, a handful of iterations), but every row
   pays `get` + `advance_by` + the allocations inside `PointStamp::join`.
   Memoizing `advance_by` per distinct time ref, or advancing the SoA column
   without materializing `T`, removes most of the 18%.
3. **Merges spend their time comparing and copying times.** `ColTimes::push_range`
   and `compare_pairs` are the merge's cost; the earlier `compare_at` fix went
   here too. Worth checking whether `push_range` extends in one `extend_from`
   per range or pushes per element.
4. The reduce's own allocation per key (`SmallVec::from_iter` 7% of retire) is
   the same pattern as the earlier `compare_at` batch-of-one: an allocation on
   a per-key path.

The export arrangement the server keeps (row-based `ValSpine` at host time)
does not show up: the corgi arrangements inside the program dominate.
