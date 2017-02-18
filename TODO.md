## Differential dataflow to-do list:

1. Batch builders need an "ordered" option where the keys and vals are already sorted (cf group.output).
2. Several trace implementations need to be fleshed out (e.g. `time` and `constant`).
	- Consider ConstantCollection type which can only be construct from known data, arranged to `constant`.
3. Several trace implementations could benefit from a RHH `keys` field; prototype and test!
	- Probably wants a Uniform<T: Unsigned> struct for "node identifiers"; needs tweaks to `Data` trait.
4. Lots of sorting, but no radix-sorting. Historically a big improvement.
	- Connects to U: Unsigned output of `hashed`; no point radix-sorting u32 keys as if u64s.
5. Several operators need revision: distinct, threshold, cogroup.
6. The `keys` trace implementation has had zero testing. Important!
7. Progressive merging under-explored; trade-offs in rate of work? (yes, but worth?)

8. High-resolution times aren't too far away. 
	- Think up alternate Collection type with new data bits.
	- Uncomment `group` implementation and get to work.

9. Join now has "deferred work"; check it out to see if it helps on large graphs.