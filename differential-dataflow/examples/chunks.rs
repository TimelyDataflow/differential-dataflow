//! Minimal dataflow over the `Vec`-backed `Chunk` container.
//!
//! Mirrors the `val` arm of `spines.rs`, but arranges through `ChunkBatcher` /
//! `ChunkBuilder` / `ChunkSpine` — i.e. the merge batcher, builder, and spine
//! built atop the `Chunk` trait and its `ChunkBatch`. Run as:
//!
//! ```text
//! cargo run --release --example chunks -- <keys> <size>
//! ```

use differential_dataflow::Hashable;
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::trace::chunk::vec::{ChunkBatcher, ChunkBuilder, ChunkSpine, VecChunk};
use differential_dataflow::trace::chunk::col::ColChunk;
use differential_dataflow::trace::implementations::chunker::ContainerChunker;
use differential_dataflow::trace::implementations::ord_neu::{OrdValBatcher, RcOrdValBuilder, OrdValSpine};

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::probe::Handle;

fn main() {
    let keys: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let size: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    // "chunk" (default): our `Chunk`-backed trace. "ord": the standard `ord_neu` trace.
    let mode: String = std::env::args().nth(3).unwrap_or_else(|| "chunk".to_string());
    println!("Running [{mode}] arrangement");

    let timer = std::time::Instant::now();

    // Skip the three positional args we consume (keys, size, mode); the rest are
    // timely's worker flags.
    timely::execute_from_args(std::env::args().skip(4), move |worker| {
        let mut probe = Handle::new();
        let (mut data_input, mut keys_input) = worker.dataflow(|scope| {
            let (data_input, data) = scope.new_collection::<u64, i64>();
            let (keys_input, keys) = scope.new_collection::<u64, i64>();
            let data = data.map(|x| (x, ()));
            let keys = keys.map(|x| (x, ()));

            match mode.as_str() {
                "chunk" => {
                    // The chunk batcher's output (`VecChunk`) differs from the stream
                    // container (`Vec`), so this is a cross-container chunker case:
                    // drop to `arrange_core` with an explicit `ContainerChunker<VecChunk>`.
                    type Ba = ChunkBatcher<u64, (), u64, i64>;
                    type Bu = ChunkBuilder<u64, (), u64, i64>;
                    type Sp = ChunkSpine<u64, (), u64, i64>;
                    type Chu = ContainerChunker<VecChunk<u64, (), u64, i64>>;
                    let data = arrange_core::<_, _, Chu, Ba, Bu, Sp>(
                        data.inner, Exchange::new(|u: &((u64, ()), u64, i64)| (u.0).0.hashed().into()), "Data");
                    let keys = arrange_core::<_, _, Chu, Ba, Bu, Sp>(
                        keys.inner, Exchange::new(|u: &((u64, ()), u64, i64)| (u.0).0.hashed().into()), "Keys");
                    keys.join_core(data, |_k, &(), &()| Option::<()>::None).probe_with(&mut probe);
                }
                "col" => {
                    // Same harness, but the columnar (`UpdatesTyped` trie) `Chunk`.
                    type Ba = differential_dataflow::trace::chunk::col::ChunkBatcher<u64, (), u64, i64>;
                    type Bu = differential_dataflow::trace::chunk::col::ChunkBuilder<u64, (), u64, i64>;
                    type Sp = differential_dataflow::trace::chunk::col::ChunkSpine<u64, (), u64, i64>;
                    type Chu = ContainerChunker<ColChunk<(u64, (), u64, i64)>>;
                    let data = arrange_core::<_, _, Chu, Ba, Bu, Sp>(
                        data.inner, Exchange::new(|u: &((u64, ()), u64, i64)| (u.0).0.hashed().into()), "Data");
                    let keys = arrange_core::<_, _, Chu, Ba, Bu, Sp>(
                        keys.inner, Exchange::new(|u: &((u64, ()), u64, i64)| (u.0).0.hashed().into()), "Keys");
                    // The columnar cursor yields `Val = columnar::Ref<()> = ()`, not `&()`.
                    keys.join_core(data, |_k, _, _| Option::<()>::None).probe_with(&mut probe);
                }
                "ord" => {
                    type Ba = OrdValBatcher<u64, (), u64, i64>;
                    type Bu = RcOrdValBuilder<u64, (), u64, i64>;
                    type Sp = OrdValSpine<u64, (), u64, i64>;
                    let data = data.arrange::<Ba, Bu, Sp>();
                    let keys = keys.arrange::<Ba, Bu, Sp>();
                    keys.join_core(data, |_k, &(), &()| Option::<()>::None).probe_with(&mut probe);
                }
                other => panic!("unrecognized mode: {other:?} (expected `chunk`, `col`, or `ord`)"),
            }

            (data_input, keys_input)
        });

        // Load `data`, advancing round by round.
        let mut counter = 0;
        let mut t: u64 = 1;
        while counter < 10 * keys {
            let mut i = worker.index();
            while i < size {
                data_input.update(((counter + i) % keys) as u64, 1i64);
                i += worker.peers();
            }
            counter += size;
            data_input.advance_to(t); data_input.flush();
            keys_input.advance_to(t); keys_input.flush();
            while probe.less_than(data_input.time()) { worker.step(); }
            t += 1;
        }
        println!("{:?}\tloading complete", timer.elapsed());

        // Issue `keys` queries against the arranged `data`.
        let mut queries = 0;
        while queries < 10 * keys {
            let mut i = worker.index();
            while i < size {
                keys_input.update(((queries + i) % keys) as u64, 1i64);
                i += worker.peers();
            }
            queries += size;
            data_input.advance_to(t); data_input.flush();
            keys_input.advance_to(t); keys_input.flush();
            while probe.less_than(keys_input.time()) { worker.step(); }
            t += 1;
        }
        println!("{:?}\tqueries complete", timer.elapsed());
    }).unwrap();

    println!("{:?}\tshut down", timer.elapsed());
}
