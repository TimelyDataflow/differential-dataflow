//! Quick spines benchmark over `val`, `key`, and `col` arrangements.
//!
//! `col` mode feeds the columnar batcher directly via `InputHandle` +
//! `ValColBuilder`, formatting integers into a reusable `String` buffer rather
//! than allocating per record. This way `col`'s measurements aren't polluted
//! by `String` allocation overhead, since the row-based `val`/`key` paths
//! genuinely need owned strings.

use std::fmt::Write as _;

use timely::dataflow::operators::probe::Handle;
use timely::dataflow::InputHandle;

use differential_dataflow::input::{Input, InputSession};

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// What each arm needs to feed the dataflow round-by-round.
trait Workload {
    fn insert_data(&mut self, val: usize);
    fn insert_keys(&mut self, val: usize);
    fn advance_to(&mut self, t: u64);
}

/// Row-based mode (`val`, `key`): each insert allocates a `String`.
struct RowWorkload {
    data_input: InputSession<u64, String, isize>,
    keys_input: InputSession<u64, String, isize>,
}
impl Workload for RowWorkload {
    fn insert_data(&mut self, val: usize) { self.data_input.insert(format!("{:?}", val)); }
    fn insert_keys(&mut self, val: usize) { self.keys_input.insert(format!("{:?}", val)); }
    fn advance_to(&mut self, t: u64) {
        self.data_input.advance_to(t); self.data_input.flush();
        self.keys_input.advance_to(t); self.keys_input.flush();
    }
}

/// Columnar mode: format into a reusable buffer, push refs into the col builder.
type DataU = (String, (), u64, i64);
type ColBuilder = differential_dataflow::columnar::ValColBuilder<DataU>;
struct ColWorkload {
    data_input: InputHandle<u64, ColBuilder>,
    keys_input: InputHandle<u64, ColBuilder>,
    buf: String,
}
impl Workload for ColWorkload {
    fn insert_data(&mut self, val: usize) {
        self.buf.clear();
        write!(&mut self.buf, "{:?}", val).unwrap();
        let t = *self.data_input.time();
        self.data_input.send((self.buf.as_str(), (), t, 1i64));
    }
    fn insert_keys(&mut self, val: usize) {
        self.buf.clear();
        write!(&mut self.buf, "{:?}", val).unwrap();
        let t = *self.keys_input.time();
        self.keys_input.send((self.buf.as_str(), (), t, 1i64));
    }
    fn advance_to(&mut self, t: u64) {
        self.data_input.advance_to(t); self.data_input.flush();
        self.keys_input.advance_to(t); self.keys_input.flush();
    }
}

fn main() {

    let keys: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let size: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    let mode: String = std::env::args().nth(3).unwrap();

    println!("Running [{:?}] arrangement", mode);

    let timer1 = ::std::time::Instant::now();
    let timer2 = timer1.clone();

    timely::execute_from_args(std::env::args(), move |worker| {

        let mut probe = Handle::new();
        let mut workload: Box<dyn Workload> = worker.dataflow(|scope| {

            use differential_dataflow::operators::arrange::Arrange;

            match mode.as_str() {
                "key" => {
                    use differential_dataflow::trace::implementations::ord_neu::{OrdKeyBatcher, RcOrdKeyBuilder, OrdKeySpine};
                    let (data_input, data) = scope.new_collection::<String, isize>();
                    let (keys_input, keys) = scope.new_collection::<String, isize>();
                    let data = data.arrange::<OrdKeyBatcher<String,_,isize>, RcOrdKeyBuilder<String,_,isize>, OrdKeySpine<String,_,isize>>();
                    let keys = keys.arrange::<OrdKeyBatcher<String,_,isize>, RcOrdKeyBuilder<String,_,isize>, OrdKeySpine<String,_,isize>>();
                    keys.join_core(data, |_k, &(), &()| Option::<()>::None)
                        .probe_with(&mut probe);
                    Box::new(RowWorkload { data_input, keys_input }) as Box<dyn Workload>
                },
                "val" => {
                    use differential_dataflow::trace::implementations::ord_neu::{OrdValBatcher, RcOrdValBuilder, OrdValSpine};
                    let (data_input, data) = scope.new_collection::<String, isize>();
                    let (keys_input, keys) = scope.new_collection::<String, isize>();
                    let data = data.map(|x| (x, ())).arrange::<OrdValBatcher<String,(),_,isize>, RcOrdValBuilder<String,(),_,isize>, OrdValSpine<String,(),_,isize>>();
                    let keys = keys.map(|x| (x, ())).arrange::<OrdValBatcher<String,(),_,isize>, RcOrdValBuilder<String,(),_,isize>, OrdValSpine<String,(),_,isize>>();
                    keys.join_core(data, |_k, &(), &()| Option::<()>::None)
                        .probe_with(&mut probe);
                    Box::new(RowWorkload { data_input, keys_input })
                },
                "col" => {
                    use timely::dataflow::operators::Input as _;
                    use differential_dataflow::columnar::{ValBatcher, ValBuilder, ValPact, ValSpine};
                    use differential_dataflow::operators::arrange::arrangement::arrange_core;

                    fn string_hash(s: columnar::Ref<'_, String>) -> u64 {
                        use std::hash::{Hash, Hasher};
                        let mut h = std::collections::hash_map::DefaultHasher::new();
                        s.hash(&mut h);
                        h.finish()
                    }

                    let mut data_input = <InputHandle<u64, ColBuilder>>::new_with_builder();
                    let mut keys_input = <InputHandle<u64, ColBuilder>>::new_with_builder();

                    let data_stream = scope.input_from(&mut data_input);
                    let keys_stream = scope.input_from(&mut keys_input);

                    let data = arrange_core::<_, ValBatcher<String,(),u64,i64>, ValBuilder<String,(),u64,i64>, ValSpine<String,(),u64,i64>>(
                        data_stream, ValPact { hashfunc: |k: columnar::Ref<'_, String>| string_hash(k) }, "DataArrange",
                    );
                    let keys = arrange_core::<_, ValBatcher<String,(),u64,i64>, ValBuilder<String,(),u64,i64>, ValSpine<String,(),u64,i64>>(
                        keys_stream, ValPact { hashfunc: |k: columnar::Ref<'_, String>| string_hash(k) }, "KeysArrange",
                    );
                    keys.join_core(data, |_k, (), ()| Option::<()>::None)
                        .probe_with(&mut probe);

                    Box::new(ColWorkload { data_input, keys_input, buf: String::new() })
                },
                _ => {
                    panic!("unrecognized mode: {:?}", mode);
                }
            }
        });

        // Load up data in batches.
        let mut counter = 0;
        let mut t: u64 = 1;
        while counter < 10 * keys {
            let mut i = worker.index();
            while i < size {
                let val = (counter + i) % keys;
                workload.insert_data(val);
                i += worker.peers();
            }
            counter += size;
            workload.advance_to(t);
            while probe.less_than(&t) {
                worker.step();
            }
            t += 1;
        }
        println!("{:?}\tloading complete", timer1.elapsed());

        let mut queries = 0;

        while queries < 10 * keys {
            let mut i = worker.index();
            while i < size {
                let val = (queries + i) % keys;
                workload.insert_keys(val);
                i += worker.peers();
            }
            queries += size;
            workload.advance_to(t);
            while probe.less_than(&t) {
                worker.step();
            }
            t += 1;
        }

        println!("{:?}\tqueries complete", timer1.elapsed());

    }).unwrap();

    println!("{:?}\tshut down", timer2.elapsed());

}
