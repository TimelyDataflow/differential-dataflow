use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::Hashable;

fn main() {

    // The number of distinct values to associate with each key.
    let values: u64 = std::env::args().nth(1).unwrap().parse().unwrap();
    // The base to use in the hierarchical reduction. Defaults to 16.
    let base: u64 = std::env::args().nth(2).unwrap_or("16".to_string()).parse().unwrap();

    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        // define BFS dataflow; return handles to roots and edges inputs
        let mut probe = Handle::new();
        let mut input = worker.dataflow(|scope| {

            let (data_input, data) = scope.new_collection::<((), u64), isize>();

            // Here `data` is a collection of records `(key, val)`.
            // We'll start by counting the records for each key.
            // We'll do this without distinctness, for efficiency, but we'd prefer to know the distinct count.
            let counts: Collection<_, ((), isize), _, _> = data.map(|(key, _)| key).count();

            // Given a `base`, for a level `k` and `count`, the interval `[0, (count - base^k) / (base - 1))` satisfies
            //   1. at `count = base^k` the interval is empty, and 
            //   2. at `count = base^(k+1)` the interval is `[0, base^k)`.
            //  
            // If we clamp `count` to at most `base^(k+1)`, this should work out.

            // First, plan out which levels may be full, in the form of intervals in each level.
            // At most one level is partially full; higher levels are empty and lower levels are full.
            let full: Collection<_, (((), u32, u64), ())> = 
            counts
                .map(|(key, count)| (key, count as u64))
                .flat_map(move |(key, count)| {
                    // println!("count: {:?}; count.ilog(base): {:?}", count, count.ilog(base));
                    (0 ..= count.ilog(base)).map(move |l| {
                        let clamp_upper = std::cmp::min(count, base.pow(l+1));
                        (key, (l, l, (0, (clamp_upper - base.pow(l)) / (base - 1))))
                    })
                })
                .consolidate()
                .inspect(|x| println!("\tpre-full: {:?}", x))
                .iterate(|inner| {
                    let active = inner.filter(|(_, (_, ttl, _))| ttl > &0);
                    inner.concat(&active.negate())
                         .concat(&active.flat_map(move |(key, (k, t, (l, u)))| (0 .. base).filter(move |i| l + base.pow(t-1) * i < u).map(move |i| (key, (k, t-1, (l + base.pow(t-1) * i, std::cmp::min(u, l + base.pow(t-1) * (i + 1))))))))
                         .consolidate()
                })
                .map(|(key, (k, _, (l, _)))| ((key, k, l), ()));

            // Our next goal is to determine at which "level" we should introduce each `(key, val)` pair.
            // We'll do this by repeatedly semijoining `(key, hash(val) / base^k)` with the `full` collection, incrementing `k` if full.
            let insertion = 
            data.map(|(key, val)| (key, val, 0u32))
                .iterate(|inner| {
                    let full = full.enter(&inner.scope());
                    let hits = inner.map(move |(key, val, lvl)| ((key, lvl, val.hashed() % base.pow(lvl as u32)), val))
                                    .join_map(&full, |&(key, lvl, _), &val, &()| (key, val, lvl));

                    // retract any hits at this level, and re-introduce at the next level.
                    inner.concat(&hits.negate())
                         .concat(&hits.map(|(key, val, lvl)| (key, val, lvl + 1)))
                         .consolidate()
                });
            
            // full.inspect(|x| println!("\tfull: {:?}", x));

            insertion
                .consolidate()
                .map(|(key, _, lvl)| (key, lvl))
                .consolidate()
                .inspect(|x| println!("\t(key, lvl): {:?}", x))
                .probe_with(&mut probe);

            data_input
        });

        for value in 0 .. values {
            if (value as usize) % worker.peers() == worker.index() {
                input.insert(((), value));
            }
        }

        println!("{:?}\tloaded", timer.elapsed());

        input.advance_to(1);
        input.flush();
        worker.step_while(|| probe.less_than(input.time()));

        println!("{:?}\tstable", timer.elapsed());

        for round in 0 .. 50 {
            let timer = ::std::time::Instant::now();
            let value = round + values;
            if (value as usize) % worker.peers() == worker.index() {
                input.insert(((), value));
            }
            input.advance_to(round + 2);
            input.flush();

            worker.step_while(|| probe.less_than(&input.time()));
            if worker.index() == 0 {
                println!("{:?}\t{:?}", timer.elapsed(), round);
            }
        }

    }).unwrap();
}
