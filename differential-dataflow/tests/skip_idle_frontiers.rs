//! Idle operators skipping frontier advances, per `Config::skip_idle_frontiers`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{CountTotal, ThresholdTotal};
use timely::logging::{StartStop, TimelyEvent, TimelyEventBuilder};

type Updates = Vec<((u8, u64, u64), u64, isize)>;

const EPOCHS: u64 = 200;

/// Runs join, reduce, `count_total`, and `threshold_total`, with data in the first few of
/// `EPOCHS` epochs. Returns their consolidated outputs and how often each operator ran, by name.
fn run(skip: bool) -> (Updates, HashMap<String, usize>) {
    let mut config = timely::Config::thread();
    let options = differential_dataflow::Config::default().skip_idle_frontiers(skip);
    differential_dataflow::configure(&mut config.worker, &options);

    let guards = timely::execute(config, |worker| {
        let names = Rc::new(RefCell::new(HashMap::new()));
        let runs = Rc::new(RefCell::new(HashMap::new()));
        let runs_log = Rc::clone(&runs);
        worker.log_register().unwrap().insert::<TimelyEventBuilder, _>("timely", move |_time, data| {
            for (_, event) in data.iter().flatten() {
                match event {
                    TimelyEvent::Operates(op) => { names.borrow_mut().insert(op.id, op.name.clone()); }
                    TimelyEvent::Schedule(event) if event.start_stop == StartStop::Start => {
                        if let Some(name) = names.borrow().get(&event.id) {
                            *runs_log.borrow_mut().entry(name.clone()).or_insert(0) += 1;
                        }
                    }
                    _ => { }
                }
            }
        });

        let updates = Rc::new(RefCell::new(Vec::new()));
        let sink = Rc::clone(&updates);
        let mut input = InputSession::<u64, (u64, u64), isize>::new();
        let probe = worker.dataflow(|scope| {
            let edges = input.to_collection(scope);
            let keys = edges.clone().map(|(k, _)| k);
            let joined = edges.clone().join(edges).map(|(k, (a, b))| (0, k, 10 * a + b));
            let counts = keys.clone().count().map(|(k, c)| (1, k, c as u64));
            let totals = keys.clone().count_total().map(|(k, c)| (2, k, c as u64));
            let distinct = keys.distinct_total().map(|k| (3, k, 0));
            joined
                .concat(counts)
                .concat(totals)
                .concat(distinct)
                .inspect(move |update| sink.borrow_mut().push(*update))
                .probe()
                .0
        });

        for epoch in 0 .. EPOCHS {
            if epoch < 3 {
                input.insert((epoch % 2, epoch));
            }
            input.advance_to(epoch + 1);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));
        }
        drop(input);
        worker.step_while(|| !probe.done());
        worker.log_register().unwrap().remove("timely");

        let mut updates = updates.take();
        differential_dataflow::consolidation::consolidate_updates(&mut updates);
        (updates, runs.take())
    })
    .unwrap();
    guards.join().pop().unwrap().unwrap()
}

/// Operators that skip idle frontier advances, named as in `run`.
const SKIPPING: &[&str] = &[
    "ArrangeByKey", "Join",
    "Arrange: Count", "Count",
    "Arrange: CountTotal", "CountTotal",
    "Arrange: ThresholdTotal", "ThresholdTotal",
];

#[test]
fn skip_idle_frontiers() {
    let (updates_always, runs_always) = run(false);
    let (updates_skip, runs_skip) = run(true);
    assert!(!updates_skip.is_empty());
    assert_eq!(updates_always, updates_skip);
    for name in SKIPPING {
        // Each operator runs on every epoch, or only around the epochs with data.
        assert!(runs_always[*name] >= EPOCHS as usize, "{name} ran {} times", runs_always[*name]);
        assert!(runs_skip[*name] < 25, "{name} ran {} times", runs_skip[*name]);
    }
}
