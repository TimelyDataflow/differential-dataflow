use std::{cell::RefCell, collections::HashMap, rc::Rc};
use differential_dataflow::{input::Input, trace::implementations::{ValBatcher, ValSpine}};
use timely::{dataflow::{operators::{Capture, capture::Extract}, ProbeHandle}, logging::{StartStop, TimelyEvent, TimelyEventBuilder}};

#[test]
fn intra_region_sleeps_and_resumes() {
    let mut config = timely::Config::thread();
    config.worker.set(differential_dataflow::idle::IDLE_READERS.to_owned(), true);
    let (captured, schedules) = timely::execute(config, |worker| {
        let names = Rc::new(RefCell::new(HashMap::new()));
        let schedules = Rc::new(RefCell::new(HashMap::<usize, usize>::new()));
        let (names_log, schedules_log) = (names.clone(), schedules.clone());
        worker.log_register().unwrap().insert::<TimelyEventBuilder, _>("timely", move |_, data| {
            if let Some(events) = data {
                for (_, event) in events.drain(..) {
                    match event {
                        TimelyEvent::Operates(op) => { names_log.borrow_mut().insert(op.id, (op.name, op.addr)); }
                        TimelyEvent::Schedule(s) if s.start_stop == StartStop::Start => {
                            *schedules_log.borrow_mut().entry(s.id).or_default() += 1;
                        }
                        _ => {}
                    }
                }
            }
        });
        let mut probe = ProbeHandle::new();
        let (mut input, captured) = worker.dataflow::<usize, _, _>(|scope| {
            let (input, data) = scope.new_collection::<(usize, usize), isize>();
            // An Always writer outside the region exercises empty-span fusion while its reader sleeps.
            let shared = data.clone().arrange_by_key();
            let result = scope.region_named("Intra region", |region| {
                data.enter_region(region)
                    .arrange_intra::<_, ValSpine<_, _, _, _>>(ValBatcher::new)
                    .join_core(shared.enter_region(region), |k, a, b| [(*k, *a, *b)])
                    .leave_region(scope)
            });
            (input, result.probe_with(&mut probe).inner.capture())
        });
        input.insert((0, 1));
        for epoch in 1..=10 {
            input.advance_to(epoch); input.flush();
            worker.step_while(|| probe.less_than(input.time()));
        }
        worker.log_register().unwrap().flush();
        schedules.borrow_mut().clear();
        for epoch in 11..=60 {
            input.advance_to(epoch); input.flush();
            worker.step_while(|| probe.less_than(input.time()));
        }
        worker.log_register().unwrap().flush();
        let names = names.borrow();
        let region = &names.values().find(|(name, _)| name == "Intra region").unwrap().1;
        let measured: usize = schedules.borrow().iter()
            .filter(|(id, _)| names[id].1.starts_with(region)).map(|(_, n)| n).sum();
        input.insert((0, 2));
        input.advance_to(61); input.flush();
        worker.step_while(|| probe.less_than(input.time()));
        input.remove((0, 1));
        input.advance_to(62); input.flush();
        worker.step_while(|| probe.less_than(input.time()));
        (captured, measured)
    }).unwrap().join().pop().unwrap().unwrap();
    let mut updates: Vec<_> = captured.extract().into_iter().flat_map(|(_, data)| data).collect();
    differential_dataflow::consolidation::consolidate_updates(&mut updates);
    let mut expected = vec![
        ((0, 1, 1), 0, 1), ((0, 1, 2), 60, 1), ((0, 2, 1), 60, 1), ((0, 2, 2), 60, 1),
        ((0, 1, 1), 61, -1), ((0, 1, 2), 61, -1), ((0, 2, 1), 61, -1),
    ];
    differential_dataflow::consolidation::consolidate_updates(&mut expected);
    assert_eq!(updates, expected);
    assert_eq!(schedules, 0);
}
