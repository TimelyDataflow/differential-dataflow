//! The `_with_trace` operator variants build their trace with the supplied factory.

use std::cell::Cell;
use std::rc::Rc;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::ToStream;

use differential_dataflow::AsCollection;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::operators::arrange::arrangement::arrange_core_with_trace;
use differential_dataflow::operators::cursor::reduce::CursorTactic;
use differential_dataflow::operators::reduce::reduce_with_tactic_and_trace;
use differential_dataflow::trace::Trace;
use differential_dataflow::trace::implementations::{ValBatcher, ValBuilder, ValSpine};

type Spine = ValSpine<u64, u64, u64, isize>;

#[test]
fn arrange_builds_its_trace_with_the_factory() {
    timely::example(|scope| {
        let calls = Rc::new(Cell::new(0));
        let built_for = Rc::new(Cell::new(None));
        let (calls_in, built_for_in) = (Rc::clone(&calls), Rc::clone(&built_for));
        let stream = vec![((1u64, 2u64), 0u64, 1isize)].to_stream(scope);
        let exchange = Exchange::new(|update: &((u64, u64), u64, isize)| (update.0).0.hashed().into());
        let arranged = arrange_core_with_trace::<_, _, _, Spine>(
            stream,
            exchange,
            "Arrange",
            ValBatcher::new,
            move |info, logger, activator| {
                calls_in.set(calls_in.get() + 1);
                built_for_in.set(Some(info.global_id));
                Spine::new(info, logger, activator)
            },
        );
        assert_eq!(calls.get(), 1);
        assert_eq!(built_for.get(), Some(arranged.trace.operator().global_id));
    });
}

#[test]
fn reduce_builds_its_output_trace_with_the_factory() {
    timely::example(|scope| {
        let calls = Rc::new(Cell::new(0));
        let built_for = Rc::new(Cell::new(None));
        let (calls_in, built_for_in) = (Rc::clone(&calls), Rc::clone(&built_for));
        let input = vec![((1u64, 2u64), 0u64, 1isize)]
            .to_stream(scope)
            .as_collection()
            .arrange_by_key();
        let tactic = CursorTactic::<_, _, ValBuilder<u64, u64, u64, isize>, _, _>::new(
            |_key: &u64, input: &[(&u64, isize)], _output: &mut Vec<(u64, isize)>, change: &mut Vec<(u64, isize)>| {
                change.extend(input.iter().map(|(v, r)| (**v, *r)));
            },
            |vec: &mut Vec<((u64, u64), u64, isize)>, key: &u64, upds: &mut Vec<(u64, u64, isize)>| {
                vec.clear();
                vec.extend(upds.drain(..).map(|(v, t, r)| ((*key, v), t, r)));
            },
        );
        let reduced = reduce_with_tactic_and_trace::<_, Spine, _>(
            input,
            "Reduce",
            tactic,
            move |info, logger, activator| {
                calls_in.set(calls_in.get() + 1);
                built_for_in.set(Some(info.global_id));
                Spine::new(info, logger, activator)
            },
        );
        assert_eq!(calls.get(), 1);
        assert_eq!(built_for.get(), Some(reduced.trace.operator().global_id));
    });
}
