//! A caller-supplied `Agent` carries a handle the trace shares out of the arranging operators.

use std::cell::Cell;
use std::rc::Rc;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::OperatorInfo;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::{Probe, ToStream};
use timely::progress::frontier::AntichainRef;
use timely::scheduling::Activator;

use differential_dataflow::AsCollection;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::logging::Logger;
use differential_dataflow::operators::arrange::arrangement::arrange_core_with_agent;
use differential_dataflow::operators::arrange::{Agent, TraceAgent, TraceWriter};
use differential_dataflow::operators::cursor::reduce::CursorTactic;
use differential_dataflow::operators::reduce::reduce_with_tactic_and_agent;
use differential_dataflow::trace::implementations::{ValBatcher, ValBuilder, ValSpine};
use differential_dataflow::trace::{ExertionLogic, Span, Trace, TraceReader};

type Spine = ValSpine<u64, u64, u64, isize>;

/// A trace that counts the spans inserted into it, through a counter it shares.
struct Counted<Tr> {
    inner: Tr,
    inserts: Rc<Cell<usize>>,
}

impl<Tr> Counted<Tr> {
    fn handle(&self) -> Rc<Cell<usize>> {
        Rc::clone(&self.inserts)
    }
}

impl<Tr: TraceReader> TraceReader for Counted<Tr> {
    type Time = Tr::Time;
    type Batch = Tr::Batch;
    fn spans_through(&mut self, upper: AntichainRef<Self::Time>) -> Option<Vec<Span<Self::Time, Self::Batch>>> {
        self.inner.spans_through(upper)
    }
    fn set_logical_compaction(&mut self, frontier: AntichainRef<Self::Time>) {
        self.inner.set_logical_compaction(frontier)
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Self::Time> {
        self.inner.get_logical_compaction()
    }
    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Self::Time>) {
        self.inner.set_physical_compaction(frontier)
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Self::Time> {
        self.inner.get_physical_compaction()
    }
    fn map_spans<F: FnMut(&Span<Self::Time, Self::Batch>)>(&self, f: F) {
        self.inner.map_spans(f)
    }
}

impl<Tr: Trace> Trace for Counted<Tr> {
    fn new(info: OperatorInfo, logging: Option<Logger>, activator: Option<Activator>) -> Self {
        Counted { inner: Tr::new(info, logging, activator), inserts: Rc::new(Cell::new(0)) }
    }
    fn exert(&mut self) { self.inner.exert() }
    fn set_exert_logic(&mut self, logic: ExertionLogic) { self.inner.set_exert_logic(logic) }
    fn insert(&mut self, span: Span<Self::Time, Self::Batch>) {
        self.inserts.set(self.inserts.get() + 1);
        self.inner.insert(span)
    }
    fn close(&mut self) { self.inner.close() }
}

/// A `TraceAgent` plus the trace's handle, obtained before the agent takes ownership of the trace.
struct HandleAgent<Tr: Trace> {
    inner: TraceAgent<Counted<Tr>>,
    handle: Rc<Cell<usize>>,
}

impl<Tr: Trace> Clone for HandleAgent<Tr> {
    fn clone(&self) -> Self {
        HandleAgent { inner: self.inner.clone(), handle: Rc::clone(&self.handle) }
    }
}

impl<Tr: Trace> TraceReader for HandleAgent<Tr> {
    type Time = Tr::Time;
    type Batch = Tr::Batch;
    fn spans_through(&mut self, upper: AntichainRef<Self::Time>) -> Option<Vec<Span<Self::Time, Self::Batch>>> {
        self.inner.spans_through(upper)
    }
    fn set_logical_compaction(&mut self, frontier: AntichainRef<Self::Time>) {
        self.inner.set_logical_compaction(frontier)
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Self::Time> {
        self.inner.get_logical_compaction()
    }
    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Self::Time>) {
        self.inner.set_physical_compaction(frontier)
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Self::Time> {
        self.inner.get_physical_compaction()
    }
    fn map_spans<F: FnMut(&Span<Self::Time, Self::Batch>)>(&self, f: F) {
        self.inner.map_spans(f)
    }
}

impl<Tr: Trace> Agent for HandleAgent<Tr> {
    type Trace = Counted<Tr>;
    fn new(trace: Counted<Tr>, operator: OperatorInfo, logging: Option<Logger>) -> (Self, TraceWriter<Counted<Tr>>) {
        let handle = trace.handle();
        let (inner, writer) = TraceAgent::new(trace, operator, logging);
        (HandleAgent { inner, handle }, writer)
    }
}

#[test]
fn arrange_returns_the_supplied_agent() {
    timely::execute_directly(|worker| {
        let mut probe = ProbeHandle::new();
        let agent = worker.dataflow::<u64, _, _>(|scope| {
            let stream = vec![((1u64, 2u64), 0u64, 1isize)].to_stream(scope);
            let exchange = Exchange::new(|update: &((u64, u64), u64, isize)| (update.0).0.hashed().into());
            let arranged = arrange_core_with_agent::<_, _, _, HandleAgent<Spine>>(stream, exchange, "Arrange", ValBatcher::new);
            arranged.stream.probe_with(&mut probe);
            // The imported arrangement is a plain `TraceAgent`: the handle lives on the owning agent.
            let mut agent = arranged.trace.inner.clone();
            agent.import(scope).as_collection(|k, v| (*k, *v)).inner.probe_with(&mut probe);
            arranged.trace
        });
        while !probe.done() { worker.step(); }
        assert!(agent.handle.get() > 0, "the handle observes inserts into the trace");
    });
}

#[test]
fn reduce_returns_the_supplied_agent() {
    timely::execute_directly(|worker| {
        let mut probe = ProbeHandle::new();
        let agent = worker.dataflow::<u64, _, _>(|scope| {
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
            let reduced = reduce_with_tactic_and_agent::<_, HandleAgent<Spine>, _>(input, "Reduce", tactic);
            reduced.stream.probe_with(&mut probe);
            reduced.trace
        });
        while !probe.done() { worker.step(); }
        assert!(agent.handle.get() > 0, "the handle observes inserts into the trace");
    });
}
