//! Frontier-observing operator builders that honor `Config::skip_idle_frontiers`.
//!
//! These match timely's `unary_frontier` and `binary_frontier`, except that when the worker is
//! configured to skip idle frontiers, each input declares `FrontierInterest::IfCapability`.

use timely::Container;
use timely::ContainerBuilder;
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::{InputHandleCore, OperatorInfo, OutputBuilder, OutputBuilderSession};
use timely::progress::Timestamp;
use timely::progress::frontier::MutableAntichain;
use timely::progress::operate::FrontierInterest;

/// The frontier interest of an operator's inputs, per the worker configuration.
fn interest<T: Timestamp, C>(stream: &Stream<'_, T, C>) -> FrontierInterest {
    let skip = stream.scope().worker().config().get::<bool>("differential/skip_idle_frontiers").copied().unwrap_or(false);
    if skip { FrontierInterest::IfCapability } else { FrontierInterest::Always }
}

/// As timely's `unary_frontier`, with the configured frontier interest.
pub(crate) fn unary_frontier<'scope, T, C1, CB, B, L, P>(
    stream: Stream<'scope, T, C1>,
    pact: P,
    name: &str,
    constructor: B,
) -> Stream<'scope, T, CB::Container>
where
    T: Timestamp,
    C1: Container,
    CB: ContainerBuilder,
    B: FnOnce(Capability<T>, OperatorInfo) -> L,
    L: FnMut((&mut InputHandleCore<T, C1, P::Puller>, &MutableAntichain<T>), &mut OutputBuilderSession<'_, T, CB>) + 'static,
    P: ParallelizationContract<T, C1>,
{
    let interest = interest(&stream);
    let mut builder = OperatorBuilder::new(name.to_owned(), stream.scope());
    let operator_info = builder.operator_info();
    let mut input = builder.new_input(stream, pact);
    builder.set_notify_for(0, interest);
    let (output, output_stream) = builder.new_output();
    let mut output = OutputBuilder::from(output);
    builder.build(move |mut capabilities| {
        // `capabilities` should be a single-element vector.
        let capability = capabilities.pop().unwrap();
        let mut logic = constructor(capability, operator_info);
        move |frontiers| {
            let mut output_handle = output.activate();
            logic((&mut input, &frontiers[0]), &mut output_handle);
        }
    });
    output_stream
}

/// As timely's `binary_frontier`, with the configured frontier interest.
pub(crate) fn binary_frontier<'scope, T, C1, C2, CB, B, L, P1, P2>(
    stream1: Stream<'scope, T, C1>,
    stream2: Stream<'scope, T, C2>,
    pact1: P1,
    pact2: P2,
    name: &str,
    constructor: B,
) -> Stream<'scope, T, CB::Container>
where
    T: Timestamp,
    C1: Container,
    C2: Container,
    CB: ContainerBuilder,
    B: FnOnce(Capability<T>, OperatorInfo) -> L,
    L: FnMut(
            (&mut InputHandleCore<T, C1, P1::Puller>, &MutableAntichain<T>),
            (&mut InputHandleCore<T, C2, P2::Puller>, &MutableAntichain<T>),
            &mut OutputBuilderSession<'_, T, CB>,
        ) + 'static,
    P1: ParallelizationContract<T, C1>,
    P2: ParallelizationContract<T, C2>,
{
    let interest = interest(&stream1);
    let mut builder = OperatorBuilder::new(name.to_owned(), stream1.scope());
    let operator_info = builder.operator_info();
    let mut input1 = builder.new_input(stream1, pact1);
    let mut input2 = builder.new_input(stream2, pact2);
    builder.set_notify_for(0, interest);
    builder.set_notify_for(1, interest);
    let (output, output_stream) = builder.new_output();
    let mut output = OutputBuilder::from(output);
    builder.build(move |mut capabilities| {
        // `capabilities` should be a single-element vector.
        let capability = capabilities.pop().unwrap();
        let mut logic = constructor(capability, operator_info);
        move |frontiers| {
            let mut output_handle = output.activate();
            logic((&mut input1, &frontiers[0]), (&mut input2, &frontiers[1]), &mut output_handle);
        }
    });
    output_stream
}
