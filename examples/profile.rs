extern crate timely;
extern crate differential_dataflow;

use timely::progress::timestamp::RootTimestamp;
use timely::dataflow::operators::{ToStream, Capture, Map};
use timely::dataflow::operators::capture::Extract;
use differential_dataflow::AsCollection;
use differential_dataflow::operators::{Join, Count};


fn main() {

    let data = timely::example(|scope| {

        let scale = std::env::args().nth(1).unwrap().parse().unwrap();

        let counts = (0 .. 1).to_stream(scope)
                             .flat_map(move |_| (0..scale).map(|i| ((), RootTimestamp::new(i), 1)))
                             .as_collection()
                             .count();

        let odds = counts.filter(|x| x.1 % 2 == 1);
        let evens = counts.filter(|x| x.1 % 2 == 0);

        odds.join(&evens).inner.capture()
    });

    let extracted = data.extract();
    assert_eq!(extracted.len(), 0);
}