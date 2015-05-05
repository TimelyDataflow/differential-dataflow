// NOTE : Literally just use example_static::concat::*;

// use timely::example_static::*;
// use timely::communication::*;
//
// pub trait ConcatDPExt<G, D> {
//     fn concat(&mut self, &Stream<G::Timestamp, D>) -> ActiveStream<G, (D, i64)>;
// }
//
// impl<G: GraphBuilder, D: Data> ConcatDPExt for ActiveStream<G, (D, i64)> {
//     fn concat(&mut self, other: &mut Stream<G, (D, i64)>) -> ActiveStream<G, (D, i64)> {
//         self.concat(other)
//     }
// }
