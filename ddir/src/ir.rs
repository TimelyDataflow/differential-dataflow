//! IR types for DD IR programs.
//!
//! The IR is a flat list of nodes addressed by index, generic over
//! the row type `R` via the `RowLike` trait.

use std::sync::Arc;

pub type Diff = i64;
pub type Id = usize;
pub type Time = timely::order::Product<u64, differential_dataflow::dynamic::pointstamp::PointStamp<u64>>;

/// Minimal interface for a row type used in the IR.
pub trait RowLike: Clone + Ord + std::fmt::Debug + Send + Sync + 'static {
    fn new() -> Self;
    fn push(&mut self, v: i64);
    fn as_slice(&self) -> &[i64];
    fn extend_from_slice(&mut self, other: &[i64]);
}

impl RowLike for Vec<i64> {
    fn new() -> Self { Vec::new() }
    fn push(&mut self, v: i64) { Vec::push(self, v); }
    fn as_slice(&self) -> &[i64] { self }
    fn extend_from_slice(&mut self, other: &[i64]) { Vec::extend_from_slice(self, other); }
}

/// IR node, generic over the row type `R`.
pub enum Node<R: RowLike> {
    Input(usize),
    Map { input: Id, logic: Arc<dyn Fn((R, R)) -> smallvec::SmallVec<[((R, R), Time, Diff); 2]> + Send + Sync> },
    Concat(Vec<Id>),
    Arrange(Id),
    Join { left: Id, right: Id, logic: Arc<dyn Fn(&R, &R, &R) -> smallvec::SmallVec<[(R, R); 2]> + Send + Sync> },
    Reduce { input: Id, logic: Arc<dyn Fn(&R, &[(&R, Diff)], &mut Vec<(R, Diff)>) + Send + Sync> },
    Variable,
    Inspect { input: Id, label: String },
    Leave(Id, usize),
    Scope,
    EndScope,
    Bind { variable: Id, value: Id },
}

pub struct Program<R: RowLike> {
    pub nodes: Vec<Node<R>>,
    pub result: Id,
}
