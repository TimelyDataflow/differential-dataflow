//! Loggers and logging events for differential dataflow.

/// Logger for differential dataflow events.
pub type Logger = ::timely::logging::Logger<DifferentialEvent>;

/// Possible different differential events.
#[derive(Debug, Clone, Abomonation, Ord, PartialOrd, Eq, PartialEq)]
pub enum DifferentialEvent {
    /// Batch creation.
    Batch(BatchEvent),
    /// Merge start and stop events.
    Merge(MergeEvent),
    /// A merge failed to complete in time.
    MergeShortfall(MergeShortfall),
}

/// Either the start or end of a merge event.
#[derive(Debug, Clone, Abomonation, Ord, PartialOrd, Eq, PartialEq)]
pub struct BatchEvent {
    /// Operator identifier.
    pub operator: usize,
    /// Which order of magnitude.
    pub length: usize,
}

impl From<BatchEvent> for DifferentialEvent { fn from(e: BatchEvent) -> Self { DifferentialEvent::Batch(e) } }

/// Either the start or end of a merge event.
#[derive(Debug, Clone, Abomonation, Ord, PartialOrd, Eq, PartialEq)]
pub struct MergeEvent {
    /// Operator identifier.
    pub operator: usize,
    /// Which order of magnitude.
    pub scale: usize,
    /// Length of first trace.
    pub length1: usize,
    /// Length of second trace.
    pub length2: usize,
    /// None implies a start.
    pub complete: Option<usize>,
}

impl MergeEvent {
    /// Creates a new merge event from its members.
    pub fn new(operator: usize, scale: usize, length1: usize, length2: usize, complete: Option<usize>) -> Self {
        MergeEvent { operator, scale, length1, length2, complete }
    }
}

impl From<MergeEvent> for DifferentialEvent { fn from(e: MergeEvent) -> Self { DifferentialEvent::Merge(e) } }

/// A merge failed to complete in time.
#[derive(Debug, Clone, Abomonation, Ord, PartialOrd, Eq, PartialEq)]
pub struct MergeShortfall {
    /// Operator identifer.
    pub operator: usize,
    /// Which order of magnitude.
    pub scale: usize,
    /// By how much were we short.
    pub shortfall: usize,
}

impl MergeShortfall {
    /// Creates a new merge event from its members.
    pub fn new(operator: usize, scale: usize, shortfall: usize) -> Self {
        MergeShortfall { operator, scale, shortfall }
    }
}
impl From<MergeShortfall> for DifferentialEvent { fn from(e: MergeShortfall) -> Self { DifferentialEvent::MergeShortfall(e) } }
