//! Loggers and logging events for differential dataflow.

/// Logger for differential dataflow events.
pub type Logger = ::timely::logging::Logger<DifferentialEvent>;

/// Possible different differential events.
#[derive(Debug, Clone, Abomonation, Ord, PartialOrd, Eq, PartialEq)]
pub enum DifferentialEvent {
    /// Arrangement related event.
    Merge(MergeEvent),
    /// A merge failed to complete in time.
    MergeShortfall(MergeShortfall),
}

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

impl From<MergeShortfall> for DifferentialEvent { fn from(e: MergeShortfall) -> Self { DifferentialEvent::MergeShortfall(e) } }
