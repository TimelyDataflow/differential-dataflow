//! Types and traits for arranging collections.

use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::collections::VecDeque;

use timely::scheduling::Activator;
use trace::TraceReader;

/// Operating instructions on how to replay a trace.
pub enum TraceReplayInstruction<Tr>
where
    Tr: TraceReader,
{
    /// Describes a frontier advance.
    Frontier(Vec<Tr::Time>),
    /// Describes a batch of data and a capability hint.
    Batch(Tr::Batch, Option<Tr::Time>),
}

// Short names for strongly and weakly owned activators and shared queues.
type BatchQueue<Tr> = VecDeque<TraceReplayInstruction<Tr>>;
type TraceAgentQueueReader<Tr> = Rc<(Activator, RefCell<BatchQueue<Tr>>)>;
type TraceAgentQueueWriter<Tr> = Weak<(Activator, RefCell<BatchQueue<Tr>>)>;

pub mod writer;
pub mod agent;
pub mod arrangement;

pub use self::writer::TraceWriter;
pub use self::agent::TraceAgent;

pub use self::arrangement::{Arranged, ArrangeByKey, ArrangeBySelf};