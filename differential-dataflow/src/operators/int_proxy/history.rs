//! Time-ordered replay of proxy update histories, with meet-advancement.

/// A value history suitable for ordered proxy values.
pub(in crate::operators) type IdHistory<T, R, V = u64> = crate::operators::history::ValueHistory<V, T, R>;
