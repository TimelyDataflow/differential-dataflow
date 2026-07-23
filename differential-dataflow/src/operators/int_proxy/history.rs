//! Time-ordered replay of proxy update histories, with meet-advancement.

/// A value history over proxy value tokens.
pub(in crate::operators) type IdHistory<I, T, R> = crate::operators::ValueHistory<I, T, R>;

