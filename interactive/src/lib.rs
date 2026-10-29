//! DDIR: an interpreted intermediate representation for differential dataflow.
//!
//! The modules below are this crate's whole public surface, grouped by layer. Outside tests, a
//! module uses only its own layer and the ones above it. The comment on each module says what
//! escapes it, meaning what the server, the tests and the examples use. Everything else is
//! `pub(crate)` or private. `unreachable_pub` flags a `pub` item that cannot escape; it does not
//! flag one that escapes but that nothing outside uses, so check for those by hand.

#![warn(unreachable_pub)]

// The IR's vocabulary, and the expression language its slots are written in.
// `Lambda`, `LinearOp`, `Reducer`, `Diff`.
pub mod ir;
// The timestamp: `Time`, a list of integer coordinates, and its path summary.
pub mod time;
// `Lambda`, and the registry of host functions: `Function`, `register`, `lookup`.
pub mod lambda;
// Crate-only: canonical reducer definitions and value projection composition.
mod reducers;
// The scope tree: `Program`, `Scope`, `Item`, `Node`, `Ref`, `Source` and their parts. Plain data,
// so every field is public.
pub mod scope_ir;

// The language: text to a typed scope tree.
// `pipe::parse`, the `.ddp` parser, and the statements it returns (`Stmt`, `Expr`).
pub mod parse;
// `lower_tree`: parsed statements to a scope tree.
pub mod lower;
// `infer`, every collection's shape (`CollShape`, `ScopeShapes`, `Problems`), and `linear_shapes`.
pub mod shapes;

// Scope-tree rewriting: one module, with `passes::run` as the preparation entry point.
// `optimize` runs just the optimizer; `projection` also exposes the demand analysis.
pub mod passes;
// Explanation construction: `explain`, `explain_with`, `Options`, `clone_identity`.
pub mod explain;

// `report`: read-only planning stages, and their diagnostic printer.
pub mod plan;

// The runtime: rendering a scope tree onto differential dataflow.
// Only the statement labels escape (`LabelEvent`, `LabelEventBuilder`), for a logger to read;
// the `Backend` trait and `render_tree` are the crate's own.
pub mod backend;
// The columnar runtime. Only its data formats escape: `container`'s `Updates` and
// `CorgiContainer`, `chunk::CorgiChunk`, their time columns (`col_times::ColTimes`),
// `feed::FeedDecoder`, `frame`'s encoders and decoders, and `exchange::GatherPact`.
pub mod runtime;

// The host: installing, feeding and reading programs.
// `register`: the server's own diagnostics as `diag.*` collections, kept in a `Diagnostics`.
pub mod diag;
// `Server`, `evaluate`, `generated_shape`, and the host's time and container types.
pub mod server;
