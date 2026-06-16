//! Scope-tree rendering, generic over the substrate.
//!
//! The tree walk — scopes as timely regions, feedback variables, item-order
//! rendering — lives once in [`render_tree`], generic over a [`Backend`]. A
//! backend fixes the differential container and supplies the substrate leaf
//! operators (`linear`/`join`/`reduce`/`arrange`/`inspect`/`leave_dynamic`);
//! everything else (enter/leave/concat/feedback) is DD's own container-generic
//! machinery. Time is always `ir::Time`; only the container varies.
//!
//! The example binaries, the server, and a wasm front-end are thin drivers
//! that pick a backend and call [`render_tree`].

pub mod vec;
// The columnar substrate is deferred on the Value-model port (it needs a
// `Columnar`/columnar-storage story for `Value`); see `backend::vec`.
// pub mod col;

use timely::Container;
use timely::order::Product;
use timely::dataflow::Scope;
use timely::progress::Timestamp;
use differential_dataflow::Collection;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::dynamic::pointstamp::PointStampSummary;
use differential_dataflow::dynamic::feedback_summary;
use differential_dataflow::collection::containers::{Enter, Leave, ResultsIn};

use crate::ir::{Time, LinearOp};
use crate::parse::{Projection, Reducer};
use crate::scope_ir as st;

/// A rendering substrate: a differential container plus the leaf operators over
/// it. Collections are the plain container-generic `Collection<'s, Time, C>`;
/// only the arrangement type (`Arr`) is substrate-specific, and the walk only
/// ever clones it, so the bound is just `Clone`.
pub trait Backend {
    /// The differential update container. Region enter/leave (same timestamp)
    /// must be the identity on it, so the walk can recurse into child scopes.
    type Container: Container + Clone
        + ResultsIn<<Time as Timestamp>::Summary>
        + Enter<Time, Time, InnerContainer = Self::Container>
        + Leave<Time, Time, OuterContainer = Self::Container>;
    /// The arrangement produced by `arrange`/`reduce` and consumed by `join`.
    type Arr<'scope>: Clone;

    fn linear<'s>(c: Collection<'s, Time, Self::Container>, ops: Vec<LinearOp>, level: usize) -> Collection<'s, Time, Self::Container>;
    fn arrange<'s>(c: Collection<'s, Time, Self::Container>) -> Self::Arr<'s>;
    fn as_collection<'s>(a: Self::Arr<'s>) -> Collection<'s, Time, Self::Container>;
    fn join<'s>(l: Self::Arr<'s>, r: Self::Arr<'s>, projection: &Projection) -> Collection<'s, Time, Self::Container>;
    fn reduce<'s>(a: Self::Arr<'s>, reducer: &Reducer) -> Self::Arr<'s>;
    fn inspect<'s>(c: Collection<'s, Time, Self::Container>, label: String) -> Collection<'s, Time, Self::Container>;
    fn leave_dynamic<'s>(c: Collection<'s, Time, Self::Container>, depth: usize) -> Collection<'s, Time, Self::Container>;
}

/// A rendered item's value: a collection, or an arrangement.
enum Rendered<'s, B: Backend> {
    Collection(Collection<'s, Time, B::Container>),
    Arrangement(B::Arr<'s>),
}

impl<'s, B: Backend> Rendered<'s, B> {
    fn collection(&self) -> Collection<'s, Time, B::Container> {
        match self {
            Rendered::Collection(c) => c.clone(),
            Rendered::Arrangement(a) => B::as_collection(a.clone()),
        }
    }
    fn arrange(&self) -> B::Arr<'s> {
        match self {
            Rendered::Arrangement(a) => a.clone(),
            Rendered::Collection(c) => B::arrange(c.clone()),
        }
    }
}

/// A rendered scope item: an operator's value, or a child scope's surrendered
/// exports (already returned to this scope's depth via `leave_dynamic`).
enum RItem<'s, B: Backend> {
    Op(Rendered<'s, B>),
    Sub(Vec<Collection<'s, Time, B::Container>>),
}

fn resolve<'s, B: Backend>(
    items: &[RItem<'s, B>],
    imports: &[Collection<'s, Time, B::Container>],
    var_cols: &[Collection<'s, Time, B::Container>],
    r: &st::Ref,
) -> Rendered<'s, B> {
    match r {
        st::Ref::Local(i) => match &items[*i] {
            RItem::Op(Rendered::Collection(c)) => Rendered::Collection(c.clone()),
            RItem::Op(Rendered::Arrangement(a)) => Rendered::Arrangement(a.clone()),
            RItem::Sub(_) => panic!("Ref::Local points at a child scope"),
        },
        st::Ref::Import(i) => Rendered::Collection(imports[*i].clone()),
        st::Ref::Var(i) => Rendered::Collection(var_cols[*i].clone()),
        st::Ref::ChildExport(i, j) => match &items[*i] {
            RItem::Sub(exports) => Rendered::Collection(exports[*j].clone()),
            RItem::Op(_) => panic!("Ref::ChildExport points at an operator"),
        },
    }
}

/// Render one scope at `depth` (root = 0): feedback vars first (they're listed,
/// not scanned for), then items in order, then binds close the loops, then the
/// exports are surrendered. The returned collections are at this scope's depth;
/// popping the coordinate (`leave_dynamic`) is the caller's job.
pub fn render_tree<'s, B: Backend>(
    s: &st::Scope,
    scope: Scope<'s, Time>,
    depth: usize,
    imports: Vec<Collection<'s, Time, B::Container>>,
) -> Vec<Collection<'s, Time, B::Container>> {
    let mut var_handles: Vec<Option<Variable<'s, Time, B::Container>>> = Vec::new();
    let mut var_cols: Vec<Collection<'s, Time, B::Container>> = Vec::new();
    for _ in &s.vars {
        let step: Product<u64, PointStampSummary<u64>> = Product::new(0, feedback_summary::<u64>(depth, 1));
        let (var, col) = Variable::new(scope, step);
        var_handles.push(Some(var));
        var_cols.push(col);
    }

    let mut items: Vec<RItem<'s, B>> = Vec::new();
    for item in &s.items {
        match item {
            st::Item::Op(node) => {
                let rendered = match node {
                    st::Node::Linear { input, ops } => {
                        let c = resolve(&items, &imports, &var_cols, input).collection();
                        Rendered::Collection(B::linear(c, ops.clone(), depth))
                    },
                    st::Node::Concat(refs) => {
                        let mut c = resolve(&items, &imports, &var_cols, &refs[0]).collection();
                        for r in &refs[1..] { c = c.concat(resolve(&items, &imports, &var_cols, r).collection()); }
                        Rendered::Collection(c)
                    },
                    st::Node::Arrange(r) => Rendered::Arrangement(resolve(&items, &imports, &var_cols, r).arrange()),
                    st::Node::Join { left, right, projection } => {
                        let l = resolve(&items, &imports, &var_cols, left).arrange();
                        let r = resolve(&items, &imports, &var_cols, right).arrange();
                        Rendered::Collection(B::join(l, r, projection))
                    },
                    st::Node::Reduce { input, reducer } => {
                        let a = resolve(&items, &imports, &var_cols, input).arrange();
                        Rendered::Arrangement(B::reduce(a, reducer))
                    },
                    st::Node::Inspect { input, label } => {
                        let c = resolve(&items, &imports, &var_cols, input).collection();
                        Rendered::Collection(B::inspect(c, label.clone()))
                    },
                };
                items.push(RItem::Op(rendered));
            },
            st::Item::Sub(child) => {
                let child_imports: Vec<Collection<'s, Time, B::Container>> = child.imports.iter().map(|imp| match &imp.from {
                    st::Source::Parent(r) => resolve(&items, &imports, &var_cols, r).collection(),
                    other => panic!("non-root scope with external source {:?}", other),
                }).collect();
                // Each `{}` scope is a real timely region: imports enter it,
                // the child renders inside, exports leave it structurally —
                // and then pop the child's dynamic coordinate.
                let exported = scope.region_named(&child.name, |region| {
                    let entered: Vec<_> = child_imports.iter().map(|c| c.clone().enter(region)).collect();
                    let exports = render_tree::<B>(child, region, depth + 1, entered);
                    exports.into_iter().map(|c| c.leave(scope)).collect::<Vec<_>>()
                });
                let left: Vec<Collection<'s, Time, B::Container>> = exported.into_iter().map(|c| B::leave_dynamic(c, depth + 1)).collect();
                items.push(RItem::Sub(left));
            },
        }
    }

    for bind in &s.binds {
        let c = resolve(&items, &imports, &var_cols, &bind.value).collection();
        var_handles[bind.var].take().expect("bind: variable already bound").set(c);
    }

    s.exports.iter().map(|e| resolve(&items, &imports, &var_cols, &e.value).collection()).collect()
}
