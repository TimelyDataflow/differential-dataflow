//! Scope-tree rendering, generic over the substrate.
//!
//! The tree walk — scopes as timely regions, feedback variables, item-order
//! rendering — lives once in `render_tree`, generic over a `Backend`. A
//! backend fixes the differential container and supplies the substrate leaf
//! operators (`linear`/`join`/`reduce`/`arrange`/`inspect`); everything else
//! (enter/leave/concat/feedback, and dropping child coordinates with `retime`)
//! is container-generic. Time is always `ir::Time`; only the container varies.
//!
//! The server is a thin driver: it picks a backend and calls `render_tree`.


pub(crate) mod corgi;

use timely::Container;
use timely::dataflow::Scope;
use timely::progress::{PathSummary, Timestamp};
use differential_dataflow::{AsCollection, Collection};
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::collection::containers::{Enter, Leave, ResultsIn};

use crate::ir::{Time, Lambda, LinearOp, Reducer};
use crate::time::TimeSummary;
use crate::scope_ir as st;
use crate::shapes::{ItemShapes, ScopeShapes};

/// A rendering substrate: a differential container plus the leaf operators over
/// it. Collections are the plain container-generic `Collection<'s, Time, C>`;
/// only the arrangement type (`Arr`) is substrate-specific, and the walk only
/// ever clones it, so the bound is just `Clone`.
pub(crate) trait Backend {
    /// The differential update container. Region enter/leave (same timestamp)
    /// must be the identity on it, so the walk can recurse into child scopes.
    type Container: Container + Clone
        + ResultsIn<<Time as Timestamp>::Summary>
        + Enter<Time, Time, InnerContainer = Self::Container>
        + Leave<Time, Time, OuterContainer = Self::Container>;
    /// The arrangement produced by `arrange`/`reduce` and consumed by `join`.
    type Arr<'scope>: Clone;

    /// `shape` is the input's row shape, as install inferred it.
    fn linear<'s>(c: Collection<'s, Time, Self::Container>, ops: Vec<LinearOp>, level: usize, shape: st::RowShape) -> Collection<'s, Time, Self::Container>;
    fn arrange<'s>(c: Collection<'s, Time, Self::Container>) -> Self::Arr<'s>;
    /// Bring an arrangement into a child region. Regions share the timestamp, so this shares
    /// the trace rather than rebuilding it.
    fn enter_region<'s, 'r>(a: Self::Arr<'s>, region: Scope<'r, Time>) -> Self::Arr<'r>;
    fn as_collection<'s>(a: Self::Arr<'s>) -> Collection<'s, Time, Self::Container>;
    /// `shapes` are the two inputs' row shapes.
    fn join<'s>(l: Self::Arr<'s>, r: Self::Arr<'s>, projection: &Lambda, shapes: (st::RowShape, st::RowShape)) -> Collection<'s, Time, Self::Container>;
    /// See `scope_ir::Node::HalfJoin`. `level` is the scope depth, which fixes the width of a time
    /// as data; `shapes` are the stream's and the arrangement's row shapes.
    fn half_join<'s>(stream: Collection<'s, Time, Self::Container>, arrangement: Self::Arr<'s>, strict: bool, projection: &Lambda, level: usize, shapes: (st::RowShape, st::RowShape)) -> Collection<'s, Time, Self::Container>;
    /// See `scope_ir::Node::Reduce`; `shape` is the input's row shape.
    fn reduce<'s>(a: Self::Arr<'s>, reducer: &Reducer, limit: Option<usize>, shape: st::RowShape) -> Self::Arr<'s>;
    fn inspect<'s>(c: Collection<'s, Time, Self::Container>, label: String) -> Collection<'s, Time, Self::Container>;
}

/// Apply `step` to capabilities and rows, advertising the same summary to progress tracking.
fn retime<'s, C>(c: Collection<'s, Time, C>, step: TimeSummary, name: &str) -> Collection<'s, Time, C>
where
    C: Container + ResultsIn<TimeSummary>,
{
    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::generic::{builder_rc::OperatorBuilder, OutputBuilder};
    use timely::dataflow::operators::CapabilitySet;
    use timely::progress::Antichain;

    let mut builder = OperatorBuilder::new(name.to_string(), c.inner.scope());
    let (output, stream) = builder.new_output();
    let mut output = OutputBuilder::from(output);
    let mut input = builder.new_input_connection(c.inner, Pipeline, [(0, Antichain::from_elem(step.clone()))]);
    builder.build(move |_capability| move |_frontier| {
        let mut output = output.activate();
        input.for_each(|cap, data| {
            // A message may carry several times: hold a capability for each, moved as the rows are.
            let caps: CapabilitySet<_> = cap.stamp().iter().filter_map(|t| step.results_in(t)).map(|t| cap.delayed(&t, 0)).collect();
            let mut moved = std::mem::take(data).results_in(&step);
            output.session(&caps).give_container(&mut moved);
        });
    });
    stream.as_collection()
}

/// Which statement built which operators: every operator whose worker-unique identifier is in
/// `first .. end` was rendered from a scope item labelled `label` (see `scope_ir::Scope::labels`).
/// Rendering logs one per item to the logger registered as `"ddir/labels"`, if any. A child
/// scope's items log before the scope's own event, so the first label an operator gets is the
/// most specific.
#[derive(Clone, Debug)]
pub struct LabelEvent {
    pub first: usize,
    pub end: usize,
    pub label: String,
}

/// The container builder of the `"ddir/labels"` logger.
pub type LabelEventBuilder = timely::container::CapacityContainerBuilder<Vec<(std::time::Duration, LabelEvent)>>;

/// A rendered item's value: a collection, or an arrangement.
///
/// Imports are rendered values too, so a driver can hand a scope an arrangement it already
/// maintains (a published trace) and the scope's `arrange`s and joins of it reuse it as is.
pub(crate) enum Rendered<'s, B: Backend> {
    Collection(Collection<'s, Time, B::Container>),
    Arrangement(B::Arr<'s>),
    /// An imported arrangement that the scope also reads as rows: both forms, so the rows
    /// are flattened out of it once rather than once per use.
    Both(B::Arr<'s>, Collection<'s, Time, B::Container>),
}

impl<'s, B: Backend> Clone for Rendered<'s, B> {
    fn clone(&self) -> Self {
        match self {
            Rendered::Collection(c) => Rendered::Collection(c.clone()),
            Rendered::Arrangement(a) => Rendered::Arrangement(a.clone()),
            Rendered::Both(a, c) => Rendered::Both(a.clone(), c.clone()),
        }
    }
}

impl<'s, B: Backend> Rendered<'s, B> {
    /// Bring the value into a child region unchanged. An absent child coordinate is already zero.
    fn enter<'r>(&self, region: Scope<'r, Time>) -> Rendered<'r, B> {
        match self {
            Rendered::Collection(c) => Rendered::Collection(c.clone().enter(region)),
            Rendered::Arrangement(a) => Rendered::Arrangement(B::enter_region(a.clone(), region)),
            Rendered::Both(a, c) => Rendered::Both(B::enter_region(a.clone(), region), c.clone().enter(region)),
        }
    }
    pub(crate) fn collection(&self) -> Collection<'s, Time, B::Container> {
        match self {
            Rendered::Collection(c) | Rendered::Both(_, c) => c.clone(),
            Rendered::Arrangement(a) => B::as_collection(a.clone()),
        }
    }
    pub(crate) fn arrange(&self) -> B::Arr<'s> {
        match self {
            Rendered::Arrangement(a) | Rendered::Both(a, _) => a.clone(),
            Rendered::Collection(c) => B::arrange(c.clone()),
        }
    }
}

/// A rendered scope item: an operator's value, or a child scope's surrendered
/// exports (already returned to this scope's depth: the child's coordinate dropped).
enum RItem<'s, B: Backend> {
    Op(Rendered<'s, B>),
    Sub(Vec<Collection<'s, Time, B::Container>>),
}

fn resolve<'s, B: Backend>(
    items: &[RItem<'s, B>],
    imports: &[Rendered<'s, B>],
    var_cols: &[Collection<'s, Time, B::Container>],
    r: &st::Ref,
) -> Rendered<'s, B> {
    match r {
        st::Ref::Local(i) => match &items[*i] {
            RItem::Op(rendered) => rendered.clone(),
            RItem::Sub(_) => panic!("Ref::Local points at a child scope"),
        },
        st::Ref::Import(i) => imports[*i].clone(),
        st::Ref::Var(i) => Rendered::Collection(var_cols[*i].clone()),
        st::Ref::ChildExport(i, j) => match &items[*i] {
            RItem::Sub(exports) => Rendered::Collection(exports[*j].clone()),
            RItem::Op(_) => panic!("Ref::ChildExport points at an operator"),
        },
    }
}

/// Whether scope `s` reads import `k` as rows: as a linear op's, concat's, or inspect's input,
/// or as a bind or export. A child's import is not a read: the arrangement enters the child.
fn reads_rows(s: &st::Scope, k: usize) -> bool {
    let is_k = |r: &st::Ref| matches!(r, st::Ref::Import(i) if *i == k);
    s.items.iter().any(|item| match item {
        st::Item::Op(st::Node::Linear { input, .. } | st::Node::Inspect { input, .. }) => is_k(input),
        st::Item::Op(st::Node::HalfJoin { stream, .. }) => is_k(stream),
        st::Item::Op(st::Node::Concat(refs)) => refs.iter().any(is_k),
        st::Item::Op(st::Node::Arrange(_) | st::Node::Join { .. } | st::Node::Reduce { .. }) => false,
        st::Item::Sub(_) => false,
    }) || s.binds.iter().any(|b| is_k(&b.value)) || s.exports.iter().any(|e| is_k(&e.value))
}

/// Render one scope at `depth` (root = 0): feedback vars first (they're listed,
/// not scanned for), then items in order, then binds close the loops, then the
/// exports are surrendered. The returned collections are at this scope's depth;
/// popping the coordinate (`retime`) is the caller's job. `shapes` are the
/// scope's collections' shapes, which install inferred, all of them known: the
/// operators compile against them before any data arrives.
pub(crate) fn render_tree<'s, B: Backend>(
    s: &st::Scope,
    scope: Scope<'s, Time>,
    depth: usize,
    imports: Vec<Rendered<'s, B>>,
    shapes: &ScopeShapes,
) -> Vec<Collection<'s, Time, B::Container>> {
    render_scope::<B>(s, scope, depth, imports, shapes).iter().map(Rendered::collection).collect()
}

/// [`render_tree`], with each export as it was rendered: an exported arrangement stays one, so
/// a driver can publish it as it is.
pub(crate) fn render_scope<'s, B: Backend>(
    s: &st::Scope,
    scope: Scope<'s, Time>,
    depth: usize,
    imports: Vec<Rendered<'s, B>>,
    shapes: &ScopeShapes,
) -> Vec<Rendered<'s, B>> {
    let shape_of = |r: &st::Ref| {
        let c = shapes.of(r).expect("install refuses a program with a collection of unknown shape");
        (c.key, c.val)
    };
    // An arranged import that is also read as rows gets its rows flattened once, here.
    let imports: Vec<Rendered<'s, B>> = imports.into_iter().enumerate().map(|(k, imp)| match imp {
        Rendered::Arrangement(a) if reads_rows(s, k) => {
            let rows = B::as_collection(a.clone());
            Rendered::Both(a, rows)
        }
        other => other,
    }).collect();
    let mut var_handles: Vec<Option<Variable<'s, Time, B::Container>>> = Vec::new();
    let mut var_cols: Vec<Collection<'s, Time, B::Container>> = Vec::new();
    for _ in &s.vars {
        // The scope's own coordinate is the last of its `depth + 1`.
        let (var, col) = Variable::new(scope, TimeSummary::advance(depth, 1));
        var_handles.push(Some(var));
        var_cols.push(col);
    }

    let labels = (s.labels.len() == s.items.len()).then_some(&s.labels);
    let label_logger = labels.and_then(|_| scope.worker().logger_for::<LabelEventBuilder>("ddir/labels"));
    let mut items: Vec<RItem<'s, B>> = Vec::new();
    for (index, item) in s.items.iter().enumerate() {
        let first = scope.worker().peek_identifier();
        match item {
            st::Item::Op(node) => {
                let rendered = match node {
                    st::Node::Linear { input, ops } => {
                        let c = resolve(&items, &imports, &var_cols, input).collection();
                        Rendered::Collection(B::linear(c, ops.clone(), depth, shape_of(input)))
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
                        Rendered::Collection(B::join(l, r, projection, (shape_of(left), shape_of(right))))
                    },
                    st::Node::HalfJoin { stream, arrangement, strict, projection } => {
                        let c = resolve(&items, &imports, &var_cols, stream).collection();
                        let a = resolve(&items, &imports, &var_cols, arrangement).arrange();
                        Rendered::Collection(B::half_join(c, a, *strict, projection, depth, (shape_of(stream), shape_of(arrangement))))
                    },
                    st::Node::Reduce { input, reducer, limit } => {
                        let a = resolve(&items, &imports, &var_cols, input).arrange();
                        Rendered::Arrangement(B::reduce(a, reducer, *limit, shape_of(input)))
                    },
                    st::Node::Inspect { input, label } => {
                        let c = resolve(&items, &imports, &var_cols, input).collection();
                        Rendered::Collection(B::inspect(c, label.clone()))
                    },
                };
                items.push(RItem::Op(rendered));
            },
            st::Item::Sub(child) => {
                let child_imports: Vec<Rendered<'s, B>> = child.imports.iter().map(|imp| match &imp.from {
                    st::Source::Parent(r) => resolve(&items, &imports, &var_cols, r),
                    other => panic!("non-root scope with external source {:?}", other),
                }).collect();
                // Each `{}` scope is a real timely region: imports enter unchanged, the child
                // renders inside, exports leave structurally, then drop the child's coordinate.
                let exported = scope.region_named(&child.name, |region| {
                    let entered: Vec<_> = child_imports.iter().map(|imp| imp.enter(region)).collect();
                    let ItemShapes::Sub(child_shapes) = &shapes.items[index] else { panic!("a child scope's shapes are a scope's") };
                    let exports = render_tree::<B>(child, region, depth + 1, entered, child_shapes);
                    exports.into_iter().map(|c| c.leave(scope)).collect::<Vec<_>>()
                });
                let left: Vec<Collection<'s, Time, B::Container>> = exported.into_iter().map(|c| retime(c, TimeSummary::leave(depth + 1), "LeaveDynamic")).collect();
                items.push(RItem::Sub(left));
            },
        }
        if let (Some(logger), Some(labels)) = (&label_logger, labels) {
            let end = scope.worker().peek_identifier();
            logger.log(LabelEvent { first, end, label: labels[index].clone() });
        }
    }
    if let Some(logger) = &label_logger {
        logger.flush();
    }

    for bind in &s.binds {
        let c = resolve(&items, &imports, &var_cols, &bind.value).collection();
        var_handles[bind.var].take().expect("bind: variable already bound").set(c);
    }

    s.exports.iter().map(|e| resolve(&items, &imports, &var_cols, &e.value)).collect()
}
