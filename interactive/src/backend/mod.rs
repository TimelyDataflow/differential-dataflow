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
pub mod corgi;

use timely::Container;
use timely::order::Product;
use timely::dataflow::Scope;
use timely::progress::{Antichain, Timestamp};
use differential_dataflow::{Collection, VecCollection};
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::dynamic::pointstamp::PointStampSummary;
use differential_dataflow::dynamic::feedback_summary;
use differential_dataflow::collection::containers::{Enter, Leave, ResultsIn};

use crate::ir::{Diff, Time, LinearOp, Value};
use crate::parse::{Projection, Reducer};
use crate::scope_ir as st;

/// A partial binding of a delta join's attributes: one `Value` per attribute,
/// `unit` where nothing is bound yet. It travels beside a *payload* time and is
/// internal to the join — it never reaches a dataflow edge as a DDIR row.
pub type Prefix = Vec<Value>;

/// A delta join's indexes must not compact logically: the construction rests on
/// telling "strictly before" from "at the same time" in the total order on
/// times, and compaction advances times to a frontier, which is exactly what
/// erases that distinction.
///
/// Holding the frontier at the minimum is the always-correct answer for any
/// timestamp. A tighter one would name the largest time strictly below the
/// operator's own frontier, but [`Time`] is `Product<u64, PointStamp<u64>>` and
/// has no predecessor: decrementing a coordinate is unsound, because a lattice
/// join against the decremented frontier can carry an update from before the
/// boundary to after it. (Take `d = (0, [3, 5])`, `f = (0, [3, 4])` and an
/// update at `(0, [2, 9])`, which is below `d`; advanced by `f` it becomes
/// `(0, [3, 9])`, which is above.) So: correct first, and this retention is the
/// cost the construction should be measured on.
pub fn no_compaction(_time: &Time, antichain: &mut Antichain<Time>) {
    antichain.insert(Time::minimum());
}

/// Build the probe key and the value-binding closure for one delta-join stage.
///
/// Shared by both backends' `delta_stage`: what a stage probes with, and what it
/// does with a match, is a property of the plan, not of the substrate.
pub fn stage_probe(kind: &st::StageKind) -> (impl Fn(&Prefix) -> Value + Clone + 'static, Option<usize>) {
    let kind = *kind;
    let probe = move |prefix: &Prefix| match kind {
        // Probe by the bound attribute; the matched value binds another.
        st::StageKind::Propose { key, .. } => prefix[key].clone(),
        // Both attributes are bound: probe the pair, keep the binding. The
        // matched multiplicity still multiplies in, which is what makes this the
        // multiset join rather than an existence test.
        st::StageKind::Validate { key, val } => Value::Tuple(vec![prefix[key].clone(), prefix[val].clone()]),
    };
    let bind = match kind { st::StageKind::Propose { bind, .. } => Some(bind), st::StageKind::Validate { .. } => None };
    (probe, bind)
}

/// Extend a prefix with a matched value, for a `Propose`; a `Validate` leaves it.
pub fn stage_extend(prefix: &Prefix, bind: Option<usize>, val: &Value) -> Prefix {
    let mut prefix = prefix.clone();
    if let Some(bind) = bind { prefix[bind] = val.clone(); }
    prefix
}

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

    /// Arrange a relation for delta-join probes. The three orientations are
    /// re-keyings, so this is `arrange` composed with a projection and every
    /// backend gets it for free; see [`st::Node::DeltaIndex`] for why these are
    /// separate arrangements rather than shared with `arrange`.
    fn delta_index<'s>(c: Collection<'s, Time, Self::Container>, orient: st::Orient) -> Self::Arr<'s> {
        let ops = orient.ops();
        if ops.is_empty() { Self::arrange(c) } else { Self::arrange(Self::linear(c, ops, 0)) }
    }

    /// Row egress and ingress at the delta join's boundary.
    ///
    /// A delta join's intermediate value is a *partial binding*, which is not a
    /// DDIR row and has no columnar form until the join completes — so the
    /// prefix stream is rows in every backend, and each one says here how its
    /// container converts. Everything between the two conversions is the same
    /// walk for all backends; only [`Self::delta_stage`] differs.
    fn to_rows<'s>(c: Collection<'s, Time, Self::Container>) -> VecCollection<'s, Time, (Value, Value), Diff>;
    /// The inverse of [`Self::to_rows`].
    fn from_rows<'s>(c: VecCollection<'s, Time, (Value, Value), Diff>) -> Collection<'s, Time, Self::Container>;

    /// One stage of a delta path: a single `half_join` of the prefix stream
    /// against `index`.
    ///
    /// Each prefix travels with a *payload* time, and the collection's own time
    /// stays at the delta's. The stage keeps updates whose arrangement time
    /// precedes the delta's own time in the total order (`strict` decides
    /// whether equality counts), advances the payload by the lattice join of
    /// the two, and — for a `Propose` — binds the matched value.
    fn delta_stage<'s>(
        stream: VecCollection<'s, Time, (Prefix, Time), Diff>,
        index: Self::Arr<'s>,
        kind: &st::StageKind,
        strict: bool,
    ) -> VecCollection<'s, Time, (Prefix, Time), Diff>;

    /// Render a delta join: one path per atom, concatenated.
    ///
    /// `atoms[i]` is atom `i`'s collection — the deltas that drive path `i`.
    /// `indexes[i][j]` is the arrangement `paths[i][j]` probes, resolved.
    /// Output rows are `(Tuple(attribute values), ())`.
    fn delta_join<'s>(
        atoms: Vec<Collection<'s, Time, Self::Container>>,
        body: &[st::Atom],
        paths: &[Vec<st::Stage>],
        indexes: Vec<Vec<Self::Arr<'s>>>,
    ) -> Collection<'s, Time, Self::Container> {
        use differential_dataflow::AsCollection;
        use timely::dataflow::operators::core::Map;
        use timely::dataflow::operators::Concatenate;

        let n = st::attr_count(body);
        let scope = atoms[0].inner.scope();
        let mut fragments = Vec::with_capacity(paths.len());

        for (i, path) in paths.iter().enumerate() {
            // The delta's own row is the binding it makes, and the payload
            // starts at the delta's own time — which is also the update's.
            let (akey, aval) = (body[i].key, body[i].val);
            // A path with any strict stage cannot produce anything at the minimum time:
            // a strict lookup needs an arrangement time strictly below the delta's own,
            // and nothing precedes the minimum. Every path but the last probes at least
            // one later atom, so only the last path survives a snapshot. Dropping those
            // deltas at the source is what stops the doomed paths from materializing an
            // intermediate they will then discard.
            let doomed_at_minimum = i + 1 < paths.len();
            let mut cur: VecCollection<'s, Time, (Prefix, Time), Diff> = Self::to_rows(atoms[i].clone())
                .inner
                .flat_map(move |((k, v), t, d)| {
                    if doomed_at_minimum && t == Time::minimum() { return None; }
                    let mut prefix = vec![Value::unit(); n];
                    prefix[akey] = k;
                    prefix[aval] = v;
                    Some(((prefix, t.clone()), t, d))
                })
                .as_collection();

            for (stage, index) in path.iter().zip(&indexes[i]) {
                cur = Self::delta_stage(cur, index.clone(), &stage.kind, stage.order.strict());
            }

            // Leave the delta region: the payload is the moment the match takes
            // effect, and it is at or beyond the update's own time in the
            // lattice order, so the capability held already covers it.
            fragments.push(cur.inner.map(|((prefix, payload), _time, diff)| {
                ((Value::Tuple(prefix), Value::unit()), payload, diff)
            }));
        }

        Self::from_rows(scope.concatenate(fragments).as_collection())
    }
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
                    st::Node::DeltaIndex { input, orient } => {
                        let c = resolve(&items, &imports, &var_cols, input).collection();
                        Rendered::Arrangement(B::delta_index(c, *orient))
                    },
                    st::Node::DeltaJoin { atoms, paths } => {
                        let cols = atoms.iter()
                            .map(|a| resolve(&items, &imports, &var_cols, &a.input).collection())
                            .collect();
                        let indexes = paths.iter()
                            .map(|p| p.iter().map(|s| resolve(&items, &imports, &var_cols, &s.index).arrange()).collect())
                            .collect();
                        Rendered::Collection(B::delta_join(cols, atoms, paths, indexes))
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
