//! Narrow what statements produce and arrange to the fields something reads: the transform
//! over the `demand` analysis. Switched by `Config::projection`.
//!
//! Each dead field of a lambda's output becomes `()`, written into the lambda's own text where
//! its body is a tuple (`(author, (created neg, id))` becomes `(author, (created neg, ()))`)
//! and otherwise respelled from the graph. A reduce whose output has dead value fields drops
//! them in its reducer. A sum-only reduce reads only the value fields it sums, so the rows
//! its arrangement stores narrow too (`demand::summed_fields`). An arrangement whose input
//! keeps fields nobody reads past it gets a map that drops them, so the arrangement stores
//! what is read. Nothing else is inserted: a concat's arms keep what any arm keeps.
//!
//! Each site is accepted on its own: a lambda that cannot be narrowed (its narrowed text does
//! not type, or does not reparse to the narrowed graph) is left as written, its fields are
//! pinned live, and the rest proceeds in another round. The result is checked once by shape
//! inference; the caller keeps the original if that fails.

use std::sync::Arc;

use corgi::Shape;

use crate::ir::{Lambda, LinearOp, Reducer};
use crate::lambda::{normalize::canonical, slot_shape, Projection};
use crate::scope_ir::{Item, Node, Program, Ref, RowShape, Scope, Source};
use crate::shapes::{self, CollShape, ItemShapes, ScopeShapes};
use super::demand::{Demand, ItemSites, ScopeSites, Site};

/// What one call did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Outcome {
    /// Field positions that became `()`, in lambdas, reducers, and inserted maps.
    pub fields: usize,
    /// Lambdas rewritten, and how many of them were respelled from their graphs because
    /// their text could not be edited in place.
    pub lambdas: usize,
    pub respelled: usize,
    /// Reducers that drop output fields.
    pub reducers: usize,
    /// Maps inserted before arrangements.
    pub inserted: usize,
    /// Sites left as written because their narrowing did not type or respell.
    pub declined: usize,
    /// Rounds taken; one when nothing declined.
    pub rounds: usize,
}

const ROUNDS: usize = 8;

/// Narrow `program`, which must type (`source` as for `shapes::infer`). A program with shape
/// problems is left as it is. `Err` means the result would not type; the program is unchanged.
pub fn narrow(program: &mut Program, source: &dyn Fn(&Source) -> Option<RowShape>) -> Result<Outcome, String> {
    narrow_timed(program, source, &mut super::Measurement::default())
}

pub(super) fn narrow_timed(program: &mut Program, source: &dyn Fn(&Source) -> Option<RowShape>, inference: &mut super::Measurement) -> Result<Outcome, String> {
    let (shapes, problems) = inference.infer(program, source);
    if !problems.conflicts.is_empty() || !problems.unknown.is_empty() { return Ok(Outcome::default()); }
    let demand = Demand::analyze(program, &shapes)?;
    let scope = parse_scope();
    let mut pinned = Vec::new();
    let mut declined = 0;
    for round in 1..=ROUNDS {
        let live = demand.live(&pinned);
        let mut candidate = program.clone();
        let mut walk = Walk { live: &live, scope: &scope, pins: Vec::new(), outcome: Outcome::default() };
        walk.scope(&mut candidate.root, &shapes, &demand.root, None, 0)?;
        if !walk.pins.is_empty() {
            declined += walk.pins.len();
            pinned.extend(walk.pins.into_iter().flat_map(|site| site.into_iter()));
            continue;
        }
        let mut outcome = walk.outcome;
        outcome.declined = declined;
        outcome.rounds = round;
        if outcome.fields == 0 { return Ok(outcome); }
        let (checked, problems) = inference.infer(&candidate, source);
        if !problems.conflicts.is_empty() || !problems.unknown.is_empty() || checked.exports != shapes.exports {
            return Err(format!("narrowing gave a program that does not type: {:?} {:?}", problems.conflicts, problems.unknown));
        }
        *program = candidate;
        return Ok(outcome);
    }
    Err(format!("narrowing did not settle in {ROUNDS} rounds"))
}

/// The scope narrowed text is parsed in: the base words and the registered functions, as the
/// program's own lambdas were, without its `type` declarations (a lambda naming one is respelled
/// with the declaration inline, or left as written).
fn parse_scope() -> corgi::Scope {
    let mut scope = crate::lambda::base_scope();
    for f in crate::lambda::registered() { let _ = scope.word(f.name(), crate::lambda::function_word(&f)); }
    scope
}

struct Walk<'a> {
    live: &'a [bool],
    scope: &'a corgi::Scope,
    /// Sites to pin live for the next round, as their facts.
    pins: Vec<Vec<usize>>,
    outcome: Outcome,
}

/// Which collections of a scope are arrangements, as the renderer decides: arranges, reduces,
/// trace imports (provider arrangements), and parent imports of those.
#[derive(Default)]
struct Forms { imports: Vec<bool>, items: Vec<bool> }

impl Forms {
    fn arranged(&self, r: &Ref) -> bool {
        match r { Ref::Local(i) => self.items[*i], Ref::Import(i) => self.imports[*i], Ref::Var(_) | Ref::ChildExport(..) => false }
    }
}

fn remap_ref(r: &mut Ref, remap: &[usize]) {
    if let Ref::Local(i) | Ref::ChildExport(i, _) = r { *i = remap[*i]; }
}

impl Walk<'_> {
    fn decline(&mut self, sites: &[&Site]) {
        self.pins.push(sites.iter().flat_map(|site| site.all()).collect());
    }

    fn narrowed(&self, site: &Site) -> (Shape, Shape) { site.narrowed(self.live) }

    /// Parse edited text where `original` was parsed, so its types and functions resolve.
    fn parse(&self, src: String, original: &Lambda) -> Lambda {
        match &original.scope {
            Some(scope) => Lambda::new_in(src, scope.clone()),
            None => Lambda::new(src, self.scope),
        }
    }

    /// Rewrite one scope, returning the shapes its collections now have.
    fn scope(&mut self, scope: &mut Scope, shapes: &ScopeShapes, sites: &ScopeSites, parent: Option<(&ScopeShapes, &Forms)>, depth: usize) -> Result<ScopeShapes, String> {
        let mut realized = ScopeShapes::default();
        let mut forms = Forms::default();
        for (i, import) in scope.imports.iter().enumerate() {
            realized.imports.push(match (&import.from, parent) {
                (Source::Parent(r), Some((parent, _))) => parent.of(r).map(|c| CollShape::new(c.key, c.val, depth)),
                _ => shapes.imports[i].clone(),
            });
            forms.imports.push(match (&import.from, parent) {
                (Source::Parent(r), Some((_, forms))) => forms.arranged(r),
                (Source::Trace(_), _) => true,
                _ => false,
            });
        }
        for site in &sites.vars {
            let (key, val) = self.narrowed(site);
            realized.vars.push(Some(CollShape::new(key, val, depth)));
        }
        let labeled = scope.labels.len() == scope.items.len();
        let old_labels = std::mem::take(&mut scope.labels);
        let mut items = Vec::with_capacity(scope.items.len());
        let mut labels = Vec::with_capacity(old_labels.len());
        let mut remap = Vec::with_capacity(scope.items.len());
        for (i, mut item) in std::mem::take(&mut scope.items).into_iter().enumerate() {
            let label = old_labels.get(i).cloned().unwrap_or_default();
            let shape = match (&mut item, &sites.items[i], &shapes.items[i]) {
                (Item::Op(node), ItemSites::Op { output, stages }, ItemShapes::Op(Some(original))) => {
                    // Input sites are looked up before the remap, realized shapes after it.
                    let inputs: Vec<&Site> = node.refs().map(|r| sites.at(r)).collect();
                    node.for_each_ref(|r| remap_ref(r, &remap));
                    let mut inserted = Vec::new();
                    let shape = self.node(node, &inputs, output, stages, original, &realized, &forms, depth, &mut inserted)?;
                    for (map, shape) in inserted {
                        items.push(map);
                        if labeled { labels.push(label.clone()); }
                        realized.items.push(ItemShapes::Op(Some(shape)));
                        forms.items.push(false);
                    }
                    forms.items.push(matches!(node, Node::Arrange(_) | Node::Reduce { .. }));
                    ItemShapes::Op(Some(shape))
                }
                (Item::Sub(child), ItemSites::Sub(child_sites), ItemShapes::Sub(child_shapes)) => {
                    for import in &mut child.imports {
                        if let Source::Parent(r) = &mut import.from { remap_ref(r, &remap); }
                    }
                    forms.items.push(false);
                    ItemShapes::Sub(self.scope(child, child_shapes, child_sites, Some((&realized, &forms)), depth + 1)?)
                }
                _ => return Err(format!("narrow: item {i} disagrees with shape inference")),
            };
            remap.push(items.len());
            items.push(item);
            if labeled { labels.push(label); }
            realized.items.push(shape);
        }
        scope.items = items;
        if labeled { scope.labels = labels; }
        for bind in &mut scope.binds {
            remap_ref(&mut bind.value, &remap);
            if realized.of(&bind.value) != realized.vars[bind.var] { self.decline(&[&sites.vars[bind.var]]); }
        }
        for (_, r) in &mut scope.names { remap_ref(r, &remap); }
        for (i, export) in scope.exports.iter_mut().enumerate() {
            remap_ref(&mut export.value, &remap);
            let value = realized.of(&export.value);
            if let (Some(value), Some((key, val))) = (&value, &export.shape) {
                if (&value.key, &value.val) != (key, val) { self.decline(&[sites.at(&export.value)]); }
            }
            realized.exports.push(value.map(|c| CollShape::new(c.key, c.val, depth.saturating_sub(1))).or_else(|| shapes.exports[i].clone()));
        }
        Ok(realized)
    }

    /// Rewrite one operator, given its inputs' sites (before the remap), its output site, the
    /// sites of a linear chain's steps, and its original shape. Maps inserted before it go to
    /// `inserted`, with their shapes.
    #[allow(clippy::too_many_arguments)]
    fn node(&mut self, node: &mut Node, inputs: &[&Site], output: &Site, stages: &[Site], original: &CollShape, realized: &ScopeShapes, forms: &Forms, depth: usize, inserted: &mut Vec<(Item, CollShape)>) -> Result<CollShape, String> {
        let input_of = |r: &Ref| realized.of(r).ok_or_else(|| format!("narrow: no shape for {r:?}"));
        let coll = |(key, val): (Shape, Shape)| CollShape::new(key, val, depth);
        let unchanged = original.clone();
        let output_live = self.live[output.presence];
        match node {
            Node::Linear { input, ops } => {
                let start = input_of(input)?;
                let mut before = inputs[0];
                let (mut orig, mut real) = (before.shape.clone(), (start.key, start.val));
                for (j, op) in ops.iter_mut().enumerate() {
                    let stage = &stages[j];
                    let orig_out = stage.shape.clone();
                    let real_out = match op {
                        LinearOp::Project(lambda) => {
                            let target = if self.live[stage.presence] { self.narrowed(stage) } else { orig_out.clone() };
                            match self.narrow_lambda(lambda, &[orig.0.clone(), orig.1.clone()], &[real.0.clone(), real.1.clone()], &orig_out, &target) {
                                Some(new) => { *lambda = new; target }
                                None => { self.decline(&[stage, before]); orig_out.clone() }
                            }
                        }
                        LinearOp::Filter(lambda) | LinearOp::EnterAt(lambda) => {
                            let slots = [real.0.clone(), real.1.clone()];
                            match self.retype(lambda, &[orig.0.clone(), orig.1.clone()], &slots, &Shape::Int) {
                                Some(new) => { *lambda = new; real.clone() }
                                None => { self.decline(&[before]); orig_out.clone() }
                            }
                        }
                        LinearOp::Negate => real.clone(),
                        LinearOp::LiftIter => (real.0.clone(), match real.1.clone() {
                            Shape::Prod(mut fs) => { fs.push(Shape::Int); Shape::Prod(fs) }
                            Shape::Unit => Shape::Prod(vec![Shape::Int]),
                            other => Shape::Prod(vec![other, Shape::Int]),
                        }),
                        LinearOp::Lift => (real.0.clone(), Shape::Prod(vec![real.1.clone(), Shape::Prod(vec![Shape::Int; depth + 1])])),
                        LinearOp::FlatMap(lambda) | LinearOp::JoinFunction(lambda) => {
                            // The output follows from the lambda at the narrowed input; it is not narrowed itself.
                            let slots = [real.0.clone(), real.1.clone()];
                            match slot_shape(lambda, &slots).ok().and_then(|_| shapes::linear_shapes(std::slice::from_ref(op), real.0.clone(), real.1.clone(), depth).ok()) {
                                Some(steps) => steps[1].clone(),
                                None => { self.decline(&[before]); orig_out.clone() }
                            }
                        }
                    };
                    orig = orig_out;
                    real = real_out;
                    before = stage;
                }
                Ok(coll(real))
            }
            Node::Concat(refs) => {
                let arms = refs.iter().map(input_of).collect::<Result<Vec<_>, _>>()?;
                let first = arms.first().ok_or("narrow: empty concat")?;
                if arms.iter().all(|arm| arm == first) { Ok(first.clone()) }
                else { self.decline(&[output]); Ok(unchanged) }
            }
            Node::Arrange(r) => {
                let input = input_of(r)?;
                // Reusing an arrangement is free; narrowing it would mean building another.
                if !output_live || forms.arranged(r) { return Ok(input); }
                let target = self.narrowed(output);
                if (input.key.clone(), input.val.clone()) == target { return Ok(coll(target)); }
                // The arrangement stores what is read past it: drop the rest before it.
                let Some((lambda, dropped)) = dropping_map(&(input.key.clone(), input.val.clone()), &target, self.scope) else {
                    self.decline(&[output, inputs[0]]);
                    return Ok(input);
                };
                let map = Item::Op(Node::Linear { input: r.clone(), ops: vec![LinearOp::Project(lambda)] });
                *r = Ref::Local(realized.items.len() + inserted.len());
                inserted.push((map, coll(target.clone())));
                self.outcome.inserted += 1;
                self.outcome.fields += dropped;
                Ok(coll(target))
            }
            Node::Join { left, right, projection } => {
                let (l, r) = (input_of(left)?, input_of(right)?);
                if l.key != r.key { self.decline(&[inputs[0], inputs[1]]); return Ok(unchanged); }
                if !output_live { return Ok(unchanged); }
                let orig_out = (original.key.clone(), original.val.clone());
                let target = self.narrowed(output);
                let orig_slots = [inputs[0].shape.0.clone(), inputs[0].shape.1.clone(), inputs[1].shape.1.clone()];
                match self.narrow_lambda(projection, &orig_slots, &[l.key, l.val, r.val], &orig_out, &target) {
                    Some(new) => { *projection = new; Ok(coll(target)) }
                    None => { self.decline(&[output, inputs[0], inputs[1]]); Ok(unchanged) }
                }
            }
            Node::HalfJoin { stream, arrangement, projection, .. } => {
                let (s, a) = (input_of(stream)?, input_of(arrangement)?);
                let time = Shape::Prod(vec![Shape::Int; depth + 1]);
                let stream_value = match &s.val { Shape::Prod(fs) if fs.len() == 2 && fs[1] == time => fs[0].clone(), _ => { self.decline(&[inputs[0]]); return Ok(unchanged); } };
                if s.key != a.key { self.decline(&[inputs[0], inputs[1]]); return Ok(unchanged); }
                if !output_live { return Ok(unchanged); }
                // The lambda gives `(key, val)` and the operator appends the time.
                let Shape::Prod(orig_val) = &original.val else { return Err("narrow: half_join output is not (value, time)".into()) };
                let orig_out = (original.key.clone(), orig_val[0].clone());
                let (target_key, target_val) = self.narrowed(output);
                let Shape::Prod(target_val) = target_val else { return Err("narrow: half_join output is not (value, time)".into()) };
                let target = (target_key, target_val[0].clone());
                let Shape::Prod(orig_stream) = &inputs[0].shape.1 else { return Err("narrow: half_join stream is not (value, time)".into()) };
                let orig_slots = [inputs[0].shape.0.clone(), orig_stream[0].clone(), inputs[1].shape.1.clone()];
                match self.narrow_lambda(projection, &orig_slots, &[s.key, stream_value, a.val], &orig_out, &target) {
                    Some(new) => { *projection = new; Ok(coll((target.0, Shape::Prod(vec![target.1, time])))) }
                    None => { self.decline(&[output, inputs[0], inputs[1]]); Ok(unchanged) }
                }
            }
            Node::Reduce { input, reducer, .. } => {
                let in_shape = input_of(input)?;
                let Reducer::Expr(lambda) = reducer;
                // The reducer reads its (possibly narrowed) rows as written.
                let Ok(value) = shapes::reduce_shape(lambda, &in_shape.key, &in_shape.val) else {
                    self.decline(&[inputs[0]]);
                    return Ok(unchanged);
                };
                if !output_live { return Ok(CollShape::new(in_shape.key, value, depth)); }
                let (_, target) = self.narrowed(output);
                let Some((projection, dropped)) = dropping_projection(&value, &target) else {
                    return Ok(CollShape::new(in_shape.key, value, depth));
                };
                let narrowed = reducer.project(&projection, &in_shape.key, &in_shape.val);
                let Reducer::Expr(check) = &narrowed;
                if shapes::reduce_shape(check, &in_shape.key, &in_shape.val).ok() != Some(target.clone()) {
                    self.decline(&[output]);
                    return Ok(CollShape::new(in_shape.key, value, depth));
                }
                *reducer = narrowed;
                self.outcome.reducers += 1;
                self.outcome.fields += dropped;
                Ok(CollShape::new(in_shape.key, target, depth))
            }
            Node::Inspect { input, .. } => input_of(input),
        }
    }

    /// A lambda over `real_slots` giving `target`, from one over `orig_slots` giving `orig_out`.
    /// `None` when neither its text nor the graph printer gives a spelling that types.
    fn narrow_lambda(&mut self, lambda: &Lambda, orig_slots: &[Shape], real_slots: &[Shape], orig_out: &(Shape, Shape), target: &(Shape, Shape)) -> Option<Lambda> {
        let mask = (Mask::new(&orig_out.0, &target.0), Mask::new(&orig_out.1, &target.1));
        let dropped = mask.0.dropped() + mask.1.dropped();
        let expected = Shape::Prod(vec![target.0.clone(), target.1.clone()]);
        if dropped == 0 { return self.retype(lambda, orig_slots, real_slots, &expected); }
        let orig_input = Shape::Prod(orig_slots.to_vec());
        let real_input = Shape::Prod(real_slots.to_vec());
        let reference = masked_graph(lambda, &mask, &orig_input)?;
        let text = edit_source(&lambda.src, &mask)
            .map(|src| self.parse(src, lambda))
            .filter(|candidate| accept(candidate, &reference, &real_input, &expected));
        let new = match text {
            Some(text) => text,
            None => { self.outcome.respelled += 1; respell(&reference, &real_input, &expected, lambda.scope.as_deref().unwrap_or(self.scope))? }
        };
        self.outcome.fields += dropped;
        self.outcome.lambdas += 1;
        Some(new)
    }

    /// The lambda as written when it types over `real_slots`, else respelled from its graph
    /// compiled over `orig_slots`, where any read of a narrowed field was dead.
    fn retype(&mut self, lambda: &Lambda, orig_slots: &[Shape], real_slots: &[Shape], expected: &Shape) -> Option<Lambda> {
        if orig_slots == real_slots { return Some(lambda.clone()); }
        if slot_shape(lambda, real_slots).ok().as_ref() == Some(expected) { return Some(lambda.clone()); }
        let graph = canonical(lambda.graph().ok()?, &Shape::Prod(orig_slots.to_vec())).ok()?;
        let new = respell(&graph, &Shape::Prod(real_slots.to_vec()), expected, lambda.scope.as_deref().unwrap_or(self.scope))?;
        self.outcome.lambdas += 1;
        self.outcome.respelled += 1;
        Some(new)
    }
}

/// Which top-level fields of one side of a lambda's output become `()`.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Mask { Keep, Unit, Prod(Vec<Mask>) }

impl Mask {
    fn new(original: &Shape, target: &Shape) -> Self {
        if original == target { return Mask::Keep; }
        match (original, target) {
            (Shape::Prod(os), Shape::Prod(ts)) if os.len() == ts.len() => Mask::Prod(os.iter().zip(ts).map(|(o, t)| Mask::new(o, t)).collect()),
            (o, Shape::Unit) if *o != Shape::Unit => Mask::Unit,
            _ => Mask::Keep,
        }
    }

    fn dropped(&self) -> usize {
        match self { Mask::Keep => 0, Mask::Unit => 1, Mask::Prod(parts) => parts.iter().map(Mask::dropped).sum() }
    }

    /// The expression selecting the kept part of `name`.
    fn text(&self, name: &str) -> String {
        match self {
            Mask::Keep => name.into(),
            Mask::Unit => "()".into(),
            Mask::Prod(parts) => format!("({})", parts.iter().enumerate().map(|(i, p)| p.text(&format!("{name}.{i}"))).collect::<Vec<_>>().join(", ")),
        }
    }
}

/// The lambda's graph followed by the mask, canonical at `input`: dead work is gone.
fn masked_graph(lambda: &Lambda, mask: &(Mask, Mask), input: &Shape) -> Option<corgi::Graph<corgi::NumOp>> {
    let text = format!("let (k, v) = input in ({}, {})", mask.0.text("k"), mask.1.text("v"));
    let mask = corgi::parse_ml(&text).ok()?;
    let mut builder = corgi::Builder::default();
    let start = builder.input();
    let mid = builder.graft(lambda.graph().ok()?, start);
    let end = builder.graft(&mask, mid);
    canonical(&builder.finish(end), input).ok()
}

/// A candidate spelling is accepted when it gives `expected` over `input` and its canonical
/// graph there is the reference's.
fn accept(candidate: &Lambda, reference: &corgi::Graph<corgi::NumOp>, input: &Shape, expected: &Shape) -> bool {
    let Ok(graph) = candidate.graph() else { return false };
    corgi::shape_of(graph, input).ok().as_ref() == Some(expected)
        && canonical(graph, input).ok().as_ref() == canonical(reference, input).ok().as_ref()
}

/// Spell a graph through the lambda printer, over `input`.
fn respell(graph: &corgi::Graph<corgi::NumOp>, input: &Shape, expected: &Shape, scope: &corgi::Scope) -> Option<Lambda> {
    let lambda = Lambda { src: String::new(), graph: Ok(Arc::new(graph.clone())), added_by: None, scope: None };
    let printed = crate::lambda::print::print(&lambda, Some(input), false);
    if printed.fallback { return None; }
    let (pattern, body) = printed.text.split_once(" -> ")?;
    let candidate = Lambda::new(format!("let {pattern} = input in {body}"), scope);
    accept(&candidate, graph, input, expected).then_some(candidate)
}

/// A map from `input` to `target`, which differs only by fields that become `()`, with the
/// number of fields it drops. `None` if it would drop nothing.
fn dropping_map(input: &(Shape, Shape), target: &(Shape, Shape), scope: &corgi::Scope) -> Option<(Lambda, usize)> {
    let mask = (Mask::new(&input.0, &target.0), Mask::new(&input.1, &target.1));
    let dropped = mask.0.dropped() + mask.1.dropped();
    if dropped == 0 { return None; }
    fn pattern(shape: &Shape, mask: &Mask, name: &str, names: &mut Vec<String>) -> (String, String) {
        match (shape, mask) {
            (Shape::Prod(fields), Mask::Prod(masks)) => {
                let parts: Vec<_> = fields.iter().zip(masks).enumerate().map(|(i, (f, m))| pattern(f, m, &format!("{name}{i}"), names)).collect();
                (format!("({})", parts.iter().map(|p| p.0.clone()).collect::<Vec<_>>().join(", ")), format!("({})", parts.iter().map(|p| p.1.clone()).collect::<Vec<_>>().join(", ")))
            }
            (_, Mask::Unit) | (Shape::Unit, _) => ("_".into(), "()".into()),
            _ => { names.push(name.to_owned()); (name.to_owned(), name.to_owned()) }
        }
    }
    let mut names = Vec::new();
    let (kp, kb) = pattern(&input.0, &mask.0, "k", &mut names);
    let (vp, vb) = pattern(&input.1, &mask.1, "v", &mut names);
    let mut lambda = Lambda::new(format!("let ({kp}, {vp}) = input in ({kb}, {vb})"), scope);
    lambda.added_by = Some("projection");
    let expected = Shape::Prod(vec![target.0.clone(), target.1.clone()]);
    (slot_shape(&lambda, &[input.0.clone(), input.1.clone()]).ok().as_ref() == Some(&expected)).then_some((lambda, dropped))
}

/// The projection of a reducer's output values from `value` to `target`, with the number of
/// fields it drops. `None` if it would drop nothing.
fn dropping_projection(value: &Shape, target: &Shape) -> Option<(Projection, usize)> {
    let mask = Mask::new(value, target);
    let dropped = mask.dropped();
    if dropped == 0 { return None; }
    fn projection(mask: &Mask, path: Vec<usize>) -> Projection {
        match mask {
            Mask::Keep => Projection::Field(path),
            Mask::Unit => Projection::Unit,
            Mask::Prod(parts) => Projection::Tuple(parts.iter().enumerate().map(|(i, p)| {
                let mut path = path.clone(); path.push(i); projection(p, path)
            }).collect()),
        }
    }
    Some((projection(&mask, Vec::new()), dropped))
}

// ---------------------------------------------------------------------------
// Editing a lambda's text: `let PATTERN = input in LETS... BODY`, the body a tuple.

/// Write `()` into the body's tuple at each dropped field, and `_` for pattern names the body
/// no longer mentions. `None` when the body is not a tuple where a field is dropped.
fn edit_source(src: &str, mask: &(Mask, Mask)) -> Option<String> {
    let (pattern, body) = src.split_once(" = input in ")?;
    let pattern = pattern.strip_prefix("let ")?;
    let (lets, expr) = split_lets(body);
    let expr = apply_mask(expr.trim(), &Mask::Prod(vec![mask.0.clone(), mask.1.clone()]))?;
    let body = prune_lets(lets, &expr);
    let pattern = if pattern.contains(':') { pattern.to_owned() } else { retire_unused(pattern, &body) };
    Some(format!("let {pattern} = input in {body}"))
}

/// The leading `let … in ` bindings of a body, and the expression after them.
fn split_lets(body: &str) -> (&str, &str) {
    let mut at = 0;
    while let Some(end) = next_let(body, at) { at = end; }
    (&body[..at], &body[at..])
}

/// `lets` followed by `expr`, without the bindings whose names nothing after them mentions.
fn prune_lets(lets: &str, expr: &str) -> String {
    let mut bindings = Vec::new();
    let mut at = 0;
    while let Some(end) = next_let(lets, at) { bindings.push(&lets[at..end]); at = end; }
    let mut kept = vec![expr.to_owned()];
    for binding in bindings.iter().rev() {
        let rest = kept.iter().rev().cloned().collect::<Vec<_>>().join("");
        let used: std::collections::HashSet<&str> = tokens(&rest).into_iter().filter_map(|(_, t)| match t { Token::Word(w) => Some(w), _ => None }).collect();
        let header = binding.trim_start();
        let names = header.strip_prefix("let ").and_then(|p| p.split_once('=')).map(|(pattern, _)| pattern);
        let dead = names.is_some_and(|pattern| tokens(pattern).into_iter().all(|(_, t)| !matches!(t, Token::Word(w) if used.contains(w))));
        if !dead { kept.push(binding.to_string()); }
    }
    kept.iter().rev().cloned().collect::<Vec<_>>().join("")
}

/// The end (after trailing whitespace) of the `let … in` or `enum … in` starting at `at`.
fn next_let(body: &str, at: usize) -> Option<usize> {
    {
        let rest = &body[at..];
        let trimmed = rest.trim_start();
        if !starts_with_word(trimmed, "let") && !starts_with_word(trimmed, "enum") { return None; }
        let start = at + (rest.len() - trimmed.len());
        // The `in` closing this binding, at parenthesis depth zero, past any nested bindings.
        let (mut depth, mut pending, mut end) = (0usize, 0usize, None);
        for (offset, token) in tokens(&body[start..]) {
            match token {
                Token::Open => depth += 1,
                Token::Close => depth = depth.saturating_sub(1),
                Token::Word("let" | "enum") if depth == 0 => pending += 1,
                Token::Word("in") if depth == 0 => { pending -= 1; if pending == 0 { end = Some(start + offset + 2); break; } }
                _ => {}
            }
        }
        let end = end?;
        Some(end + (body[end..].len() - body[end..].trim_start().len()))
    }
}

fn starts_with_word(text: &str, word: &str) -> bool {
    text.starts_with(word) && !text[word.len()..].starts_with(|c: char| c.is_alphanumeric() || c == '_')
}

enum Token<'a> { Open, Close, Comma, Word(&'a str), Other }

/// Tokens with their byte offsets: parentheses, commas, words, and anything else (string
/// literals are one token).
fn tokens(text: &str) -> Vec<(usize, Token<'_>)> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_whitespace() { i += 1; continue; }
        let (len, token) = if c == b'(' { (1, Token::Open) }
            else if c == b')' { (1, Token::Close) }
            else if c == b',' { (1, Token::Comma) }
            else if c == b'"' {
                let mut j = i + 1;
                while j < bytes.len() && bytes[j] != b'"' { if bytes[j] == b'\\' { j += 1; } j += 1; }
                ((j + 1).min(bytes.len()) - i, Token::Other)
            }
            else if c.is_ascii_alphanumeric() || c == b'_' {
                let mut j = i + 1;
                while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') { j += 1; }
                (j - i, Token::Word(&text[i..j]))
            }
            else { (1, Token::Other) };
        out.push((i, token));
        i += len;
    }
    out
}

/// `expr` with `()` at each dropped field, when its tuple structure reaches every one. A
/// leaf that is a name or a field path, such as `v` or `row.1`, is spelled out as the tuple
/// of its fields (`(v.0, ())`); typing decides later whether it was one.
fn apply_mask(expr: &str, mask: &Mask) -> Option<String> {
    match mask {
        Mask::Keep => Some(expr.to_owned()),
        Mask::Unit => Some("()".into()),
        Mask::Prod(parts) => {
            if parts.iter().all(|p| *p == Mask::Keep) { return Some(expr.to_owned()); }
            let fields: Vec<String> = match tuple_fields(expr) {
                Some(fields) => fields.into_iter().map(|f| f.trim().to_owned()).collect(),
                None if is_path(expr) => (0..parts.len()).map(|i| format!("{expr}.{i}")).collect(),
                None => return None,
            };
            if fields.len() != parts.len() { return None; }
            let edited = fields.iter().zip(parts).map(|(f, p)| apply_mask(f, p)).collect::<Option<Vec<_>>>()?;
            Some(format!("({})", edited.join(", ")))
        }
    }
}

/// A name, or a name followed by `.N` field selections.
fn is_path(expr: &str) -> bool {
    let mut parts = expr.split('.');
    let Some(name) = parts.next() else { return false };
    !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_') && !name.starts_with(|c: char| c.is_ascii_digit())
        && parts.all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
}

/// The fields of `expr` when it is one parenthesized tuple, split at its top-level commas.
fn tuple_fields(expr: &str) -> Option<Vec<&str>> {
    if !expr.starts_with('(') || !expr.ends_with(')') { return None; }
    let inner = &expr[1..expr.len() - 1];
    let (mut depth, mut start, mut fields) = (0usize, 0, Vec::new());
    for (offset, token) in tokens(inner) {
        match token {
            Token::Open => depth += 1,
            Token::Close => { if depth == 0 { return None; } depth -= 1; }
            Token::Comma if depth == 0 => { fields.push(&inner[start..offset]); start = offset + 1; }
            _ => {}
        }
    }
    if depth != 0 { return None; }
    if inner.trim().is_empty() { return Some(Vec::new()); }
    fields.push(&inner[start..]);
    Some(fields)
}

/// Pattern names the body never mentions become `_`.
fn retire_unused(pattern: &str, body: &str) -> String {
    let used: std::collections::HashSet<&str> = tokens(body).into_iter().filter_map(|(_, t)| match t { Token::Word(w) => Some(w), _ => None }).collect();
    let mut out = String::with_capacity(pattern.len());
    let mut at = 0;
    for (offset, token) in tokens(pattern) {
        if let Token::Word(word) = token {
            out.push_str(&pattern[at..offset]);
            out.push_str(if used.contains(word) { word } else { "_" });
            at = offset + word.len();
        }
    }
    out.push_str(&pattern[at..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{lower, parse};

    fn narrowed(text: &str) -> (Program, Outcome) {
        let mut program = lower::lower_tree(parse::pipe::parse(text));
        crate::passes::optimize(&mut program);
        let outcome = narrow(&mut program, &|_| None).unwrap();
        let (_, problems) = shapes::infer(&program, &|_| None);
        assert!(problems.conflicts.is_empty() && problems.unknown.is_empty(), "{problems:?}");
        (program, outcome)
    }

    fn lambdas(program: &Program) -> Vec<String> {
        let mut out = Vec::new();
        for item in &program.root.items {
            match item {
                Item::Op(Node::Linear { ops, .. }) => for op in ops {
                    if let LinearOp::Project(l) | LinearOp::Filter(l) = op { out.push(l.src.clone()); }
                },
                Item::Op(Node::Join { projection, .. }) => out.push(projection.src.clone()),
                Item::Op(Node::Reduce { reducer: Reducer::Expr(l), .. }) => out.push(l.src.clone()),
                _ => {}
            }
        }
        out
    }

    #[test]
    fn text_edits_keep_names_and_retire_unused_ones() {
        // Two readers keep `x` from fusing with either of them.
        let (program, outcome) = narrowed("let x = input 0 : (int, (int, int, int)) | map((id, (a, created, author)) -> (author, (created neg, id, (a, 1) add)));
            export \"out\" = x | map((k, (c, _, _)) -> (k, c));
            export \"two\" = x | map((k, (c, _, _)) -> (k, (c, 1) add));");
        assert_eq!(lambdas(&program)[0], "let (_, (_, created, author)) = input in (author, (created neg, (), ()))");
        assert_eq!((outcome.fields, outcome.lambdas, outcome.respelled, outcome.rounds), (2, 1, 0, 1));
        let (program, _) = narrowed("let x = input 0 : (int, (int, int)) | map((k, (a, b)) -> (k, (b, a)));
            export \"out\" = x | map((k, (a, _)) -> (k, a));
            export \"two\" = x | map((k, (a, _)) -> (k, (a, 1) add));");
        assert_eq!(lambdas(&program)[0], "let (k, (_, b)) = input in (k, (b, ()))");
    }

    #[test]
    fn a_path_leaf_is_spelled_out_as_its_fields() {
        let (program, outcome) = narrowed("let x = input 0 : (int, (int, int)) | map((k, v) -> (v, k));
            export \"out\" = x | map(((a, _), k) -> (a, k));
            export \"two\" = x | map(((a, _), k) -> (a, (k, 1) add));");
        assert_eq!(lambdas(&program)[0], "let (k, v) = input in ((v.0, ()), k)");
        assert_eq!((outcome.fields, outcome.respelled), (1, 0));
        // A body that computes its tuple is respelled from the graph.
        let (program, outcome) = narrowed("let x = input 0 : (int, List((int, int))) | map((k, l) -> (k, l head));
            export \"out\" = x | map((k, (a, _)) -> (k, a));
            export \"two\" = x | map((k, (a, _)) -> (k, (a, 1) add));");
        assert_eq!(lambdas(&program)[0], "let (a, b) = input in (a, ((0, b) gather.0, ()))");
        assert_eq!((outcome.fields, outcome.respelled), (1, 1));
        // Edited text keeps the program's types in scope.
        let (program, outcome) = narrowed("type T = A int | B;
            let x = input 0 : (int, (T, int)) | map((k, (t, b)) -> (k, (t match (A (n -> (n, 1) add), B (() -> 0)), t, b)));
            export \"out\" = x | map((k, (n, _, _)) -> (k, n));
            export \"two\" = x | map((k, (n, _, _)) -> (k, (n, 1) add));");
        assert_eq!(lambdas(&program)[0], "let (k, (t, _)) = input in (k, (t match (A (n -> (n, 1) add), B (() -> 0)), (), ()))");
        assert_eq!((outcome.fields, outcome.respelled), (2, 0));
    }

    #[test]
    fn dead_work_feeding_a_dropped_field_disappears_from_the_text() {
        let (program, _) = narrowed("let x = input 0 : (int, (int, int)) | map((k, (a, b)) -> let s = (a, b) add in (k, (a, s)));
            export \"out\" = x | map((k, (a, _)) -> (k, a));
            export \"two\" = x | map((k, (a, _)) -> (k, (a, 1) add));");
        assert!(!lambdas(&program)[0].contains("add"), "{:?}", lambdas(&program));
    }

    #[test]
    fn reduce_outputs_narrow_and_sum_only_inputs_narrow() {
        // Two readers of the minimum keep compose-maps from folding either into the reducer.
        let (program, outcome) = narrowed("let x = input 0 : (int, (int, int)) | map((k, (a, b)) -> (k, (b, a)));
            export \"least\" = x | min | map((k, (_, b)) -> (k, b));
            export \"least2\" = x | min | map((k, (_, b)) -> (k, (b, 1) add));
            export \"counted\" = x | count;");
        assert_eq!(outcome.reducers, 1, "{outcome:?}");
        let text = lambdas(&program).join("\n");
        assert!(text.contains("head"), "{text}");
        // Both reduces share one arrangement of `x`, which min reads whole: nothing narrows there.
        assert_eq!(lambdas(&program)[0], "let (k, (a, b)) = input in (k, (b, a))");
        assert_eq!(outcome.inserted, 0);
        // Alone, count's arrangement stores only keys.
        let (program, outcome) = narrowed("export \"counted\" = input 0 : (int, (int, int)) | count;");
        assert_eq!((outcome.inserted, outcome.fields), (1, 2), "{:?}", program.root.items);
        assert!(program.root.items.iter().any(|item| matches!(item, Item::Op(Node::Linear { ops, .. })
            if matches!(ops.as_slice(), [LinearOp::Project(l)] if l.added_by == Some("projection") && l.src == "let (k, (_, _)) = input in (k, ((), ()))"))), "{:?}", program.root.items);
    }

    #[test]
    fn declined_sites_pin_their_fields_and_the_rest_proceeds() {
        // `x` is bound to a filter of a root input, which keeps every field, so the dead field
        // of `x` cannot be realized: that binding declines and pins `x`. The other map narrows.
        let (program, outcome) = narrowed("let rows = input 0 : (int, (int, int));
            s: { set x = ::rows | filter((k, _) -> (k, 0) gt); let y = x | map((k, (a, _)) -> (k, a)); }
            let other = rows | map((k, (a, b)) -> (k, (b, a)));
            export \"y\" = s::y;
            export \"one\" = other | map((k, (b, _)) -> (k, b));
            export \"two\" = other | map((k, (b, _)) -> (k, (b, 1) add));");
        assert_eq!((outcome.fields, outcome.declined, outcome.rounds), (1, 1, 2), "{outcome:?}");
        assert!(lambdas(&program).contains(&"let (k, (_, b)) = input in (k, (b, ()))".to_string()), "{:?}", lambdas(&program));
        let (shapes, _) = shapes::infer(&program, &|_| None);
        let ItemShapes::Sub(child) = shapes.items.iter().find(|i| matches!(i, ItemShapes::Sub(_))).unwrap() else { unreachable!() };
        assert_eq!(child.vars[0].as_ref().unwrap().val, Shape::Prod(vec![Shape::Int, Shape::Int]));
    }

    #[test]
    fn narrowing_is_idempotent_and_round_trips_through_text() {
        for text in [
            "let x = input 0 : (int, (int, int, int)) | map((id, (a, b, c)) -> (c, (b, id, (a, 1) add)));
             export \"out\" = x | map((k, (b, _, _)) -> (k, b));",
            "let rows = input 0 : ((int, int), ()); let left = rows | map((row, ()) -> (row, ()));
             let merged = rows + left; export \"out\" = merged | map(((a, _), ()) -> (a, ()));",
            "let a = input 0 : ((int, int), ()) | map(((k, x), ()) -> (k, x)); let b = input 1 : ((int, int, int), ()) | map(((k, y, z), ()) -> (k, (y, z)));
             export \"out\" = a | lift | half_join(b, 0, (k, x, yz) -> (k, x)) | count;",
            "let x = input 0 : (int, (List(int), List(int))) | map((k, v) -> (k, v)); export \"out\" = x | flatmap((k, v) -> v.0) | count;",
        ] {
            let config = crate::passes::Config::all();
            let first = crate::plan::report(text, &|_| None, &config).unwrap();
            assert!(first.stages.iter().all(|s| s.problems.is_empty()), "{text}");
            let printed = first.emit_ddp().unwrap();
            assert_eq!(printed.lambdas.graph_changed, 0, "{text}");
            let second = crate::plan::report(&printed.text, &|_| None, &config).unwrap();
            assert!(first.compare_final(&second).is_empty(), "{text}\n{}", printed.text);
            assert_eq!(printed.text, second.emit_ddp().unwrap().text, "{text}");
            let mut again = first.stages.last().unwrap().program.clone();
            assert_eq!(narrow(&mut again, &|_| None).unwrap().fields, 0, "{text}\n{}", printed.text);
        }
    }

    #[test]
    fn source_editing_handles_lets_strings_and_nesting() {
        assert_eq!(split_lets("let a = (x, y) add in let b = let c = 1 in c in (a, b)"), ("let a = (x, y) add in let b = let c = 1 in c in ", "(a, b)"));
        assert_eq!(split_lets("(a, b)"), ("", "(a, b)"));
        assert_eq!(prune_lets("let a = x in let (b, c) = y in let d = b in ", "(a, ())"), "let a = x in (a, ())");
        assert_eq!(prune_lets("let a = x in let b = a in ", "(b, ())"), "let a = x in let b = a in (b, ())");
        assert_eq!(tuple_fields("(a, (b, c), \"x,y\" len)"), Some(vec!["a", " (b, c)", " \"x,y\" len"]));
        assert_eq!(tuple_fields("(a) b"), None);
        assert_eq!(apply_mask("(k, (a, b))", &Mask::Prod(vec![Mask::Keep, Mask::Prod(vec![Mask::Unit, Mask::Keep])])), Some("(k, ((), b))".into()));
        assert_eq!(apply_mask("(k, v)", &Mask::Prod(vec![Mask::Keep, Mask::Prod(vec![Mask::Unit, Mask::Keep])])), Some("(k, ((), v.1))".into()));
        assert_eq!(apply_mask("(k, v add)", &Mask::Prod(vec![Mask::Keep, Mask::Prod(vec![Mask::Unit, Mask::Keep])])), None);
        assert_eq!(retire_unused("(id, (a_1, b))", "(b, 1) add"), "(_, (_, b))");
    }
}
