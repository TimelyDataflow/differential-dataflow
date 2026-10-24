//! Field demand over the scope IR, computed in Rust so that an installed program can be
//! rewritten without starting a second dataflow inside `Server::install`.
//!
//! Lambda input demand comes from Corgi's typed demand transform. Unknown or
//! unsupported lambda shapes conservatively read their entire input.

use std::collections::VecDeque;

use corgi::Shape;

use crate::ir::{Lambda, LinearOp, Reducer};
use crate::scope_ir::{self as st, Item, Node, Ref, Source};
use crate::shapes::{self, CollShape, ItemShapes, ScopeShapes};

#[derive(Clone, Debug)]
struct Site {
    shape: (Shape, Shape),
    presence: usize,
    key: Vec<usize>,
    val: Vec<usize>,
}

impl Site {
    fn all(&self) -> Vec<usize> {
        std::iter::once(self.presence)
            .chain(self.key.iter().copied())
            .chain(self.val.iter().copied())
            .collect()
    }

    fn fields(&self, side: usize) -> &[usize] {
        if side == 0 {
            &self.key
        } else {
            &self.val
        }
    }

    fn field_paths(&self) -> Vec<(usize, Vec<usize>)> {
        let mut fields = Vec::new();
        for (side, shape) in [&self.shape.0, &self.shape.1].into_iter().enumerate() {
            for (index, &id) in self.fields(side).iter().enumerate() {
                let path = match shape {
                    Shape::Prod(_) => vec![side, index],
                    _ => vec![side],
                };
                fields.push((id, path));
            }
        }
        fields
    }
}

fn width(shape: &Shape) -> usize {
    match shape {
        Shape::Unit => 0,
        Shape::Prod(fields) => fields.len(),
        _ => 1,
    }
}

#[derive(Debug)]
enum PlannedItem {
    Op { output: Site, stages: Vec<Site> },
    Sub(Box<PlannedScope>),
}

#[derive(Debug)]
struct PlannedScope {
    imports: Vec<Site>,
    vars: Vec<Site>,
    items: Vec<PlannedItem>,
    exports: Vec<Site>,
}

impl PlannedScope {
    fn at(&self, r: &Ref) -> Result<&Site, String> {
        match r {
            Ref::Import(i) => self.imports.get(*i),
            Ref::Var(i) => self.vars.get(*i),
            Ref::Local(i) => match self.items.get(*i) {
                Some(PlannedItem::Op { output, .. }) => Some(output),
                _ => None,
            },
            Ref::ChildExport(i, e) => match self.items.get(*i) {
                Some(PlannedItem::Sub(scope)) => scope.exports.get(*e),
                _ => None,
            },
        }
        .ok_or_else(|| format!("projection: invalid reference {r:?}"))
    }
}

/// One row in `demand.ddp` is a field or a collection's presence. An edge
/// `(consumer, producer)` says that demanding the first fact demands the second.
#[derive(Clone, Debug)]
pub struct DemandGraph {
    pub seeds: Vec<usize>,
    pub edges: Vec<(usize, usize)>,
    pub facts: usize,
}

impl DemandGraph {
    /// The in-process mirror of `demand.ddp`, for opt-in server installs.
    pub fn closure(&self) -> Vec<bool> {
        let mut next = vec![Vec::new(); self.facts];
        for &(from, to) in &self.edges {
            next[from].push(to);
        }
        let mut live = vec![false; self.facts];
        let mut queue = VecDeque::new();
        for &id in &self.seeds {
            if !live[id] {
                live[id] = true;
                queue.push_back(id);
            }
        }
        while let Some(id) = queue.pop_front() {
            for &target in &next[id] {
                if !live[target] {
                    live[target] = true;
                    queue.push_back(target);
                }
            }
        }
        live
    }
}

#[derive(Default)]
struct Builder {
    graph: DemandGraph,
}

impl Default for DemandGraph {
    fn default() -> Self {
        Self {
            seeds: Vec::new(),
            edges: Vec::new(),
            facts: 0,
        }
    }
}

impl Builder {
    fn fact(&mut self) -> usize {
        let id = self.graph.facts;
        self.graph.facts += 1;
        id
    }

    fn site(&mut self, key: Shape, val: Shape) -> Site {
        let presence = self.fact();
        let key_ids = (0..width(&key)).map(|_| self.fact()).collect();
        let val_ids = (0..width(&val)).map(|_| self.fact()).collect();
        Site {
            shape: (key, val),
            presence,
            key: key_ids,
            val: val_ids,
        }
    }

    fn shape_site(&mut self, shape: &CollShape) -> Site {
        self.site(shape.key.clone(), shape.val.clone())
    }

    fn edge(&mut self, from: usize, to: usize) {
        self.graph.edges.push((from, to));
    }

    fn require(&mut self, from: usize, site: &Site) {
        for to in site.all() {
            self.edge(from, to);
        }
    }

    fn presence(&mut self, from: &Site, input: &Site) {
        for id in from.all() {
            self.edge(id, input.presence);
        }
    }

    fn identity(&mut self, from: &Site, input: &Site) {
        self.presence(from, input);
        for side in 0..2 {
            let (out, src) = (from.fields(side), input.fields(side));
            if out.len() == src.len() {
                for (&o, &i) in out.iter().zip(src) {
                    self.edge(o, i);
                }
            } else {
                for &o in out {
                    self.require(o, input);
                }
            }
        }
    }

    fn key_identity(&mut self, from: &Site, input: &Site) {
        self.presence(from, input);
        for (&out, &src) in from.key.iter().zip(&input.key) {
            self.edge(out, src);
        }
    }

    fn lift_iter(&mut self, from: &Site, input: &Site) {
        self.key_identity(from, input);
        for (&out, &src) in from.val.iter().zip(&input.val) {
            self.edge(out, src);
        }
    }

    fn lift(&mut self, from: &Site, input: &Site) {
        self.key_identity(from, input);
        if let Some(&value) = from.val.first() {
            for &field in &input.val {
                self.edge(value, field);
            }
        }
    }

    fn opaque(&mut self, from: &Site, inputs: &[&Site]) {
        for id in from.all() {
            for input in inputs {
                self.require(id, input);
            }
        }
    }

    fn lambda(&mut self, from: &Site, inputs: &[&Site], lambda: &Lambda) {
        for (output_id, path) in from.field_paths() {
            for (slot, field) in lambda_fields(lambda, inputs, &path) {
                if let Some(&id) = slot_fields(inputs, slot).and_then(|fields| fields.get(field)) {
                    self.edge(output_id, id);
                }
            }
        }
    }

    fn scalar_lambda(&mut self, from: &Site, inputs: &[&Site], lambda: &Lambda) {
        for (slot, field) in lambda_fields(lambda, inputs, &[]) {
            if let Some(&id) = slot_fields(inputs, slot).and_then(|fields| fields.get(field)) {
                for source_id in from.all() {
                    self.edge(source_id, id);
                }
            }
        }
    }

    fn half_join(
        &mut self,
        from: &Site,
        stream: &Site,
        arrangement: &Site,
        lambda: &Lambda,
    ) -> Result<(), String> {
        self.presence(from, stream);
        self.presence(from, arrangement);
        // Keys decide matching, and the lifted stream time decides whether a
        // match is admissible and supplies the output's data time.
        for id in from.all() {
            for &key in stream.key.iter().chain(&arrangement.key) {
                self.edge(id, key);
            }
            self.edge(
                id,
                *stream
                    .val
                    .get(1)
                    .ok_or("projection: half_join has no time field")?,
            );
        }
        let Shape::Prod(stream_parts) = &stream.shape.1 else {
            return Err("projection: half_join stream value is not (value, time)".into());
        };
        let stream_value = stream_parts
            .first()
            .ok_or("projection: half_join has no value field")?;
        let shapes = [
            stream.shape.0.clone(),
            stream_value.clone(),
            arrangement.shape.1.clone(),
        ];
        for (output_id, path) in from.field_paths() {
            if path.as_slice() == [1, 1] {
                continue;
            }
            // The half join appends time after the projected value. Its value
            // is one atomic fact at this top-level field precision.
            let projected = if path.as_slice() == [1, 0] {
                vec![1]
            } else {
                path
            };
            for (slot, field) in graph_fields(lambda, &shapes, &projected) {
                let source = match slot {
                    0 => stream.key.get(field),
                    1 => stream.val.first(),
                    2 => arrangement.val.get(field),
                    _ => None,
                };
                if let Some(&source) = source {
                    self.edge(output_id, source);
                }
            }
        }
        Ok(())
    }

    fn count(&mut self, from: &Site, stream: &Site, arrangement: &Site, lambda: &Lambda) {
        self.presence(from, stream);
        self.presence(from, arrangement);
        // Keys decide which arranged updates are counted, and whether the row survives at all.
        for id in from.all() {
            for &key in stream.key.iter().chain(&arrangement.key) {
                self.edge(id, key);
            }
        }
        // The lambda's third slot is the count, an `Int` no field supplies.
        let shapes = [stream.shape.0.clone(), stream.shape.1.clone(), Shape::Int];
        for (output_id, path) in from.field_paths() {
            for (slot, field) in graph_fields(lambda, &shapes, &path) {
                let source = match slot {
                    0 => stream.key.get(field),
                    1 => stream.val.get(field),
                    _ => None,
                };
                if let Some(&source) = source {
                    self.edge(output_id, source);
                }
            }
        }
    }
}

fn slot_fields<'a>(inputs: &'a [&Site], slot: usize) -> Option<&'a [usize]> {
    match (inputs, slot) {
        ([site], 0) => Some(&site.key),
        ([site], 1) => Some(&site.val),
        ([left, _], 0) => Some(&left.key),
        ([left, _], 1) => Some(&left.val),
        ([_, right], 2) => Some(&right.val),
        _ => None,
    }
}

fn all_lambda_fields(shapes: &[Shape]) -> Vec<(usize, usize)> {
    shapes
        .iter()
        .enumerate()
        .flat_map(|(slot, shape)| (0..width(shape)).map(move |field| (slot, field)))
        .collect()
}

fn output_projection(shape: &Shape, path: &[usize]) -> Option<Shape> {
    let Some((&field, tail)) = path.split_first() else {
        return Some(shape.clone());
    };
    let Shape::Prod(parts) = shape else {
        return None;
    };
    let chosen = output_projection(parts.get(field)?, tail)?;
    Some(Shape::Prod(
        parts
            .iter()
            .enumerate()
            .map(|(i, _)| {
                if i == field {
                    chosen.clone()
                } else {
                    Shape::Unit
                }
            })
            .collect(),
    ))
}

/// Input slot and top-level field index for one demanded output path. If Corgi
/// cannot prove the demand, every input field is treated as read.
fn graph_fields(lambda: &Lambda, shapes: &[Shape], output_path: &[usize]) -> Vec<(usize, usize)> {
    let all = || all_lambda_fields(shapes);
    let Ok(graph) = lambda.graph() else {
        return all();
    };
    let input_shape = Shape::Prod(shapes.to_vec());
    let Ok(output_shape) = corgi::shape_of(graph, &input_shape) else {
        return all();
    };
    let Some(projection) = output_projection(&output_shape, output_path) else {
        return all();
    };
    // demand_transform prunes the unobserved outputs and calls Corgi's
    // demand_projection on the result.
    let Ok(transform) = corgi::demand_transform(graph, &input_shape, &projection) else {
        return all();
    };
    let Ok(projected_input) = transform.input_demand.to_shape(&input_shape) else {
        return all();
    };
    let Shape::Prod(slots) = projected_input else {
        return if projected_input == Shape::Unit {
            Vec::new()
        } else {
            all()
        };
    };
    if slots.len() != shapes.len() {
        return all();
    }
    let mut fields = Vec::new();
    for (slot, (original, projected_slot)) in shapes.iter().zip(&slots).enumerate() {
        match (original, projected_slot) {
            (Shape::Unit, Shape::Unit) | (_, Shape::Unit) => {}
            (Shape::Prod(parts), Shape::Prod(kept)) if parts.len() == kept.len() => {
                fields.extend(
                    kept.iter().enumerate().filter_map(|(field, shape)| {
                        (shape != &Shape::Unit).then_some((slot, field))
                    }),
                );
            }
            (Shape::Prod(_), _) => return all(),
            _ => fields.push((slot, 0)),
        }
    }
    fields
}

fn lambda_fields(lambda: &Lambda, inputs: &[&Site], output_path: &[usize]) -> Vec<(usize, usize)> {
    // A linear lambda has two row slots from one site; a join lambda has a
    // shared key and one value slot from each of two sites.
    let shapes = match inputs {
        [site] => vec![site.shape.0.clone(), site.shape.1.clone()],
        [left, right] => vec![
            left.shape.0.clone(),
            left.shape.1.clone(),
            right.shape.1.clone(),
        ],
        _ => return Vec::new(),
    };
    graph_fields(lambda, &shapes, output_path)
}

fn known(shape: &Option<CollShape>, where_: &str) -> Result<CollShape, String> {
    shape
        .clone()
        .ok_or_else(|| format!("projection: no shape for {where_}"))
}

fn build_scope(
    builder: &mut Builder,
    scope: &st::Scope,
    shapes: &ScopeShapes,
    parent: Option<&PlannedScope>,
    depth: usize,
) -> Result<PlannedScope, String> {
    let imports = shapes
        .imports
        .iter()
        .enumerate()
        .map(|(i, shape)| {
            known(shape, &format!("import {i}")).map(|shape| builder.shape_site(&shape))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let vars = shapes
        .vars
        .iter()
        .enumerate()
        .map(|(i, shape)| known(shape, &format!("var {i}")).map(|shape| builder.shape_site(&shape)))
        .collect::<Result<Vec<_>, _>>()?;
    let mut plan = PlannedScope {
        imports,
        vars,
        items: Vec::new(),
        exports: Vec::new(),
    };

    for (import, site) in scope.imports.iter().zip(&plan.imports) {
        if let Source::Parent(r) = &import.from {
            let source = parent
                .ok_or("projection: a root import names its parent")?
                .at(r)?;
            builder.identity(site, source);
            if import.shape.is_some() {
                builder.graph.seeds.extend(site.all());
            }
        }
    }
    for (var, site) in scope.vars.iter().zip(&plan.vars) {
        if var.shape.is_some() {
            builder.graph.seeds.extend(site.all());
        }
    }

    for (i, item) in scope.items.iter().enumerate() {
        match (item, &shapes.items[i]) {
            (Item::Op(node), ItemShapes::Op(shape)) => {
                let shape = known(shape, &format!("item {i}"))?;
                let output = builder.shape_site(&shape);
                let mut stages = Vec::new();
                match node {
                    Node::Linear { input, ops } => {
                        let mut before = plan.at(input)?.clone();
                        let steps = shapes::linear_shapes(
                            ops,
                            before.shape.0.clone(),
                            before.shape.1.clone(),
                            depth,
                        )?;
                        if ops.is_empty() {
                            builder.identity(&output, &before);
                        }
                        for (j, op) in ops.iter().enumerate() {
                            let (key, val) = steps[j + 1].clone();
                            let after = if j + 1 == ops.len() {
                                output.clone()
                            } else {
                                builder.site(key, val)
                            };
                            match op {
                                LinearOp::Project(lambda) => {
                                    builder.presence(&after, &before);
                                    builder.lambda(&after, &[&before], lambda);
                                }
                                LinearOp::Filter(lambda) | LinearOp::EnterAt(lambda) => {
                                    builder.identity(&after, &before);
                                    builder.scalar_lambda(&after, &[&before], lambda);
                                }
                                LinearOp::Negate => builder.identity(&after, &before),
                                LinearOp::LiftIter => builder.lift_iter(&after, &before),
                                LinearOp::Lift => builder.lift(&after, &before),
                                LinearOp::FlatMap(lambda) => {
                                    builder.key_identity(&after, &before);
                                    // The list determines output presence and positions,
                                    // even when no element payload is demanded.
                                    builder.scalar_lambda(&after, &[&before], lambda);
                                }
                                LinearOp::JoinFunction(lambda) => {
                                    builder.presence(&after, &before);
                                    builder.scalar_lambda(&after, &[&before], lambda);
                                }
                            }
                            stages.push(after.clone());
                            before = after;
                        }
                    }
                    Node::Concat(refs) => {
                        for r in refs {
                            let input = plan.at(r)?;
                            builder.identity(&output, input);
                        }
                    }
                    Node::Arrange(r) => {
                        let input = plan.at(r)?;
                        builder.identity(&output, input);
                        for id in output.all() {
                            for &key in &input.key {
                                builder.edge(id, key);
                            }
                        }
                    }
                    Node::Join {
                        left,
                        right,
                        projection,
                    } => {
                        let (left, right) = (plan.at(left)?, plan.at(right)?);
                        builder.presence(&output, left);
                        builder.presence(&output, right);
                        for id in output.all() {
                            for &key in left.key.iter().chain(&right.key) {
                                builder.edge(id, key);
                            }
                        }
                        builder.lambda(&output, &[left, right], projection);
                    }
                    Node::HalfJoin {
                        stream,
                        arrangement,
                        projection,
                        ..
                    } => {
                        builder.half_join(
                            &output,
                            plan.at(stream)?,
                            plan.at(arrangement)?,
                            projection,
                        )?;
                    }
                    Node::Count {
                        stream,
                        arrangement,
                        projection,
                    } => {
                        builder.count(
                            &output,
                            plan.at(stream)?,
                            plan.at(arrangement)?,
                            projection,
                        );
                    }
                    Node::Reduce { input, reducer } => {
                        let input = plan.at(input)?;
                        match reducer {
                            Reducer::Count => {
                                builder.presence(&output, input);
                                for id in output.all() {
                                    for &key in &input.key {
                                        builder.edge(id, key);
                                    }
                                }
                            }
                            Reducer::Distinct
                            | Reducer::Min
                            | Reducer::Collect
                            | Reducer::Expr(_) => {
                                builder.opaque(&output, &[input]);
                            }
                        }
                    }
                    Node::Inspect { input, .. } => {
                        // Inspection is observable even if its collection has
                        // no downstream consumer.
                        builder.graph.seeds.extend(output.all());
                        builder.opaque(&output, &[plan.at(input)?]);
                    }
                }
                plan.items.push(PlannedItem::Op { output, stages });
            }
            (Item::Sub(child), ItemShapes::Sub(child_shapes)) => {
                let child = build_scope(builder, child, child_shapes, Some(&plan), depth + 1)?;
                plan.items.push(PlannedItem::Sub(Box::new(child)));
            }
            _ => {
                return Err(format!(
                    "projection: item {i} disagrees with shape inference"
                ))
            }
        }
    }

    for bind in &scope.binds {
        let var = plan.vars[bind.var].clone();
        let source = plan.at(&bind.value)?.clone();
        builder.identity(&var, &source);
    }
    for (i, export) in scope.exports.iter().enumerate() {
        let shape = known(&shapes.exports[i], &format!("export {i}"))?;
        let site = builder.shape_site(&shape);
        let source = plan.at(&export.value)?.clone();
        builder.identity(&site, &source);
        if export.shape.is_some() {
            builder.graph.seeds.extend(site.all());
        }
        plan.exports.push(site);
    }
    Ok(plan)
}

fn mask_part(name: &str, shape: &Shape, facts: &[usize], live: &[bool]) -> String {
    match shape {
        Shape::Unit => "()".into(),
        Shape::Prod(fields) => {
            let values = fields
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    if live[facts[i]] {
                        format!("{name}.{i}")
                    } else {
                        "()".into()
                    }
                })
                .collect::<Vec<_>>();
            format!("({})", values.join(", "))
        }
        _ => {
            if live[facts[0]] {
                name.into()
            } else {
                "()".into()
            }
        }
    }
}

fn mask_lambda(
    original: &Lambda,
    key_shape: &Shape,
    key_facts: &[usize],
    value_shape: Option<&Shape>,
    value_facts: &[usize],
    live: &[bool],
) -> Option<Lambda> {
    // A half join appends time after its projection. Its projected value is
    // one demand fact even when the value itself is a tuple.
    if width(key_shape) != key_facts.len()
        || match value_shape {
            Some(shape) => width(shape) != value_facts.len(),
            None => value_facts.len() != 1,
        }
    {
        return None;
    }
    if key_facts.iter().chain(value_facts).all(|&id| live[id]) {
        return None;
    }
    // Select live fields directly from the call result. Corgi can fold those
    // field selections through the tuple and eliminate dead work. The parsed
    // source and the running graph then take the same optimization path.
    Some(Lambda::compose(
        "arg",
        &[(original, "arg".into())],
        |result| {
            let key = mask_part(&format!("{}.0", result[0]), key_shape, key_facts, live);
            let val = match value_shape {
                Some(shape) => mask_part(&format!("{}.1", result[0]), shape, value_facts, live),
                None if live[value_facts[0]] => format!("{}.1", result[0]),
                None => "()".into(),
            };
            format!("({key}, {val})")
        },
    ))
}

fn mask_site_lambda(original: &Lambda, output: &Site, live: &[bool]) -> Option<Lambda> {
    mask_lambda(
        original,
        &output.shape.0,
        &output.key,
        Some(&output.shape.1),
        &output.val,
        live,
    )
}

fn mask_half_join(original: &Lambda, output: &Site, live: &[bool]) -> Option<Lambda> {
    // The half-join operator appends time after this lambda has produced
    // (key, value), so only the first output value fact belongs to the lambda.
    let Shape::Prod(parts) = &output.shape.1 else {
        return None;
    };
    if parts.len() != 2 {
        return None;
    }
    let Some(&value_fact) = output.val.first() else {
        return None;
    };
    mask_lambda(
        original,
        &output.shape.0,
        &output.key,
        None,
        &[value_fact],
        live,
    )
}

fn rewrite_scope(scope: &mut st::Scope, plan: &PlannedScope, live: &[bool]) -> usize {
    let mut masked = 0;
    let mut remap = Vec::with_capacity(scope.items.len());
    let mut items = Vec::with_capacity(scope.items.len());
    let identity = Lambda::new("input".into(), &corgi::Scope::default());
    for (mut item, planned) in std::mem::take(&mut scope.items)
        .into_iter()
        .zip(&plan.items)
    {
        match (&mut item, planned) {
            (Item::Op(Node::Linear { ops, .. }), PlannedItem::Op { stages, .. }) => {
                for (op, site) in ops.iter_mut().zip(stages) {
                    if let LinearOp::Project(lambda) = op {
                        if let Some(new) = mask_site_lambda(lambda, site, live) {
                            masked += site
                                .key
                                .iter()
                                .chain(&site.val)
                                .filter(|&&id| !live[id])
                                .count();
                            *lambda = new;
                        }
                    }
                }
            }
            (Item::Op(Node::Join { projection, .. }), PlannedItem::Op { output, .. }) => {
                if let Some(new) = mask_site_lambda(projection, output, live) {
                    masked += output
                        .key
                        .iter()
                        .chain(&output.val)
                        .filter(|&&id| !live[id])
                        .count();
                    *projection = new;
                }
            }
            (Item::Op(Node::Count { projection, .. }), PlannedItem::Op { output, .. }) => {
                if let Some(new) = mask_site_lambda(projection, output, live) {
                    masked += output
                        .key
                        .iter()
                        .chain(&output.val)
                        .filter(|&&id| !live[id])
                        .count();
                    *projection = new;
                }
            }
            (Item::Op(Node::HalfJoin { projection, .. }), PlannedItem::Op { output, .. }) => {
                if let Some(new) = mask_half_join(projection, output, live) {
                    masked += output.key.iter().filter(|&&id| !live[id]).count();
                    masked += usize::from(!live[output.val[0]]);
                    *projection = new;
                }
            }
            (Item::Sub(child), PlannedItem::Sub(plan)) => {
                masked += rewrite_scope(child, plan, live)
            }
            _ => {}
        }
        match &mut item {
            Item::Op(node) => match node {
                Node::Linear { input, .. }
                | Node::Arrange(input)
                | Node::Reduce { input, .. }
                | Node::Inspect { input, .. } => remap_ref(input, &remap),
                Node::Join { left, right, .. } => {
                    remap_ref(left, &remap);
                    remap_ref(right, &remap);
                }
                Node::HalfJoin {
                    stream,
                    arrangement,
                    ..
                }
                | Node::Count {
                    stream,
                    arrangement,
                    ..
                } => {
                    remap_ref(stream, &remap);
                    remap_ref(arrangement, &remap);
                }
                Node::Concat(refs) => {
                    for r in refs.iter_mut() {
                        remap_ref(r, &remap);
                    }
                    if let PlannedItem::Op { output, .. } = planned {
                        if let Some(lambda) = mask_site_lambda(&identity, output, live) {
                            let fields = output
                                .key
                                .iter()
                                .chain(&output.val)
                                .filter(|&&id| !live[id])
                                .count();
                            for r in refs.iter_mut() {
                                let project = Item::Op(Node::Linear {
                                    input: r.clone(),
                                    ops: vec![LinearOp::Project(lambda.clone())],
                                });
                                *r = Ref::Local(items.len());
                                items.push(project);
                                masked += fields;
                            }
                        }
                    }
                }
            },
            Item::Sub(child) => {
                for import in &mut child.imports {
                    if let Source::Parent(r) = &mut import.from {
                        remap_ref(r, &remap);
                    }
                }
            }
        }
        // Distinct, min, collect, and user reductions must see their full input.
        // Mask only the rows they emit, before any downstream consumer sees them.
        let reduced_output = match (&item, planned) {
            (Item::Op(Node::Reduce { .. }), PlannedItem::Op { output, .. }) => Some(output),
            _ => None,
        };
        items.push(item);
        if let Some(output) = reduced_output {
            if let Some(lambda) = mask_site_lambda(&identity, output, live) {
                let fields = output
                    .key
                    .iter()
                    .chain(&output.val)
                    .filter(|&&id| !live[id])
                    .count();
                let project = Item::Op(Node::Linear {
                    input: Ref::Local(items.len() - 1),
                    ops: vec![LinearOp::Project(lambda)],
                });
                remap.push(items.len());
                items.push(project);
                masked += fields;
                continue;
            }
        }
        remap.push(items.len() - 1);
    }
    for bind in &mut scope.binds {
        remap_ref(&mut bind.value, &remap);
    }
    for export in &mut scope.exports {
        remap_ref(&mut export.value, &remap);
    }
    scope.items = items;
    masked
}

fn remap_ref(r: &mut Ref, remap: &[usize]) {
    match r {
        Ref::Local(i) | Ref::ChildExport(i, _) => {
            *i = remap[*i];
        }
        Ref::Import(_) | Ref::Var(_) => {}
    }
}

/// Build the backward demand graph for a typed program. `source` supplies any
/// root import shape absent from the program text, as for `shapes::infer`.
pub fn demand_graph(
    program: &st::Program,
    source: &dyn Fn(&Source) -> Option<st::RowShape>,
) -> Result<DemandGraph, String> {
    let (shapes, problems) = shapes::infer(program, source);
    if !problems.conflicts.is_empty() {
        return Err(problems.conflicts.join("; "));
    }
    if !problems.unknown.is_empty() {
        return Err(problems.unknown.join("; "));
    }
    let mut builder = Builder::default();
    let plan = build_scope(&mut builder, &program.root, &shapes, None, 0)?;
    for export in &plan.exports {
        builder.graph.seeds.extend(export.all());
    }
    Ok(builder.graph)
}

/// Replace dead tuple fields in producer lambdas by unit. Returns the number of
/// replaced top-level fields. On a shape disagreement the original is restored.
pub fn pushdown(
    program: &mut st::Program,
    source: &dyn Fn(&Source) -> Option<st::RowShape>,
) -> Result<usize, String> {
    let original = program.clone();
    let (shapes, problems) = shapes::infer(program, source);
    if !problems.conflicts.is_empty() || !problems.unknown.is_empty() {
        return Ok(0);
    }
    let mut builder = Builder::default();
    let Ok(plan) = build_scope(&mut builder, &program.root, &shapes, None, 0) else {
        return Ok(0);
    };
    for export in &plan.exports {
        builder.graph.seeds.extend(export.all());
    }
    let live = builder.graph.closure();
    let masked = rewrite_scope(&mut program.root, &plan, &live);
    let (_, check) = shapes::infer(program, source);
    if !check.conflicts.is_empty() || !check.unknown.is_empty() {
        *program = original;
        // A rewritten intermediate shape disagrees with a consumer. The pass
        // is optional, so keep the original program in that case.
        return Ok(0);
    }
    Ok(masked)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{lower, parse};

    #[test]
    fn graph_reads_selected_tuple_fields() {
        let lambda = Lambda::new(
            "let (row, ()) = input in ((row.0, (row.1, 1) add), ())".into(),
            &corgi::Scope::default(),
        );
        let shapes = [Shape::Prod(vec![Shape::Int, Shape::Int]), Shape::Unit];
        assert_eq!(graph_fields(&lambda, &shapes, &[0, 0]), vec![(0, 0)]);
        assert_eq!(graph_fields(&lambda, &shapes, &[0, 1]), vec![(0, 1)]);
        assert!(graph_fields(&lambda, &shapes, &[1]).is_empty());
    }

    #[test]
    fn corgi_removes_work_feeding_a_masked_field() {
        let lambda = Lambda::new(
            "let (row, ()) = input in ((row.0, (row.1, 1) add), ())".into(),
            &corgi::Scope::default(),
        );
        let mut builder = Builder::default();
        let output = builder.site(Shape::Prod(vec![Shape::Int, Shape::Int]), Shape::Unit);
        let mut live = vec![false; builder.graph.facts];
        live[output.key[0]] = true;
        let masked = mask_site_lambda(&lambda, &output, &live).unwrap();
        let before = corgi::explain::explain(&corgi::optimize(&lambda.graph().unwrap()));
        let after = corgi::explain::explain(&corgi::optimize(&masked.graph().unwrap()));
        assert!(before.contains("BinImm(Add"));
        assert!(!after.contains("BinImm(Add"));
        // Reparse the serializable text, then check that its graph loses
        // exactly the same dead computation as the graph used immediately.
        let reparsed = Lambda::new(masked.src.clone(), &corgi::Scope::default());
        let reparsed_after = corgi::explain::explain(&corgi::optimize(reparsed.graph().unwrap()));
        assert!(!reparsed_after.contains("BinImm(Add"));
    }

    #[test]
    fn count_can_drop_values_but_distinct_keeps_them() {
        let source = "let x = input 0 : (int, (int, int)) | map((k, v) -> (k, v));\n";
        let count = format!("{source}export \"out\" = x | count;\n");
        let distinct = format!("{source}export \"out\" = x | distinct;\n");
        let mut count = lower::lower_tree(parse::pipe::parse(&count));
        let mut distinct = lower::lower_tree(parse::pipe::parse(&distinct));
        assert_eq!(pushdown(&mut count, &|_| None).unwrap(), 2);
        assert_eq!(pushdown(&mut distinct, &|_| None).unwrap(), 0);
    }

    #[test]
    fn an_unexported_inspect_still_reads_its_input() {
        let text = "let x = input 0 : (int, (int, int)) | map((k, v) -> (k, v));
                    let seen = x | inspect(seen);
                    export \"out\" = x | count;";
        let mut program = lower::lower_tree(parse::pipe::parse(text));
        assert_eq!(pushdown(&mut program, &|_| None).unwrap(), 0);
    }

    #[test]
    fn count_keeps_its_grouping_key() {
        let text = "let x = input 0 : ((int, int), ()) | map((k, v) -> (k, v));
                    export \"out\" = x | count | map(((a, _), n) -> ((a), n));";
        let mut program = lower::lower_tree(parse::pipe::parse(text));
        // Count still groups by both key fields; the second is masked only
        // after the reduction, where the next map no longer reads it.
        assert_eq!(pushdown(&mut program, &|_| None).unwrap(), 1);
    }

    #[test]
    fn distinct_projects_only_after_deduplicating_full_rows() {
        use crate::server::Server;
        let text = "let x = input 0 : ((int, int), ())
                    | map((row, ()) -> (row, ()));
                    export \"out\" = x | distinct
                    | map((row, ()) -> (row.0, ())) | count;";
        let mut original = lower::lower_tree(parse::pipe::parse(text));
        original.optimize();
        let mut projected = original.clone();
        assert!(pushdown(&mut projected, &|_| None).unwrap() >= 1);
        original.root.exports[0].name = "before.out".into();
        projected.root.exports[0].name = "after.out".into();
        timely::execute_directly(move |worker| {
            let mut server = Server::new();
            server.install(worker, "before", &original).unwrap();
            server.install(worker, "after", &projected).unwrap();
            for name in ["before", "after"] {
                server.feed(name, 0, "1,10", None).unwrap();
                server.feed(name, 0, "1,20", None).unwrap();
            }
            server.tick(worker);
            assert_eq!(
                server.snapshot(worker, "before.out").unwrap().rows(),
                server.snapshot(worker, "after.out").unwrap().rows()
            );
        });
    }

    #[test]
    fn concat_projects_both_arms_of_a_feedback_loop() {
        let text = "let source = input 0 : ((int, int), ());
                    let initial = source | map((row, ()) -> ((row.0, row.1), ()));
                    toy: {
                        var state = (initial) + (state | filter((row, ()) -> 0));
                    }
                    export \"out\" = toy::state | map((row, ()) -> ((row.0), ())) | count;";
        let mut program = lower::lower_tree(parse::pipe::parse(text));
        program.optimize();
        assert!(pushdown(&mut program, &|_| None).unwrap() >= 3);
        let (_, check) = shapes::infer(&program, &|_| None);
        assert!(check.conflicts.is_empty(), "{:?}", check.conflicts);
        assert!(check.unknown.is_empty(), "{:?}", check.unknown);
    }

    #[test]
    fn flatmap_reads_only_the_list_it_explodes() {
        let text = "let x = input 0 : (int, (List(int), List(int)))
                    | map((k, v) -> (k, v));
                    export \"out\" = x | flatmap((k, v) -> v.0) | count;";
        let mut program = lower::lower_tree(parse::pipe::parse(text));
        assert_eq!(pushdown(&mut program, &|_| None).unwrap(), 1);
    }

    #[test]
    fn lift_does_not_read_an_unused_value() {
        let text = "let x = input 0 : (int, (int, int))
                    | map((k, v) -> (k, v));
                    export \"out\" = x | lift | map((k, (v, time)) -> (k, time));";
        let mut program = lower::lower_tree(parse::pipe::parse(text));
        assert_eq!(pushdown(&mut program, &|_| None).unwrap(), 2);
    }

    #[test]
    fn half_join_reads_keys_and_time_without_unused_payloads() {
        let text = "let a = input 0 : ((int, int), ())
                    | map(((k, x), ()) -> (k, x));
                    let b = input 1 : ((int, int, int), ())
                    | map(((k, y, z), ()) -> (k, (y, z)));
                    export \"out\" = a | lift
                    | half_join(b, 0, (k, x, yz) -> (k, x)) | count;";
        let mut program = lower::lower_tree(parse::pipe::parse(text));
        program.optimize();
        assert!(pushdown(&mut program, &|_| None).unwrap() >= 4);
        let (_, check) = shapes::infer(&program, &|_| None);
        assert!(check.conflicts.is_empty(), "{:?}", check.conflicts);
    }

    #[test]
    fn masked_half_join_matches_the_original_rows() {
        use crate::server::Server;
        let text = "let a = input 0 : ((int, int), ())
                    | map(((k, x), ()) -> (k, x));
                    let b = input 1 : ((int, int, int), ())
                    | map(((k, y, z), ()) -> (k, (y, z)));
                    export \"out\" = a | lift
                    | half_join(b, 0, (k, x, yz) -> (k, x)) | count;";
        let mut original = lower::lower_tree(parse::pipe::parse(text));
        original.optimize();
        let mut projected = original.clone();
        assert!(pushdown(&mut projected, &|_| None).unwrap() >= 4);
        original.root.exports[0].name = "before.out".into();
        projected.root.exports[0].name = "after.out".into();
        timely::execute_directly(move |worker| {
            let mut server = Server::new();
            server.install(worker, "before", &original).unwrap();
            server.install(worker, "after", &projected).unwrap();
            for name in ["before", "after"] {
                server.feed(name, 0, "1,10", None).unwrap();
                server.feed(name, 1, "1,20,30", None).unwrap();
            }
            server.tick(worker);
            assert_eq!(
                server.snapshot(worker, "before.out").unwrap().rows(),
                server.snapshot(worker, "after.out").unwrap().rows()
            );
        });
    }
}
