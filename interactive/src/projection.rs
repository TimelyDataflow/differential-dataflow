//! Field demand over the scope IR. The closure itself is also written in
//! `examples/self_opt/demand.ddp`; this Rust form lets an installed program be
//! rewritten without starting a second dataflow inside `Server::install`.
//!
//! The lambda dependency probe reads `corgi::explain` conservatively. It is a
//! spike interface, not a stable corgi contract: unknown graph operations read
//! their entire input. A structured graph dependency API belongs in corgi.

use std::collections::{HashSet, VecDeque};

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
    fn at(&self, r: &Ref) -> &Site {
        match r {
            Ref::Import(i) => &self.imports[*i],
            Ref::Var(i) => &self.vars[*i],
            Ref::Local(i) => match &self.items[*i] {
                PlannedItem::Op { output, .. } => output,
                PlannedItem::Sub(_) => panic!("a local reference points at a scope"),
            },
            Ref::ChildExport(i, e) => match &self.items[*i] {
                PlannedItem::Sub(scope) => &scope.exports[*e],
                PlannedItem::Op { .. } => panic!("a child export points at an operator"),
            },
        }
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

#[derive(Clone, Debug)]
enum GraphNode {
    Input,
    Tuple(Vec<usize>),
    Field(usize, usize),
    Pattern(usize),
    Constant,
    Op(Vec<usize>),
}

fn percent_ids(text: &str) -> Vec<usize> {
    text.split(|c: char| !c.is_ascii_digit() && c != '%')
        .filter_map(|word| word.strip_prefix('%').and_then(|s| s.parse().ok()))
        .collect()
}

fn parse_graph(text: &str) -> Option<(Vec<GraphNode>, usize)> {
    let mut nodes = Vec::new();
    let mut output = None;
    for line in text.lines().filter(|line| line.starts_with('%')) {
        let (name, rhs) = line.split_once(" = ")?;
        let id: usize = name.strip_prefix('%')?.parse().ok()?;
        if id != nodes.len() {
            return None;
        }
        let is_output = rhs.contains("<- out");
        let rhs = rhs.split("   <- out").next()?.trim();
        let inputs = percent_ids(rhs);
        let node = if rhs == "input" {
            GraphNode::Input
        } else if rhs.starts_with("(%") || rhs == "()" {
            GraphNode::Tuple(inputs)
        } else if let Some(rest) = rhs.strip_prefix("Field(") {
            let (index, _) = rest.split_once(')')?;
            GraphNode::Field(index.parse().ok()?, *inputs.first()?)
        } else if rhs.starts_with("Pattern(") {
            GraphNode::Pattern(*inputs.first()?)
        } else if rhs.starts_with("Unit ") || rhs.starts_with("lit ") {
            GraphNode::Constant
        } else {
            GraphNode::Op(inputs)
        };
        if is_output {
            output = Some(id);
        }
        nodes.push(node);
    }
    Some((nodes, output?))
}

fn input_paths(nodes: &[GraphNode], output: usize, path: &[usize]) -> Option<Vec<Vec<usize>>> {
    let mut stack = vec![(output, path.to_vec())];
    let mut seen = HashSet::new();
    let mut paths = Vec::new();
    while let Some((id, path)) = stack.pop() {
        if !seen.insert((id, path.clone())) {
            continue;
        }
        match nodes.get(id)? {
            GraphNode::Input => paths.push(path),
            GraphNode::Tuple(parts) => {
                if let Some((&first, rest)) = path.split_first() {
                    stack.push((*parts.get(first)?, rest.to_vec()));
                } else {
                    for &part in parts {
                        stack.push((part, Vec::new()));
                    }
                }
            }
            GraphNode::Field(field, arg) => {
                let mut next = vec![*field];
                next.extend(path);
                stack.push((*arg, next));
            }
            GraphNode::Pattern(arg) => stack.push((*arg, path)),
            GraphNode::Constant => {}
            GraphNode::Op(args) => {
                for &arg in args {
                    stack.push((arg, Vec::new()));
                }
            }
        }
    }
    Some(paths)
}

fn all_lambda_fields(shapes: &[Shape]) -> Vec<(usize, usize)> {
    shapes
        .iter()
        .enumerate()
        .flat_map(|(slot, shape)| (0..width(shape)).map(move |field| (slot, field)))
        .collect()
}

/// Input slot and top-level field index for one demanded output path. A missing
/// graph parse is an all-fields result, never a guessed absence of reads.
fn graph_fields(lambda: &Lambda, shapes: &[Shape], output_path: &[usize]) -> Vec<(usize, usize)> {
    let all = || all_lambda_fields(shapes);
    let Ok(graph) = lambda.graph() else {
        return all();
    };
    let input_shape = Shape::Prod(shapes.to_vec());
    if corgi::shape_of(graph, &input_shape).is_err() {
        return all();
    }
    let graph = corgi::optimize(graph);
    let Some((nodes, output)) = parse_graph(&corgi::explain::explain(&graph)) else {
        return all();
    };
    let Some(paths) = input_paths(&nodes, output, output_path) else {
        return all();
    };
    let mut fields = HashSet::new();
    for path in paths {
        let Some((&slot, tail)) = path.split_first() else {
            return all();
        };
        let Some(shape) = shapes.get(slot) else {
            return all();
        };
        match shape {
            Shape::Unit => {}
            Shape::Prod(parts) => {
                if let Some(&field) = tail.first() {
                    if field >= parts.len() {
                        return all();
                    }
                    fields.insert((slot, field));
                } else {
                    for field in 0..parts.len() {
                        fields.insert((slot, field));
                    }
                }
            }
            _ => {
                fields.insert((slot, 0));
            }
        }
    }
    let mut fields: Vec<_> = fields.into_iter().collect();
    fields.sort_unstable();
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
                .at(r);
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
                        let mut before = plan.at(input).clone();
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
                            let input = plan.at(r);
                            builder.identity(&output, input);
                        }
                    }
                    Node::Arrange(r) => {
                        let input = plan.at(r);
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
                        let (left, right) = (plan.at(left), plan.at(right));
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
                        ..
                    } => {
                        builder.opaque(&output, &[plan.at(stream), plan.at(arrangement)]);
                    }
                    Node::Reduce { input, reducer } => {
                        let input = plan.at(input);
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
                        builder.opaque(&output, &[plan.at(input)]);
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
        let source = plan.at(&bind.value).clone();
        builder.identity(&var, &source);
    }
    for (i, export) in scope.exports.iter().enumerate() {
        let shape = known(&shapes.exports[i], &format!("export {i}"))?;
        let site = builder.shape_site(&shape);
        let source = plan.at(&export.value).clone();
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

fn mask_graph_part(
    builder: &mut corgi::Builder<corgi::NumOp>,
    input: usize,
    source: usize,
    shape: &Shape,
    facts: &[usize],
    live: &[bool],
) -> usize {
    let unit =
        |builder: &mut corgi::Builder<corgi::NumOp>| builder.add(corgi::Op::Unit, vec![input]);
    match shape {
        Shape::Unit => unit(builder),
        Shape::Prod(fields) if fields.is_empty() => unit(builder),
        Shape::Prod(fields) => {
            let children = fields
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    if live[facts[i]] {
                        builder.add(corgi::Op::Field(i), vec![source])
                    } else {
                        unit(builder)
                    }
                })
                .collect();
            builder.tuple(children)
        }
        _ => {
            if live[facts[0]] {
                source
            } else {
                unit(builder)
            }
        }
    }
}

fn mask_lambda(original: &Lambda, output: &Site, live: &[bool]) -> Option<Lambda> {
    if output.key.iter().chain(&output.val).all(|&id| live[id]) {
        return None;
    }
    let mut masked = Lambda::compose("arg", &[(original, "arg".into())], |result| {
        let key = mask_part("k", &output.shape.0, &output.key, live);
        let val = mask_part("v", &output.shape.1, &output.val, live);
        format!("let (k, v) = {} in ({key}, {val})", result[0])
    });
    if let Ok(graph) = original.graph() {
        // Every corgi node preserves top-level row count. Anchor dead fields'
        // Unit columns to the graph input, so graph DCE can discard the work
        // that fed them. Parsing the equivalent source alone anchors Unit to
        // the original result and keeps that dead work reachable.
        let mut builder = corgi::Builder::<corgi::NumOp>::default();
        let input = builder.input();
        let result = builder.graft(graph, input);
        let key = builder.add(corgi::Op::Field(0), vec![result]);
        let val = builder.add(corgi::Op::Field(1), vec![result]);
        let key = mask_graph_part(&mut builder, input, key, &output.shape.0, &output.key, live);
        let val = mask_graph_part(&mut builder, input, val, &output.shape.1, &output.val, live);
        let result = builder.tuple(vec![key, val]);
        // The original was already typed at its original input shape. Remove
        // dead subgraphs now, before a masked feedback variable changes that
        // input shape and shape inference visits this lambda again.
        masked.graph = Ok(std::sync::Arc::new(corgi::optimize(
            &builder.finish(result),
        )));
    }
    Some(masked)
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
                        if let Some(new) = mask_lambda(lambda, site, live) {
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
                if let Some(new) = mask_lambda(projection, output, live) {
                    masked += output
                        .key
                        .iter()
                        .chain(&output.val)
                        .filter(|&&id| !live[id])
                        .count();
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
                } => {
                    remap_ref(stream, &remap);
                    remap_ref(arrangement, &remap);
                }
                Node::Concat(refs) => {
                    for r in refs.iter_mut() {
                        remap_ref(r, &remap);
                    }
                    let PlannedItem::Op { output, .. } = planned else {
                        unreachable!("a concat has an operator plan")
                    };
                    if let Some(lambda) = mask_lambda(&identity, output, live) {
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
            },
            Item::Sub(child) => {
                for import in &mut child.imports {
                    if let Source::Parent(r) = &mut import.from {
                        remap_ref(r, &remap);
                    }
                }
            }
        }
        remap.push(items.len());
        items.push(item);
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
    let live = builder.graph.closure();
    let masked = rewrite_scope(&mut program.root, &plan, &live);
    let (_, check) = shapes::infer(program, source);
    if !check.conflicts.is_empty() || !check.unknown.is_empty() {
        *program = original;
        // Some otherwise valid consumers require the producer's original
        // shape, including explanation plans synthesized after installation.
        // The pass is optional, so keep the original program in that case.
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
        let masked = mask_lambda(&lambda, &output, &live).unwrap();
        let before = corgi::explain::explain(&corgi::optimize(&lambda.graph().unwrap()));
        let after = corgi::explain::explain(&corgi::optimize(&masked.graph().unwrap()));
        assert!(before.contains("BinImm(Add"));
        assert!(!after.contains("BinImm(Add"));
    }

    #[test]
    fn count_can_drop_values_but_distinct_keeps_them() {
        let source = "let x = input 0 : (int ; (int, int)) | map((k, v) -> (k, v));\n";
        let count = format!("{source}export \"out\" = x | count;\n");
        let distinct = format!("{source}export \"out\" = x | distinct;\n");
        let mut count = lower::lower_tree(parse::pipe::parse(&count));
        let mut distinct = lower::lower_tree(parse::pipe::parse(&distinct));
        assert_eq!(pushdown(&mut count, &|_| None).unwrap(), 2);
        assert_eq!(pushdown(&mut distinct, &|_| None).unwrap(), 0);
    }

    #[test]
    fn an_unexported_inspect_still_reads_its_input() {
        let text = "let x = input 0 : (int ; (int, int)) | map((k, v) -> (k, v));
                    let seen = x | inspect(seen);
                    export \"out\" = x | count;";
        let mut program = lower::lower_tree(parse::pipe::parse(text));
        assert_eq!(pushdown(&mut program, &|_| None).unwrap(), 0);
    }

    #[test]
    fn count_keeps_its_grouping_key() {
        let text = "let x = input 0 : ((int, int) ; ()) | map((k, v) -> (k, v));
                    export \"out\" = x | count | map(((a, _), n) -> ((a), n));";
        let mut program = lower::lower_tree(parse::pipe::parse(text));
        assert_eq!(pushdown(&mut program, &|_| None).unwrap(), 0);
    }

    #[test]
    fn concat_projects_both_arms_of_a_feedback_loop() {
        let text = "let source = input 0 : ((int, int) ; ());
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
        let text = "let x = input 0 : (int ; (List(int), List(int)))
                    | map((k, v) -> (k, v));
                    export \"out\" = x | flatmap((k, v) -> v.0) | count;";
        let mut program = lower::lower_tree(parse::pipe::parse(text));
        assert_eq!(pushdown(&mut program, &|_| None).unwrap(), 1);
    }

    #[test]
    fn lift_does_not_read_an_unused_value() {
        let text = "let x = input 0 : (int ; (int, int))
                    | map((k, v) -> (k, v));
                    export \"out\" = x | lift | map((k, (v, time)) -> (k, time));";
        let mut program = lower::lower_tree(parse::pipe::parse(text));
        assert_eq!(pushdown(&mut program, &|_| None).unwrap(), 2);
    }
}
