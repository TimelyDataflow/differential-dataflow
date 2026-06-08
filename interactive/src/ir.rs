//! IR types for DD IR programs.
//!
//! The IR is a flat map of nodes addressed by index.
//! Nodes are symbolic — no closures, no generics.

use std::collections::BTreeMap;

use crate::parse::{Projection, Condition, FieldExpr, Reducer};

pub type Diff = i64;
pub type Id = usize;
pub type Time = timely::order::Product<u64, differential_dataflow::dynamic::pointstamp::PointStamp<u64>>;

/// Minimal interface for a row type used by renderers.
pub trait RowLike: Clone + Ord + std::fmt::Debug + Send + Sync + 'static {
    fn new() -> Self;
    fn push(&mut self, v: i64);
    fn as_slice(&self) -> &[i64];
    fn extend_from_slice(&mut self, other: &[i64]);
}

impl RowLike for Vec<i64> {
    fn new() -> Self { Vec::new() }
    fn push(&mut self, v: i64) { Vec::push(self, v); }
    fn as_slice(&self) -> &[i64] { self }
    fn extend_from_slice(&mut self, other: &[i64]) { Vec::extend_from_slice(self, other); }
}

impl<A> RowLike for smallvec::SmallVec<A>
where
    A: smallvec::Array<Item = i64> + Send + Sync + 'static,
{
    fn new() -> Self { smallvec::SmallVec::new() }
    fn push(&mut self, v: i64) { smallvec::SmallVec::push(self, v); }
    fn as_slice(&self) -> &[i64] { self }
    fn extend_from_slice(&mut self, other: &[i64]) { smallvec::SmallVec::extend_from_slice(self, other); }
}

/// An individual step within a Linear node.
#[derive(Debug, Clone)]
pub enum LinearOp {
    /// Rekey/reval: project to new (key, val).
    Project(Projection),
    /// Keep or discard the record.
    Filter(Condition),
    /// Negate the diff.
    Negate,
    /// Shift the timestamp based on a field value.
    EnterAt(FieldExpr),
    /// Append the current user-iter coord (at the row's scope depth) to
    /// the value as one i64 field. Time itself is unchanged. See
    /// `Expr::LiftIter` for the discipline restriction.
    LiftIter,
}

/// Symbolic IR node.
pub enum Node {
    Input(usize),
    /// A named external trace, resolved against a registry at install time;
    /// shape is inferred from the registry, not the IR.
    ///
    /// STUB: only the server resolves this; the example renderers don't, and no
    /// example program uses it yet. The intended end-state is a single
    /// named-source substrate that also subsumes `Input(usize)` — there should
    /// not be two ways to bring in a source. Until that cutover, `Input` is the
    /// working input and `Import` is forward-looking.
    Import { name: String },
    /// A chain of linear operations on a stream of (data, time, diff) triples.
    Linear { input: Id, ops: Vec<LinearOp> },
    Concat(Vec<Id>),
    Arrange(Id),
    Join { left: Id, right: Id, projection: Projection },
    Reduce { input: Id, reducer: Reducer },
    Variable,
    Inspect { input: Id, label: String },
    Leave(Id, usize),
    Scope,
    EndScope,
    Bind { variable: Id, value: Id },
}

pub struct Program {
    pub nodes: BTreeMap<Id, Node>,
    /// Named outputs of the program.
    pub export: Vec<(String, Id)>,
}

impl Program {
    /// Print a human-readable summary of the IR.
    pub fn dump(&self) {
        for (&id, node) in &self.nodes {
            let desc = match node {
                Node::Input(i) => format!("Input({})", i),
                Node::Import { name } => format!("Import({:?})", name),
                Node::Linear { input, ops } => {
                    let ops_str: Vec<String> = ops.iter().map(|op| match op {
                        LinearOp::Project(_) => "Project".into(),
                        LinearOp::Filter(_) => "Filter".into(),
                        LinearOp::Negate => "Negate".into(),
                        LinearOp::EnterAt(_) => "EnterAt".into(),
                        LinearOp::LiftIter => "LiftIter".into(),
                    }).collect();
                    format!("Linear({}, [{}])", input, ops_str.join(", "))
                },
                Node::Concat(ids) => format!("Concat({:?})", ids),
                Node::Arrange(input) => format!("Arrange({})", input),
                Node::Join { left, right, .. } => format!("Join({}, {})", left, right),
                Node::Reduce { input, .. } => format!("Reduce({})", input),
                Node::Variable => "Variable".into(),
                Node::Inspect { input, label } => format!("Inspect({}, {:?})", input, label),
                Node::Leave(id, lvl) => format!("Leave({}, {})", id, lvl),
                Node::Scope => "Scope".into(),
                Node::EndScope => "EndScope".into(),
                Node::Bind { variable, value } => format!("Bind({} <- {})", variable, value),
            };
            println!("  {:3}: {}", id, desc);
        }
        for (name, id) in &self.export {
            println!("  export {:?} = {}", name, id);
        }
    }

    /// Per-node user-scope depth. Computed by walking `nodes` in id order
    /// and tracking `Scope` / `EndScope` markers. A node sits at the depth
    /// active at the moment it was lowered; `Scope` itself sits at its
    /// outer depth (the increment applies to subsequent nodes), and
    /// `EndScope` sits at its inner depth (the decrement applies after).
    ///
    /// Note: this is purely positional, so `enter_at` — a data→time lift that
    /// semantically adds one scope coordinate (its output is one level deeper
    /// than its input) — is counted depth-NEUTRAL here. The coordinate it
    /// introduces is instead absorbed by a neighboring Project's depth jump and
    /// stripped *unconstrained* in the reverse rewrite: sound but over-broad.
    /// The fix is to let `enter_at` own its level (output = input depth + 1);
    /// see the `LinearOp::EnterAt` arm in `explain.rs::emit_reverse`.
    pub fn depths(&self) -> BTreeMap<Id, usize> {
        let mut out = BTreeMap::new();
        let mut depth = 0usize;
        for (&id, node) in &self.nodes {
            match node {
                Node::Scope => { out.insert(id, depth); depth += 1; },
                Node::EndScope => { out.insert(id, depth); depth = depth.saturating_sub(1); },
                _ => { out.insert(id, depth); },
            }
        }
        out
    }

    /// Reject programs where a `LinearOp::LiftIter` result is referenced
    /// inside its own scope. See `Expr::LiftIter` for the rationale: in-
    /// scope use would let loop bodies branch on iter, defeating the
    /// time-invariant-body property fixpoints rely on.
    pub fn validate_lift_iter(&self) -> Result<(), String> {
        let depths = self.depths();
        // Build a map: producer id -> list of (user id, user node).
        let mut users: BTreeMap<Id, Vec<Id>> = BTreeMap::new();
        for (&user_id, node) in &self.nodes {
            let inputs: Vec<Id> = match node {
                Node::Linear { input, .. } | Node::Arrange(input)
                | Node::Reduce { input, .. } | Node::Inspect { input, .. } => vec![*input],
                Node::Join { left, right, .. } => vec![*left, *right],
                Node::Concat(ids) => ids.clone(),
                Node::Leave(id, _) => vec![*id],
                Node::Bind { value, .. } => vec![*value],
                Node::Input(_) | Node::Import { .. } | Node::Variable | Node::Scope | Node::EndScope => vec![],
            };
            for input in inputs {
                users.entry(input).or_default().push(user_id);
            }
        }
        for (&id, node) in &self.nodes {
            if let Node::Linear { ops, .. } = node {
                if !ops.iter().any(|o| matches!(o, LinearOp::LiftIter)) { continue; }
                let my_depth = depths[&id];
                if my_depth == 0 {
                    return Err(format!(
                        "lift_iter at node {} is at scope depth 0; lift_iter is only meaningful inside a user scope",
                        id
                    ));
                }
                if let Some(uses) = users.get(&id) {
                    for &user in uses {
                        let user_depth = depths[&user];
                        if user_depth >= my_depth {
                            return Err(format!(
                                "lift_iter at node {} (depth {}) referenced by node {} (depth {}); lift_iter result must be referenced only from an enclosing scope",
                                id, my_depth, user, user_depth
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Redirect every reference to node `from` so it points at `to`, across all
    /// nodes' inputs and the export list. Used by `optimize` when it collapses
    /// or fuses one node into another and the old id must be retargeted.
    fn rewrite(&mut self, from: Id, to: Id) {
        for node in self.nodes.values_mut() {
            match node {
                Node::Linear { input, .. } | Node::Arrange(input)
                | Node::Reduce { input, .. } | Node::Inspect { input, .. } => {
                    if *input == from { *input = to; }
                },
                Node::Join { left, right, .. } => {
                    if *left == from { *left = to; }
                    if *right == from { *right = to; }
                },
                Node::Concat(ids) => {
                    for id in ids.iter_mut() { if *id == from { *id = to; } }
                },
                Node::Leave(id, _) => {
                    if *id == from { *id = to; }
                },
                Node::Bind { variable, value } => {
                    if *variable == from { *variable = to; }
                    if *value == from { *value = to; }
                },
                Node::Input(_) | Node::Import { .. } | Node::Variable | Node::Scope | Node::EndScope => {},
            }
        }
        for (_, id) in self.export.iter_mut() {
            if *id == from { *id = to; }
        }
    }

    /// Optimize the IR in place, iterating to a fixed point.
    pub fn optimize(&mut self) {
        loop {
        let before = self.nodes.len();

        // Arrange(x) where x already produces an arrangement -> collapse.
        let collapses: Vec<(Id, Id)> = self.nodes.iter()
            .filter_map(|(&id, node)| {
                if let Node::Arrange(input) = node {
                    if matches!(self.nodes.get(input), Some(Node::Arrange(_) | Node::Reduce { .. })) {
                        return Some((id, *input));
                    }
                }
                None
            })
            .collect();
        for (outer, inner) in collapses {
            self.rewrite(outer, inner);
            self.nodes.remove(&outer);
        }

        // Fuse Linear chains: if a Linear's input is another Linear (with no other consumers),
        // concatenate the ops into one node.
        // First, count references to each node.
        let mut ref_counts: std::collections::HashMap<Id, usize> = std::collections::HashMap::new();
        for node in self.nodes.values() {
            match node {
                Node::Linear { input, .. } | Node::Arrange(input)
                | Node::Reduce { input, .. } | Node::Inspect { input, .. } => {
                    *ref_counts.entry(*input).or_default() += 1;
                },
                Node::Join { left, right, .. } => {
                    *ref_counts.entry(*left).or_default() += 1;
                    *ref_counts.entry(*right).or_default() += 1;
                },
                Node::Concat(ids) => {
                    for id in ids { *ref_counts.entry(*id).or_default() += 1; }
                },
                Node::Leave(id, _) => { *ref_counts.entry(*id).or_default() += 1; },
                Node::Bind { variable, value } => {
                    *ref_counts.entry(*variable).or_default() += 1;
                    *ref_counts.entry(*value).or_default() += 1;
                },
                _ => {},
            }
        }
        for (_, id) in &self.export { *ref_counts.entry(*id).or_default() += 1; }

        let fusions: Vec<(Id, Id)> = self.nodes.iter()
            .filter_map(|(&id, node)| {
                if let Node::Linear { input, .. } = node {
                    if matches!(self.nodes.get(input), Some(Node::Linear { .. })) {
                        if ref_counts.get(input).copied().unwrap_or(0) == 1 {
                            return Some((id, *input));
                        }
                    }
                }
                None
            })
            .collect();
        for (outer_id, inner_id) in fusions {
            // Take both nodes out, fuse, put back.
            let outer = self.nodes.remove(&outer_id).unwrap();
            let inner = self.nodes.remove(&inner_id).unwrap();
            if let (Node::Linear { ops: mut outer_ops, .. }, Node::Linear { input: inner_input, ops: inner_ops }) = (outer, inner) {
                let mut fused = inner_ops;
                fused.append(&mut outer_ops);
                self.nodes.insert(outer_id, Node::Linear { input: inner_input, ops: fused });
            }
        }

        // Deduplicate structurally identical nodes.
        use std::collections::HashMap;
        fn structural_key(node: &Node) -> Option<String> {
            match node {
                Node::Variable | Node::Scope | Node::EndScope | Node::Bind { .. } => None,
                other => Some(format!("{:?}", DebugNode(other))),
            }
        }
        struct DebugNode<'a>(&'a Node);
        impl<'a> std::fmt::Debug for DebugNode<'a> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self.0 {
                    Node::Input(i) => write!(f, "Input({})", i),
                    Node::Import { name } => write!(f, "Import({:?})", name),
                    Node::Linear { input, ops } => write!(f, "Linear({},{:?})", input, ops),
                    Node::Concat(ids) => write!(f, "Concat({:?})", ids),
                    Node::Arrange(input) => write!(f, "Arrange({})", input),
                    Node::Join { left, right, projection } => write!(f, "Join({},{},{:?})", left, right, projection),
                    Node::Reduce { input, reducer } => write!(f, "Reduce({},{:?})", input, reducer),
                    Node::Inspect { input, label } => write!(f, "Inspect({},{:?})", input, label),
                    Node::Leave(id, lvl) => write!(f, "Leave({},{})", id, lvl),
                    _ => write!(f, ""),
                }
            }
        }

        let mut seen: HashMap<String, Id> = HashMap::new();
        let dupes: Vec<(Id, Id)> = self.nodes.iter()
            .filter_map(|(&id, node)| {
                let key = structural_key(node)?;
                if let Some(&canonical) = seen.get(&key) {
                    Some((id, canonical))
                } else {
                    seen.insert(key, id);
                    None
                }
            })
            .collect();
        for (dupe, canonical) in dupes {
            self.rewrite(dupe, canonical);
            self.nodes.remove(&dupe);
        }
        if self.nodes.len() == before { break; }
        } // loop
    }
}

/// Evaluate fields into a row.
pub fn eval_fields<R: RowLike>(fields: &[FieldExpr], inputs: &[&[i64]]) -> R {
    let mut r = R::new();
    for f in fields { eval_field_into(f, inputs, &mut r); }
    r
}

pub fn eval_field_into<R: RowLike>(field: &FieldExpr, inputs: &[&[i64]], result: &mut R) {
    match field {
        FieldExpr::Pos(i) => result.extend_from_slice(inputs[*i]),
        FieldExpr::Index(row, idx) => result.push(inputs[*row][*idx]),
        FieldExpr::Const(v) => result.push(*v),
        FieldExpr::Neg(inner) => { let mut tmp = R::new(); eval_field_into(inner, inputs, &mut tmp); for v in tmp.as_slice().iter() { result.push(-v); } },
        FieldExpr::Sub(a, b) => {
            let mut la = R::new(); eval_field_into(a, inputs, &mut la);
            let mut lb = R::new(); eval_field_into(b, inputs, &mut lb);
            // Element-wise subtract; rows must have matching length.
            assert_eq!(la.as_slice().len(), lb.as_slice().len(),
                "FieldExpr::Sub: operands must produce same-length rows");
            for (x, y) in la.as_slice().iter().zip(lb.as_slice().iter()) { result.push(x - y); }
        }
    }
}

pub fn eval_condition(cond: &Condition, inputs: &[&[i64]]) -> bool {
    let cmp = |l, r, op: fn(&Vec<i64>, &Vec<i64>) -> bool| {
        let mut a = Vec::<i64>::new(); let mut b = Vec::<i64>::new();
        eval_field_raw(l, inputs, &mut a); eval_field_raw(r, inputs, &mut b);
        op(&a, &b)
    };
    match cond {
        Condition::And(l, r) => eval_condition(l, inputs) && eval_condition(r, inputs),
        Condition::Eq(l, r) => cmp(l, r, |a, b| a == b),
        Condition::Ne(l, r) => cmp(l, r, |a, b| a != b),
        Condition::Lt(l, r) => cmp(l, r, |a, b| a < b),
        Condition::Le(l, r) => cmp(l, r, |a, b| a <= b),
        Condition::Gt(l, r) => cmp(l, r, |a, b| a > b),
        Condition::Ge(l, r) => cmp(l, r, |a, b| a >= b),
    }
}

fn eval_field_raw(field: &FieldExpr, inputs: &[&[i64]], result: &mut Vec<i64>) {
    match field {
        FieldExpr::Pos(i) => result.extend_from_slice(inputs[*i]),
        FieldExpr::Index(row, idx) => result.push(inputs[*row][*idx]),
        FieldExpr::Const(v) => result.push(*v),
        FieldExpr::Neg(inner) => { let mut tmp = Vec::new(); eval_field_raw(inner, inputs, &mut tmp); for v in tmp.iter() { result.push(-v); } },
        FieldExpr::Sub(a, b) => {
            let mut la = Vec::new(); eval_field_raw(a, inputs, &mut la);
            let mut lb = Vec::new(); eval_field_raw(b, inputs, &mut lb);
            assert_eq!(la.len(), lb.len(), "FieldExpr::Sub: operands must produce same-length rows");
            for (x, y) in la.iter().zip(lb.iter()) { result.push(x - y); }
        }
    }
}
