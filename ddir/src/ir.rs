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
}

/// Symbolic IR node.
pub enum Node {
    Input(usize),
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
    pub result: Id,
}

impl Program {
    /// Print a human-readable summary of the IR.
    pub fn dump(&self) {
        for (&id, node) in &self.nodes {
            let desc = match node {
                Node::Input(i) => format!("Input({})", i),
                Node::Linear { input, ops } => {
                    let ops_str: Vec<String> = ops.iter().map(|op| match op {
                        LinearOp::Project(_) => "Project".into(),
                        LinearOp::Filter(_) => "Filter".into(),
                        LinearOp::Negate => "Negate".into(),
                        LinearOp::EnterAt(_) => "EnterAt".into(),
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
        println!("  result: {}", self.result);
    }

    /// Replace all references to `from` with `to` across the IR.
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
                Node::Input(_) | Node::Variable | Node::Scope | Node::EndScope => {},
            }
        }
        if self.result == from { self.result = to; }
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
        if self.result != usize::MAX { *ref_counts.entry(self.result).or_default() += 1; }

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
    }
}

pub fn eval_condition(cond: &Condition, inputs: &[&[i64]]) -> bool {
    let (l, r) = match cond {
        Condition::Eq(l, r) | Condition::Ne(l, r) | Condition::Lt(l, r)
        | Condition::Le(l, r) | Condition::Gt(l, r) | Condition::Ge(l, r) => (l, r),
    };
    let mut a = Vec::<i64>::new(); let mut b = Vec::<i64>::new();
    eval_field_raw(l, inputs, &mut a); eval_field_raw(r, inputs, &mut b);
    match cond {
        Condition::Eq(..) => a == b, Condition::Ne(..) => a != b,
        Condition::Lt(..) => a < b,  Condition::Le(..) => a <= b,
        Condition::Gt(..) => a > b,  Condition::Ge(..) => a >= b,
    }
}

fn eval_field_raw(field: &FieldExpr, inputs: &[&[i64]], result: &mut Vec<i64>) {
    match field {
        FieldExpr::Pos(i) => result.extend_from_slice(inputs[*i]),
        FieldExpr::Index(row, idx) => result.push(inputs[*row][*idx]),
        FieldExpr::Const(v) => result.push(*v),
        FieldExpr::Neg(inner) => { let mut tmp = Vec::new(); eval_field_raw(inner, inputs, &mut tmp); for v in tmp.iter() { result.push(-v); } },
    }
}
