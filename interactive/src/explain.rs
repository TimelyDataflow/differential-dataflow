//! DDIR-to-DDIR explanation rewrite.
//!
//! `explain(p)` transforms a Program into one whose execution produces
//! per-input demand-set explanations for queries against the original program's result.
//!
//! ### Architecture (per design notes)
//!
//! ```text
//! -- INPUT:
//! --    a. a user dataflow,
//! --    b. live input sources,
//! --    c. live output queries.
//! --
//! -- This module is a pure IR-to-IR transform: it returns a Program. Any DDIR
//! -- backend (`ddir_vec`, `ddir_col`) then executes that Program; running it on
//! -- the inputs + a stream of output queries produces, for each output query,
//! -- a subset of the data sources that "explains" the query — running the user
//! -- dataflow on that subset reproduces the queried output. The explanation
//! -- updates live as the data sources and queries change.
//! --
//! -- The returned Program contains two clones of the user dataflow plus a
//! -- reverse-tracing dataflow:
//! --
//! -- The *witness* clone runs on the actual data sources and produces the
//! -- reference output the explanation must reproduce.
//! --
//! -- Alongside, in an iterative scope `explain`, the *forward* clone runs the
//! -- user dataflow on the data sources restricted to per-input *demand-sets*.
//! -- Demand-sets start empty and grow until the forward clone reproduces every
//! -- queried output.
//! --
//! -- The demand-sets are populated by a reverse-tracing dataflow. Each output
//! -- query enters the reverse dataflow as an "input" to it; it traverses each
//! -- operator in turn, asking that operator's inputs for the updates that
//! -- could play a role in forming the queried output.
//! --
//! --        IN0 \                       d_IN0 <-\
//! --             OP -> OUT    becomes            OP^d <- d_OUT
//! --        IN1 /                       d_IN1 <-/
//! --
//! -- The unit of demand is `(data, time)`, where `time` is the user dataflow's
//! -- iteration coordinate(s) nested under the ambient (host) time. Time is
//! -- load-bearing: an input may only "explain" an output if it came before it.
//! --
//! -- Both clones use `clone_with_lifts`, which inserts `lift_iter` at every
//! -- scope exit so that every collection inside a user scope has a "host-
//! -- visible" form at the outer (explain) scope with its user-iter coords
//! -- folded into the value. The `CloneResult.host[id]` map exposes this
//! -- outer-scope id for every node; the reverse rules read from it to build
//! -- their witness pair tables.
//! --
//! -- The result is one demand-set per input — the subset of input rows that,
//! -- taken together, reproduce every queried output.
//!
//! witness: { <user dataflow, cloned with lift_iter at scope exits> }
//!
//! explain: {
//!     -- per-input demand-set variables; grown by `reverse` below
//!     -- to whatever proves sufficient to reproduce the demanded outputs.
//!
//!     forward: { <user dataflow, inputs restricted to demand-sets, lifted> }
//!     reverse: { backwards trace from output queries to demand-sets }
//! }
//! ```
//!
//! ### Vocabulary
//!
//! Three related but distinct concepts share the "demand" word:
//!
//! - **demand-set** (`demand_set_<i>`, one per Input): the accumulating
//!   per-input subset — the *result* of explanation.
//! - **demand variable** (`demand_<N>`, one per non-trivial IR node): an
//!   intermediate feedback variable through which the reverse-tracing
//!   dataflow propagates demand from outputs back to inputs.
//! - **demand row**: a single record `(data, time, q)` flowing through
//!   the reverse dataflow — "output query `q` needs `data` produced at
//!   `time` from this point."
//!
//! For an IR node `N` at user-scope depth `D`, a demand row in `demand_<N>`
//! has shape:
//!
//! ```text
//! (K_N ; V_N ++ user_chain[D] ++ [q])
//! ```
//!
//! where `user_chain[D]` is one i64 per enclosing user scope (innermost
//! first; the position lifted by the innermost scope's `lift_iter` comes
//! first). `q` is the query id.
//!
//! ### Out of scope for this pass
//!
//! - The §3.2.2 value-based narrowing for Min — currently pass-through-on-key
//!   (sound but loose; tightens `stable.ddp` / `scc.ddp` outputs once added).
//! - `Reduce::Count` — handled by the same keyed lookup as Min/Distinct;
//!   value-level narrowing would also apply here once it lands.
//! - Multi-op `Linear` chains. `emit_reverse` currently panics on these;
//!   the rewrite assumes the optimizer leaves Linear nodes single-op.
//! - Pre-optimization. The optimizer can clean up dead lifts and unused
//!   nodes after the rewrite emits them.
//!
//! ### Future investigation
//!
//! Per-IR-node demand variables (`demand_<N>`) may be redundant. They are
//! inherited from the timely reference implementation, which needed a
//! feedback Variable per operator to express backward edges. The rewrite
//! likely doesn't: each non-user-`var` node's demand is a pure function of
//! downstream demand + its pair table, and could be composed as plain DD
//! operators. The only structurally required feedbacks are (a) one
//! demand-set per Input and (b) one demand feedback per user-program `var`.
//! Worth measuring intermediate-stream sizes before committing — the
//! per-node `distinct`s today act as natural fan-out limiters.

use std::collections::BTreeMap;

use crate::ir::{Id, Node, Program};
use crate::parse::{FieldExpr, Projection, Reducer};

use builder::Builder;
use arities::compute_arities;
use clone::CloneResult;

/// Transform a `Program` into one whose execution produces per-input
/// demand-set explanations for queries against the original's result.
/// See the module doc for the architecture.
///
/// `input_arities` gives `(key_arity, val_arity)` per input, in input
/// order. Necessary because input row shapes aren't recoverable from the
/// IR alone (Projections only invert with known input arity).
pub fn explain(p: &Program, input_arities: &[(usize, usize)]) -> Program {
    let mut b = Builder::new();
    let arities = compute_arities(p, input_arities);
    let depths = p.depths();
    // Per node, how many user-iter coords the lifted (host-visible) collection
    // carries in val. For non-Leave nodes this equals `depths[N]`; for
    // `Leave(inner, _)`, host[Leave] aliases host[inner] so the user-chain
    // length is the inner's depth.
    let user_lens: BTreeMap<Id, usize> = p.nodes.iter().map(|(&id, node)| {
        let len = if let Node::Leave(inner, _) = node {
            *depths.get(inner).unwrap_or(&0)
        } else {
            *depths.get(&id).unwrap_or(&0)
        };
        (id, len)
    }).collect();
    let n_inputs = input_arities.len();

    // ---- outer scope ----
    // Original inputs of `p`, plus one extra "query" input at index n.
    let original_inputs: Vec<Id> = (0..n_inputs).map(|i| b.input(i)).collect();
    let query_input = b.input(n_inputs);

    // witness: a clone of `p`, with lift_iter chains so every witness
    // collection has a host-visible `(data, user)` form via auto-leave at
    // each enclosing user scope's exit.
    let witness = b.clone_with_lifts(p, &original_inputs, 0);

    // ---- explain scope ----
    b.scope_open();

    // Demand-set Variables (one per input).
    let demand_sets: Vec<Id> = (0..n_inputs).map(|_| b.variable()).collect();

    // forward inputs: demand_set_<i> | semijoin(actual_input_<i>).
    // Enter actual inputs into explain scope implicitly; semijoin restricts to
    // demanded rows.
    let forward_inputs: Vec<Id> = (0..n_inputs)
        .map(|i| {
            let (k, v) = input_arities[i];
            b.semijoin_data(demand_sets[i], original_inputs[i], k, v)
        })
        .collect();

    // forward: same clone procedure as witness, with substituted inputs.
    // Offset = 1 because this clone lives INSIDE the explain scope: its real
    // PointStamp depth at any point is one more than its local user_level.
    let forward = b.clone_with_lifts(p, &forward_inputs, 1);

    // Demand Variables (one per witness data node).
    let mut demand_var: BTreeMap<Id, Id> = BTreeMap::new();
    for (&id, node) in &p.nodes {
        if !matches!(node, Node::Scope | Node::EndScope | Node::Bind { .. }) {
            demand_var.insert(id, b.variable());
        }
    }

    // reverse rules. Per-op, push contributions onto `contribs[input_id]`.
    // Query input directly seeds `contribs[result]` — the result demand
    // starts with the query rows.
    let mut contribs: BTreeMap<Id, Vec<Id>> = BTreeMap::new();
    contribs.entry(p.result).or_default().push(query_input);

    for (&id, node) in &p.nodes {
        b.emit_reverse(
            id,
            node,
            &witness,
            &forward,
            &demand_var,
            &arities,
            &user_lens,
            &mut contribs,
        );
    }

    // Bind demand variables: demand_<N> := distinct(demand_<N> + contribs).
    for (&id, &var) in &demand_var {
        let cs = contribs.remove(&id).unwrap_or_default();
        let mut all = vec![var];
        all.extend(cs);
        let combined = if all.len() == 1 { all[0] } else { b.concat(all) };
        let (k, v) = arities[&id];
        let user_len = user_lens[&id];
        let val_arity = v + user_len + 1; // V + user_chain + [q]
        let dist = b.distinct_full(combined, k, val_arity);
        b.debug_inspect(dist, format!("demand_{}", id));
        b.bind(var, dist);
    }

    // Bind demand-set variables: demand_set_<i> := distinct(demand_set_<i> + (demand_<Input(i)> | strip | semijoin actual)).
    // Build a Vec mapping input index `i` to its IR id in `p`, so the
    // per-input loop below is O(n) total instead of O(n^2).
    let mut input_ids: Vec<Option<Id>> = vec![None; n_inputs];
    for (&id, node) in &p.nodes {
        if let Node::Input(i) = node {
            input_ids[*i] = Some(id);
        }
    }
    for i in 0..n_inputs {
        let in_id = input_ids[i].expect("input not found in program");
        let (kx, vx) = arities[&in_id];
        // Inputs are always at depth 0 → user_chain is empty.
        let stripped = b.project(demand_var[&in_id], strip_user_and_q(kx, vx));
        let semi = b.semijoin_data(stripped, original_inputs[i], kx, vx);
        let combined = b.concat(vec![demand_sets[i], semi]);
        let dist = b.distinct_full(combined, kx, vx);
        b.bind(demand_sets[i], dist);
    }

    // Inspects on demand-sets.
    let mut last_inspect: Option<Id> = None;
    for (i, &mv) in demand_sets.iter().enumerate() {
        last_inspect = Some(b.inspect(mv, format!("demand_set_{}", i)));
    }
    let result_inner = last_inspect.unwrap_or_else(|| demand_sets.first().copied().unwrap_or(0));

    b.scope_close();
    let result_outer = b.leave(result_inner, 1);
    b.set_result(result_outer);
    b.into_program()
}

/// Thin builder wrapper around `Program` for incremental IR construction.
///
/// Each push method appends a node and returns its fresh id. Composite
/// constructors (`reduce`, `join`) emit the implicit `Arrange` wrappers the
/// IR requires of those ops, so callers can think in terms of collections.
mod builder {
    use std::collections::BTreeMap;

    use crate::ir::{Id, LinearOp, Node, Program};
    use crate::parse::{Condition, Projection, Reducer};

    pub struct Builder {
        program: Program,
        next_id: Id,
    }

    impl Builder {
        pub(super) fn new() -> Self {
            Builder {
                program: Program { nodes: BTreeMap::new(), result: 0 },
                next_id: 0,
            }
        }
        pub(super) fn push(&mut self, n: Node) -> Id {
            let id = self.next_id;
            self.next_id += 1;
            self.program.nodes.insert(id, n);
            id
        }
        pub(super) fn input(&mut self, n: usize) -> Id { self.push(Node::Input(n)) }
        pub(super) fn variable(&mut self) -> Id { self.push(Node::Variable) }
        pub(super) fn arrange(&mut self, input: Id) -> Id { self.push(Node::Arrange(input)) }
        pub(super) fn linear(&mut self, input: Id, ops: Vec<LinearOp>) -> Id {
            self.push(Node::Linear { input, ops })
        }
        pub(super) fn project(&mut self, input: Id, p: Projection) -> Id {
            self.linear(input, vec![LinearOp::Project(p)])
        }
        pub(super) fn filter(&mut self, input: Id, c: Condition) -> Id {
            self.linear(input, vec![LinearOp::Filter(c)])
        }
        pub(super) fn concat(&mut self, ids: Vec<Id>) -> Id { self.push(Node::Concat(ids)) }
        pub(super) fn reduce(&mut self, input: Id, r: Reducer) -> Id {
            let arr = self.arrange(input);
            self.push(Node::Reduce { input: arr, reducer: r })
        }
        pub(super) fn join(&mut self, left: Id, right: Id, p: Projection) -> Id {
            let l = self.arrange(left);
            let r = self.arrange(right);
            self.push(Node::Join { left: l, right: r, projection: p })
        }
        pub(super) fn inspect(&mut self, input: Id, label: String) -> Id {
            self.push(Node::Inspect { input, label })
        }
        pub(super) fn leave(&mut self, inner: Id, scope_level: usize) -> Id {
            self.push(Node::Leave(inner, scope_level))
        }
        pub(super) fn bind(&mut self, variable: Id, value: Id) {
            self.push(Node::Bind { variable, value });
        }
        pub(super) fn scope_open(&mut self) { self.push(Node::Scope); }
        pub(super) fn scope_close(&mut self) { self.push(Node::EndScope); }
        pub(super) fn set_result(&mut self, id: Id) { self.program.result = id; }
        pub(super) fn into_program(self) -> Program { self.program }
    }
}

/// Per-IR-node (key_arity, val_arity) inference.
///
/// Walks the program to a fixed point, deriving each node's data-shape from
/// its inputs and the op-specific rule. Needed because input shapes aren't
/// recoverable from the IR alone — `Projection`s only invert with known
/// input arity, and lift_iter sites need to know how many user-iter coords
/// already sit in the val.
mod arities {
    use std::collections::BTreeMap;

    use crate::ir::{Id, LinearOp, Node, Program};
    use crate::parse::Reducer;

    pub(super) fn compute_arities(p: &Program, input_arities: &[(usize, usize)]) -> BTreeMap<Id, (usize, usize)> {
        // Variables are referenced before their Binds appear in id order;
        // resolve a Variable's shape via its body.
        let var_body: BTreeMap<Id, Id> = p.nodes.iter().filter_map(|(_, n)| {
            if let Node::Bind { variable, value } = n {
                Some((*variable, *value))
            } else { None }
        }).collect();

        let mut out: BTreeMap<Id, (usize, usize)> = BTreeMap::new();
        loop {
            let before = out.len();
            for (&id, node) in &p.nodes {
                if out.contains_key(&id) { continue; }
                let shape = match node {
                    Node::Input(i) => Some(input_arities[*i]),
                    Node::Linear { input, ops } => out.get(input).map(|s| apply_ops_arity(*s, ops)),
                    // Try each input — for self-recursive Variables that appear
                    // as `Concat([var, ...])`, the first input's shape isn't
                    // known on early passes; pick any input that has a known
                    // shape and let fixed-point iteration propagate.
                    Node::Concat(ids) => ids.iter().find_map(|i| out.get(i).copied()),
                    Node::Arrange(input) => out.get(input).copied(),
                    Node::Join { projection, .. } => Some((projection.key.len(), projection.val.len())),
                    Node::Reduce { input, reducer } => out.get(input).map(|s| match reducer {
                        Reducer::Distinct => (s.0, 0),
                        Reducer::Min => (s.0, s.1),
                        Reducer::Count => (s.0, 1),
                    }),
                    Node::Inspect { input, .. } => out.get(input).copied(),
                    Node::Leave(inner, _) => out.get(inner).copied(),
                    Node::Variable => var_body.get(&id).and_then(|v| out.get(v).copied()),
                    Node::Scope | Node::EndScope | Node::Bind { .. } => None,
                };
                if let Some(s) = shape { out.insert(id, s); }
            }
            if out.len() == before { break; }
        }
        out
    }

    fn apply_ops_arity((mut k, mut v): (usize, usize), ops: &[LinearOp]) -> (usize, usize) {
        for op in ops {
            match op {
                LinearOp::Project(p) => { k = p.key.len(); v = p.val.len(); }
                LinearOp::Filter(_) | LinearOp::Negate | LinearOp::EnterAt(_) => {}
                LinearOp::LiftIter => { v += 1; }
            }
        }
        (k, v)
    }
}

/// IR cloning with implicit lift_iter at scope exits.
///
/// `clone_with_lifts` emits `p` into `b` with a `lift_iter` chain at every
/// scope exit, so every non-Input data node has a host-visible version
/// reachable as `host[id]`. Inputs are aliased to `input_subst`.
///
/// Convention: the lifted user-iter coords are appended innermost-first. A
/// node at depth D has D fields of user time appended to val in its host-
/// visible form. `enclosing_scope_depth` adjusts every emitted `Leave` to
/// account for an enclosing scope the clone itself sits inside.
mod clone {
    use std::collections::BTreeMap;

    use crate::ir::{Id, LinearOp, Node, Program};

    use super::Builder;

    pub(super) struct CloneResult {
        /// The id reachable at explain scope, with user-iter coords appended to val.
        pub(super) host: BTreeMap<Id, Id>,
    }

    impl Builder {
        pub(super) fn clone_with_lifts(
            &mut self,
            p: &Program,
            input_subst: &[Id],
            enclosing_scope_depth: usize,
        ) -> CloneResult {
            let mut in_scope: BTreeMap<Id, Id> = BTreeMap::new();
            let mut host: BTreeMap<Id, Id> = BTreeMap::new();
            let mut user_level: usize = 0;
            // Pending pile per enclosing user scope: (orig_id, current_cloned_id).
            // On EndScope: lift_iter each (capturing this scope's iter coord into
            // val), close scope, leave each to next-outer scope. If we've reached
            // outer/explain scope, record in `host`; otherwise push onto the next-
            // outer pile.
            let mut pending: Vec<Vec<(Id, Id)>> = Vec::new();

            for (&id, node) in &p.nodes {
                match node {
                    Node::Scope => {
                        self.scope_open();
                        user_level += 1;
                        pending.push(Vec::new());
                        continue;
                    }
                    Node::EndScope => {
                        let pile = pending.pop().expect("EndScope without Scope");
                        let scope_lvl = user_level; // level inside the scope
                        // Lift each pending node BEFORE closing the scope.
                        let lifted: Vec<(Id, Id)> = pile
                            .into_iter()
                            .map(|(orig, cur)| (orig, self.linear(cur, vec![LinearOp::LiftIter])))
                            .collect();
                        self.scope_close();
                        user_level -= 1;
                        // Leave each to the outer scope. `leave_dynamic(k)` truncates
                        // the PointStamp to length `k - 1`, so we must pass the
                        // *real* depth at this point, which is local `scope_lvl`
                        // plus the outer offset from the enclosing explain scope.
                        for (orig, lifted_id) in lifted {
                            let leaved = self.leave(lifted_id, scope_lvl + enclosing_scope_depth);
                            if user_level == 0 {
                                host.insert(orig, leaved);
                            } else {
                                pending.last_mut().unwrap().push((orig, leaved));
                            }
                        }
                        continue;
                    }
                    Node::Bind { variable, value } => {
                        self.bind(in_scope[variable], in_scope[value]);
                        continue;
                    }
                    Node::Input(i) => {
                        // Inputs are at depth 0, host-visible directly.
                        in_scope.insert(id, input_subst[*i]);
                        host.insert(id, input_subst[*i]);
                        continue;
                    }
                    _ => {}
                }
                // Emit the clone for this data node.
                let cloned = match node {
                    Node::Linear { input, ops } => self.linear(in_scope[input], ops.clone()),
                    Node::Concat(ids) => {
                        let mapped: Vec<Id> = ids.iter().map(|i| in_scope[i]).collect();
                        self.concat(mapped)
                    }
                    Node::Arrange(input) => self.arrange(in_scope[input]),
                    Node::Join { left, right, projection } => self.push(Node::Join {
                        left: in_scope[left],
                        right: in_scope[right],
                        projection: projection.clone(),
                    }),
                    Node::Reduce { input, reducer } => self.push(Node::Reduce {
                        input: in_scope[input],
                        reducer: reducer.clone(),
                    }),
                    Node::Variable => self.variable(),
                    Node::Inspect { input, label } => self.inspect(in_scope[input], label.clone()),
                    Node::Leave(inner, scope_level) => self.leave(in_scope[inner], *scope_level + enclosing_scope_depth),
                    _ => unreachable!(),
                };
                in_scope.insert(id, cloned);
                if user_level == 0 {
                    // For Arrange nodes at outer scope: alias host[N] to the
                    // underlying Collection (= host[input]) so backward rules
                    // never refer to an Arrangement across scope boundaries.
                    let recorded = match node {
                        Node::Arrange(input) => host.get(input).copied().unwrap_or(cloned),
                        _ => cloned,
                    };
                    host.insert(id, recorded);
                } else {
                    pending.last_mut().unwrap().push((id, cloned));
                }
            }
            assert!(pending.is_empty(), "Scope/EndScope imbalance in clone");

            // Second pass: rewrite host[Leave_id] to host[inner_id] for every
            // Leave in the program. host[inner] is the lifted form backward
            // rules expect; the inline Leave clone above is the un-lifted form
            // (right for `in_scope` references, wrong for `host`). Has to be a
            // second pass because Leaves *inside* nested scopes don't have
            // their inner's host entry populated until all enclosing scopes
            // have closed.
            for (&id, node) in &p.nodes {
                if let Node::Leave(inner, _) = node {
                    if let Some(&host_inner) = host.get(inner) {
                        host.insert(id, host_inner);
                    }
                }
            }

            CloneResult { host }
        }
    }
}

/// Backward (demand-propagation) rule emission.
///
/// Per-op reverse rules all share the same skeleton: join `demand_<OUT>`
/// with `witness + forward` of the inputs (possibly projected) on the op's
/// natural key, filter `user_in ≤ user_out` element-wise for soundness,
/// project to the demanded-input shape. The four `emit_lookup_*` helpers
/// differ only in how they construct the pair table and how they map fields
/// after the join; `filter_time_and_strip` is shared.
mod reverse {
    use std::collections::BTreeMap;

    use crate::ir::{Id, LinearOp, Node};
    use crate::parse::{Condition, FieldExpr, Projection};

    use super::Builder;
    use super::CloneResult;

    /// One upstream edge into a backward rule: its host-side `(data, user)`
    /// collections from both clones, its data shape, and the user-chain
    /// length its host form carries.
    struct Side {
        witness: Id,                 // witness.host[input]
        forward: Id,                 // forward.host[input]
        shape: (usize, usize),     // (k_arity, v_arity)
        user_len: usize,
    }

    impl Side {
        fn for_input(
            id: Id,
            witness: &CloneResult,
            forward: &CloneResult,
            arities: &BTreeMap<Id, (usize, usize)>,
            user_lens: &BTreeMap<Id, usize>,
        ) -> Self {
            Self {
                witness: witness.host[&id],
                forward: forward.host[&id],
                shape: arities[&id],
                user_len: user_lens[&id],
            }
        }
    }

    impl Builder {
        pub(super) fn emit_reverse(
            &mut self,
            id: Id,
            node: &Node,
            witness: &CloneResult,
            forward: &CloneResult,
            demand: &BTreeMap<Id, Id>,
            arities: &BTreeMap<Id, (usize, usize)>,
            user_lens: &BTreeMap<Id, usize>,
            contribs: &mut BTreeMap<Id, Vec<Id>>,
        ) {
            // Bind has no demand entry of its own; it routes the *variable's*
            // demand into the *value's* contribs. Other no-demand nodes (Scope,
            // EndScope) have nothing to do.
            if let Node::Bind { variable, value } = node {
                if let Some(&dv) = demand.get(variable) {
                    let value_side = Side::for_input(*value, witness, forward, arities, user_lens);
                    let contrib = self.emit_lookup_shape_preserving(dv, &value_side, user_lens[variable]);
                    contribs.entry(*value).or_default().push(contrib);
                }
                return;
            }
            let dep_this = match demand.get(&id) {
                Some(&v) => v,
                None => return,
            };
            // `out_user_len` = number of user-iter coords in dep_<id>'s val (after V).
            let out_user_len = user_lens[&id];
            let out_shape = arities[&id];
            let side = |inp: Id| Side::for_input(inp, witness, forward, arities, user_lens);

            match node {
                Node::Input(_) => { /* terminal; feeds demand-set seeding. */ }

                Node::Linear { input, ops } => {
                    let op = match ops.as_slice() {
                        [single] => single,
                        _ => panic!("explain: multi-op Linear chain at node {}", id),
                    };
                    let input_side = side(*input);
                    match op {
                        LinearOp::Project(proj) => {
                            let contrib = self.emit_lookup_lossy(dep_this, &input_side, out_shape, out_user_len, proj);
                            contribs.entry(*input).or_default().push(contrib);
                        }
                        LinearOp::Filter(_) | LinearOp::Negate | LinearOp::EnterAt(_) => {
                            // Filter/Negate don't change shape; EnterAt only shifts
                            // the timestamp. Demand routes straight through to the
                            // input via the standard shape-preserving lookup.
                            let contrib = self.emit_lookup_shape_preserving(dep_this, &input_side, out_user_len);
                            contribs.entry(*input).or_default().push(contrib);
                        }
                        LinearOp::LiftIter => {
                            // LiftIter is rewrite-emitted only; it should never
                            // appear in a user program that reaches `explain`.
                            panic!("explain: LiftIter in user program at node {}", id);
                        }
                    }
                }

                Node::Concat(ids) => {
                    for inp in ids {
                        let input_side = side(*inp);
                        let contrib = self.emit_lookup_shape_preserving(dep_this, &input_side, out_user_len);
                        contribs.entry(*inp).or_default().push(contrib);
                    }
                }

                Node::Arrange(input) | Node::Inspect { input, .. } => {
                    let input_side = side(*input);
                    let contrib = self.emit_lookup_shape_preserving(dep_this, &input_side, out_user_len);
                    self.debug_inspect(contrib, format!("contrib_{}_to_{}", id, input));
                    contribs.entry(*input).or_default().push(contrib);
                }

                Node::Reduce { input, reducer: _ } => {
                    let input_side = side(*input);
                    let contrib = self.emit_lookup_keyed(dep_this, &input_side, out_shape, out_user_len);
                    contribs.entry(*input).or_default().push(contrib);
                }

                Node::Join { left, right, projection } => {
                    let left_side = side(*left);
                    let right_side = side(*right);
                    let (left_contrib, right_contrib) = self.emit_lookup_join(
                        dep_this, &left_side, &right_side,
                        out_shape, out_user_len, projection,
                    );
                    contribs.entry(*left).or_default().push(left_contrib);
                    contribs.entry(*right).or_default().push(right_contrib);
                }

                Node::Variable => { /* handled by Bind. */ }

                Node::Leave(inner, _) => {
                    let inner_side = side(*inner);
                    let contrib = self.emit_lookup_shape_preserving(dep_this, &inner_side, out_user_len);
                    contribs.entry(*inner).or_default().push(contrib);
                }

                Node::Bind { .. } | Node::Scope | Node::EndScope => {}
            }
        }

    /// Shape-preserving lookup: input and output have the same (k, v) shape.
    /// Used for Concat, Arrange, Inspect, Filter, Negate, Variable->body
    /// routing, and the inner/Leave aliasing.
    ///
    /// `input_depth` is the input's user-chain length, which may be less than
    /// `output_depth` (the dep's chain length) when the input lives at a shallower
    /// scope than the operator.
    ///
    /// pair = witness + forward of input. Shape (K; V ++ user_chain_in).
    /// Repack to (K+V; user_chain_in). dep repacks to (K+V; user_chain_out ++ [q]).
    /// Join on (K+V); produce (K; V ++ user_chain_in ++ user_chain_out ++ [q]),
    /// then `filter_time_and_strip` enforces `user_in ≤ user_out` and drops
    /// `user_chain_out` to yield `(K; V ++ user_chain_in ++ [q])`.
    fn emit_lookup_shape_preserving(
        &mut self,
        dep_y: Id,
        side: &Side,
        output_depth: usize,
    ) -> Id {
        let (k, v) = side.shape;
        let input_depth = side.user_len;
        let pair = self.concat(vec![side.witness, side.forward]);
        self.debug_inspect(pair, format!("pair_sp_dep={}_witness={}_forward={}", dep_y, side.witness, side.forward));
        // pair shape: (K; V ++ user_chain_in[0..input_depth])
        let pair_keyed = self.project(pair, Projection {
            key: pack_kv(k, v),
            val: (0..input_depth).map(|i| FieldExpr::Index(1, v + i)).collect(),
        });
        let dep_keyed = self.project(dep_y, Projection {
            key: pack_kv(k, v),
            val: (0..output_depth + 1).map(|i| FieldExpr::Index(1, v + i)).collect(),
        });
        self.debug_inspect(dep_keyed, format!("dep_keyed_dep={}", dep_y));
        self.debug_inspect(pair_keyed, format!("pair_keyed_dep={}", dep_y));
        // After arrange-join on (K+V): $0 = K+V, $1 = dep val, $2 = pair val.
        // Keep user_out in val for the time filter, then strip.
        let key: Vec<FieldExpr> = (0..k).map(|i| FieldExpr::Index(0, i)).collect();
        let mut val: Vec<FieldExpr> = Vec::new();
        for i in 0..v { val.push(FieldExpr::Index(0, k + i)); }
        for i in 0..input_depth { val.push(FieldExpr::Index(2, i)); }
        for i in 0..output_depth { val.push(FieldExpr::Index(1, i)); }
        val.push(FieldExpr::Index(1, output_depth)); // q
        let joined = self.join(dep_keyed, pair_keyed, Projection { key, val });
        self.filter_time_and_strip(joined, k, v, input_depth, output_depth)
    }

    /// Apply the soundness filter (`user_in[i] ≤ user_out[i]` element-wise) and
    /// strip `user_out` from a collection whose row shape is
    /// `(K[k_out]; V_pre[v_pre] ++ user_in[in_len] ++ user_out[out_len] ++ [q])`.
    /// Result shape: `(K[k_out]; V_pre[v_pre] ++ user_in[in_len] ++ [q])`.
    ///
    /// The filter excludes contributions whose witness input row was produced at
    /// a strictly-later user-iter than the demanded output — an output cannot be
    /// "explained by" an input that came after it. When `in_len` and `out_len`
    /// differ, we compare only the common prefix (innermost-first ordering).
    fn filter_time_and_strip(
        &mut self,
        coll: Id,
        k_out: usize,
        v_pre: usize,
        in_len: usize,
        out_len: usize,
    ) -> Id {
        let mut cur = coll;
        let cmp_len = in_len.min(out_len);
        if cmp_len > 0 {
            let mut acc: Option<Condition> = None;
            for i in 0..cmp_len {
                let cond = Condition::Le(
                    FieldExpr::Index(1, v_pre + i),               // user_in[i]
                    FieldExpr::Index(1, v_pre + in_len + i),      // user_out[i]
                );
                acc = Some(match acc {
                    None => cond,
                    Some(prev) => Condition::And(Box::new(prev), Box::new(cond)),
                });
            }
            cur = self.filter(cur, acc.unwrap());
        }
        // Strip user_out, preserving (K; V_pre ++ user_in ++ [q]).
        let key: Vec<FieldExpr> = (0..k_out).map(|i| FieldExpr::Index(0, i)).collect();
        let mut val: Vec<FieldExpr> = Vec::new();
        for i in 0..v_pre { val.push(FieldExpr::Index(1, i)); }
        for i in 0..in_len { val.push(FieldExpr::Index(1, v_pre + i)); }
        val.push(FieldExpr::Index(1, v_pre + in_len + out_len)); // q
        self.project(cur, Projection { key, val })
    }

    /// Keyed lookup (Reduce-style): demand on `(K; V_out ++ user_out ++ q)`
    /// maps to every input row at the same K, time-filtered against user_out.
    /// Output: `(K; V_in ++ user_in ++ [q])`.
    fn emit_lookup_keyed(
        &mut self,
        dep_y: Id,
        side: &Side,
        output_shape: (usize, usize),
        out_user_len: usize,
    ) -> Id {
        let (k_in, v_in) = side.shape;
        let in_user_len = side.user_len;
        let (_, v_out) = output_shape;
        let pair = self.concat(vec![side.witness, side.forward]);
        // After arrange-join on K: $0 = K, $1 = dep val (V_out + user_out + q),
        // $2 = pair val (V_in + user_in).
        // Keep user_out in val for the time filter, then strip.
        let mut val: Vec<FieldExpr> = Vec::new();
        for i in 0..v_in + in_user_len { val.push(FieldExpr::Index(2, i)); }
        for i in 0..out_user_len { val.push(FieldExpr::Index(1, v_out + i)); }
        val.push(FieldExpr::Index(1, v_out + out_user_len));
        let proj = Projection {
            key: (0..k_in).map(|i| FieldExpr::Index(0, i)).collect(),
            val,
        };
        let joined = self.join(dep_y, pair, proj);
        self.filter_time_and_strip(joined, k_in, v_in, in_user_len, out_user_len)
    }

    /// Lossy lookup (Linear[Project]).
    ///
    /// pair = (witness_input + forward_input) projected to
    /// `(chained_K_out; K_in ++ V_in ++ user_in)`. dep_y has shape
    /// `(K_out; V_out ++ user_out ++ q)`. After join on K_out:
    /// `$0=K_out, $1=dep_val, $2=pair_val`.
    /// Output: `(K_in; V_in ++ user_in ++ [q])`.
    fn emit_lookup_lossy(
        &mut self,
        dep_y: Id,
        side: &Side,
        output_shape: (usize, usize),
        out_user_len: usize,
        proj: &Projection,
    ) -> Id {
        let (k_in, v_in) = side.shape;
        let in_user_len = side.user_len;
        let (_, v_out) = output_shape;
        let pair_src = self.concat(vec![side.witness, side.forward]);
        // pair_src shape: (K_in; V_in ++ user_in[0..in_user_len]).
        // Build pair: key = proj.key (computes chained K_out), val = K_in ++ V_in ++ user_in.
        let mut pair_val: Vec<FieldExpr> = Vec::with_capacity(k_in + v_in + in_user_len);
        for i in 0..k_in { pair_val.push(FieldExpr::Index(0, i)); }
        for i in 0..v_in + in_user_len { pair_val.push(FieldExpr::Index(1, i)); }
        let pair = self.project(pair_src, Projection { key: proj.key.clone(), val: pair_val });
        // After arrange-join on K_out: $0 = K_out, $1 = dep val (V_out + user_out + q),
        // $2 = pair val (K_in + V_in + user_in).
        // Keep user_out in val for the time filter, then strip.
        let key: Vec<FieldExpr> = (0..k_in).map(|i| FieldExpr::Index(2, i)).collect();
        let mut val: Vec<FieldExpr> = Vec::new();
        for i in 0..v_in { val.push(FieldExpr::Index(2, k_in + i)); }
        for i in 0..in_user_len { val.push(FieldExpr::Index(2, k_in + v_in + i)); }
        for i in 0..out_user_len { val.push(FieldExpr::Index(1, v_out + i)); }
        val.push(FieldExpr::Index(1, v_out + out_user_len));
        let joined = self.join(dep_y, pair, Projection { key, val });
        self.filter_time_and_strip(joined, k_in, v_in, in_user_len, out_user_len)
    }

    /// Join's backward rule: produces two contribs (left and right).
    fn emit_lookup_join(
        &mut self,
        dep_y: Id,
        left: &Side,
        right: &Side,
        output_shape: (usize, usize),
        out_user_len: usize,
        projection: &Projection,
    ) -> (Id, Id) {
        let (k_arity, v_l) = left.shape;
        let (_, v_r) = right.shape;
        let (_, v_out) = output_shape;
        let left_user_len = left.user_len;
        let right_user_len = right.user_len;
        // Left and right inputs have shape (K; V_L/R ++ user_L/R[user_len]).
        let left_pair_src = self.concat(vec![left.witness, left.forward]);
        let right_pair_src = self.concat(vec![right.witness, right.forward]);
        // Forward-join them on K, projecting to
        //   (chained_K_out; K ++ V_L ++ V_R ++ user_L ++ user_R).
        // In the join's projection $0 = K, $1 = left val (V_L + user_L), $2 = right val.
        let mut pair_val: Vec<FieldExpr> = Vec::new();
        for i in 0..k_arity { pair_val.push(FieldExpr::Index(0, i)); }
        for i in 0..v_l { pair_val.push(FieldExpr::Index(1, i)); }
        for i in 0..v_r { pair_val.push(FieldExpr::Index(2, i)); }
        for i in 0..left_user_len { pair_val.push(FieldExpr::Index(1, v_l + i)); }
        for i in 0..right_user_len { pair_val.push(FieldExpr::Index(2, v_r + i)); }
        // The user's projection.key may reference `$1` / `$2` as whole-row
        // expansions (FieldExpr::Pos), which is correct against the *original*
        // V_L / V_R but wrong against the lift-extended host V's (which append
        // user-iter coords). Rewrite Pos(i) -> bounded Index(i, 0..arity_i) so
        // the projection only sees the original-shape fields and yields the
        // same K_out as the user's forward join.
        let pos_arities = [k_arity, v_l, v_r];
        let key_expanded = expand_pos_bounded(&projection.key, &pos_arities);
        let pair = self.join(
            left_pair_src,
            right_pair_src,
            Projection { key: key_expanded, val: pair_val },
        );
        // pair val arity: k_arity + v_l + v_r + left_user_len + right_user_len.
        // After arrange-join with dep_y on K_out:
        //   $0 = K_out, $1 = dep val (v_out + out_user_len + 1),
        //   $2 = pair val (k_arity + v_l + v_r + left_user_len + right_user_len).
        let q_pair_pos = v_out + out_user_len;
        let k_pair_start = 0;
        let vl_pair_start = k_pair_start + k_arity;
        let vr_pair_start = vl_pair_start + v_l;
        let ul_pair_start = vr_pair_start + v_r;
        let ur_pair_start = ul_pair_start + left_user_len;
        // Left contrib: (K; V_L + user_L + user_out + [q]), then filter+strip.
        let key_left: Vec<FieldExpr> =
            (0..k_arity).map(|i| FieldExpr::Index(2, k_pair_start + i)).collect();
        let mut val_left: Vec<FieldExpr> = Vec::new();
        for i in 0..v_l { val_left.push(FieldExpr::Index(2, vl_pair_start + i)); }
        for i in 0..left_user_len { val_left.push(FieldExpr::Index(2, ul_pair_start + i)); }
        for i in 0..out_user_len { val_left.push(FieldExpr::Index(1, v_out + i)); }
        val_left.push(FieldExpr::Index(1, q_pair_pos));
        let left_joined = self.join(dep_y, pair, Projection { key: key_left, val: val_left });
        let left_contrib = self.filter_time_and_strip(left_joined, k_arity, v_l, left_user_len, out_user_len);
        // Right contrib: (K; V_R + user_R + user_out + [q]), then filter+strip.
        let key_right: Vec<FieldExpr> =
            (0..k_arity).map(|i| FieldExpr::Index(2, k_pair_start + i)).collect();
        let mut val_right: Vec<FieldExpr> = Vec::new();
        for i in 0..v_r { val_right.push(FieldExpr::Index(2, vr_pair_start + i)); }
        for i in 0..right_user_len { val_right.push(FieldExpr::Index(2, ur_pair_start + i)); }
        for i in 0..out_user_len { val_right.push(FieldExpr::Index(1, v_out + i)); }
        val_right.push(FieldExpr::Index(1, q_pair_pos));
        let right_joined = self.join(dep_y, pair, Projection { key: key_right, val: val_right });
        let right_contrib = self.filter_time_and_strip(right_joined, k_arity, v_r, right_user_len, out_user_len);
        (left_contrib, right_contrib)
    }
    }

    /// Rewrite `FieldExpr::Pos(i)` in a key/val list to a bounded expansion
    /// `[Index(i, 0), Index(i, 1), ..., Index(i, arities[i]-1)]`. Used in
    /// `emit_lookup_join` where the user's projection is applied to host-side
    /// (lift-extended) inputs — `Pos(i)` against the extended row would also
    /// pick up the trailing user-iter coords, which we explicitly do not want
    /// inside the key.
    fn expand_pos_bounded(fields: &[FieldExpr], arities: &[usize]) -> Vec<FieldExpr> {
        let mut out = Vec::with_capacity(fields.len());
        for f in fields { expand_pos_one(f, arities, &mut out); }
        out
    }

    fn expand_pos_one(f: &FieldExpr, arities: &[usize], out: &mut Vec<FieldExpr>) {
        match f {
            FieldExpr::Pos(i) => {
                for c in 0..arities[*i] { out.push(FieldExpr::Index(*i, c)); }
            }
            FieldExpr::Index(_, _) | FieldExpr::Const(_) => out.push(f.clone()),
            FieldExpr::Neg(inner) => {
                let mut tmp = Vec::new();
                expand_pos_one(inner, arities, &mut tmp);
                for t in tmp { out.push(FieldExpr::Neg(Box::new(t))); }
            }
            FieldExpr::Sub(a, b) => {
                let mut ta = Vec::new();
                let mut tb = Vec::new();
                expand_pos_one(a, arities, &mut ta);
                expand_pos_one(b, arities, &mut tb);
                for (x, y) in ta.into_iter().zip(tb.into_iter()) {
                    out.push(FieldExpr::Sub(Box::new(x), Box::new(y)));
                }
            }
        }
    }

    fn pack_kv(k: usize, v: usize) -> Vec<FieldExpr> {
        let mut out: Vec<FieldExpr> = Vec::with_capacity(k + v);
        for i in 0..k { out.push(FieldExpr::Index(0, i)); }
        for i in 0..v { out.push(FieldExpr::Index(1, i)); }
        out
    }
}

// Projection helpers (used by `explain` itself, not by reverse rules).

impl Builder {
    /// Insert an `Inspect` node for diagnostic output, but only when the
    /// `EXPLAIN_DEBUG_DEP` env var is set. Used to surface dep / pair
    /// tables at construction sites without bloating the IR in normal runs.
    pub fn debug_inspect(&mut self, input: Id, label: String) {
        if std::env::var("EXPLAIN_DEBUG_DEP").is_ok() {
            self.inspect(input, label);
        }
    }

    /// Semijoin `left (K; V)` with `right (K; V)` by `(K)`, keep left's rows.
    /// (Used at demand-set seeding.)
    pub fn semijoin_data(&mut self, left: Id, right: Id, k_arity: usize, v_arity: usize) -> Id {
        let proj_key: Vec<FieldExpr> = (0..k_arity).map(|i| FieldExpr::Index(0, i)).collect();
        let proj_val: Vec<FieldExpr> = (0..v_arity).map(|i| FieldExpr::Index(1, i)).collect();
        self.join(left, right, Projection { key: proj_key, val: proj_val })
    }

    /// Set-level distinct on `(K; V)` rows (DDIR `distinct` is per-key only;
    /// pack-distinct-unpack preserves the val).
    pub fn distinct_full(&mut self, input: Id, k_arity: usize, v_arity: usize) -> Id {
        let mut pack_key: Vec<FieldExpr> = (0..k_arity).map(|i| FieldExpr::Index(0, i)).collect();
        for i in 0..v_arity {
            pack_key.push(FieldExpr::Index(1, i));
        }
        let packed = self.project(input, Projection { key: pack_key, val: vec![] });
        let dist = self.reduce(packed, Reducer::Distinct);
        let unpack_key: Vec<FieldExpr> = (0..k_arity).map(|i| FieldExpr::Index(0, i)).collect();
        let unpack_val: Vec<FieldExpr> = (0..v_arity).map(|i| FieldExpr::Index(0, k_arity + i)).collect();
        self.project(dist, Projection { key: unpack_key, val: unpack_val })
    }
}

/// Strip user-chain and `[q]` from a dep row's val: `(K; V ++ user ++ [q])` -> `(K; V)`.
fn strip_user_and_q(k_arity: usize, v_arity: usize) -> Projection {
    let key: Vec<FieldExpr> = (0..k_arity).map(|i| FieldExpr::Index(0, i)).collect();
    let val: Vec<FieldExpr> = (0..v_arity).map(|i| FieldExpr::Index(1, i)).collect();
    Projection { key, val }
}

