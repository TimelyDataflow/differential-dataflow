//! Multi-way equijoin expression plan.
//!
//! This plan provides us the opportunity to map out a non-trivial differential
//! implementation for a complex join query. In particular, we are able to invoke
//! delta-query and worst-case optimal join plans, which avoid any intermediate
//! materialization.
//!
//! Each `MultiwayJoin` indicates several source collections, equality constraints
//! among their attributes, and then the set of attributes to produce as results.
//!
//! One naive implementation would take each input collection in order, and develop
//! the join restricted to the prefix of relations so far. Ideally the order would
//! be such that joined collections have equality constraints and prevent Cartesian
//! explosion. At each step, a new collection picks out some of the attributes and
//! instantiates a primitive binary join between the accumulated collection and the
//! next collection.
//!
//! A more sophisticated implementation establishes delta queries for each input
//! collection, which responds to changes in that input collection against the
//! current other input collections. For each input collection we may choose very
//! different join orders, as the order must follow equality constraints.
//!
//! A further implementation could develop the results attribute-by-attribute, as
//! opposed to collection-by-collection, which gives us the ability to use column
//! indices rather than whole-collection indices.

use std::hash::Hash;

use timely::dataflow::Scope;

use differential_dataflow::operators::JoinCore;

use differential_dataflow::{Collection, Data};
use plan::{Plan, Render};
use {TraceManager, Time, Diff};

/// A multiway join of muliple relations.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MultiwayJoin<Value> {
    /// Attributes to extract as `(attr, input)`.
    pub results: Vec<AttrInput>,
    /// Source collections.
    pub sources: Vec<Box<Plan<Value>>>,
    /// Equality constraints.
    ///
    /// Equality constraints are presented as lists of `(attr, input)` equivalence classes.
    /// This means that each `(attr, input)` pair can exist in at most one list; if it would
    /// appear in more than one list, those two lists should be merged.
    pub equalities: Vec<Vec<AttrInput>>,
}

impl<V: Data+Hash> Render for MultiwayJoin<V> {

    type Value = V;

    fn render<S: Scope<Timestamp = Time>>(
        &self,
        scope: &mut S,
        arrangements: &mut TraceManager<Self::Value>) -> Collection<S, Vec<Self::Value>, Diff>
    {
        // The plan here is that each stream of changes comes in, and has a delta query.
        // For each each stream, we need to work through each other relation ensuring that
        // each new relation we add has some attributes in common with the developing set.

        // Materialize each source as an update stream (a `Collection`).
        // Sniff around to see if we have them available beforehand.
        let mut sources = Vec::new();
        for plan in self.sources.iter() {
            // Cache the plan; not obviously required, but whatever.
            if arrangements.get_unkeyed(&plan).is_none() {
                arrangements.set_unkeyed(plan, plan.render(scope, arrangements));
            }

            sources.push(arrangements.get_unkeyed(&plan).as_collection(|val,&()| val.clone()));
        }

        // Attributes we may need from any and all relations.
        let mut relevant_attributes = Vec::new();
        relevant_attributes.extend(self.results.iter().cloned());
        relevant_attributes.extend(self.equalities.iter().flat_map(|list| list.iter().cloned()));
        relevant_attributes.sort();
        relevant_attributes.dedup();

        // Into which we accumelate changes.
        let mut accumulated_changes = Vec::new();

        // For each source relation
        for (index, source) in self.sources.iter().enumerate() {

            // We only need attributes from a relation if they are an output,
            // or if they are equated with an attribute in another relation.
            // TODO: only retain attributes need for *future* joins.
            let attributes =
            relevant_attributes
                .iter()
                .filter(|x| x.input == index)
                .collect::<Vec<_>>();

            // Start from the necessary attributes from this relation.
            let mut deltas = source.map(move |values| attributes.iter(|x| values[x.attribute]).collect::<Vec<_>>());

            // Acquire a sane sequence in which to join the relations.
            let join_order = plan_join_order(index, &self.equalities);

            for join_idx in join_order.into_iter().skip(1) {

                let ((keys, pres), vals) = keys_pres_vals(join_idx, &self.equalities, &attributes, &self.results);

                // TODO: Semijoin if `vals.is_empty()`.
                // IMPORTANT: Make sure to wrap with ALT/NEU based on index < join_idx!
                let arrangement = unimplemented!();
                deltas = deltas.propose(&arrangement);

                // Flatten the representation and update `attributes`.
                unimplemented!();

            }

        }

        unimplemented!()
    }
}

struct AttributeInput {
    attribute: usize,
    input: usize,
}

/// Sequences relations in `constraints`.
///
/// Relations become available for sequencing as soon as they share a constraint with
/// either `source` or another sequenced relation.
fn plan_join_order(source: usize, constraints: &[Vec<(usize, usize)>]) -> Vec<usize> {

    let mut result = vec![source];
    let mut active = true;
    while active {
        active = false;
        for constraint in constraints.iter() {
            // Check to see if the constraint contains a sequenced relation.
            if constraint.iter().any(|(_,index)| result.contains(index)) {
                // If so, sequence any unsequenced relations.
                for &(_,index) in constraint.iter() {
                    if !result.contains(index) {
                        result.push(index);
                        active = true;
                    }
                }
            }
        }
    }

    result
}

/// Identifies keys and values for a join.
///
/// The result is a sequence, for each
fn sequence_keys_and_joins(
    relation: usize,
    constraints: &[Vec<(usize, usize)>],
    current_attributes: &[AttributeInput],
    relevant_attributes: &[AttributeInput],
)
-> (Vec<(usize, AttributeInput)>, Vec<usize>)
{

    // The fields in `sources[join_idx]` that should be keys are those
    // that share an equality constraint with an element of `attributes`.
    // For each key, we should capture the associated `attributes` entry
    // so that we can easily prepare the keys of the `delta` stream.
    let mut keys = Vec::new();
    for constraint in constraints.iter() {
        if let Some(prior) = constraint.iter().first(|x| current_attributes.contains(x)) {
            keys.extend(constraint.iter().filter(|&(_,index)| index == join_idx).map(|&x| (x, prior.clone())));
        }
    }

    // The fields in `sources[join_idx]` that should be values are those
    // that are required output or participate in an equality constraint,
    // but *WHICH ARE NOT* in `keys`.
    let vals =
    relevant_attributes
        .iter()
        .filter(|&(attr,index)| index == join_idx && !keys.contains(&(attr, index)))
        .collect::<Vec<_>>();

}
