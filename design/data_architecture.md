# Differential dataflow's data architecture

This document describes the logical data architecture of differential dataflow.
It leaves unstated several implementation considerations, intentionally, to focus on the requirements rather than options.
The goal of the document is to state requirements, rather than prescribe or imagine implementations.

This document is aspirational, rather than a description of the current implementation.
In particular, the concept of restricting batches by predicates on the data is entirely unimplemented.

## Key players

Differential dataflow's core data abstraction is a **collection**, a partial map from (data, time) to diff.
    `data`  are members of a set completely defined by its equality function.
            any element must equal itself, two equal elements are indistinguishable.
    `time`  may be any members of a specified lattice,
    `diff`  may be any members of a specified semigroup.
            the semigroup is often a monoid, and often an Abelian group, but can be only a semigroup.

Differential dataflow models an evolving collection, one not yet fully defined, by an append-only set of **updates**, triples `(data, time, diff)`, called a **trace**.
A collection can be recovered from the trace at some query `time` by the accumulation of `diff` values of the trace at times less than or equal to the query `time`.
The intended intuition is that the trace is the derivative of the collection, and the collection is the integral of the trace.
When diff is the integers this can be interpreted as a "multiplicity" of data, with absent data interpreted as having a multiplicity of zero.

As the trace can be appended to, the collection is never quite finished.
As time passes a lower bound called the "write frontier" advances, and imposes a lower bound on the times of new triples that might be added to the trace.
A **frontier** is an antichain of times from the lattice, which may only have a partial order; elements are greater or equal to the frontier if they are greater or equal to any element of it.
The collection's contents at times not greater or equal to the write frontier are "locked": they cannot change because no times may be added that will change their accumulation.
An observer of an append-only stream of updates (the trace) combined with the advancing write frontier, for example a timely dataflow operator, can repeatedly identify bundles of updates that are "finalized".
Specifically, the set of updates at times greater or equal to the previously observed write frontier, but not greater or equal to the new write frontier, can be branded as a **batch** of updates that are a faithful representation of the changes in that interval of times.
There is no rule to how often batches should be created, other than that they should be created with some regularity in response to write frontier movement in order to keep the system moving along.

Differential dataflow collects **batches** into a structured form of collection called an **arrangement**.
An arrangement is an evolving set of immutable batches, each of which is described by two frontiers:
    **lower**: a frontier such that all update times in the batch are greater or equal to it.
    **upper**: a frontier such that no update times in the batch are greater or equal to it.
A correct batch contains exactly the updates at times greater or equal to lower, but not greater or equal to upper.
Any correct batch remains correct forever, as their updates cannot change.

A batch can only be formed after the write frontier of the collection reaches the upper frontier, otherwise it could not certainly contain all updates.
A batch has at most one diff for each (data, time), and if diff is a monoid it must be non-zero; this property is called **consolidation**.
Any (data, time) that are absent are not unknown: they are certainly absent from the partial map (and should be interpreted as zero in a monoid).

Optionally, a batch may have a third frontier:
    **since**: a frontier such that all accumulations at times greater or equal to it are correct.
The times in the batch may be altered based on since, and are only guaranteed to accumulate correctly at times greater or equal to since.
This frontier allows for the historical detail to be erased, without making the current accumulations incorrect.
Most often this is realized by "rounding" times to some common granularity.
Typically the since is absent when a batch is created, as the detail is important, but as time passes new batches may be formed with since frontiers that allow the compaction of updates, to be described (in "As time passes").

Optionally, a batch may have a predicate on data:
    **keys**: a predicate identifying the subset of data for which the batch is certainly accurate.
This predicate is used to isolate the updates for popular subsets of the data, without losing certainty about the information at hand.
It is motivated by the size of data, and the value of isolating active subsets of it, described further in "Location of data".

There is no requirement that batches have `[lower, upper)` intervals that are disjoint, but each one must be correct.
However, to be *useful* we must be able to find a **contiguous** sequence of batches that each have their upper equal the lower of the next batch.
A contiguous sequence of batches can be viewed as one larger batch, logically if not physically, with
    the lower of the first batch,
    the upper of the last batch,
    a since that is the maximum of all involved sinces, and
    a keys that is the intersection of involved keys (the predicates must all be true to be certain we have all updates).
A contiguous sequence of batches that starts from a lower equal to the minimum time lattice element allows for accurate reconstruction of the collection at any time greater or equal to all of the since frontiers, for data satisfying the keys predicates.

## As time passes

As time passes, adjacent batches may be merged, forming a new batch with the first lower, the last upper, and the maximum of the two since frontiers.
Optionally, we may also choose to further advance the since frontier of the new batch, permitting us to further advance the times in updates.
As this happens, we may find updates with the same (data, time) in both inputs, and their diff fields should be accumulated, and discarded if the semigroup is a monoid and they accumulate to zero.
Generally the merging of batches improves our ability to work with them, reducing the number of locations to look for updates, but merging costs resources so we perform it only over time as batches age.

Generally the merging of batches wants to maintain a "work proportionality": not spending more time merging than is justified by the number of arriving updates.
This often leads to an exponential scheme, where adjacent batches of similar sizes are merged.
This results in pairwise merging, with older batches becoming increasingly large.

There are other practical considerations, not fully detailed here, that result in better or worse performing implementations.

## Location of data

As batches age and merge, they tend to become larger.
This is not inevitable, as batches also consolidate, and in consolidating may yield and discard updates that accumulate to a zero diff.
Nonetheless, we anticipate that older batches become unmanageably large, and need to accommodate their size through remote storage.

Some of these batches may be small and "local", while others may be large and "remote": somewhere like cloud storage, disk, or compressed that provide relatively slow random access.
We imagine the local batches as in memory and accessed directly, and the large batches as remote and needing to be fetched and deserialized.

Commonly in arrangements the data type is a pair (key, val) corresponding to a data-parallel key, and some associated value.
Batches are designed to support random access by key, retrieving all associated values (there may be many) and their many time and diff values.
The optional keys predicate in a batch is often an explicit list of specific keys from (key, val) pairs in the data, satisfied by any (key, val) whose key is present in the list.

When operators work with collections of batches, they can always identify a subset of keys that suffice for their work.
To perform their work they typically need the updates for a collection of keys, with updates accurate from some operator-identified frontier onward.
To satisfy the operator's requirements we need to retrieve the subset of key updates from a collection of aligned batches with since values less than or equal to the query frontier.
Having done so, we can record them in a new batch that identifies the lower, upper, since, and now a predicate listing the keys that were fetched.

These "cached" batches are correct for their subset of keys, and smaller; potentially small enough to store "locally" where their backing batches must remain remote.
Their correctness means that they too will never become incorrect; the cached results are never invalid.
Contiguous sequences of batches can also be cached as one batch, with the first lower, last upper, and maximum since frontier.
Any requests for keys present in cached batches can be returned from the cached batch, correctly, subject to the since frontier.
They can be discarded at any point, as the backing storage can always reproduce their contents, at some cost.

## Arrangement function

The main task of an arrangement is to manage the "sea of batches".
There is a great deal of flexibility in picking the policy, and so we focus on the requirements.

Batches are shared among many "clients" of the arrangement, for example downstream consumers who wish to respond to and act on changes to the data.
Batches are immutable, so this sharing can be as simple as reference counting.
The sharing prevents the batches from disappearing out from under a client; once they have a batch they can finish their work with it.
The arrangement itself also holds references to some of these batches, and must be able to mint new references subject to some constraints.

Each client of an arrangement maintains a **read frontier**, the read analogue of a write frontier: the client expects to be able to form a correct accumulation of the collection at all times greater or equal to this frontier.
The arrangement maintains the invariant that a contiguous sequence of batches exists, starting from the minimum time, with since values not greater than any client's read frontier.
Batches should only be discarded when they are redundant with respect to this invariant.
This invariant allows the arrangement to respond to client requests for a contiguous sequence of batches, starting from the minimum time, with since values not greater than its read frontier.
The clients regularly increment their read frontiers, allowing the arrangement to advance the since frontiers it uses when merging, and to discard batches that are now redundant with respect to its obligations.
We say the arrangement has a read frontier that is the lower bound of the read frontiers of its clients.

The arrangement is also able to manage the "cached" batches that have keys predicates.
These are necessarily redundant, by design, in that an arrangement cannot safely discard batches with no predicates for fear of not being able to reproduce a contiguous sequence valid for all data.
The arrangement then is able to determine which of these cached batches should stay, potentially using any conventional cache replacement strategy based on size, recency of use, efficacy.
It is up to the arrangement to determine if cached batches should merge, narrowing their predicates, or should only be formed in response to requests for subsets of keys.

The primary function of the arrangement is therefore to:
1. Manage the merging of batches to form new batches, potentially with advanced since frontiers.
2. Discard now redundant batches, to reclaim resources.
3. Form cached batches for subsets of keys, and allow them to lapse.

An ideal implementation would ensure that it neither spends too many resources doing this, nor fails to spend the appropriate resources when needed.
It should continually perform merges as new batches are created, ideally maintaining a number at most logarithmic in the number of updates that are distinguishable with respect to the arrangement's read frontier.
It should continually advance the since frontier to track the read frontiers of its clients, so that updates can consolidate as soon as possible.
It should appropriately form cached batches in response to reads from large, remote batches, while remaining within resource bounds by discarding ineffective cached batches.
