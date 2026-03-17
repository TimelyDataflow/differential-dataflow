## Data ownership in differential dataflow

Differential dataflow fundamentally move *data* around.
Data is at the heart of the mechanisms in differential dataflow, and we should be clear on the roles it plays.

Generally, in Rust and in live data can have a few forms:
1. Owned: Data that can be mutated, returned, moved around.
2. Borrowed: Data that can be read, though not mutated or transfered.

In differential dataflow we typically have "containers" of data.
Containers are also "owning" or "borrowed".
As an example, Vec<T> is an owning container, and &[T] would be its borrowing container.
Owning containers provide more power, but we prefer not to rely on this power as it closes other doors (e.g. around sharing, zero copy serialization).

At the moment there are at least three forms of container, one of which we actively hope to deprecate (columnation) and one of which we wish to stop relying on ownership with (vec).
1.  Vec:    This is an easy container to use, but it makes it easy to use owned data in contexts where we should not rely on having ownership.
            The reference type of elements in a Vec is &T, which coincides with a lot of Rust idioms, but which we cannot always rely on having.
2.  columnation This is an unsafe container we hope to completely remove.
3.  columnar    This is a safe container we hope to support, perhaps centrally.
                Reference types here are T::Ref<_> which is a GAT that may not be a &T, and we cannot rely on that being the case.

Our long term goal is to have data reside in containers, with the potential for transient owned data in the interest of ergonomics.
Rather than have users take references and return owned data, we prefer to have them read references and write into containers.
This may be annoying for the casual author, and we would like convenience methods that can create owned data, which can then be copied into containers.

## migration

Our goal is to migrate the codebase to one where
0. data are by default in containers, only accessible via reference.
1. we avoid relying on ownership other than for ergonomics,
2. we support a zero copy serialization framework (columnar is an example) to be able to get references from serialized data,
3. we avoid the current tower of abstraction where types name both owned and reference variants (although, we may eventually return here, once we have more command of the code).

As a candidate migration, we could:
1.  remove the generality of arbitrary containers, and return to `Vec<T>` as the one container type.
    we should avoid introducing any new reliance on owned data, mutable references, or other behavior that would not work on a slice &[T] as a container.
    perhaps as a rule: containers must be borrowed before they can be used; they cannot be drained, mutated in place, things like that.
    we have the ability to convert `&T` into `T` througLeh `.cloned()` but are aiming to avoid introducing this at all, but we can allow existing uses to remain.
    the has the potential to dramatically reduce the tower of abstractions.

    This change alone would be a "regression" in that the code used to look like this;
    our goal is to get the code to a borrow-forward state where we do not rely on ownership.

2.  introduce the `Columnar` constraint on data.
    this provides a new container type, T::Container, and a new way to present references.
    it will also smoke out moments where we used &T but it does not generalize to T::Ref.
    problem here include lifetimes, the "invariant" nature of their lifetimes, which we can work around with the `reborrow` method.
    as with &T we are able to call `into_owned()` to produce owned data if needed, but ideally we would have isolated those cases in the previous step.
    we may discover moments where &T is hard to change to T::Ref for other reasons, and should call those out if they are architectural blockers.

3.  aspire to make the Vec / Col distinction be as small as a feature flag that controls the required types and names of the containers.
    no rush to make this happen, but the goal is to arrive at the clarity around borrowing that makes the difference between these a matter of opinion.
    ideally we could return to where we are today, with container types that announce owned and borrowed variants, using Rust's trait system rather than feature flags.

We currently have a container abstraction that announces owned and borrowed types because we cannot insist that all owned types announce borrowed and container types, as they seem to fight on this matter due to coherence.
E.g. `i32` is a type, and it has specific opinions already about how to borrow it.
If we wanted to make containers pluggable, they seem to be the insertion points for introducing opinions about owned and borrowed data, and an abstraction barrier that allows different opinions to co-exist (one can read from one container and push to another, if the reference types align).

There are moments in the current codebase where we use &T because we currently *must* mutate data, and return a reference to that owned data.
We do eventually want to remove these, and replace the owned data with data in containers, but we do not need to start with this.
Generally, we'll want to start with a *migration* and leave *improvements* until we see if the migration helps.