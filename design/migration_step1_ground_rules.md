# Step 1: Simplify to Vec<T>

## Context

This is step 1 of a three-step migration toward columnar storage in differential dataflow (see `data_ownership.md` for the full plan). The goal is to remove abstraction complexity and establish borrow-forward patterns, making the codebase ready for columnar types without introducing them yet.

## Goal

Collapse to `Vec<T>` as the sole container type. Remove the tower of generic container abstractions. Establish the principle that containers are borrowed before they are used.

This step is a "regression" in that the code used to look like this before the generic container abstractions were introduced. The purpose is to arrive at a borrow-forward state where we do not rely on ownership of container contents.

## What to remove

- `Layout`, `WithLayout`, `LayoutExt` traits and all their implementations
- `Vector<U>` and `TStack<U>` layout structs
- `BuilderInput` trait
- `TimelyStack<T>` as a `BatchContainer` implementation
- All columnation-based paths: `ColMerger`, `ColumnationChunker`, `ColValSpine`, `ColKeySpine`, `ColValBatcher`, `ColKeyBatcher`, etc.
- The `layout` type alias module

## What to keep

- `BatchContainer` trait (implemented only for `Vec<T>` and `OffsetList`)
- `OffsetList` (compact offset storage)
- The cursor module and `Cursor` trait
- The merge/builder/batcher traits (simplified)
- The spine (`Spine<B>`)
- `SliceContainer` if used

## Principles

### Containers are borrowed before they are used

A container, once populated, should be accessed only through borrows. We should not rely on draining, mutating in place, or other ownership-dependent operations on containers that hold data we want to read.

We have the ability to convert `&T` into `T` through `.clone()`, but we are aiming to avoid introducing new uses of this. Existing uses may remain — the migration is about establishing the pattern, not perfecting it.

### Owned scalars for mutation

Times and diffs are commonly mutated via `plus_equals`, `advance_by`, `join_assign`, `negate`. These mutations happen on *owned individual values*, not on containers. The pattern is:

1. Read a reference from a container
2. Clone into an owned value
3. Mutate the owned value
4. Push the result into an output container

This pattern is correct and should be preserved. Times and diffs live in small, bounded-scope "scratch" collections (e.g., `EditList`, `UpdsBuilder::stash`) for accumulation and consolidation. These are not the containers we are trying to discipline — they are working memory for owned scalars.

### Draining: replace with iterate-and-push

Where the current code drains a container to transport data to a different form (e.g., `Builder::push` draining a Vec, `seal` draining a chain), replace with iterating the source by reference and pushing into the target.

This may be suboptimal (extra clones), but it is correct and establishes the borrow-forward pattern. Flag these sites with comments for future optimization.

The intent of draining is almost always "transport data from one container form to another." Iterating + pushing achieves the same result without relying on ownership of the source.

### Simplify even naively

The builder and batcher are the main sites of abstraction complexity. Simplify them, even if the result is naive. If the builder can iterate over `&[((K,V),T,R)]` and push refs into batch storage, that's sufficient.

Performance optimization is not the goal of this step. Clarity and correctness are.

### Leave improvements for later

Some patterns (like the reduce operator's drain-and-negate output) are real design questions that would benefit from architectural changes. These are *improvements*, not part of the migration. Leave them as-is and return to them once the migration is complete and we can evaluate with fresh eyes.

Similarly, the cursor trait works fine with `&T` references. Don't try to change cursor signatures in this step.

## What NOT to do

- Do not introduce the `Columnar` trait or constraint
- Do not change `&T` to `columnar::Ref<'_, T>`
- Do not replace `Vec<T>` with `T::Container`
- Do not try to fix architectural issues (cursor design, reduce output, join API layers)
- Do not optimize transport patterns (iterate+push may clone more than drain, and that's fine)

These are all step 2 or step 3 work.

## Audit findings

An audit of the codebase identified three usage patterns:

1. **Borrow-compatible** (reading): Cursor navigation, batch metadata, `map_batches`. These only index into or iterate over containers. Fully compatible with borrow-first. This is the majority of the read path.

2. **Owned scalar mutation** (legitimate): `time.advance_by()`, `diff.plus_equals()`, consolidation. These create owned values, mutate them, then push into output containers. The container itself is never mutated. Compatible with borrow-first.

3. **Container draining** (needs attention): `Builder::push` drains input, `seal` drains chains, `MergeBatcher` takes ownership via `std::mem::take`. These are all on the write path (producing batches). Replace with iterate-and-push where possible, flag for future work where not.

## Success criteria

After step 1:
- The code compiles with `Vec<T>` as the only data container
- The Layout/WithLayout/LayoutExt tower is gone
- No new reliance on owned data has been introduced
- Container contents are accessed through borrows (indexing, iteration), not through draining or mutation
- The trait signatures use `&T` for borrowed data (which will become `T::Ref` in step 2)
- Sites where we had to clone to satisfy borrow-forward are flagged with comments
