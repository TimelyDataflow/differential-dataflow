# Trace wrappers

There are many cases where we make small manipulations of a collection, and we might hope to retain the arrangement structure rather than re-build and maintain new arrangements. In some cases this is possible, using what we call *trace wrappers*.

The set of trace wrappers grows as more idioms are discovered and implemented, but the intent is that we can often avoid reforming new collections, and instead just push logic into a layer around the arrangement.

## Filter

The filter wrapper has been deprecated.
It's still a neat filter, but its existence was constraining the development velocity.
Reach out if you worry this was wrong, and we can discuss alternatives!

## Entering scopes

Differential dataflow programs often contain nested scopes, used for loops and iteration. Collections in a nested scope have different timestamps than collections outside the scope, which means we can not immediately re-use arrangements from outside the scope inside the scope.

Like collections, arrangements support an `enter(scope)` method for entering a scope, which will wrap the arrangement so that access to timestamps automatically enriches it as if the collection had entered the scope.

The following example demonstrates arranging the `knows` relation outside an iterative scope, and then bringing it in to the scope (along with the collection `query`). Unlike `query`, which is a collection, `knows` is an arrangement and will simply be wrapped with timestamp-extending logic.

```rust
extern crate timely;
extern crate differential_dataflow;

use differential_dataflow::operators::Join;
use differential_dataflow::operators::Iterate;

fn main() {

    // define a new timely dataflow computation.
    timely::execute_from_args(::std::env::args(), move |worker| {

        let mut knows = differential_dataflow::input::InputSession::new();
        let mut query = differential_dataflow::input::InputSession::new();

        worker.dataflow(|scope| {

            let knows = knows.to_collection(scope);
            let query = query.to_collection(scope);

            // Arrange the data first! (by key).
            let knows = knows.arrange_by_key();

            // Reachability queries.
            query.iterate(|reach| {

                let knows = knows.enter(&reach.scope());
                let query = query.enter(&reach.scope());

                knows.join_map(reach, |x,y,q| (*y,*q))
                     .concat(&query)
                     .distinct()
            });

        });

#       // to help with type inference ...
#       knows.update_at((0,0), 0usize, 1isize);
#       query.update_at((0,0), 0usize, 1isize);
    });
}
```

## Other wrappers

Other wrappers exist, but are still in development and testing. Generally, if the same physical layout of the index would support a collection transformation, a wrapper may be appropriate. If you think you have such an operation, the `src/trace/wrappers/` directory is where the current examples reside.