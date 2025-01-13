use timely::dataflow::operators::probe::Handle;

use differential_dataflow::{
    Collection,
    input::InputSession,
    operators::{Join, Iterate, Reduce},
};

// We want to represent collections of ast nodes, perhaps as `(Id, (Op, Vec<Id>))`.
// Here the first `Id` is a unique identifier for the node, and the `Vec<Id>` are
// identifiers for the children. We'll want to be able to mutate the child identifiers.

// Perhaps we represent this as collections
// 1. Ops: (Id, Op),
// 2. Args: (Id, usize, Id),
// Congruence amounts to 

fn main() {
    // Define a timely dataflow computation
    timely::execute_from_args(std::env::args(), move |worker| {

        // Create an input collection of data
        let mut input = InputSession::new();
        let mut equivs = InputSession::new();

        // Probe to determine progress / completion.
        let mut probe = Handle::new();

        // Define a new computation
        worker.dataflow(|scope| {

            // Create a new collection from our input
            // Our AST nodes will be modeled as a collection of `(Id, (Op, Vec<Id>))` records.
            // Each entry is an AST node, with an identifier, an operator, and a list of child identifiers.
            let ast_nodes: Collection<_, (usize, (String, Vec<usize>))> = input.to_collection(scope);

            // Exogenous equivalences, mapping identifiers to (lesser) canonical identifiers.
            // This map is not necessarily transitively closed, nor complete for all identifiers.
            let equivs = equivs.to_collection(scope);

            // Iteratively develop a map from `Id` to `Id` that canonicalizes identifiers.
            // This involves both exogenous equivalences, and those from equivalent AST nodes.
            ast_nodes
                .map(|(id, _)| (id, id))
                .iterate(|canonical| {

                    // Collection is loop invariant, but must be brought in scope.
                    let ast_nodes = ast_nodes.enter(&canonical.scope());
                    let equivs = equivs.enter(&canonical.scope());

                    // Separate AST node operators and their arguments.
                    let ops = ast_nodes.map(|(id, (op, _))| (id, op));
                    let args = ast_nodes.flat_map(|(id, (_, args))| args.into_iter().enumerate().map(move |(index, arg)| (arg, (id, index))));

                    // Update argument identifiers, and then equate `(Ops, Args)` tuples to inform equivalences.
                    let equivalent_asts =
                    args.join_map(canonical, |_child, &(node, index), &canonical| (node, (index, canonical)))
                        .reduce(|_node, input, output| {
                            let mut args = Vec::new();
                            for ((_index, canonical), _) in input.iter() {
                                args.push(*canonical);
                            }
                            output.push((args, 1isize));
                        })
                        .join_map(&ops, |node, children, op| ((children.clone(), op.clone()), *node))
                        .concat(&ast_nodes.filter(|(_, (_, args))| args.is_empty()).map(|(node, (op, _))| ((vec![], op), node)))
                        .reduce(|_key, input, output| {
                            for node in input.iter() {
                                output.push(((*(node.0), *input[0].0), 1));
                            }
                        })
                        .map(|(_key, (node, canonical))| (node, canonical));

                    // Blend together the two forms of equivalence, and compute the transitive closure.
                    equivalent_asts
                        .concat(&equivs)
                        .reduce(|_node, input, output| { output.push((*input[0].0, 1)); } )
                        .iterate(|inner| {
                            inner.map(|(node, canonical)| (canonical, node))
                                .join_map(&inner, |_canonical, &node, &canonical| (node, canonical))
                        })
                })
                .consolidate()
                .inspect(|x| println!("{:?}", x))
                .probe_with(&mut probe);
        });

        input.advance_to(0);
        input.insert((0, ("a".to_string(), vec![])));
        input.insert((1, ("b".to_string(), vec![])));
        input.insert((2, ("c".to_string(), vec![])));
        input.insert((3, ("add".to_string(), vec![0, 2])));
        input.insert((4, ("add".to_string(), vec![1, 2])));

        equivs.advance_to(0);

        input.advance_to(1);
        equivs.advance_to(1);
        input.flush();
        equivs.flush();

        worker.step_while(|| probe.less_than(&input.time()));
        println!("");
        println!("Marking 0 equivalent to 1");

        equivs.insert((1, 0));
]
        input.advance_to(2);
        equivs.advance_to(2);
        input.flush();
        equivs.flush();

        worker.step_while(|| probe.less_than(&input.time()));
        println!("");
        println!("Un-marking 0 equivalent to 1");

        equivs.remove((1, 0));

    }).expect("Computation terminated abnormally");
}