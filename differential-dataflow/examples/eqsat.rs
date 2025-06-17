use timely::dataflow::operators::probe::Handle;

use differential_dataflow::{
    input::InputSession,
    operators::{Join, Iterate, Reduce, Threshold},
};

// Types for representing an AST as a collection of data.
type AstName = usize;
type AstNode = (String, Vec<AstName>);

fn main() {
    // Define a timely dataflow runtime
    timely::execute_from_args(std::env::args(), move |worker| {

        // Input AST as pairs of `name` and `node`.
        let mut nodes = InputSession::<_,(AstName, AstNode),_>::new();
        // Exogenous equivalences associating AST nodes by name.
        let mut equiv = InputSession::<_,(AstName, AstName),_>::new();

        // Probe to determine progress / completion.
        let mut probe = Handle::new();

        // Set up a new computation
        worker.dataflow(|scope| {

            let nodes = nodes.to_collection(scope);
            let equiv = equiv.to_collection(scope);

            // Iteratively develop a map from `Name` to `Name` that closes `equiv` under congruence.
            // Specifically, pairs `(a, b)` where a >= b and b names the equivalence class of a.
            nodes
                .map(|(name, _)| (name, name))
                .iterate(|eq_class| {

                    // Collection is loop invariant, but must be brought in scope.
                    let nodes = nodes.enter(&eq_class.scope());
                    let equiv = equiv.enter(&eq_class.scope());

                    // Separate AST node operators and their arguments.
                    let ops  = nodes.map(|(name, (op, _))| (name, op));
                    let args = nodes.flat_map(|(name, (_, args))| args.into_iter().enumerate().map(move |(index, arg)| (arg, (name, index))));

                    // Update argument identifiers, and then equate `(Ops, Args)` tuples to inform equivalences.
                    let equivalent_asts =
                    args.join_map(eq_class, |_child, &(node, index), &eq_class| (node, (index, eq_class)))
                        .reduce(|_node, input, output| {
                            let mut args = Vec::new();
                            for ((_index, eq_class), _) in input.iter() {
                                args.push(*eq_class);
                            }
                            output.push((args, 1isize));
                        })
                        .join_map(&ops, |node, children, op| ((children.clone(), op.clone()), *node))
                        .concat(&nodes.filter(|(_, (_, args))| args.is_empty()).map(|(node, (op, _))| ((vec![], op), node)))
                        .reduce(|_key, input, output| {
                            for node in input.iter() {
                                output.push(((*(node.0), *input[0].0), 1));
                            }
                        })
                        .map(|(_key, (node, eq_class))| (node, eq_class));

                    // Blend exogenous and endogenous equivalence; find connected components.
                    // NB: don't *actually* write connected components this way
                    let edges = equivalent_asts.concat(&equiv);
                    let symms = edges.map(|(x,y)|(y,x)).concat(&edges);
                    symms.iterate(|reach| 
                        reach.join_map(&reach, |_b, a, c| (*a, *c))
                             .distinct()
                    )
                    .reduce(|_a, input, output| output.push((*input[0].0, 1)))

                })
                .consolidate()
                .inspect(|x| println!("{:?}", x))
                .probe_with(&mut probe);
        });

        nodes.advance_to(0);
        equiv.advance_to(0);

        println!("Insert `(a x 2) / 2`");
        nodes.insert((0, ("a".to_string(), vec![])));
        nodes.insert((1, ("2".to_string(), vec![])));
        nodes.insert((2, ("mul".to_string(), vec![0, 1])));
        nodes.insert((3, ("div".to_string(), vec![2, 1])));

        nodes.advance_to(1); nodes.flush();
        equiv.advance_to(1); equiv.flush();

        worker.step_while(|| probe.less_than(&nodes.time()));
        println!("");


        println!("Insert `a x (2 / 2)`");
        nodes.insert((4, ("2".to_string(), vec![])));
        nodes.insert((5, ("div".to_string(), vec![4, 4])));
        nodes.insert((6, ("a".to_string(), vec![])));
        nodes.insert((7, ("mul".to_string(), vec![6, 5])));
        println!("Equate with the prior term");
        equiv.insert((3, 7));

        nodes.advance_to(2); nodes.flush();
        equiv.advance_to(2); equiv.flush();

        worker.step_while(|| probe.less_than(&nodes.time()));
        println!("");


        println!("Insert `(2 / 2)` and `1` and equate them.");
        nodes.insert((8, ("2".to_string(), vec![])));
        nodes.insert((9, ("div".to_string(), vec![8, 8])));
        nodes.insert((10, ("1".to_string(), vec![])));
        equiv.insert((9, 10));

        nodes.advance_to(3); nodes.flush();
        equiv.advance_to(3); equiv.flush();

        worker.step_while(|| probe.less_than(&nodes.time()));
        println!("");


        println!("Insert `a * 1` and `a` and equate them.");
        nodes.insert((11, ("a".to_string(), vec![])));
        nodes.insert((12, ("1".to_string(), vec![])));
        nodes.insert((13, ("mul".to_string(), vec![11, 12])));
        equiv.insert((11, 13));

        nodes.advance_to(4); nodes.flush();
        equiv.advance_to(4); equiv.flush();

        worker.step_while(|| probe.less_than(&nodes.time()));
        println!("");


        println!("Oh shoot; '2' could equal zero; undo '2'/'2' == '1')");
        equiv.remove((9, 10));

    }).expect("Computation terminated abnormally");
}