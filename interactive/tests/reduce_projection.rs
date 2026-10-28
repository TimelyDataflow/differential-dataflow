//! Projection changes what reduce stores, without changing which input wins.

use corgi::Shape;
use interactive::{ir::{LinearOp, Reducer}, lower, parse, passes::narrow};
use interactive::scope_ir::{Item, Node, Ref};
use interactive::server::Server;

#[test]
fn latest_write_stores_only_the_payload_and_suppresses_unchanged_payloads() {
    let text = r#"
        let rows = input 0 : ((int, int, int), ()) | map(((k, neg_wid, v), ()) -> (k, (neg_wid, v)));
        let best = rows | min;
        export "answer" = best | map((k, (_, v)) -> (k, ((), v)));
    "#;
    let mut original = lower::lower_tree(parse::pipe::parse(text));
    let mut projected = original.clone();
    // The optimizer folds the one-reader projection into the reducer; nothing is left to narrow.
    interactive::passes::optimize(&mut projected);
    let Ref::Local(index) = projected.root.exports[0].value else { panic!("a local reduce") };
    assert!(matches!(&projected.root.items[index], Item::Op(Node::Reduce { reducer: Reducer::Expr(_), .. })));
    assert_eq!(narrow::narrow(&mut projected, &|_| None).unwrap().fields, 0);
    original.root.exports[0].name = "before".into();
    projected.root.exports[0].name = "after".into();
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        server.set_export_taps(true);
        server.install(worker, "before", &original).unwrap();
        server.install(worker, "after", &projected).unwrap();
        for (tick, rows) in [
            "1,-1,7\n1,0,99",
            "1,-1,7 diff=-1\n1,-2,7", // Only the ordering field changes.
            "1,-3,8", // The new winner has a different payload.
            "1,-3,8 diff=-1\n1,-2,7 diff=-1\n1,0,99 diff=-1", // The key disappears.
        ].into_iter().enumerate() {
            for name in ["before", "after"] { server.feed(name, 0, rows, None).unwrap(); }
            server.tick(worker);
            let after = server.snapshot(worker, "after").unwrap();
            assert_eq!(server.snapshot(worker, "before").unwrap().rows(), after.rows());
            if tick < 3 {
                // This export directly reads the reduce's output arrangement.
                assert_eq!(corgi::shape_of_value(&after.vals), Shape::Prod(vec![Shape::Unit, Shape::Int]));
                assert_eq!(after.rows(), vec![("1".into(), format!("tuple(tuple(),{})", if tick < 2 { 7 } else { 8 }), 1)]);
            }
            let changes = server.take_changes("after");
            let records: usize = changes.iter().map(|c| c.diffs.len()).sum();
            if tick == 1 { assert_eq!(records, 0, "no output update, even before consolidation"); }
            else { assert!(records > 0); }
        }
    });
}

#[test]
fn demand_mask_enters_an_arbitrary_reduce_and_keeps_its_diffs() {
    let text = r#"
        let rows = input 0 : ((int, int, int), ()) | map(((k, a, b), ()) -> (k, (a, b)));
        let reduced = rows | reduce((_, vals) -> vals map ((v, d) -> (v, (d, 2) mul)));
        export "answer" = reduced | map((k, (_, b)) -> ((k, b), ()));
    "#;
    let mut original = lower::lower_tree(parse::pipe::parse(text));
    interactive::passes::optimize(&mut original);
    let mut projected = original.clone();
    assert!(narrow::narrow(&mut projected, &|_| None).unwrap().fields > 0);
    assert_eq!(narrow::narrow(&mut projected, &|_| None).unwrap().fields, 0);
    let (shapes, problems) = interactive::shapes::infer(&projected, &|_| None);
    assert!(problems.conflicts.is_empty());
    for (i, item) in projected.root.items.iter().enumerate() {
        if matches!(item, Item::Op(Node::Reduce { .. })) {
            assert_eq!(shapes.of(&Ref::Local(i)).unwrap().val, Shape::Prod(vec![Shape::Unit, Shape::Int]));
        }
    }
    original.root.exports[0].name = "before".into();
    projected.root.exports[0].name = "after".into();
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        server.install(worker, "before", &original).unwrap();
        server.install(worker, "after", &projected).unwrap();
        for rows in ["1,10,7\n1,20,7 diff=-2", "1,10,7 diff=-1\n1,20,7 diff=2"] {
            for name in ["before", "after"] { server.feed(name, 0, rows, None).unwrap(); }
            server.tick(worker);
            assert_eq!(server.snapshot(worker, "before").unwrap().rows(), server.snapshot(worker, "after").unwrap().rows());
        }
    });
}

#[test]
fn a_reduce_keeps_only_the_output_fields_its_readers_use() {
    // A one-reader map that only rewires the value folds into the reducer (compose-maps);
    // otherwise narrowing drops the fields no reader uses, and the reader keeps its map.
    for (map, extra, folded, value) in [
        ("(k, (a, b)) -> (k, (b, a, b))", "", true, Shape::Prod(vec![Shape::Int, Shape::Int, Shape::Int])),
        ("(k, (a, b)) -> (k, b)", "", true, Shape::Int),
        ("(k, (a, b)) -> (k, b)", "export \"also\" = best | map((k, (_, b)) -> (k, (b, 1) add));", false, Shape::Prod(vec![Shape::Unit, Shape::Int])),
        ("(k, (a, b)) -> (k, (b, 1) add)", "", false, Shape::Prod(vec![Shape::Unit, Shape::Int])),
        ("(k, (a, b)) -> (k, (k, b))", "", false, Shape::Prod(vec![Shape::Unit, Shape::Int])),
        // An identity map goes; the other reader keeps the reduce's second field live.
        ("(k, (a, b)) -> (k, (a, b))", "export \"also\" = best | map((k, (a, _)) -> (k, a));", true, Shape::Prod(vec![Shape::Int, Shape::Int])),
    ] {
        let text = format!("let rows = input 0 : (int, (int, int)); let best = rows | min; export \"out\" = best | map({map}); {extra}");
        let mut program = lower::lower_tree(parse::pipe::parse(&text));
        interactive::passes::optimize(&mut program);
        let outcome = narrow::narrow(&mut program, &|_| None).unwrap();
        let reduce = program.root.items.iter().position(|item| matches!(item, Item::Op(Node::Reduce { .. }))).unwrap();
        let shape = interactive::shapes::infer(&program, &|_| None).0.of(&Ref::Local(reduce)).unwrap();
        assert_eq!(shape.val, value, "{map}; {extra}: {outcome:?}");
        let Ref::Local(index) = program.root.exports[0].value else { panic!("local output") };
        assert_eq!(index == reduce, folded, "{map}; {extra}");
        if !folded { assert!(matches!(&program.root.items[index], Item::Op(Node::Linear { ops, .. }) if matches!(ops.last(), Some(LinearOp::Project(_)))), "{map}; {extra}"); }
    }
}
