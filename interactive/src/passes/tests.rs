use super::*;
use crate::scope_ir::Program;

fn program(text: &str) -> Program { crate::lower::lower_tree(crate::parse::pipe::parse(text)) }
fn optimized(text: &str) -> Program { let mut p = program(text); crate::passes::optimize(&mut p); p }
fn linears(scope: &Scope) -> Vec<&[LinearOp]> {
    scope.items.iter().filter_map(|item| match item {
        Item::Op(Node::Linear { ops, .. }) => Some(ops.as_slice()), _ => None,
    }).collect()
}

#[test]
fn composition_discards_arithmetic_and_uses_graphs() {
    let mut p = program(r#"let x = input 0 : (int, int);
        export "out" = x | map((k, v) -> (k, (v, 19) mul)) | map((k, _) -> (k, ()));
    "#);
    // A composed slot's diagnostic source is allowed to disagree with its graph.
    if let Item::Op(Node::Linear { ops, .. }) = &mut p.root.items[0] {
        let LinearOp::Project(lambda) = &mut ops[0] else { unreachable!() };
        lambda.src = "let (k, v) = input in (v, k)".into();
    }
    crate::passes::optimize(&mut p);
    let chains = linears(&p.root);
    let [LinearOp::Project(lambda)] = chains[0] else { panic!("{p:?}") };
    assert_eq!(lambda.src, "let (a, _) = input in (a, ())");
    assert!(!format!("{:?}", lambda.graph().unwrap()).contains("Mul"));
    let once = format!("{p:?}");
    crate::passes::optimize(&mut p);
    assert_eq!(once, format!("{p:?}"));
}

#[test]
fn shared_edges_and_rejected_shapes_stay() {
    let p = optimized(r#"let x = input 0 : (int, int);
        let shared = x | map((k, v) -> (k, (v, 1) add));
        export "first" = shared | map((k, v) -> (v, k));
        export "second" = shared | map((k, v) -> (k, (v, 2) mul));
    "#);
    assert!(p.root.names.iter().any(|(name, _)| name == "shared"));
    let p = optimized(r#"export "bad" = input 0 : (int, int)
        | map(((k, v), _) -> (k, v)) | map((k, _) -> (k, ()));
    "#);
    assert!(!crate::shapes::infer(&p, &|_| None).1.conflicts.is_empty());
}

#[test]
fn unprintable_composition_keeps_maps() {
    let first = Lambda::new("let (k, v) = input in (k, v)".into(), &crate::lambda::base_scope());
    let mut b = corgi::Builder::default();
    let x = b.input();
    let k = b.add(Op::Field(0), vec![x]);
    let v = b.add(Op::Field(1), vec![x]);
    let woven = b.add(Op::Weave, vec![v]);
    let out = b.tuple(vec![k, woven]);
    let second = Lambda { src: "deliberately not the graph".into(), graph: Ok(std::sync::Arc::new(b.finish(out))), added_by: None };
    let list = Shape::List(Box::new(Shape::Int));
    let shape = Shape::Prod(vec![Shape::Int, Shape::Prod(vec![list.clone(), list.clone(), list])]);
    assert!(compose_run(&[&first, &second], &shape).is_none());
}

#[test]
fn dead_imports_keep_contracts_inputs_inspects_and_nested_readers() {
    let mut p = program(r#"
        let unused = import "unused" : (int, int);
        let fed = input 7 : (int, int);
        let alias = fed;
        let republished = import "republished" : (int, int);
        let observed = import "observed" : (int, int) | inspect(observed);
        let nested = import "nested" : (int, int);
        child: { let result = nested; }
        export "out" = republished;
        export "child" = child::result;
    "#);
    assert!(dead_bindings(&mut p.root));
    assert!(p.root.imports.iter().any(|i| matches!(i.from, Source::Input(7))));
    assert!(!p.root.names.iter().any(|(n, _)| n == "alias" || n == "unused"));
    assert!(crate::shapes::infer(&p, &|_| None).1.conflicts.is_empty());
    assert!(!dead_bindings(&mut p.root));
    let report = crate::plan::report(r#"let dead = import "absent"; export "out" = input 0 : (int, ());"#, &|_| None).unwrap();
    assert!(report.unresolved.is_empty());
    assert!(report.emit_ddp().is_ok());
}

#[test]
fn a_long_map_run_matches_pairwise_composition() {
    let text = "let x = input 0 : (int, int); export \"out\" = x | map((k, v) -> (k, (v, 1) add));";
    let p = crate::lower::lower_tree(crate::parse::pipe::parse(text));
    let Item::Op(Node::Linear { ops, .. }) = &p.root.items[0] else { panic!() };
    let LinearOp::Project(map) = &ops[0] else { panic!() };
    let input = Shape::Prod(vec![Shape::Int, Shape::Int]);
    let mut pairwise = map.clone();
    for _ in 1..64 { pairwise = compose_run(&[&pairwise, map], &input).unwrap(); }
    let once = compose_run(&vec![map; 64], &input).unwrap();
    assert_eq!(once.src, pairwise.src);
    assert_eq!(once.graph().unwrap(), pairwise.graph().unwrap());
}
