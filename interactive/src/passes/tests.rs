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
    let report = crate::plan::report(r#"let dead = import "absent"; export "out" = input 0 : (int, ());"#, &|_| None, &crate::passes::Config::all()).unwrap();
    assert!(report.unresolved.is_empty());
    assert!(report.emit_ddp().is_ok());
}

#[test]
fn dead_child_exports_remap_parent_and_sibling_readers() {
    let mut p = program(r#"let rows = input 0 : (int, int);
        s: {
            let a = rows | map((k,v) -> (v,k));
            let b = rows | map((k,v) -> (k,(v,1) add));
            let c = rows | map((k,v) -> (k,(v,2) add));
        }
        let unused = s::a;
        t: { set x = s::b; let observed = s::c | inspect(observed); }
        export "out" = s::b;
    "#);
    assert!(dead_bindings(&mut p.root));
    let Item::Sub(s) = &p.root.items[0] else { panic!("expected s") };
    assert_eq!(s.exports.iter().map(|e| e.name.as_str()).collect::<Vec<_>>(), ["b", "c"]);
    assert_eq!(p.root.exports[0].value, Ref::ChildExport(0, 0));
    let Item::Sub(t) = &p.root.items[1] else { panic!("expected t") };
    assert_eq!(t.imports[0].from, Source::Parent(Ref::ChildExport(0, 0)));
    assert_eq!(t.imports[1].from, Source::Parent(Ref::ChildExport(0, 1)));
    assert!(!p.root.names.iter().any(|(name, _)| name == "unused"));
    assert!(crate::shapes::infer(&p, &|_| None).1.conflicts.is_empty());
    assert!(!dead_bindings(&mut p.root));
}

#[test]
fn identical_imports_merge_but_different_contracts_remain() {
    let mut p = program(r#"let rows = input 0 : (int, int);
        s: { export "a" = rows; }
        export "out" = rows;"#);
    let mut duplicate = p.root.imports[0].clone();
    duplicate.name = "alias".into();
    p.root.imports.push(duplicate.clone());
    duplicate.shape = Some((Shape::Int, Shape::Unit));
    p.root.imports.push(duplicate);
    p.root.exports[0].value = Ref::Import(1);
    p.root.names.push(("alias".into(), Ref::Import(1)));
    let Item::Sub(child) = &mut p.root.items[0] else { panic!("expected child") };
    child.imports[0].from = Source::Parent(Ref::Import(1));
    assert!(crate::passes::structural::merge_imports(&mut p.root));
    assert_eq!(p.root.imports.len(), 2);
    assert_eq!(p.root.exports[0].value, Ref::Import(0));
    assert_eq!(p.root.names.last().unwrap().1, Ref::Import(0));
    let Item::Sub(child) = &p.root.items[0] else { panic!("expected child") };
    assert_eq!(child.imports[0].from, Source::Parent(Ref::Import(0)));
    assert!(!crate::passes::structural::merge_imports(&mut p.root));
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
