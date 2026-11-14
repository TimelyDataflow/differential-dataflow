//! Essential surface semantics and source diagnostics.
use interactive::{lower, parse, plan, passes::Config, server::Server, surface};
fn program(src: &str) -> interactive::scope_ir::Program {
    lower::lower_tree(parse::pipe::parse(&surface::expand(src).unwrap()))
}
fn report(src: &str) -> plan::Report {
    plan::report(&surface::expand(src).unwrap(), &|_| None, &Config::all()).unwrap()
}
fn rows(src: &str, feeds: Vec<(usize, String)>) -> Vec<(String, String, i64)> {
    let p = program(src);
    timely::execute_directly(move |worker| {
        let mut s = Server::new(); s.set_passes(Config::all()); s.install(worker, "p", &p).unwrap();
        for (i, data) in feeds { s.feed("p", i, &data, None).unwrap(); }
        s.tick(worker); s.snapshot(worker, "p::out").unwrap().rows()
    })
}
fn rejects(src: &str, message: &str, token: &str) {
    let error = surface::expand(src).unwrap_err();
    let before = &src[..src.rfind(token).unwrap()];
    let line = before.bytes().filter(|c| *c == b'\n').count() + 1;
    let col = before.rsplit('\n').next().unwrap().chars().count() + 1;
    assert!(error.starts_with(&format!("{line}:{col}: ")) && error.contains(message), "{error}");
}


#[test]
fn joins_multiply_weights_and_handle_retractions() {
    let src = r#"
        let a = input 0 : (int, int);
        let b = input 1 : (int, int);
        let c = input 2 : ((int, int), ());
        output "out" = ((a))(x, y) * (b)(y, z) * c((x, z), ())
            | map(((x, y, z), ()) -> ((x, y, z), ()));
    "#;
    let p = program(src);
    timely::execute_directly(move |worker| {
        let mut s = Server::new();
        s.set_passes(Config::parse("all").unwrap());
        s.install(worker, "p", &p).unwrap();
        for (i, text) in [(0,"1 val=2 diff=2"),(1,"2 val=3 diff=3"),(2,"1,3 diff=4")] { s.feed("p",i,text,None).unwrap(); }
        s.tick(worker);
        assert_eq!(s.snapshot(worker,"p::out").unwrap().rows(), vec![("tuple(1,2,3)".into(),"tuple()".into(),24)]);
        s.feed("p",0,"1 val=2 diff=-1",None).unwrap(); s.tick(worker);
        assert_eq!(s.snapshot(worker,"p::out").unwrap().rows()[0].2,12);
        s.feed("p",1,"2 val=3 diff=-3",None).unwrap(); s.tick(worker);
        assert!(s.snapshot(worker,"p::out").unwrap().is_empty());
    });
}

#[test]
fn first_literal_precedes_the_first_rekey_and_arrangement() {
    use interactive::{ir::LinearOp, scope_ir::{Item, Node, Ref}};
    let src = r#"
        let message = input 0 : (int, (int,int,int));
        output "out" = message(_, (1, who, parent)) * message(parent, (_, to, _));
    "#;
    let p = program(src);
    let items = &p.root.items;
    let filter = items.iter().position(|i| matches!(i, Item::Op(Node::Linear { ops, .. }) if matches!(ops.as_slice(), [LinearOp::Filter(_)]))).unwrap();
    let join = items.iter().position(|i| matches!(i, Item::Op(Node::Join { .. }))).unwrap();
    let Item::Op(Node::Join { left: Ref::Local(arrange), .. }) = &items[join] else { panic!() };
    let Item::Op(Node::Arrange(Ref::Local(map))) = &items[*arrange] else { panic!() };
    let Item::Op(Node::Linear { input: Ref::Local(input), ops }) = &items[*map] else { panic!() };
    assert_eq!(*input, filter);
    assert!(filter < *map && *map < *arrange && *arrange < join);
    let LinearOp::Project(lambda) = &ops[0] else { panic!() };
    assert!(!format!("{lambda:?}").contains("v.0"), "tested kind must not reach the arrangement");
    assert_eq!(rows(src, vec![(0, "10 val=tuple(1,2,20)\n11 val=tuple(0,3,20)\n20 val=tuple(0,4,0)".into())]),
        vec![("tuple(2,20,4)".into(), "tuple()".into(), 1)]);
}

#[test]
fn first_atom_tests_do_not_prevent_direct_arrangement_reuse() {
    use interactive::scope_ir::{Item, Node, Ref};
    let src = r#"
        let a=input 0 : (int,(Option(int),int,int));
        let b=input 1 : (int,int);
        let c=input 2 : (int,int);
        output "out"=a(x,(Some(y),x,7)) * b(x,z) * c(z,w);
    "#;
    let report = report(src);
    let root = &report.stages[0].program.root;
    let left = root.items.iter().find_map(|item| match item {
        Item::Op(Node::Join { left, .. }) => Some(left), _ => None,
    }).unwrap();
    let Ref::Local(i) = left else { panic!("expected arrangement") };
    assert!(matches!(&root.items[*i], Item::Op(Node::Arrange(Ref::Import(0)))));
    let lowered = report.emit_ddp_at(plan::ddp::DdpStage::Lowered).unwrap().text;
    assert_eq!(lowered.matches("| filter(").count(), 1);
    assert_eq!(lowered.matches("| flatmap(").count(), 1);
    assert!(lowered.find("| join(").unwrap() < lowered.find("| filter(").unwrap());
    assert!(lowered.find("| filter(").unwrap() < lowered.find("| flatmap(").unwrap());
    assert!(lowered.find("| flatmap(").unwrap() < lowered.rfind("| join(").unwrap());
    assert_eq!(rows(src, vec![
        (0,"1 val=tuple(inject(1,9),1,7)\n1 val=tuple(inject(0,tuple()),1,7)\n1 val=tuple(inject(1,8),2,7)\n1 val=tuple(inject(1,8),1,6)".into()),
        (1,"1 val=2 diff=3".into()),(2,"2 val=4 diff=2".into()),
    ]), vec![("tuple(1,9,2,4)".into(),"tuple()".into(),6)]);
}

#[test]
fn later_literals_follow_their_join_and_leave_before_the_next_arrangement() {
    use interactive::{ir::LinearOp, scope_ir::{Item, Node, Ref}};
    let src = r#"
        let asked = input 0 : (int,(int,int));
        let by_name = input 1 : (int,int);
        let place = input 2 : (int,(int,int));
        output "out" = asked(xname,(rid,yname)) * by_name(xname,x)
            * place(x,(7,_)) * by_name(yname,y) * place(y,(7,_));
    "#;
    let p = program(src);
    let items = &p.root.items;
    let joins: Vec<_> = items.iter().enumerate().filter_map(|(i,n)| matches!(n, Item::Op(Node::Join { .. })).then_some(i)).collect();
    let filters: Vec<_> = items.iter().enumerate().filter_map(|(i,n)| matches!(n, Item::Op(Node::Linear { ops, .. }) if matches!(ops.as_slice(), [LinearOp::Filter(_)] )).then_some(i)).collect();
    assert_eq!(filters.len(), 2);
    assert!(joins[1] < filters[0] && filters[0] < joins[2] && joins[3] < filters[1]);
    let Item::Op(Node::Linear { input, .. }) = &items[filters[0]] else { panic!() };
    assert_eq!(input, &Ref::Local(joins[1]));
    let report = report(src);
    let shapes = &report.stages[0].shapes;
    let Item::Op(Node::Join { left: Ref::Local(arrange), .. }) = &items[joins[2]] else { panic!() };
    let before = shapes.of(&Ref::Local(filters[0])).unwrap();
    let after = shapes.of(&Ref::Local(*arrange)).unwrap();
    let (corgi::Shape::Prod(before), corgi::Shape::Prod(after)) = (&before.val, &after.val) else { panic!() };
    assert_eq!(before.len(), after.len() + 1, "discard the tested kind before arranging");
}

#[test]
fn later_keys_combine_bound_variables_with_constants() {
    let src = r#"
        enum K = Group (int,int) | Empty;
        let a = input 0 : (int,());
        let b = input 1 : ((int,int),int);
        let c = input 2 : (K,int);
        let d = input 3 : ((int,K,()),int);
        output "out" = a(rid,()) * b((rid,7),v) * c(Group((rid,v)),w) * d((w,Empty,()),z);
    "#;
    assert_eq!(rows(src, vec![(0,"1\n2".into()), (1,"1,7 val=10\n2,8 val=20".into()),
        (2,"inject(0,tuple(1,10)) val=99".into()), (3,"tuple(99,inject(1,tuple()),tuple()) val=5".into())]),
        vec![("tuple(1,10,99,5)".into(), "tuple()".into(), 1)]);
    for (key, error, token) in [
        ("7", "a constant fragment key would move every incoming row to one key; write a cross join as plain DDP", "7"),
        ("missing", "fragment key variable `missing` is not bound by an earlier atom", "missing"),
        ("_", "a later fragment key cannot contain a wildcard", "_"),
    ] {
        let src = format!("-- π\nlet a=input 0 : (int,int); let b=a;\noutput \"out\"=a(x,y)*b({key},z);");
        rejects(&src, error, token);
    }
}

#[test]
fn variant_payload_rekeys_before_its_join_without_head_or_tag_columns() {
    use interactive::{ir::LinearOp, scope_ir::{Item, Node, Ref}};
    let src = r#"
        enum Member = User int | Group int;
        let members = input 0 : (int, Member);
        let groups = input 1 : (int, Member);
        output "out" = members(doc, Group(g)) * groups(g, m);
    "#;
    let p = program(src);
    let items = &p.root.items;
    let join = items.iter().position(|i| matches!(i, Item::Op(Node::Join { .. }))).unwrap();
    let Item::Op(Node::Join { left: Ref::Local(arrange), .. }) = &items[join] else { panic!() };
    let Item::Op(Node::Arrange(Ref::Local(flatmap))) = &items[*arrange] else { panic!() };
    let Item::Op(Node::Linear { ops, .. }) = &items[*flatmap] else { panic!() };
    let [LinearOp::FlatMap(lambda)] = ops.as_slice() else { panic!() };
    let text = format!("{lambda:?}");
    assert!(text.contains("unweave .2"));
    assert!(!text.contains(" head") && !text.contains(" match ("), "{text}");
    assert!(!items.iter().any(|i| matches!(i, Item::Op(Node::Linear { ops, .. }) if ops.iter().any(|op| matches!(op, LinearOp::Filter(_))))));
    assert_eq!(rows(src, vec![(0, "100 val=inject(1,10)\n200 val=inject(0,20)".into()), (1, "10 val=inject(0,3)".into())]),
        vec![("tuple(100,10,inject(0,3))".into(), "tuple()".into(), 1)]);
    assert_eq!(rows("let a=input 0 : (int,Option(int)); output \"out\"=a(x,None);",
        vec![(0,"1 val=inject(0,tuple()) diff=3\n2 val=inject(1,7)".into())]),
        vec![("1".into(),"tuple()".into(),3)]);
    rejects("let a=input 0 : (int,Option(int)); output \"out\"=a(x,Some);",
        "fragment constructor `Some` requires a payload; write `Some(pattern)`", "Some");
    rejects("enum A=Shared; enum B=Shared; let a=input 0 : (int,A); output \"out\"=a(x,Shared);",
        "fragment constructor `Shared`: 'Shared' is a variant of", "Shared");
}

#[test]
fn fragments_read_their_own_feedback_variable() {
    let src = r#"
        let edges = input 0 : (int,int);
        let roots = input 1 : (int,());
        s: {
            set x = roots + (x(src,()) * edges(src,dst) | map(((src,dst),()) -> (dst,()))) | distinct;
        }
        output "out" = s::x;
    "#;
    assert_eq!(rows(src, vec![(0,"1 val=2\n2 val=3\n3 val=2\n4 val=5\n9 val=10".into()), (1,"1\n4".into())]),
        (1..=5).map(|n| (n.to_string(), "tuple()".into(), 1)).collect::<Vec<_>>());
}

#[test]
fn splices_preserve_plain_text_and_report_pattern_errors() {
    let prefix = "-- π: a(x,y) * b(y,z)\nenum K=SomeK int | Empty;\nlet a=input 0 : (int,Option(int));\nlet plain=a | map((k,v) -> let unused = \"a(x,y) * text\" in (k,v));\n";
    assert_eq!(surface::expand(prefix).unwrap(), prefix);
    let suffix = " | map(((x,y,z),()) -> (x,y)); -- untouched\n";
    let text = format!("{prefix}output \"out\" = a(x,Some(y)) -- inside\n * a(x,z){suffix}");
    let expanded = surface::expand(&text).unwrap();
    assert!(expanded.starts_with(prefix) && expanded.ends_with(suffix));
    assert!(expanded.contains("-- inside\n"));
    assert!(!expanded.contains("a(x,Some(y))"));
    assert!(report(&text).stages.iter().all(|s| s.problems.is_empty()));
    for (pattern, message, token) in [
        ("input 0(x,y)", "a fragment atom names an existing binding", "0"),
        ("a(x)", "Expected Comma, got RParen", ")"),
        ("a(x,[y])", "a list pattern is a literal", "y"),
        ("a(x,[[]])", "DDIR fragment list literal `[[]]` needs a complete element shape", "]"),
    ] {
        let text = format!("let a=input 0 : (int,List(List(int)));\noutput \"π\"={pattern};");
        rejects(&text, message, token);
    }
}
