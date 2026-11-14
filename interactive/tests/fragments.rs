//! Weighted semantics, restrictions, variants, and the two printable syntax views.
use interactive::{lower, parse, plan, passes::Config, server::Server};

fn program(src: &str) -> interactive::scope_ir::Program { lower::lower_tree(parse::pipe::parse(src)) }
fn rejects(src: &str, expected: &str) {
    let panic = std::panic::catch_unwind(|| program(src)).expect_err("expected a fragment error");
    let message = panic.downcast_ref::<String>().map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied()).expect("a string panic");
    assert!(message.contains(expected), "expected {expected:?}, got {message:?} for {src}");
}
fn rows(src: &str, feeds: Vec<(usize, String)>) -> Vec<(String, String, i64)> {
    let p = program(src);
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        server.set_passes(Config::parse("all").unwrap());
        server.install(worker, "p", &p).unwrap();
        for (i, text) in feeds { server.feed("p", i, &text, None).unwrap(); }
        server.tick(worker);
        server.snapshot(worker, "p::out").unwrap().rows()
    })
}

#[test]
fn joins_multiply_weights_and_handle_retractions() {
    let src = r#"
        let a = input 0 : (int, int);
        let b = input 1 : (int, int);
        let c = input 2 : ((int, int), ());
        output "out" = a(x, y) * b(y, z) * c((x, z), ())
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
fn patterns_tests_and_output_order() {
    let src = r#"
        let a = input 0 : ((int, int), (List(int), int));
        let b = input 1 : (int, int);
        output "out" = a((x, x), ("hi", y)) * b(y, -7);
    "#;
    assert_eq!(rows(src,vec![(0,"1,1 val=tuple(list(104,105),2)\n1,9 val=tuple(list(104,105),2)\n3,3 val=tuple(list(104),2)".into()),(1,"2 val=-7 diff=3\n2 val=7".into())]),vec![("tuple(1,2)".into(),"tuple()".into(),3)]);
    assert_eq!(rows("let a = input 0 : (int,int); output \"out\" = a(_, _);",vec![(0,"1 val=2 diff=4".into())]),vec![("tuple()".into(),"tuple()".into(),4)]);
    assert_eq!(rows("let a = input 0 : (int,int); output \"out\" = a(x, _);",vec![(0,"1 val=2".into())]),vec![("1".into(),"tuple()".into(),1)]);
}

#[test]
fn empty_list_pattern_does_not_choose_an_element_shape() {
    assert_eq!(rows("let a = input 0 : (int,List((int,int))); output \"out\" = a(x, []);",
        vec![(0,"1 val=list()\n2 val=list(tuple(3,4))".into())]),
        vec![("1".into(),"tuple()".into(),1)]);
}

#[test]
fn nested_open_variants_and_early_key_extraction() {
    let src = r#"
        enum A = Shared int | Empty;
        enum B = Shared int | Other;
        let a = input 0 : (int, Option(Result((int, List(int)), ())));
        let b = input 1 : (int, A);
        let c = input 2 : (A, int);
        output "out" = a(_, Some(Ok((x, text)))) * b(x, A::Shared(y)) * c(A::Shared(y), z);
    "#;
    let got = rows(src,vec![
        (0,"1 val=inject(1,inject(0,tuple(2,list(65))))\n2 val=inject(0,tuple())\n3 val=inject(1,inject(1,tuple()))".into()),
        (1,"2 val=inject(0,3)\n2 val=inject(1,tuple())".into()),
        (2,"inject(0,3) val=4\ninject(1,tuple()) val=9".into()),
    ]);
    assert_eq!(got,vec![("tuple(2,list(65),3,4)".into(),"tuple()".into(),1)]);
    let report=plan::report(src,&|_|None,&Config::parse("all").unwrap()).unwrap();
    assert!(report.stages.last().unwrap().problems.is_empty());
    let text = report.emit_ddp().unwrap().text;
    for tail in text.lines().filter(|line| line.contains("| flatmap(")) {
        assert!(!tail.contains(" head"), "{tail}");
    }
    let nested_key = r#"
        enum Inner = Leaf int | Other;
        enum Key = Wrap Inner | Empty;
        let a = input 0 : (int, ());
        let b = input 1 : (Key, int);
        output "out" = a(y, ()) * b(Key::Wrap(Inner::Leaf(y)), z);
    "#;
    assert_eq!(rows(nested_key, vec![(0, "1".into()),
        (1, "inject(0,inject(0,1)) val=9".into())]),
        vec![("tuple(1,9)".into(), "tuple()".into(), 1)]);
    let report = plan::report(nested_key, &|_| None, &Config::parse("all").unwrap()).unwrap();
    let text = report.emit_ddp().unwrap().text;
    assert!(!text.lines().filter(|line| line.contains("| flatmap(")).any(|line| line.contains(" head")), "{text}");
}

#[test]
fn paths_feedback_pipeline_union_and_difference() {
    let src = r#"
        let a = input 0 : (int, int);
        looped: { var x : (int,int); set x = a; }
        let b = ::looped::x;
        output "out" = (looped::x(x,y) * b(x,z) | map(((x,y,z),()) -> (y,z)))
            + (a(x,y) | map(((x,y),()) -> (x,y))) - a;
    "#;
    assert_eq!(rows(src,vec![(0,"1 val=2".into())]),vec![("2".into(),"2".into(),1)]);
}

#[test]
fn key_restrictions_and_arity() {
    for (atom, error) in [
        ("b(_,z)", "a later fragment key cannot contain a wildcard"),
        ("b((x,_),z)", "a later fragment key cannot contain a wildcard"),
        ("b(missing,z)", "fragment key variable `missing` is not bound by an earlier atom"),
        ("b((x,missing),z)", "fragment key variable `missing` is not bound by an earlier atom"),
        ("b(Some(_),z)", "a later fragment key cannot contain a wildcard"),
    ] {
        let src=format!("let a = input 0 : (int,int); let b = a; output \"out\" = a(x,y) * {atom};");
        rejects(&src, error);
    }
    for (bad, error) in [("a(x)", "Expected Comma, got RParen"),
        ("a(x,y,z)", "Expected RParen, got Comma"), ("a(x,[y])", "a list pattern is a literal")] {
        rejects(&format!("let a=input 0 : (int,int); output \"out\"={bad};"), error);
    }
}

#[test]
fn constant_later_keys_explain_cross_joins() {
    for key in ["1", "-7", "\"ok\"", "[1,2]", "[]", "[[]]", "()", "(1,())", "Empty", "Empty()", "Group(7)", "(Empty(),[1,2])"] {
        let src = format!("enum K=Group int | Empty; let a=input 0 : (int,int); let b=a; output \"out\"=a(x,y)*b({key},z);");
        rejects(&src, "a constant fragment key would move every incoming row to one key; write a cross join as plain DDP");
    }
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
}

#[test]
fn bare_constructors_match_empty_payloads_and_do_not_bind_variables() {
    assert_eq!(rows("let a=input 0 : (int,Option(int)); output \"out\"=a(x,None);",
        vec![(0,"1 val=inject(0,tuple()) diff=3\n2 val=inject(1,7)".into())]),
        vec![("1".into(), "tuple()".into(), 3)]);
    let src = "let a=input 0 : (int,(Option(int),Option(int))); output \"out\"=a(x,(None,None));";
    assert_eq!(rows(src, vec![(0,"1 val=tuple(inject(0,tuple()),inject(0,tuple()))\n2 val=tuple(inject(0,tuple()),inject(1,7))\n3 val=tuple(inject(1,7),inject(1,7))".into())]),
        vec![("1".into(), "tuple()".into(), 1)]);
    let text = plan::report(src, &|_| None, &Config::all()).unwrap().emit_ddp().unwrap().text;
    assert!(!text.contains("| filter("), "repeated None must not become equality: {text}");
    assert_eq!(rows("enum K=Empty | Full int; let a=input 0 : (K,int); output \"out\"=a(K::Empty,v);",
        vec![(0,"inject(0,tuple()) val=9\ninject(1,7) val=8".into())]),
        vec![("9".into(), "tuple()".into(), 1)]);
    assert_eq!(rows("let a=input 0 : (int,int); output \"out\"=a(x,Fresh);", vec![(0,"1 val=2".into())]),
        vec![("tuple(1,2)".into(), "tuple()".into(), 1)]);
}

#[test]
fn bare_constructors_requiring_payloads_report_the_constructor() {
    for name in ["Some", "Group", "K::Group", "Ok"] {
        let src = format!("enum K=Group int | Empty; let a=input 0 : (int,K); output \"out\"=a(x,{name});");
        rejects(&src, &format!("fragment constructor `{name}` requires a payload; write `{name}(pattern)`"));
    }
    rejects("enum A=Shared; enum B=Shared; let a=input 0 : (int,A); output \"out\"=a(x,Shared);",
        "fragment constructor `Shared`: 'Shared' is a variant of");
}

#[test]
fn nested_empty_list_literals_report_a_ddir_error() {
    for literal in ["[[]]", "[[[]]]", "[([], 7)]"] {
        let src = format!("let a=input 0 : (int,List(List(int))); output \"out\"=a(x,{literal});");
        rejects(&src, &format!("DDIR fragment list literal `{literal}` needs a complete element shape; use plain DDP with an explicit list shape"));
    }
    for (literal, value) in [("[[1],[2]]", "list(list(1),list(2))"),
        ("[[1],[]]", "list(list(1),list())"), ("[[],[1]]", "list(list(),list(1))")] {
        let src = format!("let a=input 0 : (int,List(List(int))); output \"out\"=a(x,{literal});");
        assert_eq!(rows(&src, vec![(0,format!("1 val={value}\n2 val=list()"))]),
            vec![("1".into(), "tuple()".into(), 1)]);
    }
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
fn fragments_are_absent_from_every_emitted_plan_stage() {
    use plan::ddp::DdpStage;
    let src = "let a=input 0 : (int,int); let b=a; output \"out\"=a(x,7)*b(x,y);";
    let report = plan::report(src, &|_| None, &Config::all()).unwrap();
    assert!(report.emit_ddp_at(DdpStage::Parsed).unwrap().text.contains("a(x, 7) * b(x, y)"));
    for stage in [DdpStage::Lowered, DdpStage::Optimized, DdpStage::Projected, DdpStage::AsOf] {
        let text = report.emit_ddp_at(stage).unwrap().text;
        assert!(!text.contains('*'), "{stage:?}: {text}");
        assert!(text.contains("| join("), "{stage:?}: {text}");
    }
    assert!(!report.emit_ddp().unwrap().text.contains('*'));
}

#[test]
fn parsed_retains_fragments_and_direct_first_tests_follow_the_first_join() {
    use plan::ddp::DdpStage;
    let src="let a = input 0 : (int,int); let b = a; let c = a; output \"out\" = a(x,7) * b(x,y) * c(y,9);";
    let report=plan::report(src,&|_|None,&Config::parse("all").unwrap()).unwrap();
    let parsed=report.emit_ddp_at(DdpStage::Parsed).unwrap().text;
    assert!(parsed.contains("a(x, 7) * b(x, y) * c(y, 9)"));
    let lowered=report.emit_ddp_at(DdpStage::Lowered).unwrap().text;
    assert!(!lowered.contains('*'));
    assert_eq!(lowered.matches("| join(").count(),2);
    assert_eq!(lowered.matches("| filter(").count(), 2);
    assert!(!lowered.contains("| flatmap("));
    assert!(lowered.find("| join(").unwrap() < lowered.find("| filter(").unwrap());
    assert!(lowered.find("| filter(").unwrap() < lowered.rfind("| join(").unwrap());
    let again=plan::report(&parsed,&|_|None,&Config::parse("all").unwrap()).unwrap();
    assert!(report.compare_final(&again).is_empty());
}

#[test]
fn open_result_constructor_key_exposes_corgis_missing_lane_shape() {
    let src = "let a=input 0 : (int,()); let b=input 1 : (Result(int,()),int); output \"out\"=a(x,()) * b(Ok(x),y);";
    let report=plan::report(src,&|_|None,&Config::parse("all").unwrap()).unwrap();
    assert!(report.stages[0].problems.iter().any(|s|s.contains("declares no payload shape")));
}

#[test]
fn direct_first_join_reads_the_key_and_carries_it_when_rekeyed() {
    use interactive::scope_ir::{Item, Node, Ref};
    let src = "let a=input 0 : (int,int); let b=input 1 : (int,int); let c=input 2 : (int,int); output \"out\"=a(x,y) * b(x,z) * c(y,w);";
    let report = plan::report(src, &|_| None, &Config::parse("all").unwrap()).unwrap();
    let root = &report.stages[0].program.root;
    let (left, projection) = root.items.iter().find_map(|item| match item {
        Item::Op(Node::Join { left, projection, .. }) => Some((left, projection)), _ => None,
    }).unwrap();
    let Ref::Local(i) = left else { panic!("expected arrangement") };
    assert!(matches!(&root.items[*i], Item::Op(Node::Arrange(Ref::Import(0)))));
    let source = format!("{projection:?}");
    let graph = corgi::parse_ml_in(&source[1..source.len()-1], &corgi::Scope::default()).unwrap();
    let got = corgi::eval_graph(&graph, corgi::Value::Prod(vec![
        corgi::Value::int([10]), corgi::Value::int([20]), corgi::Value::int([30]),
    ]));
    // y becomes the key. x leaves the key and must survive in the value with z.
    assert_eq!(got, corgi::Value::Prod(vec![corgi::Value::int([20]),
        corgi::Value::Prod(vec![corgi::Value::int([10]), corgi::Value::int([30])])]));
    assert_eq!(rows(src, vec![(0,"10 val=20".into()),(1,"10 val=30".into()),(2,"20 val=40".into())]),
        vec![("tuple(10,20,30,40)".into(),"tuple()".into(),1)]);
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
    let report = plan::report(src, &|_| None, &Config::parse("all").unwrap()).unwrap();
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
fn first_key_variants_and_repeated_variables_are_tested_after_direct_join() {
    use interactive::scope_ir::{Item, Node, Ref};
    let src = r#"
        enum Key = Group (int,int) | User (int,int);
        let a=input 0 : (Key,int);
        let b=input 1 : (Key,int);
        output "out"=a(Group((x,x)),y) * b(Group((x,x)),z);
    "#;
    let report=plan::report(src,&|_|None,&Config::parse("all").unwrap()).unwrap();
    let root=&report.stages[0].program.root;
    let left=root.items.iter().find_map(|item|match item {
        Item::Op(Node::Join{left,..})=>Some(left),_=>None,
    }).unwrap();
    let Ref::Local(i)=left else {panic!("expected arrangement")};
    assert!(matches!(&root.items[*i],Item::Op(Node::Arrange(Ref::Import(0)))));
    let lowered=report.emit_ddp_at(plan::ddp::DdpStage::Lowered).unwrap().text;
    assert_eq!(lowered.matches("| filter(").count(), 1);
    assert_eq!(lowered.matches("| flatmap(").count(), 1);
    assert!(lowered.find("| join(").unwrap() < lowered.find("| flatmap(").unwrap());
    assert!(lowered.find("| flatmap(").unwrap() < lowered.find("| filter(").unwrap());
    assert_eq!(rows(src,vec![
        (0,"inject(0,tuple(1,1)) val=8 diff=2\ninject(0,tuple(1,2)) val=7\ninject(1,tuple(2,2)) val=6".into()),
        (1,"inject(0,tuple(1,1)) val=9 diff=3\ninject(0,tuple(1,2)) val=5\ninject(1,tuple(2,2)) val=4".into()),
    ]),vec![("tuple(1,8,9)".into(),"tuple()".into(),6)]);
}

#[test]
fn nested_variant_lists_and_equalities_build_only_survivors() {
    let src = r#"
        let a = input 0 : (int, Option(Result((int,int),())));
        output "out" = a(doc, Some(Ok((u,u))));
    "#;
    assert_eq!(rows(src, vec![(0, "1 val=inject(1,inject(0,tuple(2,2))) diff=3\n1 val=inject(1,inject(0,tuple(2,9)))\n2 val=inject(0,tuple())\n3 val=inject(1,inject(1,tuple()))".into())]),
        vec![("tuple(1,2)".into(), "tuple()".into(), 3)]);
    let report = plan::report(src, &|_| None, &Config::parse("all").unwrap()).unwrap();
    let text = report.emit_ddp().unwrap().text;
    assert_eq!(text.matches("| flatmap(").count(), 1, "{text}");
    assert!(!text.contains(" head") && !text.contains("[]") && !text.contains(" match ("), "{text}");
    let again = plan::report(&text, &|_| None, &Config::parse("all").unwrap()).unwrap();
    assert!(report.compare_final(&again).is_empty());
}

#[test]
fn one_atom_variant_demux() {
    let src = r#"
        enum Member = User int | Group int;
        let grants = input 0 : (int, (Member, int));
        output "out" = grants(doc, (User(u), level)) | map(((doc, u, level), ()) -> (u, (doc, level)));
    "#;
    let feed = "100 val=tuple(inject(0,1),3)\n100 val=tuple(inject(1,10),2) diff=5\n200 val=tuple(inject(0,2),1) diff=2";
    assert_eq!(rows(src, vec![(0, feed.into())]),
        vec![("1".into(), "tuple(100,3)".into(), 1), ("2".into(), "tuple(200,1)".into(), 2)]);
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
    let report = plan::report(src, &|_| None, &Config::all()).unwrap();
    let shapes = &report.stages[0].shapes;
    let Item::Op(Node::Join { left: Ref::Local(arrange), .. }) = &items[joins[2]] else { panic!() };
    let before = shapes.of(&Ref::Local(filters[0])).unwrap();
    let after = shapes.of(&Ref::Local(*arrange)).unwrap();
    let (corgi::Shape::Prod(before), corgi::Shape::Prod(after)) = (&before.val, &after.val) else { panic!() };
    assert_eq!(before.len(), after.len() + 1, "discard the tested kind before arranging");
}

#[test]
fn test_free_chain_has_no_flatmap_or_layout_stage_after_the_last_join() {
    use interactive::{ir::LinearOp, scope_ir::{Item, Node}};
    let src = "let a=input 0 : (int,int); let b=a; let c=a; output \"out\"=a(x,y)*b(y,z)*c(z,w);";
    let p = program(src);
    assert!(!p.root.items.iter().any(|i| matches!(i, Item::Op(Node::Linear { ops, .. }) if ops.iter().any(|op| matches!(op, LinearOp::FlatMap(_))))));
    assert!(matches!(p.root.items.last(), Some(Item::Op(Node::Join { .. }))));
    assert_eq!(rows(src, vec![(0, "1 val=2\n2 val=3\n3 val=4".into())]),
        vec![("tuple(1,2,3,4)".into(), "tuple()".into(), 1)]);
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
}

#[test]
fn wildcard_payloads_still_check_their_tuple_shape() {
    for (val, error) in [("Some((_,_))", "wants a product of 2 fields, not Int"),
        ("Some((_))", "wants a product of 1 field, not Int")] {
        let src = format!("let a=input 0 : (int,Option(int)); output \"out\"=a(_, {val});");
        let report = plan::report(&src, &|_| None, &Config::all()).unwrap();
        assert!(report.stages[0].problems.iter().any(|e| e.contains(error)), "{src}: {:?}", report.stages[0].problems);
    }
}

#[test]
fn variants_rekey_before_pending_plain_payload_tests() {
    use interactive::{ir::LinearOp, scope_ir::{Export, Item, Node, Ref}};
    let src = r#"
        enum Member = User int | Group (int,int);
        let members = input 0 : (int,Member);
        let groups = input 1 : (int,int);
        output "out" = members(doc,Group((g,7))) * groups(g,m);
    "#;
    let mut p = program(src);
    let flatmap = p.root.items.iter().position(|i| matches!(i, Item::Op(Node::Linear { ops, .. }) if matches!(ops.as_slice(), [LinearOp::FlatMap(_)]))).unwrap();
    // Observe the variant stage before its plain filter: its key is already g.
    p.root.exports.push(Export { name: "keyed".into(), value: Ref::Local(flatmap), shape: None });
    timely::execute_directly(move |worker| {
        let mut s = Server::new();
        s.set_passes(Config::all());
        s.install(worker, "p", &p).unwrap();
        s.feed("p", 0, "100 val=inject(1,tuple(10,7))\n200 val=inject(1,tuple(20,8))\n300 val=inject(0,30)", None).unwrap();
        s.feed("p", 1, "10 val=3\n20 val=4", None).unwrap();
        s.tick(worker);
        assert_eq!(s.snapshot(worker, "p::keyed").unwrap().rows(), vec![
            ("10".into(), "tuple(100,7)".into(), 1),
            ("20".into(), "tuple(200,8)".into(), 1),
        ]);
        assert_eq!(s.snapshot(worker, "p::out").unwrap().rows(),
            vec![("tuple(100,10,3)".into(), "tuple()".into(), 1)]);
    });
}
