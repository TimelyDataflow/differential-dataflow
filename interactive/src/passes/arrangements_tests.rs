use super::*;
use crate::passes::Config;
use crate::scope_ir::Program;

fn program(text: &str) -> Program { crate::lower::lower_tree(crate::parse::pipe::parse(text)) }

fn count(scope: &Scope) -> usize {
    scope.items.iter().map(|item| match item {
        Item::Op(Node::Arrange(_)) => 1, Item::Sub(child) => count(child), _ => 0,
    }).sum()
}

fn rewrite(text: &str, config: Config, removed: usize) -> Program {
    let mut p = program(text);
    let before = count(&p.root);
    let exports = crate::shapes::infer(&p, &|_| None).0.exports;
    super::super::optimize_with(&mut p, &|_| None, &config, &mut crate::passes::Timings::default(), &mut |_, _, _, _, _, _| {});
    assert_eq!(count(&p.root) + removed, before);
    let (shapes, problems) = crate::shapes::infer(&p, &|_| None);
    assert!(problems.unknown.is_empty() && problems.conflicts.is_empty(), "{problems:?}");
    assert_eq!(shapes.exports, exports);
    let once = format!("{p:?}");
    super::super::optimize_with(&mut p, &|_| None, &config, &mut crate::passes::Timings::default(), &mut |_, _, _, _, _, _| {});
    assert_eq!(once, format!("{p:?}"));
    p
}

fn hoist() -> Config { Config { hoist_invariant: true, ..Config::none() } }
fn collapse() -> Config { Config { collapse_parent_arranges: true, ..Config::none() } }

#[test]
fn siblings_share_and_late_parent_peer_moves_before_children() {
    for peer in ["", "let parent_idx = x | arrange; export \"parent\" = parent_idx;"] {
        let p = rewrite(&format!(r#"
            let x = input 0 : (int, int);
            left: {{ let idx = ::x | arrange; let rows = ::x + idx; }}
            right: {{ let idx = ::x | arrange; }}
            {peer}
            export "left" = left::rows;
            export "right" = right::idx;
        "#), hoist(), if peer.is_empty() { 1 } else { 2 });
        assert!(matches!(p.root.items[0], Item::Op(Node::Arrange(Ref::Import(0)))));
        // The invariant concat also moves, retaining its row and trace edges.
        assert!(matches!(&p.root.items[1], Item::Op(Node::Concat(refs))
            if refs == &vec![Ref::Import(0), Ref::Local(0)]));
        let Item::Sub(left) = &p.root.items[2] else { panic!() };
        assert!(matches!(left.exports[0].value, Ref::Import(_)));
    }
}

#[test]
fn lone_arrangements_move_but_time_and_feedback_stay() {
    for child in [
        "child: { inner: { let idx = ::x | arrange; } let idx = inner::idx; }",
    ] {
        let text = format!("let x = input 0 : (int, int); {child} export \"out\" = child::idx;");
        let after = rewrite(&text, hoist(), 0);
        assert!(matches!(after.root.items[0], Item::Op(Node::Arrange(Ref::Import(0)))));
        let Item::Sub(child) = &after.root.items[1] else { panic!() };
        assert_eq!(count(child), 0);
        assert!(child.imports.iter().any(|i| matches!(i.from, Source::Parent(Ref::Local(0)))));
    }
    for op in ["lift", "delay((_, _) -> (0, 1))"] {
        let p = rewrite(&format!(r#"
            let x = input 0 : (int, int);
            let idx = x | arrange;
            child: {{ let idx = ::x | {op} | arrange; }}
            export "root" = idx;
            export "child" = child::idx;
        "#), hoist(), 0);
        let Item::Sub(child) = &p.root.items[1] else { panic!() };
        assert_eq!(child.items.len(), 2, "{op}");
    }
    rewrite(r#"
        let x = input 0 : (int, int);
        let idx = x | arrange;
        child: { var x : (int, int); let idx = x | arrange; set x = ::x; }
        export "root" = idx;
        export "child" = child::idx;
    "#, hoist(), 0);
}

#[test]
fn collapse_follows_arrangements_but_not_collection_boundaries() {
    for source in [
        "let x = import \"trace\" : (int, ());",
        "let rows = input 0 : (int, ()); let x = rows | distinct;",
    ] {
        rewrite(&format!(r#"
            {source}
            child: {{ let idx = ::x | arrange; inner: {{ let idx = child::idx | arrange; }} }}
            export "out" = ::child::inner::idx;
        "#), collapse(), 2);
    }
    rewrite(r#"
        let x = input 0 : (int, ());
        producer: { let idx = ::x | arrange; }
        consumer: { let idx = producer::idx | arrange; }
        export "out" = consumer::idx;
    "#, collapse(), 0);
    rewrite(r#"
        outer: {
            var x : (int, ());
            let idx = x | arrange;
            inner: { let idx = outer::x | arrange; }
            set x = idx;
        }
        export "out" = ::outer::inner::idx;
    "#, collapse(), 0);
}

#[test]
fn switches_and_unlabelled_ir() {
    let text = r#"let x = input 0 : (int, ()); let idx = x | arrange;
        child: { let same = ::idx | arrange; let shared = ::x | arrange; }
        export "same" = child::same; export "shared" = child::shared;"#;
    let mut p = program(text);
    fn clear(scope: &mut Scope) {
        scope.labels.clear();
        scope.names.clear();
        for item in &mut scope.items { if let Item::Sub(child) = item { clear(child); } }
    }
    clear(&mut p.root);
    assert!(run(&mut p.root, true, true, &mut super::Timings::default(), &mut |_, _, _, _, _, _| {}));
    let (_, problems) = crate::shapes::infer(&p, &|_| None);
    assert!(problems.conflicts.is_empty() && problems.unknown.is_empty());
    assert_eq!(Config::parse("none,hoist-arranges"), Config::parse("none,hoist-invariant"));
}

#[test]
fn inspections_exports_and_feedback_stay() {
    let p = rewrite(r#"
        let x = input 0 : (int, int);
        child: {
            var looped : (int, int);
            let mapped = ::x | map((k, v) -> (v, k));
            let seen = mapped | inspect(keep_this_label);
            set looped = mapped;
        }
        export "mapped" = child::mapped;
        export "seen" = child::seen;
        export "looped" = child::looped;
    "#, hoist(), 0);
    let Item::Sub(child) = p.root.items.last().unwrap() else { panic!() };
    assert!(matches!(&child.items[0], Item::Op(Node::Inspect { label, input: Ref::Import(_) }) if label == "keep_this_label"));
    assert_eq!(child.vars[0].name, "looped");
    assert!(matches!(child.binds[0].value, Ref::Import(_)));
    assert!(child.exports.iter().any(|e| e.name == "mapped" && matches!(e.value, Ref::Import(_))));
}

#[test]
fn variant_inputs_and_half_joins_stop_motion_before_fusion() {
    let p = rewrite(r#"
        let rows = input 0 : (int, int);
        outer: {
            var x : (int, int);
            let variant = x | map((k, v) -> (v, k));
            inner: {
                let mapped = outer::variant | map((k, v) -> (v, k));
                let idx = mapped | arrange;
            }
            let mixed = variant + ::rows;
            let joined = mixed | join(::rows, (k, l, r) -> (k, l));
            let reduced = joined | min;
            set x = ::rows;
        }
        export "out" = outer::reduced;
        export "inner" = ::outer::inner::idx;
    "#, hoist(), 0);
    assert_eq!(p.root.items.len(), 2); // Only the arrange of imported rows escapes.
    let Item::Sub(outer) = &p.root.items[1] else { panic!() };
    assert!(matches!(outer.items[0], Item::Op(Node::Linear { input: Ref::Var(0), .. })));
    assert!(outer.items.iter().any(|i| matches!(i, Item::Op(Node::Reduce { .. }))));
    let inner = outer.items.iter().find_map(|i| if let Item::Sub(c) = i { Some(c) } else { None }).unwrap();
    assert!(inner.items.is_empty());
    let p = rewrite(r#"
        let x = input 0 : (int, int);
        let timed = x | map((k, v) -> (k, (v, (0, 0))));
        child: { let out = ::timed | half_join(::x, 0, (k, l, r) -> (k, l)); }
        export "out" = child::out;
    "#, hoist(), 0);
    let Item::Sub(child) = p.root.items.last().unwrap() else { panic!() };
    assert!(matches!(child.items[0], Item::Op(Node::HalfJoin { .. })));
    for (input, removed) in [("::x", 0), ("::x | join(::x, (k, l, r) -> (k, l))", 1)] {
        let p = rewrite(&format!(r#"
            let x = input 0 : (int, int);
            child: {{
                var again : (int, int);
                let mapped = {input} | map((k, v) -> (v, k));
                let timed = mapped | lift;
                set again = ::x;
            }}
            export "out" = child::timed;
        "#), Config::all(), removed);
        let Item::Sub(child) = p.root.items.last().unwrap() else { panic!() };
        assert_eq!(child.items.len(), 1);
        assert!(matches!(&child.items[0], Item::Op(Node::Linear { input: Ref::Import(_), ops })
            if matches!(ops.as_slice(), [LinearOp::Lift])));
    }
}
