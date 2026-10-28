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
    super::super::optimize_with(&mut p, &|_| None, &config, &mut |_, _, _, _, _, _| {});
    assert_eq!(count(&p.root) + removed, before);
    let (shapes, problems) = crate::shapes::infer(&p, &|_| None);
    assert!(problems.unknown.is_empty() && problems.conflicts.is_empty(), "{problems:?}");
    assert_eq!(shapes.exports, exports);
    let once = format!("{p:?}");
    super::super::optimize_with(&mut p, &|_| None, &config, &mut |_, _, _, _, _, _| {});
    assert_eq!(once, format!("{p:?}"));
    p
}

fn hoist() -> Config { Config { hoist_arranges: true, ..Config::none() } }
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
        let Item::Sub(left) = &p.root.items[1] else { panic!() };
        // The original rows are still imported as rows, separately from the trace.
        assert!(left.imports.iter().any(|i| matches!(i.from, Source::Parent(Ref::Import(0)))));
        assert!(left.imports.iter().any(|i| matches!(i.from, Source::Parent(Ref::Local(0)))));
    }
}

#[test]
fn unshared_and_transformed_inputs_stay() {
    for child in [
        "child: { inner: { let idx = ::x | arrange; } let idx = inner::idx; }",
    ] {
        let text = format!("let x = input 0 : (int, int); {child} export \"out\" = child::idx;");
        let before = program(&text);
        let after = rewrite(&text, hoist(), 0);
        assert_eq!(format!("{before:?}"), format!("{after:?}"));
    }
    for op in ["map((k, v) -> (v, k))", "lift"] {
        rewrite(&format!(r#"
            let x = input 0 : (int, int);
            let idx = x | arrange;
            child: {{ let idx = ::x | {op} | arrange; }}
            export "root" = idx;
            export "child" = child::idx;
        "#), hoist(), 0);
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
fn flags_are_independent_and_unlabelled_ir_is_supported() {
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
    assert!(run(&mut p.root, true, true, &mut |_, _, _, _, _, _| {}));
    let (_, problems) = crate::shapes::infer(&p, &|_| None);
    assert!(problems.conflicts.is_empty() && problems.unknown.is_empty());
}
