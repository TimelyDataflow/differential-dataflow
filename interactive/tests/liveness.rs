//! Unread computations are removed while inputs and Inspect remain observable.
use interactive::{lower, parse, scope_ir::{Item, Program, Source}, server::Server};

fn program(src: &str) -> Program { lower::lower_tree(parse::pipe::parse(src)) }

#[test]
fn unread_operators_and_recursive_scopes_are_removed() {
    let original = program(r#"
        let x = input 0 : (int, int);
        let used = x | map((k, v) -> (v, k));
        let dead = x | map((k, v) -> (k, (v, 1) add)) | arrange;
        let dead2 = dead | reduce((k, vs) -> vs);
        loop: {
            var r;
            let step = r | map((k, v) -> (v, k));
            let unread = step | arrange;
            set r = (::x + step) | distinct;
        }
        export "out" = used;
    "#);
    let mut optimized = original.clone();
    interactive::passes::optimize(&mut optimized);
    assert_eq!(optimized.root.imports.len(), 1);
    assert_eq!(optimized.root.imports[0].from, Source::Input(0));
    assert_eq!(optimized.root.items.len(), 1);
    assert_eq!(optimized.root.labels, ["used"]);
    let inputs = vec![vec!["1 val=2".to_string(), "2 val=3".to_string()]];
    let run = |p: &Program| {
        let mut rows = interactive::server::evaluate(timely::Config::process(1), p, &inputs)["out"].rows();
        rows.sort(); rows
    };
    assert_eq!(run(&optimized), run(&original));
}

#[test]
fn inspect_keeps_its_input_and_enclosing_scopes() {
    let mut p = program(r#"
        let unused = input 0 : (int, int);
        let x = input 1 : (int, int);
        values: {
            let dead = unused | map((k, v) -> (v, k));
            let used = x | map((k, v) -> (v, k));
        }
        let unread = values::dead | arrange;
        observer: {
            inner: { let seen = values::used | inspect(seen); }
        }
    "#);
    interactive::passes::optimize(&mut p);
    assert!(p.root.exports.is_empty());
    assert_eq!(p.root.imports.len(), 2);
    assert_eq!(p.root.imports[0].from, Source::Input(0));
    assert_eq!(p.root.imports[1].from, Source::Input(1));
    assert_eq!(p.op_count(), 2);
    // Invariant maps can move before the child; find it by its retained name.
    let values = p.root.items.iter().find_map(|item| match item {
        Item::Sub(child) if child.name == "values" => Some(child), _ => None,
    }).unwrap();
    assert_eq!(values.exports.len(), 1);
    assert_eq!(values.exports[0].name, "used");
    assert_eq!(values.imports.len(), 1);
    // Installing checks the remapped child exports and imports all the way in.
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        server.install(worker, "p", &p).unwrap();
        // The unread root input is still part of the client interface.
        server.feed("p", 0, "3 val=4", None).unwrap();
        server.feed("p", 1, "1 val=2", None).unwrap();
        server.tick(worker);
    });
}

#[test]
fn dead_code_switch_controls_pruning_before_fusion() {
    use interactive::passes::{self, Config, Pass};
    let original = program(r#"
        let x = input 0 : (int, int);
        let alias = x;
        let unused = input 1 : (int, int);
        let absent = import "absent" : (int, int);
        let prefix = x | map((k, v) -> (k, (v, 1) add));
        let dead = prefix | filter((_, v) -> (v, 0) gt);
        lost: { let dead = ::absent | map((k, v) -> (v, k)); }
        export "out" = prefix | map((k, v) -> (v, k));
    "#);
    let off = Config::parse("none,structural").unwrap();
    let on = Config::parse("none,structural,dead-code").unwrap();
    assert!(Config::default().dead_code);
    assert!(!Config::parse("all,-dead-code").unwrap().dead_code);
    assert!(Config::parse("dead-bindings").is_err());
    let run = |config| {
        let mut p = original.clone();
        let timings = passes::run(&mut p, &|_| Some((corgi::Shape::Int, corgi::Shape::Int)),
            &config, &interactive::server::host_time(1), 1, &mut |_, _, _| {});
        (p, timings)
    };
    let (kept, off_times) = run(off);
    assert_eq!(format!("{kept:?}"), format!("{original:?}"));
    assert_eq!(off_times.pass(Pass::DeadCode).runs, 0);
    let (pruned, on_times) = run(on);
    assert_eq!(pruned.root.imports.len(), 2);
    assert!(pruned.root.imports.iter().all(|i| matches!(i.from, Source::Input(_))));
    assert_eq!(pruned.root.items.len(), 1); // Removing the dead reader exposes fusion.
    assert!(!pruned.root.names.iter().any(|(name, _)| name == "alias"));
    assert!(on_times.pass(Pass::DeadCode).changes > 0);
}
