//! Compare the folded stages with their original chains, including differential history.
use interactive::{ir::LinearOp, lower, parse, passes::{self, Config}, scope_ir::{Item, Node, Program, Scope}, server::{HostTime, Server}};

fn maps(scope: &Scope) -> usize {
    scope.items.iter().map(|item| match item {
        Item::Sub(child) => maps(child),
        Item::Op(Node::Linear { ops, .. }) => ops.iter().filter(|op| matches!(op, LinearOp::Project(_))).count(),
        _ => 0,
    }).sum()
}

fn prepare(text: &str, compose_maps: bool) -> Program {
    let mut p = lower::lower_tree(parse::pipe::parse(text));
    let config = Config { compose_maps, projection: false, as_of_drop: false, ..Config::all() };
    passes::run(&mut p, &|_| None, &config, &HostTime::default(), 3, &mut |_, _, _| {});
    let (_, problems) = interactive::shapes::infer(&p, &|_| None, 3);
    assert!(problems.conflicts.is_empty() && problems.unknown.is_empty(), "{problems:?}");
    p
}

#[test]
fn folded_stages_preserve_signed_updates_at_product_times() {
    let source = "let a = input 0 : (int, (int, int)); let b = input 1 : (int, (int, int));";
    let cases = [
        "a | map((k, v) -> (k, ((v.0, 7) add, v.1))) | map((k, v) -> ((v.1, k), v.0))",
        "a | join(b, (k, x, y) -> (k, (x, y))) | map((k, (x, y)) -> ((y.1, k), x.0))",
        "a | lift | half_join(b, 0, (k, x, y) -> (k, (x, y))) | map((k, ((x, y), t)) -> ((y.1, k), (x.0, t)))",
        "a | lift | half_join(b, 1, (k, x, y) -> (k, (x, y))) | map((k, ((x, y), t)) -> ((y.1, k), (x.0, (t.0, t.1, t.2))))",
        "a | flatmap((k, v) -> (k, (0, v.0) range) cap_list map ((k, n) -> (k, (n, k)))) | map((k, (n, _)) -> (n, k))",
        "a | reduce((_, vs) -> vs map ((v, d) -> ((v.1, v.0), d))) | map((k, v) -> (k, (v.1, k)))",
    ];
    for (case, pipeline) in cases.into_iter().enumerate() {
        let text = format!("{source} output \"out\" = {pipeline};");
        let before = prepare(&text, false);
        let after = prepare(&text, true);
        assert_eq!(maps(&before.root), maps(&after.root) + 1, "case {case}: {after:?}");
        timely::execute_directly(move |worker| {
            let mut server = Server::new();
            server.set_passes(Config::none()); // Install the actual two plans being compared.
            for (name, plan) in [("before", &before), ("after", &after)] {
                server.install_with_root(worker, name, plan, 3, HostTime::default()).unwrap();
            }
            let at = |p: [u64; 3]| HostTime::new(p.to_vec().into());
            // Retract exactly the generated insertions in the next system epoch.
            // The other coordinates include incomparable points and nontrivial joins.
            for epoch in 0..4 {
                for input in 0..2 {
                    for row in 0..37 {
                        let key = (row * 7 + input) % 9;
                        let x = (row * 11 + input * 3) % 6;
                        let y = (row * 13 + input) % 17;
                        let signed = if row % 5 == 0 { -(1 + row % 3) } else { 1 + row % 3 };
                        let diff = if epoch % 2 == 0 { signed } else { -signed };
                        let time = at([epoch as u64, (row % 3) as u64, ((row / 3 + input) % 3) as u64]);
                        let rows = format!("{key} val=tuple({x},{y}) diff={diff}");
                        for name in ["before", "after"] {
                            server.feed_at(name, input as usize, &rows, &time).unwrap();
                        }
                    }
                }
                let frontier = at([epoch as u64 + 1, 0, 0]);
                for name in ["before", "after"] {
                    for input in 0..2 { server.advance_input(name, input, &frontier).unwrap(); }
                    server.wait_for(worker, &format!("{name}::out"), &frontier).unwrap();
                }
                for a in 0..3 {
                    for b in 0..3 {
                        let point = at([epoch as u64, a, b]);
                        assert_eq!(server.snapshot_as_of(worker, "before::out", &point).unwrap().rows(),
                            server.snapshot_as_of(worker, "after::out", &point).unwrap().rows(),
                            "case {case}, point {point:?}");
                    }
                }
            }
        });
    }
}

#[test]
fn barriers_and_computing_maps_remain() {
    let source = "let a = input 0 : (int, (int, int)); let b = input 1 : (int, (int, int));";
    for pipeline in [
        "a | filter((k, _) -> k) | map((k, v) -> (v.0, k))",
        "a | count | map((k, n) -> (n, k))",
        "a | join(b, (k, x, y) -> (k, (x, y))) | map((k, v) -> ((k, 1) add, v))",
        "a | lift | half_join(b, 0, (k, x, _) -> (k, x)) | map((k, (v, t)) -> (k, v))",
        "a | lift | half_join(b, 0, (k, x, _) -> (k, x)) | map((k, (v, t)) -> (t, (v, t)))",
    ] {
        let text = format!("{source} output \"out\" = {pipeline};");
        assert_eq!(maps(&prepare(&text, false).root), maps(&prepare(&text, true).root), "{pipeline}");
    }
    for stage in [
        "a | join(b, (k, x, y) -> (k, (x, y)))",
        "a | lift | half_join(b, 0, (k, x, y) -> (k, (x, y)))",
        "a | flatmap((k, v) -> (k, v) enlist)",
        "a | reduce((_, vs) -> vs)",
    ] {
        let text = format!("{source} let shared = {stage}; output \"other\" = shared; output \"out\" = shared | map((k, _) -> (k, ()));");
        assert_eq!(maps(&prepare(&text, false).root), maps(&prepare(&text, true).root), "{stage}");
    }
}
