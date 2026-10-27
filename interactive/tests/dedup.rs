//! Structural sharing preserves outputs, including feedback and nested scope exports.
use interactive::{lower, parse, scope_ir::{Item, Node, Program}, server::Server};

fn program(src: &str) -> Program { lower::lower_tree(parse::pipe::parse(src)) }

#[test]
fn alpha_renamed_maps_keep_the_first_text_and_label() {
    let mut p = program(r#"
        let x = input 0 : (int, int);
        let a = x | map((k, v) -> (k, v)); let b = x | map((p, q) -> (p, q));
        export "a" = a; export "b" = b;
    "#);
    let Item::Op(Node::Linear { ops, .. }) = &p.root.items[0] else { panic!() };
    let original = format!("{ops:?}");
    p.optimize();
    assert_eq!(p.op_count(), 1);
    assert_eq!(p.root.labels, ["a"]);
    let Item::Op(Node::Linear { ops, .. }) = &p.root.items[0] else { panic!() };
    assert_eq!(format!("{ops:?}"), original);
    assert_eq!(p.root.exports[0].value, p.root.exports[1].value);
}

#[test]
fn nested_recursive_scopes_share_only_with_the_same_inputs() {
    let mut src = "let x = input 0 : (int, int); let y = input 1 : (int, int);\n".to_string();
    for (name, input) in [("a", "x"), ("b", "x"), ("c", "y")] {
        src += &format!(r#"
            {name}: {{
                let seed_{name} = {input} | map((k_{name}, v_{name}) -> (k_{name}, ()));
                inner_{name}: {{
                    let step_{name} = reach_{name} | join({input}, (k_{name}, v_{name}, w_{name}) -> (w_{name}, ()));
                    set reach_{name} = (seed_{name} + step_{name}) | distinct;
                }}
                let out_{name} = inner_{name}::reach_{name};
                let raw_{name} = {input} | map((k_{name}, v_{name}) -> (v_{name}, k_{name}));
            }}
            export "{name}" = {name}::out_{name};
            export "raw_{name}" = {name}::raw_{name};
        "#);
    }
    // A later child imports exports redirected from the duplicate child.
    src += r#"reader: { let both = a::out_a + b::out_b; } export "both" = reader::both;"#;
    let original = program(&src);
    let mut optimized = original.clone();
    optimized.optimize();
    assert_eq!(optimized.root.items.iter().filter(|i| matches!(i, Item::Sub(_))).count(), 3);
    assert!(optimized.op_count() < original.op_count());
    assert_eq!(optimized.root.exports[0].value, optimized.root.exports[2].value);
    assert_eq!(optimized.root.exports[1].value, optimized.root.exports[3].value);
    assert_ne!(optimized.root.exports[0].value, optimized.root.exports[4].value);
    for workers in [1, 2] {
        let (original, optimized) = (original.clone(), optimized.clone());
        timely::execute(timely::Config::process(workers), move |worker| {
            let mut before = Server::new();
            let mut after = Server::new();
            before.install(worker, "before", &original).unwrap();
            after.install(worker, "after", &optimized).unwrap();
            for batch in ["0 val=1\n1 val=2\n2 val=3", "1 val=2 diff=-1\n3 val=4", "0 val=1 diff=-1\n2 val=3 diff=-1"] {
                for (name, server) in [("before", &mut before), ("after", &mut after)] {
                    if worker.index() == 0 {
                        server.feed(name, 0, batch, None).unwrap();
                        server.feed(name, 1, "7 val=8", None).unwrap();
                    }
                    server.tick(worker);
                }
                for export in &original.root.exports {
                    let mut want = before.snapshot(worker, &export.name).unwrap().rows();
                    let mut got = after.snapshot(worker, &export.name).unwrap().rows();
                    want.sort(); got.sort();
                    assert_eq!(got, want, "{}", export.name);
                }
            }
        }).unwrap().join().into_iter().for_each(|r| r.unwrap());
    }
}
