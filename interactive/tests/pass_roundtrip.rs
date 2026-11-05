//! Minimized optional-pass round trips: compare topology, scalar graphs, and printed text.

use interactive::{explain, lower, parse, plan};
use interactive::passes::Config;

const REPROS: [(&str, &str); 7] = [
    ("F1",
    r#"let rows = input 0 : (int, ());
        s: { set x = rows; }
        export "out" = rows;
        let unused = s::x;"#),
    ("F2",
    r#"let rows = input 0 : (int, int);
        s: {
            let step = x | join(rows, (k,l,r) -> (k,l));
            set x = (rows + step) | min;
        }
        let dead = rows | map((k,v) -> (v,k));
        export "out" = rows;"#),
    ("F3",
    r#"let rows = input 0 : (int, int);
        let dead = rows | map((k,v) -> ((k,v),v)) | distinct;
        export "out" = rows;"#),
    ("F4",
    r#"let rows = input 0 : (int, int);
        let alias = rows | map((k,v) -> (k,v));
        s: { let both = rows + alias; }
        export "out" = s::both;"#),
    ("hoisted statements, exported aliases, and nested feedback",
    r#"let rows = input 0 : (int, int);
        outer: {
            var x : (int, int);
            inner: {
                let mapped = ::rows | map((k, v) -> (v, k));
                let joined = mapped | join(::rows, (k, l, r) -> (k, l));
                let reduced = joined | min;
            }
            set x = inner::reduced;
        }
        export "out" = outer::x;
        export "invariant" = ::outer::inner::reduced;"#),
    ("local arrangements survive ancestor reuse without structural cleanup (seed 20864)",
    r#"let rows = input 0 : (int, int);
        let twice = rows | arrange | arrange;
        let joined = rows | join(twice, (k, l, r) -> (k, l));
        child: { let idx = ::rows | arrange; var x : (int, int); set x = idx; }
        export "root" = joined;
        export "child" = child::x;"#),
    ("F3 after structural cleanup (seed 16619)",
    r#"let rows = input 0 : ((int, int), ());
        let both = rows + rows;
        let reduced = both | min;
        let dead = reduced + reduced;
        export "out" = rows;"#),
];

fn roundtrip(first: plan::Report, config: Config, context: &str) {
    let printed = first.emit_ddp().unwrap();
    assert_eq!(printed.lambdas.graph_changed, 0, "{context}");
    let second = plan::report(&printed.text, &|_| None, &config).unwrap();
    let differences = first.compare_final(&second);
    assert!(differences.is_empty(), "{context}: {differences:?}");
    assert_eq!(printed.text, second.emit_ddp().unwrap().text, "{context}");
}

#[test]
fn minimized_findings_roundtrip_with_every_pass_subset() {
    // Include invariant hoisting and parent-arrangement collapse as well as the original five.
    for mask in 0..1 << Config::NAMES.len() {
        let names = std::iter::once("none").chain(Config::NAMES.into_iter().enumerate()
            .filter(|(i, _)| mask & (1 << i) != 0).map(|(_, n)| n)).collect::<Vec<_>>().join(",");
        let config = Config::parse(&names).unwrap();
        for (finding, text) in REPROS {
            let report = plan::report(text, &|_| None, &config).unwrap();
            roundtrip(report, config, &format!("{finding}: {names}"));
        }
        let text = r#"let rows = input 0 : (int,int); export "out" = rows | arrange;"#;
        let program = lower::lower_tree(parse::pipe::parse(text));
        let report = plan::report_program(explain::explain(&program), &|_| None, &config).unwrap();
        roundtrip(report, config, &format!("F4 explanation: {names}"));
    }
}

#[test]
fn variable_declarations_remain_distinct_from_inferred_shapes() {
    for declaration in ["", "var x : (int, int);"] {
        let text = format!(r#"let rows = input 0 : (int, int);
            s: {{ {declaration} set x = rows | map((k,v) -> (k,v)); }}
            export "out" = rows;"#);
        let config = Config::parse("none,projection").unwrap();
        let report = plan::report(&text, &|_| None, &config).unwrap();
        let printed = report.emit_ddp().unwrap().text;
        assert_eq!(printed.contains("var x : (int, int);"), !declaration.is_empty());
        assert_eq!(printed.contains("var x;"), declaration.is_empty());
        assert!(report.stages.iter().all(|s| s.problems.is_empty()));
        roundtrip(report, config, declaration);
    }
}
