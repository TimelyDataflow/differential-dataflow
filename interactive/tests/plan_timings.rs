//! Timing display must never leak into serialized plans or deterministic reports.
use interactive::{passes::Config, plan::{self, PrintOptions}};

const PROGRAM: &str = r#"
    let x = input 0 : (int, ());
    let y = x | map((k, v) -> ((k, 1) add, v));
    let z = y | map((k, v) -> ((k, 2) add, v));
    export "out" = z;
"#;

#[test]
fn timings_are_only_a_report_display_option() {
    let report = plan::report(PROGRAM, &|_| None, &Config::all()).unwrap();
    let normal = report.print();
    let ddp = report.emit_ddp().unwrap().text;
    let options = PrintOptions { timings: true, ..Default::default() };
    let timed = report.print_with(options).text;
    assert!(timed.starts_with(&normal));
    assert!(!normal.contains("Planning timings"));
    assert!(timed.contains("Planning timings"));
    assert!(timed.contains("Fixed-point rounds:"));
    for stage in &report.stages { assert_eq!(stage.print(), stage.print_with(options).text); }
    assert_eq!(report.emit_ddp().unwrap().text, ddp);
    assert_eq!(plan::report(PROGRAM, &|_| None, &Config::all()).unwrap().print(), normal);
}
