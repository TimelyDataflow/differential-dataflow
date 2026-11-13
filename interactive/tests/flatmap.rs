//! Complete rows and zero-or-one lists; only the passing match lane builds output.
use interactive::{lower, parse, passes, plan, server::Server};

const PROGRAM: &str = r#"
    type Member = User int | Group int;
    let grants = input 0 : (int, (Member, int));
    output "users" = grants | flatmap((doc, (member, level)) ->
        ((doc, level), member) cap_sum match (
            User (((doc, level), u) -> (u call survivor, (doc, level)) enlist),
            Group (_ -> [] : List((int, (int, int))))));
    output "literal" = grants | flatmap((doc, (_, level)) ->
        ((doc, level), (level, 7) eq) branch Option map_variant None (_ -> ())
        match (None (_ -> [] : List((int, int))), Some ((doc, level) -> (level call survivor, doc) enlist)));
    output "all" = grants | flatmap(row -> row enlist);
    output "times" = grants | flatmap(row -> row enlist) | lift;
    output "empty" = grants | flatmap((doc, (_, level)) -> ((doc, level), 0) branch Result
        match (Ok (_ -> [] : List((int, int))), Err (row -> row enlist)));
    output "many" = grants | flatmap((doc, (_, level)) -> ((level, doc) enlist, (level, doc) enlist) append);
"#;

struct Survivor(corgi::Shape);
impl corgi::HostKernel for Survivor {
    fn name(&self) -> &str { "survivor" }
    fn input(&self) -> &corgi::Shape { &self.0 }
    fn output(&self) -> &corgi::Shape { &corgi::Shape::Int }
    fn eval(&self, input: corgi::Value) -> Result<corgi::Value, String> {
        let input = input.into_prod("survivor arguments")?.remove(0);
        assert!(input.as_i64("survivor")?.iter().all(|&x| x != 0), "built a dropped row");
        Ok(input)
    }
}
fn register() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| interactive::lambda::register(std::sync::Arc::new(Survivor(corgi::Shape::Prod(vec![corgi::Shape::Int])))));
}

#[test]
fn lanes_diffs_retractions_and_times() {
    register();
    for workers in [1, 3] {
        timely::execute(timely::Config::process(workers), |worker| {
            let mut program = lower::lower_tree(parse::pipe::parse(PROGRAM));
            passes::optimize(&mut program);
            passes::projection::pushdown(&mut program, &|_| None).unwrap();
            let mut server = Server::new();
            server.set_export_taps(true);
            server.install(worker, "p", &program).unwrap();
            for (epoch, diff) in [3, -2, -1].into_iter().enumerate() {
                if worker.index() == 0 {
                    server.feed("p", 0, &format!(
                        "10 val=tuple(inject(0,2),7) diff={diff}\n20 val=tuple(inject(1,0),0) diff={diff}"), None).unwrap();
                }
                server.tick(worker);
                let count = [3, 1, 0][epoch];
                for (name, k, v) in [("users", "2", "tuple(10,7)"), ("literal", "7", "10")] {
                    let got = server.snapshot(worker, &format!("p::{name}")).unwrap().rows();
                    let want = if count == 0 || worker.index() != 0 { vec![] } else { vec![(k.into(), v.into(), count)] };
                    assert_eq!(got, want, "{name}, epoch {epoch}");
                    let changes = server.take_changes(&format!("p::{name}"));
                    assert!(changes.iter().all(|c| c.times.to_vec().iter().all(|t|
                        *t == interactive::server::host_time(epoch as u64))));
                }
                assert!(server.snapshot(worker, "p::empty").unwrap().rows().is_empty());
                assert_eq!(server.snapshot(worker, "p::all").unwrap().rows().len(), if count == 0 || worker.index() != 0 { 0 } else { 2 });
                let many = server.snapshot(worker, "p::many").unwrap().rows();
                assert!(many.iter().all(|r| r.2 == 2 * count));
            }
        }).unwrap().join().into_iter().for_each(|r| r.unwrap());
    }
}

#[test]
fn printers_roundtrip_named_arms() {
    register();
    let config = passes::Config::parse("all").unwrap();
    let report = plan::report_at(PROGRAM, &|_| None, &config, 0).unwrap();
    let text = report.emit_ddp().unwrap().text;
    assert!(text.contains("flatmap("), "{text}");
    let again = plan::report_at(&text, &|_| None, &config, 0).unwrap();
    assert!(report.compare_final(&again).is_empty());
}

#[test]
fn malformed_rows_are_rejected() {
    timely::execute_directly(|worker| {
        for expr in ["flatmap(row -> row)", "flatmap(_ -> 1 enlist)"] {
            let p = lower::lower_tree(parse::pipe::parse(&format!(
                "output \"x\" = input 0 : (int, ()) | {expr};")));
            assert!(Server::new().install(worker, "p", &p).is_err(), "{expr}");
        }
    });
}
