//! Timed batches: a program's inputs each take updates at their own times, and each moves alone,
//! with no `tick`. The program's outputs are complete before the least of the times its inputs
//! stand at. A reader can revisit completed times down to the explicitly chosen read floor.

use timely::progress::Antichain;
use interactive::runtime::container::Updates;
use interactive::runtime::feed::FeedDecoder;
use interactive::server::{host_time, HostTime, Server};
use interactive::{lower, parse, scope_ir::Program};

fn program(text: &str) -> Program {
    lower::lower_tree(parse::pipe::parse(text))
}

/// Rows of shape `((int), (int))`, each `(key, val, time, diff)`, as a timed batch: columns and times.
fn batch(rows: &[(i64, i64, u64, i64)]) -> (Updates, Vec<u64>) {
    let shape = program("output \"x\" = input 0 : ((int), (int));").root.imports[0].shape.clone().unwrap();
    let mut decoder = FeedDecoder::new(&shape.0, &shape.1);
    for (key, val, _, diff) in rows {
        decoder.push(&key.to_string(), Some(&val.to_string()), *diff).unwrap();
    }
    (decoder.finish(), rows.iter().map(|row| row.2).collect())
}

/// The first coordinate of a time.
fn first(time: &HostTime) -> u64 {
    time.get(0).copied().unwrap_or(0)
}

#[test]
fn stepping_and_spinning_an_ordinary_program() {
    timely::execute_directly(|worker| {
        let mut server = Server::new();
        let p = program(r#"let a = input 0 : ((int), (int)); let b = input 1 : ((int), (int)); output "both" = a + b;"#);
        server.install(worker, "p", &p).unwrap();
        let trace = "p::both";
        let (rows, times) = batch(&[(1, 10, 0, 1), (2, 20, 4, 1)]);
        server.feed_timed("p", &0.into(), rows, times).unwrap();

        server.seal("p", &0.into(), &Antichain::from_elem(host_time(10))).unwrap();
        worker.step();
        assert_eq!(server.input_time("p", &1.into()).map(first), Ok(0));
        assert!(server.snapshot_as_of(worker, trace, &host_time(0)).is_err());

        server.seal("p", &1.into(), &Antichain::from_elem(host_time(5))).unwrap();
        worker.step();
        server.wait_for(worker, trace, &host_time(5)).unwrap();
        assert_eq!(server.trace_since(trace).map(first), Some(0));
        assert_eq!(server.snapshot_as_of(worker, trace, &host_time(0)).unwrap().len(), 1);
        assert_eq!(server.snapshot_as_of(worker, trace, &host_time(4)).unwrap().len(), 2);

        // The host can give up old reads independently of either writer.
        server.trim(worker, trace, Some(&host_time(1))).unwrap();
        assert!(server.snapshot_as_of(worker, trace, &host_time(0)).is_err());
        assert_eq!(server.snapshot_as_of(worker, trace, &host_time(1)).unwrap().len(), 1);
        assert_eq!(server.input_time("p", &0.into()).map(first), Ok(10));
        assert_eq!(server.input_time("p", &1.into()).map(first), Ok(5));

        // Spinning leaves the explicitly retained read floor in place.
        server.spin(worker);
        assert_eq!(server.trace_since(trace).map(first), Some(1));
        assert_eq!(server.snapshot_as_of(worker, trace, &host_time(1)).unwrap().len(), 1);
        assert_eq!(server.snapshot_as_of(worker, trace, &host_time(4)).unwrap().len(), 2);
        assert!(server.snapshot_as_of(worker, trace, &host_time(5)).is_err());
    });
}

#[test]
fn one_container_can_use_incomparable_capabilities() {
    use interactive::server::HostContainer;

    let point = |x, y| HostTime::new(smallvec::smallvec![x, y]);
    let guards = timely::execute(timely::Config::process(3), move |worker| {
        let mut server = Server::new();
        let p = program(r#"output "rows" = input 0 : ((int), (int));"#);
        server.install_with_root(worker, "p", &p, 2, point(0, 0)).unwrap();
        let split = [point(2, 0), point(0, 2)].into_iter().collect();
        server.seal("p", &0.into(), &split).unwrap();
        assert!(server.feed_now("p", &0.into(), "1 val=1").is_err());
        let invalid = [point(3, 0), point(1, 1)].into_iter().collect();
        assert!(server.seal("p", &0.into(), &invalid).is_err());
        assert_eq!(server.write_frontier("p", &0.into()).unwrap(), split);

        if worker.index() == 0 {
            let (Updates { keys, vals, diffs }, _) = batch(&[(7, 10, 0, 2), (8, 20, 0, -1)]);
            let mut container = HostContainer { keys, vals, diffs, times: [point(2, 0), point(1, 1)].into_iter().collect() };
            assert!(server.feed_container("p", &0.into(), container.clone()).is_err());
            container.times = [point(2, 0), point(0, 2)].into_iter().collect();
            server.feed_container("p", &0.into(), container).unwrap();
        }
        server.spin(worker);
        assert_eq!(server.target("p::rows").unwrap(), split);
        assert!(server.snapshot_as_of(worker, "p::rows", &point(1, 1)).unwrap().is_empty());
        let next = [point(3, 0), point(0, 3)].into_iter().collect();
        server.seal("p", &0.into(), &next).unwrap();
        server.spin(worker);
        for (cut, expected) in [(point(2, 0), 2), (point(0, 2), -1)] {
            let snapshot = server.snapshot_as_of(worker, "p::rows", &cut).unwrap();
            if worker.index() == 0 { assert_eq!(&*snapshot.diffs, &[expected]); }
        }
        server.seal("p", &0.into(), &Antichain::from_elem(point(3, 3))).unwrap();
        server.seal("p", &0.into(), &Antichain::new()).unwrap();
        server.spin(worker);
        assert!(server.write_frontier("p", &0.into()).unwrap().is_empty());
        assert!(server.seal("p", &0.into(), &next).is_err());
    }).unwrap();
    for result in guards.join() { result.unwrap(); }
}
