use interactive::server::Server;
use interactive::{lower, parse};

/// A tuple of integers, as a feed writes it.
fn tup(fields: &[i64]) -> String {
    format!("tuple({})", fields.iter().map(i64::to_string).collect::<Vec<_>>().join(","))
}

#[test]
fn feed_validates_before_staging_one_epoch() {
    timely::execute_directly(|worker| {
        let mut program = lower::lower_tree(parse::pipe::parse(
            "let rows = input 0 : ((int), (int)); export \"rows\" = rows;",
        ));
        interactive::passes::optimize(&mut program);

        let mut server = Server::new();
        server.install(worker, "world", &program).unwrap();
        let rows = "1 val=10\n2 val=20";

        assert!(server.feed("world", 1, rows, None).is_err());
        server.tick(worker);
        assert!(server.snapshot(worker, "rows").unwrap().is_empty());

        server.feed("world", 0, rows, None).unwrap();
        assert!(server.snapshot(worker, "rows").unwrap().is_empty());
        server.tick(worker);
        assert_eq!(
            server.snapshot(worker, "rows").unwrap().rows(),
            vec![(tup(&[1]), tup(&[10]), 1), (tup(&[2]), tup(&[20]), 1),]
        );

        // Per-input advances close new contents without moving the program clock.
        use interactive::server::host_time;
        server.feed("world", 0, "3 val=30", None).unwrap();
        server.advance_input("world", 0, &host_time(3)).unwrap();
        assert_eq!(server.now("world").unwrap(), &host_time(2));
        let rows = server.snapshot(worker, "rows").unwrap().rows();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows, server.snapshot_as_of(worker, "rows", &host_time(2)).unwrap().rows());
        assert_eq!(server.peek(worker, "rows", Some("tuple(3)")).unwrap().rows(), vec![(tup(&[3]), tup(&[30]), 1)]);

        // Bulk advances still read the point immediately before the program clock.
        server.feed("world", 0, "4 val=40", None).unwrap();
        server.advance_to(worker, "world", &host_time(4)).unwrap();
        let rows = server.snapshot(worker, "rows").unwrap().rows();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows, server.snapshot_as_of(worker, "rows", &host_time(3)).unwrap().rows());
    });
}

/// Ticks and transient snapshots must wake for delayed remote rows and progress.
#[test]
fn delayed_peers_wake_ticks_and_snapshots() {
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    {
        let barrier = Arc::new(Barrier::new(3));
        let guards = timely::execute(timely::Config::process(3), move |worker| {
            let mut program = lower::lower_tree(parse::pipe::parse(
                "let rows = input 0 : ((int), ()); export \"rows\" = rows;",
            ));
            interactive::passes::optimize(&mut program);
            let mut server = Server::new();
            server.install(worker, "world", &program).unwrap();
            for epoch in 0..6 {
                barrier.wait();
                if worker.index() == epoch % 3 {
                    std::thread::sleep(Duration::from_millis(20));
                    let diff = if epoch < 3 { 1 } else { -1 };
                    server.feed("world", 0, &format!("{} diff={diff}", epoch % 3), None).unwrap();
                }
                server.tick(worker);
                barrier.wait();
                if worker.index() == (epoch + 1) % 3 {
                    std::thread::sleep(Duration::from_millis(20));
                }
                let rows = server.snapshot(worker, "rows").unwrap().rows();
                let expected: Vec<_> = if worker.index() == 0 {
                    (0..3).filter(|&i| if epoch < 3 { i <= epoch } else { i > epoch - 3 })
                        .map(|i| (tup(&[i as i64]), "tuple()".to_string(), 1)).collect()
                } else { Vec::new() };
                assert_eq!(rows, expected);
            }
        }).unwrap();
        for result in guards.join() { result.unwrap(); }
    }
}

fn install(server: &mut Server, worker: &mut timely::worker::Worker, src: &str) {
    let mut program = lower::lower_tree(parse::pipe::parse(src));
    interactive::passes::optimize(&mut program);
    server.install(worker, "world", &program).unwrap();
}

/// Feed text read into columns at `key` and unit values, as the intake reads it.
fn decoded(key: &corgi::Shape, rows: &[&str]) -> interactive::runtime::container::Updates {
    let mut decoder = interactive::runtime::feed::FeedDecoder::new(key, &corgi::Shape::Unit);
    for row in rows {
        decoder.push(row, None, 1).unwrap();
    }
    decoder.finish()
}

/// `(int, List(int))`.
fn listed_shape() -> corgi::Shape {
    corgi::Shape::Prod(vec![corgi::Shape::Int, corgi::Shape::List(Box::new(corgi::Shape::Int))])
}

fn listed(k: i64, xs: &[i64]) -> String {
    format!("tuple({k},list({}))", xs.iter().map(i64::to_string).collect::<Vec<_>>().join(","))
}

/// A feed split across workers keeps its shapes: a worker whose share holds only empty lists
/// still has their declared element shape.
#[test]
fn split_feeds_keep_their_shapes() {
    let guards = timely::execute(timely::Config::process(2), |worker| {
        let mut server = Server::new();
        install(&mut server, worker, r#"export "rows" = input 0 : ((int, List(int)), ());"#);
        let columns = decoded(&listed_shape(), &["tuple(1,list(7))", "tuple(2,list())"]);
        let mine: Vec<usize> = (worker.index()..columns.len()).step_by(worker.peers()).collect();
        server.feed_columns("world", 0, columns.select(&mine), None).unwrap();
        server.tick(worker);
        server.snapshot(worker, "rows").unwrap().rows()
    }).unwrap();
    let rows = guards.join().into_iter().next().unwrap().unwrap();
    assert_eq!(rows, vec![(listed(1, &[7]), "tuple()".to_string(), 1), (listed(2, &[]), "tuple()".to_string(), 1)]);
}

/// Columns of another shape than the input declares are refused, and staged at none.
#[test]
fn misshaped_columns_are_refused() {
    timely::execute_directly(|worker| {
        let mut server = Server::new();
        install(&mut server, worker, r#"export "rows" = input 0 : ((int, List(int)), ());"#);
        let pair = corgi::Shape::Prod(vec![corgi::Shape::Int, corgi::Shape::Int]);
        let refused = server.feed_columns("world", 0, decoded(&pair, &["1,2"]), None).unwrap_err();
        assert_eq!(refused, "a feed of shape ((Int, Int), ()) reached an input of shape ((Int, List<Int>), ())");
        server.feed_columns("world", 0, decoded(&listed_shape(), &["tuple(1,list())"]), None).unwrap();
        server.tick(worker);
        assert_eq!(server.snapshot(worker, "rows").unwrap().rows(), vec![(listed(1, &[]), "tuple()".to_string(), 1)]);
    });
}

/// A fed sum and a feed at a later time, as the single-row `feed` stages them.
#[test]
fn fed_sums_arrive_at_their_time() {
    timely::execute_directly(|worker| {
        let mut server = Server::new();
        install(&mut server, worker, r#"
            type Node = Leaf int | Pair (int, int);
            export "rows" = input 0 : (Node, ());
        "#);
        let node = corgi::Shape::Sum(vec![corgi::Shape::Int, corgi::Shape::Prod(vec![corgi::Shape::Int, corgi::Shape::Int])]);
        server.feed_columns("world", 0, decoded(&node, &["inject(1,tuple(1,2))"]), None).unwrap();
        server.feed_columns("world", 0, decoded(&node, &["inject(0,7)"]), Some(2)).unwrap();
        server.tick(worker);
        let pair = "inject(1,tuple(1,2))".to_string();
        assert_eq!(server.snapshot(worker, "rows").unwrap().rows(), vec![(pair.clone(), "tuple()".to_string(), 1)]);
        server.tick(worker);
        server.tick(worker);
        let leaf = "inject(0,7)".to_string();
        let mut rows = server.snapshot(worker, "rows").unwrap().rows();
        rows.sort();
        assert_eq!(rows, vec![(leaf, "tuple()".to_string(), 1), (pair, "tuple()".to_string(), 1)]);
    });
}

/// A peek by key reads the key at its export's key shape, a reference to a list row included, and
/// a key not of that shape is an error rather than a crash.
#[test]
fn peek_by_key_reads_reference_keys() {
    timely::execute_directly(|worker| {
        let mut server = Server::new();
        install(&mut server, worker, r#"export "rows" = input 0 : (List(int), ()) | map((k, ()) -> (k ref, ()));"#);
        let list = corgi::Shape::List(Box::new(corgi::Shape::Int));
        server.feed_columns("world", 0, decoded(&list, &["list(1,2)", "list(3)", "list()"]), None).unwrap();
        server.tick(worker);
        let mut rows = |key: &str| server.peek(worker, "rows", Some(key)).map(|u| u.rows());
        assert_eq!(rows("list(1,2)").unwrap(), vec![("list(1,2)".to_string(), "tuple()".to_string(), 1)]);
        assert_eq!(rows("list()").unwrap(), vec![("list()".to_string(), "tuple()".to_string(), 1)]);
        assert!(rows("list(9)").unwrap().is_empty());
        assert!(rows("tuple(1)").is_err());
    });
}
