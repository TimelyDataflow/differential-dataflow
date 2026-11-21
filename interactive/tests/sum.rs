use interactive::server::{HostTime, Server};
use interactive::{lower, parse};
use timely::progress::Antichain;

#[test]
fn signed_sums_at_product_times() {
    timely::execute_directly(|worker| {
        let mut server = Server::new();
        let src = r#"
            let sum = input "amounts" : (int, int);
            output "root" = sum | sum;
            nested: { set total = sum | sum; }
            output "nested" = nested::total;
        "#;
        let mut program = lower::lower_tree(parse::pipe::parse(src));
        interactive::passes::optimize(&mut program);
        server.install_with_width(worker, "p", &program, 2).unwrap();
        let time = |a, b| HostTime::new(smallvec::smallvec![a, b]);
        server.feed_at("p", "amounts", "1 val=5 diff=2\n2 val=-3\n3 val=0", &time(0, 1)).unwrap();
        server.feed_at("p", "amounts", "1 val=10 diff=-1\n2 val=-3 diff=-2", &time(1, 0)).unwrap();
        server.feed_at("p", "amounts", "1 val=4", &time(2, 1)).unwrap();
        server.seal("p", "amounts", &Antichain::new()).unwrap();

        for output in ["p::root", "p::nested"] {
            for (point, expected) in [
                (time(0, 1), vec![(1, 10), (2, -3)]),
                (time(1, 0), vec![(1, -10), (2, 6)]),
                (time(1, 1), vec![(2, 3)]),
                (time(2, 1), vec![(1, 4), (2, 3)]),
            ] {
                let rows = server.snapshot_as_of(worker, output, &point).unwrap().rows();
                let expected: Vec<_> = expected.into_iter().map(|(k, v)| (k.to_string(), v.to_string(), 1)).collect();
                assert_eq!(rows, expected, "{output} at {point:?}");
            }
        }
    });
}

#[test]
fn sum_requires_integer_values() {
    timely::execute_directly(|worker| {
        for value in ["()", "float", "(int, int)", "List(int)"] {
            let src = format!(r#"output "total" = input 0 : (int, {value}) | sum;"#);
            let program = lower::lower_tree(parse::pipe::parse(&src));
            let error = Server::new().install(worker, "p", &program).unwrap_err();
            assert!(error.contains("join_function") && error.contains("Int"), "{value}: {error}");
        }
    });
}
