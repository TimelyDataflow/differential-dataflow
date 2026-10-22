//! Compare the DDIR field-demand fixed point with its in-process mirror, then
//! apply the Rust projection rewrite to each named program.

use corgi::Shape;
use interactive::corgi::container::Updates;
use interactive::corgi::feed::FeedDecoder;
use interactive::projection::{demand_graph, pushdown};
use interactive::server::Server;
use interactive::{lower, parse};

fn updates(keys: Vec<String>, shape: Shape) -> Updates {
    let mut decoder = FeedDecoder::new(&shape, &Shape::Unit);
    for key in keys {
        decoder.push(&key, None, 1).unwrap();
    }
    decoder.finish()
}

fn main() {
    let paths: Vec<String> = std::env::args().skip(1).collect();
    let demand_src =
        std::fs::read_to_string("examples/self_opt/demand.ddp").expect("run from interactive/");
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        let mut demand = lower::lower_tree(parse::pipe::parse(&demand_src));
        demand.optimize();
        server.install(worker, "demand", &demand).unwrap();

        for path in paths {
            let text = std::fs::read_to_string(&path).unwrap();
            let mut program = lower::lower_tree(parse::pipe::parse(&text));
            program.optimize();
            let graph = match demand_graph(&program, &|_| None) {
                Ok(graph) => graph,
                Err(err) => {
                    println!("{path}: skipped: {err}");
                    continue;
                }
            };
            let expected = graph.closure();
            let seeds = graph
                .seeds
                .iter()
                .map(|&id| id.to_string())
                .collect();
            let edges = graph
                .edges
                .iter()
                .map(|&(from, to)| {
                    format!("tuple({from},{to})")
                })
                .collect();
            server
                .feed_columns("demand", 0, updates(seeds, Shape::Int), None)
                .unwrap();
            server
                .feed_columns(
                    "demand",
                    1,
                    updates(edges, Shape::Prod(vec![Shape::Int, Shape::Int])),
                    None,
                )
                .unwrap();
            server.tick(worker);
            let got = server.snapshot(worker, "live").unwrap().rows();
            let mut actual = vec![false; graph.facts];
            for (key, _, diff) in got {
                assert_eq!(diff, 1);
                actual[key.parse::<usize>().unwrap()] = true;
            }
            assert_eq!(expected, actual, "DDIR and Rust disagree on {path}");

            let masked = pushdown(&mut program, &|_| None).unwrap();
            println!(
                "{path}: {} facts, {} live, {} dependencies, {masked} masked fields",
                graph.facts,
                expected.iter().filter(|&&yes| yes).count(),
                graph.edges.len()
            );

            // Keep each input ephemeral; the next program gets an empty demand graph.
            let seeds = graph
                .seeds
                .iter()
                .map(|&id| id.to_string())
                .collect();
            let edges = graph
                .edges
                .iter()
                .map(|&(from, to)| {
                    format!("tuple({from},{to})")
                })
                .collect();
            let mut retract_seeds = updates(seeds, Shape::Int);
            let mut retract_edges = updates(edges, Shape::Prod(vec![Shape::Int, Shape::Int]));
            retract_seeds.diffs.fill(-1);
            retract_edges.diffs.fill(-1);
            server
                .feed_columns("demand", 0, retract_seeds, None)
                .unwrap();
            server
                .feed_columns("demand", 1, retract_edges, None)
                .unwrap();
            server.tick(worker);
            assert!(server.snapshot(worker, "live").unwrap().is_empty());
        }
    });
}
