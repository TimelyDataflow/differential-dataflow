extern crate interactive;

use std::time::Duration;
use interactive::{Command, Plan, Value};

fn main() {

    let mut socket = std::net::TcpStream::connect("127.0.0.1:8000".to_string()).expect("failed to connect");

    // Create initially empty set of edges.
    Command::<Value>::CreateInput("Edges".to_string(), Vec::new()).serialize_into(&mut socket);

    let nodes = 5;

    for node_0 in 0 .. (nodes / 2) {
        println!("Inserting node: {}", node_0);
        let updates =
        (0 .. nodes)
            .map(|x| vec![Value::Usize(node_0), Value::Usize(x)])
            .map(|e| (e, Duration::from_secs(node_0 as u64), 1))
            .collect::<Vec<_>>();
        Command::<Value>::UpdateInput("Edges".to_string(), updates).serialize_into(&mut socket);
        Command::<Value>::AdvanceTime(Duration::from_secs(node_0 as u64 + 1)).serialize_into(&mut socket);
    }

    Plan::<Value>::multiway_join(
        vec![Plan::source("Edges"), Plan::source("Edges"), Plan::source("Edges")],
        vec![
            vec![(0,1), (1,0)], // b == b
            vec![(0,0), (0,2)], // a == a
            vec![(1,1), (1,2)], // c == c
        ],
        vec![(0,0), (1,0), (1,1)],
    )
        .inspect("triangles")
        .into_rule("triangles")
        .into_query()
        .into_command()
        .serialize_into(&mut socket);


    for node_0 in (nodes / 2) .. nodes {
        let updates =
        (0 .. nodes)
            .map(|x| vec![Value::Usize(node_0), Value::Usize(x)])
            .map(|e| (e, Duration::from_secs(node_0 as u64), 1))
            .collect::<Vec<_>>();
        Command::<Value>::UpdateInput("Edges".to_string(), updates).serialize_into(&mut socket);
        Command::<Value>::AdvanceTime(Duration::from_secs(node_0 as u64 + 1)).serialize_into(&mut socket);
    }

    Command::<Value>::Shutdown.serialize_into(&mut socket);
}