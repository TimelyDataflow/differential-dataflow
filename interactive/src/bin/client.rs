extern crate interactive;

use std::time::Duration;
use interactive::{Command, Query, Plan, Rule, Value};

fn main() {

    let mut socket = std::net::TcpStream::connect("127.0.0.1:8000".to_string()).expect("failed to connect");

    // Create initially empty set of edges.
    let command = Command::<Value>::CreateInput("Edges".to_string(), Vec::new());
    interactive::bincode_socket(&mut socket, &command);

    for node in 0 .. 1000 {
        let edge = vec![Value::Usize(node), Value::Usize(node+1)];
        let command = Command::<Value>::UpdateInput("Edges".to_string(), vec![(edge, Duration::from_secs(0), 1)]);
        interactive::bincode_socket(&mut socket, &command);
    }

    // Create initially empty set of edges.
    let command = Command::<Value>::CreateInput("Nodes".to_string(), Vec::new());
    interactive::bincode_socket(&mut socket, &command);

    let command =
    Command::<Value>::Query(
        Query::new()
            .add_rule(
                Rule {
                    name: "One-hop".to_string(),
                    plan: Plan::source("Nodes")
                        .join(Plan::source("Edges"), vec![(0, 0)])
                        .project(vec![1])
                        .inspect("one-hop"),
                }
            )
    );
    interactive::bincode_socket(&mut socket, &command);

    let command = Command::<Value>::AdvanceTime(Duration::from_secs(1));
    interactive::bincode_socket(&mut socket, &command);

    let command = Command::<Value>::UpdateInput("Nodes".to_string(), vec![(vec![Value::Usize(0)], Duration::from_secs(1), 1)]);
    interactive::bincode_socket(&mut socket, &command);

    let command = Command::<Value>::AdvanceTime(Duration::from_secs(2));
    interactive::bincode_socket(&mut socket, &command);

    let command =
    Command::<Value>::Query(
        Query {
            rules: vec![
                Rule {
                    name: "Ten-hop".to_string(),
                    plan: Plan::source("Nodes")
                        .join(Plan::source("Edges"), vec![(0, 0)])
                        .project(vec![1])
                        .join(Plan::source("Edges"), vec![(0, 0)])
                        .project(vec![1])
                        .join(Plan::source("Edges"), vec![(0, 0)])
                        .project(vec![1])
                        .join(Plan::source("Edges"), vec![(0, 0)])
                        .project(vec![1])
                        .join(Plan::source("Edges"), vec![(0, 0)])
                        .project(vec![1])
                        .join(Plan::source("Edges"), vec![(0, 0)])
                        .project(vec![1])
                        .join(Plan::source("Edges"), vec![(0, 0)])
                        .project(vec![1])
                        .join(Plan::source("Edges"), vec![(0, 0)])
                        .project(vec![1])
                        .join(Plan::source("Edges"), vec![(0, 0)])
                        .project(vec![1])
                        .join(Plan::source("Edges"), vec![(0, 0)])
                        .project(vec![1])
                        .inspect("ten-hop"),
                }
            ]
        }
    );
    interactive::bincode_socket(&mut socket, &command);

    let command = Command::<Value>::AdvanceTime(Duration::from_secs(3));
    interactive::bincode_socket(&mut socket, &command);

    let command = Command::<Value>::Shutdown;
    interactive::bincode_socket(&mut socket, &command);
}