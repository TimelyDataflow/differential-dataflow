extern crate interactive;

use std::time::Duration;
use interactive::{Command, Plan, Value};

fn main() {

    let mut socket = std::net::TcpStream::connect("127.0.0.1:8000".to_string()).expect("failed to connect");

    // Create initially empty set of edges.
    Command::CreateInput("Edges".to_string(), Vec::new()).serialize_into(&mut socket);

    for node in 0 .. 1000 {
        let edge = vec![Value::Usize(node), Value::Usize(node+1)];
        Command::UpdateInput("Edges".to_string(), vec![(edge, Duration::from_secs(0), 1)]).serialize_into(&mut socket);
    }

    // Create initially empty set of edges.
    Command::CreateInput("Nodes".to_string(), Vec::new()).serialize_into(&mut socket);

    Plan::<Value>::source("Nodes")
        .join(Plan::source("Edges"), vec![(0, 0)])
        .project(vec![1])
        .inspect("one-hop")
        .into_rule("One-hop")
        .into_query()
        .into_command()
        .serialize_into(&mut socket);

    Command::AdvanceTime(Duration::from_secs(1)).serialize_into(&mut socket);
    Command::UpdateInput("Nodes".to_string(), vec![(vec![Value::Usize(0)], Duration::from_secs(1), 1)]).serialize_into(&mut socket);
    Command::AdvanceTime(Duration::from_secs(2)).serialize_into(&mut socket);

    Plan::<Value>::source("Nodes")
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
        .inspect("ten-hop")
        .into_rule("Ten-hop")
        .into_query()
        .into_command()
        .serialize_into(&mut socket);

    Command::AdvanceTime(Duration::from_secs(3)).serialize_into(&mut socket);
    Command::Shutdown.serialize_into(&mut socket);
}