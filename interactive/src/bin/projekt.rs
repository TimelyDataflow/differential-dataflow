extern crate interactive;

use std::time::Duration;
use interactive::{Command, Plan, Value};

fn main() {

    let mut socket = std::net::TcpStream::connect("127.0.0.1:8000".to_string()).expect("failed to connect");

    Command::<Value>::CreateInput("XYZ".to_string(), Vec::new()).serialize_into(&mut socket);
    Command::<Value>::CreateInput("XYGoal".to_string(), Vec::new()).serialize_into(&mut socket);
    Command::<Value>::CreateInput("XZGoal".to_string(), Vec::new()).serialize_into(&mut socket);

    // Determine errors in the xy plane.
    Plan::<Value>::source("XYZ")
        .project(vec![0,1])
        .distinct()
        .negate()
        .concat(Plan::source("XYGoal"))
        .consolidate()
        // .inspect("xy error")
        .into_rule("XYErrors")
        .into_query()
        .into_command()
        .serialize_into(&mut socket);

    // Determine errors in the xy plane.
    Plan::<Value>::source("XYZ")
        .project(vec![0,2])
        .distinct()
        .negate()
        .concat(Plan::source("XZGoal"))
        .consolidate()
        // .inspect("xz error")
        .into_rule("XZErrors")
        .into_query()
        .into_command()
        .serialize_into(&mut socket);

    Command::<Value>::AdvanceTime(Duration::from_secs(1)).serialize_into(&mut socket);

    Command::<Value>::UpdateInput(
        "XYGoal".to_string(),
        vec![
            (vec![Value::Usize(0), Value::Usize(0)], Duration::from_secs(1), 1),
            (vec![Value::Usize(0), Value::Usize(1)], Duration::from_secs(1), 1),
            (vec![Value::Usize(0), Value::Usize(3)], Duration::from_secs(1), 1),
            (vec![Value::Usize(0), Value::Usize(4)], Duration::from_secs(1), 1),
            (vec![Value::Usize(1), Value::Usize(1)], Duration::from_secs(1), 1),
            (vec![Value::Usize(1), Value::Usize(3)], Duration::from_secs(1), 1),
            (vec![Value::Usize(2), Value::Usize(1)], Duration::from_secs(1), 1),
            (vec![Value::Usize(2), Value::Usize(2)], Duration::from_secs(1), 1),
            (vec![Value::Usize(3), Value::Usize(2)], Duration::from_secs(1), 1),
            (vec![Value::Usize(3), Value::Usize(3)], Duration::from_secs(1), 1),
            (vec![Value::Usize(3), Value::Usize(4)], Duration::from_secs(1), 1),
            (vec![Value::Usize(4), Value::Usize(0)], Duration::from_secs(1), 1),
            (vec![Value::Usize(4), Value::Usize(1)], Duration::from_secs(1), 1),
            (vec![Value::Usize(4), Value::Usize(2)], Duration::from_secs(1), 1),
        ],
    ).serialize_into(&mut socket);

    Command::<Value>::UpdateInput(
        "XZGoal".to_string(),
        vec![
            (vec![Value::Usize(0), Value::Usize(2)], Duration::from_secs(1), 1),
            (vec![Value::Usize(0), Value::Usize(3)], Duration::from_secs(1), 1),
            (vec![Value::Usize(0), Value::Usize(4)], Duration::from_secs(1), 1),
            (vec![Value::Usize(1), Value::Usize(2)], Duration::from_secs(1), 1),
            (vec![Value::Usize(1), Value::Usize(4)], Duration::from_secs(1), 1),
            (vec![Value::Usize(2), Value::Usize(1)], Duration::from_secs(1), 1),
            (vec![Value::Usize(2), Value::Usize(2)], Duration::from_secs(1), 1),
            (vec![Value::Usize(2), Value::Usize(3)], Duration::from_secs(1), 1),
            (vec![Value::Usize(3), Value::Usize(0)], Duration::from_secs(1), 1),
            (vec![Value::Usize(3), Value::Usize(1)], Duration::from_secs(1), 1),
            (vec![Value::Usize(3), Value::Usize(3)], Duration::from_secs(1), 1),
            (vec![Value::Usize(3), Value::Usize(4)], Duration::from_secs(1), 1),
            (vec![Value::Usize(4), Value::Usize(1)], Duration::from_secs(1), 1),
            (vec![Value::Usize(4), Value::Usize(4)], Duration::from_secs(1), 1),
        ],
    ).serialize_into(&mut socket);

    // Determine errors in the xy plane.
    Plan::<Value>::source("XYErrors")
        .distinct()
        .project(vec![])
        .concat(Plan::source("XZErrors").distinct().project(vec![]))
        .consolidate()
        .inspect("error")
        .into_rule("Errors")
        .into_query()
        .into_command()
        .serialize_into(&mut socket);

    Command::<Value>::AdvanceTime(Duration::from_secs(2)).serialize_into(&mut socket);

    Command::<Value>::UpdateInput(
        "XYZ".to_string(),
        vec![
            (vec![Value::Usize(0), Value::Usize(0), Value::Usize(2)], Duration::from_secs(2), 1),
            (vec![Value::Usize(0), Value::Usize(1), Value::Usize(3)], Duration::from_secs(2), 1),
            (vec![Value::Usize(0), Value::Usize(3), Value::Usize(4)], Duration::from_secs(2), 1),
            (vec![Value::Usize(0), Value::Usize(4), Value::Usize(4)], Duration::from_secs(2), 1),
            (vec![Value::Usize(1), Value::Usize(1), Value::Usize(2)], Duration::from_secs(2), 1),
            (vec![Value::Usize(1), Value::Usize(3), Value::Usize(4)], Duration::from_secs(2), 1),
            (vec![Value::Usize(2), Value::Usize(1), Value::Usize(1)], Duration::from_secs(2), 1),
            (vec![Value::Usize(2), Value::Usize(2), Value::Usize(2)], Duration::from_secs(2), 1),
            (vec![Value::Usize(2), Value::Usize(2), Value::Usize(3)], Duration::from_secs(2), 1),
            (vec![Value::Usize(3), Value::Usize(2), Value::Usize(0)], Duration::from_secs(2), 1),
            (vec![Value::Usize(3), Value::Usize(3), Value::Usize(1)], Duration::from_secs(2), 1),
            (vec![Value::Usize(3), Value::Usize(4), Value::Usize(3)], Duration::from_secs(2), 1),
            (vec![Value::Usize(3), Value::Usize(4), Value::Usize(4)], Duration::from_secs(2), 1),
            (vec![Value::Usize(4), Value::Usize(0), Value::Usize(1)], Duration::from_secs(2), 1),
            (vec![Value::Usize(4), Value::Usize(1), Value::Usize(4)], Duration::from_secs(2), 1),
            (vec![Value::Usize(4), Value::Usize(2), Value::Usize(4)], Duration::from_secs(2), 1),
        ],
    ).serialize_into(&mut socket);

    Command::<Value>::AdvanceTime(Duration::from_secs(2)).serialize_into(&mut socket);
    Command::<Value>::Shutdown.serialize_into(&mut socket);
}