extern crate interactive;

use interactive::{Command, Plan, Value};

fn main() {

    let mut socket = std::net::TcpStream::connect("127.0.0.1:8000".to_string()).expect("failed to connect");

    Command::SourceLogging(
         "127.0.0.1:9000".to_string(),  // port the server should listen on.
        "timely".to_string(),           // flavor of logging (of "timely", "differential").
        1,                              // number of worker connections to await.
        1_000_000_000,                  // maximum granularity in nanoseconds.
        "remote".to_string()            // name to use for publication.
    ).serialize_into(&mut socket);

    Plan::<Value>::source("logs/remote/timely/operates")
        .inspect("operates")
        .into_rule("operates")
        .into_query()
        .into_command()
        .serialize_into(&mut socket);

    Plan::<Value>::source("logs/remote/timely/channels")
        .inspect("channels")
        .into_rule("channels")
        .into_query()
        .into_command()
        .serialize_into(&mut socket);

    Plan::<Value>::source("logs/remote/timely/schedule/histogram")
        .inspect("schedule")
        .into_rule("schedule")
        .into_query()
        .into_command()
        .serialize_into(&mut socket);

    // Plan::<Value>::source("logs/remote/timely/messages")
    //     .inspect("messages")
    //     .into_rule("messages")
    //     .into_query()
    //     .into_command()
    //     .serialize_into(&mut socket);

    Command::Shutdown.serialize_into(&mut socket);
}