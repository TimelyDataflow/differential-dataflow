use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::{Duration, Instant};

struct ServerProcess {
    child: Child,
    stderr: Option<thread::JoinHandle<String>>,
}

impl ServerProcess {
    fn stop(mut self) {
        let stdin = self.child.stdin.as_mut().expect("server stdin is piped");
        stdin.write_all(b"exit\n").unwrap();
        stdin.flush().unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success(), "server exited with {status}");
                return;
            }
            assert!(Instant::now() < deadline, "server did not exit");
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if let Some(stderr) = self.stderr.take() {
            let output = stderr
                .join()
                .unwrap_or_else(|_| "stderr reader panicked".to_string());
            if thread::panicking() {
                eprintln!("server stderr:\n{output}");
            }
        }
    }
}

fn request(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    reqid: &str,
    command: &str,
) -> Vec<String> {
    request_observing(writer, reader, reqid, command, |_| {})
}

fn request_observing(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    reqid: &str,
    command: &str,
    mut observe: impl FnMut(&str),
) -> Vec<String> {
    writer.write_all(command.as_bytes()).unwrap();
    writer.flush().unwrap();
    let mut data = Vec::new();
    loop {
        let mut line = String::new();
        assert_ne!(
            reader
                .read_line(&mut line)
                .unwrap_or_else(|error| panic!("request {reqid}: {error}")),
            0,
            "server disconnected"
        );
        let line = line.trim_end();
        observe(line);
        let Some(rest) = line
            .strip_prefix(reqid)
            .and_then(|line| line.strip_prefix(' '))
        else {
            continue;
        };
        if let Some(body) = rest.strip_prefix("data ") {
            data.push(body.to_string());
        } else if rest == "ok" || rest.starts_with("ok ") {
            return data;
        } else if rest == "err" || rest.starts_with("err ") {
            panic!("request {reqid} failed: {rest}");
        }
    }
}

fn start_server(backend: &str, workers: usize) -> (ServerProcess, TcpStream, BufReader<TcpStream>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ddir_server"))
        .env("DDIR_WORKERS", workers.to_string())
        .env("DDIR_BACKEND", backend)
        // The retired polling/tick knob must not reintroduce wall-clock
        // progress if it remains in an old deployment environment.
        .env("DDIR_TICK_MS", "1")
        // The child owns these ephemeral ports from bind through shutdown.
        // Probing a free port and dropping its listener races other tests.
        .env("DDIR_BIND", "127.0.0.1:0")
        .env("DDIR_WS_BIND", "127.0.0.1:0")
        .env("DDIR_DIAG_PORT", "0")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().expect("server stderr is piped");
    let (ready_tx, ready_rx) = sync_channel(1);
    let stderr = thread::spawn(move || {
        let mut output = String::new();
        for line in BufReader::new(stderr).lines() {
            let line = match line {
                Ok(line) => line,
                Err(error) => {
                    output.push_str(&format!("reading server stderr failed: {error}\n"));
                    break;
                }
            };
            if let Some(address) = line.strip_prefix("ddir_server: tcp listening on ") {
                let _ = ready_tx.send(address.to_string());
            }
            output.push_str(&line);
            output.push('\n');
        }
        output
    });
    let server = ServerProcess {
        child,
        stderr: Some(stderr),
    };
    let address = ready_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("server did not announce its TCP listener");
    let stream = TcpStream::connect(address).expect("connect to server's bound TCP listener");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let writer = stream.try_clone().unwrap();
    let reader = BufReader::new(stream);
    (server, writer, reader)
}

fn assert_backend(backend: &str) {
    let (server, mut writer, mut reader) = start_server(backend, 4);

    request(
        &mut writer,
        &mut reader,
        "r0",
        "r0 load world begin\nlet rows = input 0;\nexport \"rows\" = rows;\nexport \"minimum\" = rows | min;\nr0 end-load\n",
    );
    request(
        &mut writer,
        &mut reader,
        "r1",
        "r1 feed world 0 begin\n7 val=9\n7 val=3\nr1 end-feed\n",
    );
    request(&mut writer, &mut reader, "r3", "r3 tick\n");
    let rows = request(&mut writer, &mut reader, "r4", "r4 peek rows\n");
    assert_eq!(
        rows,
        vec![
            "diff=1 key=Tuple([Int(7)]) val=Tuple([Int(3)])",
            "diff=1 key=Tuple([Int(7)]) val=Tuple([Int(9)])",
        ]
    );
    let minimum = request(&mut writer, &mut reader, "r5", "r5 peek minimum\n");
    assert_eq!(
        minimum,
        vec!["diff=1 key=Tuple([Int(7)]) val=Tuple([Int(3)])"]
    );

    request(&mut writer, &mut reader, "r6", "r6 tail rows\n");
    request(&mut writer, &mut reader, "r7", "r7 feed world 0 8 val=10\n");
    reader
        .get_mut()
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    let mut unexpected = String::new();
    match reader.read_line(&mut unexpected) {
        Err(error)
            if error.kind() == std::io::ErrorKind::WouldBlock
                || error.kind() == std::io::ErrorKind::TimedOut => {}
        result => panic!("tail advanced without an explicit tick: {result:?} {unexpected:?}"),
    }
    reader
        .get_mut()
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut tail_update = None;
    request_observing(&mut writer, &mut reader, "r8", "r8 tick\n", |line| {
        if let Some(body) = line.strip_prefix("r6 data ") {
            tail_update = Some(body.to_string());
        }
    });
    assert_eq!(
        tail_update.as_deref(),
        Some("time=1 diff=1 key=Tuple([Int(8)]) val=Tuple([Int(10)])")
    );

    // Server-side sourcing: every worker feeds its shard of the recipe, so the
    // union is the source exactly once however many workers there are. Its own
    // program, because a recipe's rows carry a unit value and `world`'s carry
    // an integer, and one input holds one shape.
    request(&mut writer, &mut reader, "r9", "r9 stop r6\n");
    request(
        &mut writer,
        &mut reader,
        "r10",
        "r10 load counted begin\nlet rows = input 0;\nexport \"counted\" = rows;\nr10 end-load\n",
    );
    request(&mut writer, &mut reader, "r11", "r11 feed counted 0 from iota:5\n");
    request(&mut writer, &mut reader, "r12", "r12 tick\n");
    let rows = request(&mut writer, &mut reader, "r13", "r13 peek counted\n");
    assert_eq!(
        rows,
        (0..5)
            .map(|n| format!("diff=1 key=Tuple([Int({n})]) val=Tuple([])"))
            .collect::<Vec<_>>()
    );

    drop(reader);
    drop(writer);
    server.stop();
}

#[test]
fn vec_commands_replay_on_four_workers_without_duplicating_input() {
    assert_backend("vec");
}

#[test]
fn corgi_commands_replay_on_four_workers_without_duplicating_input() {
    assert_backend("corgi");
}

fn assert_typed_sources(backend: &str) {
    let (server, mut writer, mut reader) = start_server(backend, 4);
    request(&mut writer, &mut reader, "g", "g load graph begin\nlet rows = input 0 : ((int, List(int), Option(List(int))) ; ());\nexport \"typed.rows\" = rows | arrange;\ng end-load\n");
    request(&mut writer, &mut reader, "q", "q load copy begin\nlet rows = import \"typed.rows\" : ((int, List(int), Option(List(int))) ; ());\nexport \"typed.copy\" = rows;\nq end-load\n");
    // Neither a first empty list nor a never-populated sum lane can supply
    // its encoding by example. Both the producer and imported trace use the
    // declared shape, without sentinel rows or altered data.
    request(&mut writer, &mut reader, "f", "f feed graph 0 tuple(1,list(),inject(0,tuple()))\n");
    request(&mut writer, &mut reader, "t", "t tick\n");
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek typed.copy\n"),
        vec!["diff=1 key=Tuple([Int(1), List([]), Variant(0, Tuple([]))]) val=Tuple([])"]);
    request(&mut writer, &mut reader, "f2", "f2 feed graph 0 tuple(2,list(97),inject(1,list()))\n");
    request(&mut writer, &mut reader, "f3", "f3 feed graph 0 tuple(1,list(),inject(0,tuple())) diff=-1\n");
    request(&mut writer, &mut reader, "t2", "t2 tick\n");
    assert_eq!(request(&mut writer, &mut reader, "p2", "p2 peek typed.copy\n"),
        vec!["diff=1 key=Tuple([Int(2), List([Int(97)]), Variant(1, List([]))]) val=Tuple([])"]);
    drop(reader);
    drop(writer);
    server.stop();
}

#[test]
fn vec_typed_empty_sources_and_imports_on_four_workers() {
    assert_typed_sources("vec");
}

#[test]
fn corgi_typed_empty_sources_and_imports_on_four_workers() {
    assert_typed_sources("corgi");
}

/// Two consumers join request rows against the same named graph through TCP,
/// while both the requests and graph change.
fn assert_shared_import_requests(backend: &str, workers: usize) {
    let (server, mut writer, mut reader) = start_server(backend, workers);
    request(
        &mut writer,
        &mut reader,
        "g",
        "g load graph begin\nlet e = input 0;\nexport \"edges\" = e | arrange;\ng end-load\n",
    );
    for name in ["a", "b"] {
        request(
            &mut writer,
            &mut reader,
            name,
            &format!(
                "{name} load {name} begin\nlet q = input 0;\nlet e = import \"edges\";\nexport \"{name}.answer\" = (q | key($0[1] ; $0[0])) | join(e, ($1[0] ; $2[0]));\n{name} end-load\n"
            ),
        );
    }
    request(
        &mut writer,
        &mut reader,
        "f",
        "f feed graph 0 begin\n1 val=2\n1 val=3\n2 val=4\nf end-feed\n",
    );
    request(&mut writer, &mut reader, "t", "t tick\n");
    for name in ["a", "b"] {
        assert!(request(
            &mut writer,
            &mut reader,
            "p",
            &format!("p peek {name}.answer\n")
        )
        .is_empty());
    }
    // Distinct request identities with equal bindings must not collapse. A
    // missing key completes normally with no output rows for its request id.
    request(
        &mut writer,
        &mut reader,
        "f",
        "f feed a 0 begin\n10,1\n11,1\n12,99\nf end-feed\n",
    );
    request(&mut writer, &mut reader, "f", "f feed b 0 20,2\n");
    request(&mut writer, &mut reader, "t", "t tick\n");
    let answer = |rid, node| format!("diff=1 key=Tuple([Int({rid})]) val=Tuple([Int({node})])");
    assert_eq!(
        request(&mut writer, &mut reader, "p", "p peek a.answer\n"),
        vec![answer(10, 2), answer(10, 3), answer(11, 2), answer(11, 3)]
    );
    assert_eq!(
        request(&mut writer, &mut reader, "p", "p peek b.answer\n"),
        vec![answer(20, 4)]
    );

    // Retract one request without removing the other equal binding. Graph
    // changes maintain active requests in both independently installed plans.
    request(&mut writer, &mut reader, "f", "f feed a 0 10,1 diff=-1\n");
    request(
        &mut writer,
        &mut reader,
        "f",
        "f feed graph 0 begin\n1 val=3 diff=-1\n1 val=5\n2 val=4 diff=-1\n2 val=6\nf end-feed\n",
    );
    request(&mut writer, &mut reader, "t", "t tick\n");
    assert_eq!(
        request(&mut writer, &mut reader, "p", "p peek a.answer\n"),
        vec![answer(11, 2), answer(11, 5)]
    );
    assert_eq!(
        request(&mut writer, &mut reader, "p", "p peek b.answer\n"),
        vec![answer(20, 6)]
    );
    request(
        &mut writer,
        &mut reader,
        "f",
        "f feed a 0 begin\n11,1 diff=-1\n12,99 diff=-1\nf end-feed\n",
    );
    request(&mut writer, &mut reader, "f", "f feed b 0 20,2 diff=-1\n");
    request(&mut writer, &mut reader, "t", "t tick\n");
    for name in ["a", "b"] {
        assert!(request(
            &mut writer,
            &mut reader,
            "p",
            &format!("p peek {name}.answer\n")
        )
        .is_empty());
    }
    // Reuse an id with a different parameter against the changed graph. No
    // program reinstall and no retained answer from its previous binding.
    request(&mut writer, &mut reader, "f", "f feed a 0 10,2\n");
    request(&mut writer, &mut reader, "t", "t tick\n");
    assert_eq!(
        request(&mut writer, &mut reader, "p", "p peek a.answer\n"),
        vec![answer(10, 6)]
    );
    request(&mut writer, &mut reader, "f", "f feed a 0 10,2 diff=-1\n");
    request(&mut writer, &mut reader, "t", "t tick\n");
    assert!(request(&mut writer, &mut reader, "p", "p peek a.answer\n").is_empty());
    request(&mut writer, &mut reader, "d", "d drop a\n");
    request(&mut writer, &mut reader, "d", "d drop b\n");
    request(&mut writer, &mut reader, "d", "d drop graph\n");
    drop(reader);
    drop(writer);
    server.stop();
}

#[test]
fn vec_requests_follow_shared_graph_changes_on_one_worker() {
    assert_shared_import_requests("vec", 1);
}

#[test]
fn vec_requests_follow_shared_graph_changes_on_four_workers() {
    assert_shared_import_requests("vec", 4);
}

#[test]
fn corgi_requests_follow_shared_graph_changes_on_one_worker() {
    assert_shared_import_requests("corgi", 1);
}

#[test]
fn corgi_requests_follow_shared_graph_changes_on_four_workers() {
    assert_shared_import_requests("corgi", 4);
}
