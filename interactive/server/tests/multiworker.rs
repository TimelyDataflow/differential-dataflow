use std::io::{BufRead, BufReader, Read, Write};
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
        self.wait_for_exit();
    }

    /// Close stdin without `exit`: the end of stdin stops the server too.
    fn close_stdin(mut self) {
        drop(self.child.stdin.take().expect("server stdin is piped"));
        self.wait_for_exit();
    }

    fn wait_for_exit(&mut self) {
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
    request_observing(writer, reader, reqid, command, |_, _| {})
}

/// The updates of a binary `data` frame, each as a line: `[time=<t> ]diff=<d> key=<k> val=<v>`.
fn frame_rows(bytes: &[u8]) -> Vec<String> {
    let (updates, lanes) = interactive::runtime::frame::decode(bytes).unwrap();
    updates.rows().into_iter().enumerate().map(|(i, (key, val, diff))| {
        let time = lanes.first().map_or(String::new(), |lane| format!("time={} ", lane[i]));
        format!("{time}diff={diff} key={key} val={val}")
    }).collect()
}

/// Send `command` and read through its `ok`, returning its `data` lines and the rows of its `data`
/// frames. `observe` sees every line, and every frame's header line with its payload.
fn request_observing(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    reqid: &str,
    command: &str,
    mut observe: impl FnMut(&str, Option<&[u8]>),
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
        let payload = match line.split(' ').collect::<Vec<_>>()[..] {
            [_, "frame", _, length] => {
                let mut payload = vec![0; length.parse().unwrap()];
                reader.read_exact(&mut payload).unwrap();
                Some(payload)
            }
            _ => None,
        };
        observe(line, payload.as_deref());
        let Some(rest) = line
            .strip_prefix(reqid)
            .and_then(|line| line.strip_prefix(' '))
        else {
            continue;
        };
        if let Some(body) = rest.strip_prefix("data ") {
            data.push(body.to_string());
        } else if let (true, Some(payload)) = (rest.starts_with("frame data "), payload) {
            data.extend(frame_rows(&payload));
        } else if rest == "ok" || rest.starts_with("ok ") {
            return data;
        } else if rest == "err" || rest.starts_with("err ") {
            panic!("request {reqid} failed: {rest}");
        }
    }
}

fn start_server(workers: usize) -> (ServerProcess, TcpStream, BufReader<TcpStream>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ddir_server"))
        .env("DDIR_WORKERS", workers.to_string())
        // The retired polling/tick knob must not reintroduce wall-clock
        // progress if it remains in an old deployment environment.
        .env("DDIR_TICK_MS", "1")
        // The child owns these ephemeral ports from bind through shutdown.
        // Probing a free port and dropping its listener races other tests.
        .env("DDIR_BIND", "127.0.0.1:0")
        .env("DDIR_WS_BIND", "127.0.0.1:0")
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

/// Seal the inputs contributing to this test's read at `upper - 1`.
fn seal_inputs(writer: &mut TcpStream, reader: &mut BufReader<TcpStream>, inputs: &[&str], upper: u64) {
    for input in inputs {
        request(writer, reader, "s", &format!("s seal {input} {upper}\n"));
    }
}

fn assert_commands_replay() {
    let (server, mut writer, mut reader) = start_server(4);

    request(
        &mut writer,
        &mut reader,
        "r0",
        "r0 load world begin\nlet rows = input 0 : ((int), (int));\noutput \"rows\" = rows;\noutput \"minimum\" = rows | min;\nr0 end-load\n",
    );
    request(
        &mut writer,
        &mut reader,
        "r1",
        "r1 feed world 0 time=0 begin\n7 val=9\n7 val=3\nr1 end-feed\n",
    );
    request(&mut writer, &mut reader, "r3", "r3 seal world 0 1\n");
    let rows = request(&mut writer, &mut reader, "r4", "r4 peek world::rows as corgi\n");
    assert_eq!(
        rows,
        vec![
            "diff=1 key=tuple(7) val=tuple(3)",
            "diff=1 key=tuple(7) val=tuple(9)",
        ]
    );
    // Text defaults to one feed-shaped line per row; the old dump remains available.
    let text = request(&mut writer, &mut reader, "r4", "r4 peek world::rows\n");
    assert_eq!(text, vec!["tuple(7) val=tuple(3)", "tuple(7) val=tuple(9)"]);
    let columns = request(&mut writer, &mut reader, "r4", "r4 peek world::rows as columns\n");
    assert!(matches!(&columns[..], [line] if line.starts_with("keys=") && line.ends_with("diffs=[1, 1]")), "{columns:?}");
    let minimum = request(&mut writer, &mut reader, "r5", "r5 peek world::minimum as corgi\n");
    assert_eq!(
        minimum,
        vec!["diff=1 key=tuple(7) val=tuple(3)"]
    );

    request(&mut writer, &mut reader, "r6", "r6 tail world::rows as corgi\n");
    request(&mut writer, &mut reader, "r7", "r7 feed world 0 time=1 8 val=10\n");
    reader
        .get_mut()
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    let mut unexpected = String::new();
    match reader.read_line(&mut unexpected) {
        Err(error)
            if error.kind() == std::io::ErrorKind::WouldBlock
                || error.kind() == std::io::ErrorKind::TimedOut => {}
        result => panic!("tail advanced without an input seal: {result:?} {unexpected:?}"),
    }
    reader
        .get_mut()
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut tail_update = Vec::new();
    request_observing(&mut writer, &mut reader, "r8", "advance seal world 0 2\nr8 spin\n", |line, payload| {
        if let (true, Some(payload)) = (line.starts_with("r6 frame data "), payload) {
            tail_update.extend(frame_rows(payload));
        }
    });
    assert_eq!(tail_update, vec!["time=1 diff=1 key=tuple(8) val=tuple(10)"]);

    // Every worker receives its share of a text batch. Each supplied row appears once.
    request(&mut writer, &mut reader, "r9", "r9 stop r6\n");
    request(
        &mut writer,
        &mut reader,
        "r10",
        "r10 load counted begin\nlet rows = input 0 : ((int), ());\noutput \"counted\" = rows;\nr10 end-load\n",
    );
    request(&mut writer, &mut reader, "r11", "r11 feed counted 0 time=0 begin\n0\n1\n2\n3\n4\nr11 end-feed\n");
    request(&mut writer, &mut reader, "r12", "r12 seal counted 0 1\n");
    let rows = request(&mut writer, &mut reader, "r13", "r13 peek counted::counted as corgi\n");
    assert_eq!(
        rows,
        (0..5)
            .map(|n| format!("diff=1 key=tuple({n}) val=tuple()"))
            .collect::<Vec<_>>()
    );

    drop(reader);
    drop(writer);
    server.stop();
}

#[test]
fn commands_replay_on_four_workers_without_duplicating_input() {
    assert_commands_replay();
}

#[test]
fn peek_rows_feed_another_program() {
    let (server, mut writer, mut reader) = start_server(4);
    let declaration = "let rows = input 0 : ((Option(int), List(Option(int))), Option(int));";
    for (id, program, export) in [("a", "first", "original"), ("b", "second", "copied")] {
        request(&mut writer, &mut reader, id, &format!("{id} load {program} begin\n{declaration}\noutput \"{export}\" = rows;\n{id} end-load\n"));
    }
    request(&mut writer, &mut reader, "f", "f feed first 0 time=0 begin\ntuple(inject(0,tuple()),list(inject(1,3))) val=inject(1,8)\ntuple(inject(1,-2),list()) val=inject(0,tuple())\nf end-feed\n");
    request(&mut writer, &mut reader, "t", "t seal first 0 1\n");
    let rows = request(&mut writer, &mut reader, "p", "p peek first::original\n");
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|line| line.contains("inject(0,tuple())")));
    let body = rows.join("\n");
    request(&mut writer, &mut reader, "f", &format!("f feed second 0 time=0 begin\n{body}\nf end-feed\n"));
    request(&mut writer, &mut reader, "t", "t seal second 0 1\n");
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek second::copied\n"), rows);
    drop(reader);
    drop(writer);
    server.stop();
}

#[test]
fn text_tail_rows_keep_epochs_and_retractions() {
    let (server, mut writer, mut reader) = start_server(1);
    request(&mut writer, &mut reader, "l", "l load source begin\nlet rows = input 0 : ((int), ());\noutput \"changes\" = rows;\nl end-load\n");
    request(&mut writer, &mut reader, "w", "w tail source::changes\n");
    request(&mut writer, &mut reader, "c", "c tail source::changes as columns\n");
    request(&mut writer, &mut reader, "f", "f feed source 0 time=0 tuple(7)\n");
    let mut changes = Vec::new();
    let mut columns = Vec::new();
    request_observing(&mut writer, &mut reader, "t", "advance seal source 0 1\nt spin\n", |line, _| {
        if let Some(row) = line.strip_prefix("w data ") { changes.push(row.to_string()); }
        if let Some(batch) = line.strip_prefix("c data ") { columns.push(batch.to_string()); }
    });
    request(&mut writer, &mut reader, "f", "f feed source 0 time=1 tuple(7) diff=-1\n");
    request_observing(&mut writer, &mut reader, "t", "advance seal source 0 2\nt spin\n", |line, _| {
        if let Some(row) = line.strip_prefix("w data ") { changes.push(row.to_string()); }
        if let Some(batch) = line.strip_prefix("c data ") { columns.push(batch.to_string()); }
    });
    assert_eq!(changes, ["tuple(7) time=0", "tuple(7) time=1 diff=-1"]);
    assert!(columns.iter().any(|batch| batch.starts_with("times=[0] diffs=[1] keys=") && batch.contains(" vals=Unit(1)")), "{columns:?}");
    assert!(columns.iter().any(|batch| batch.starts_with("times=[1] diffs=[-1] keys=") && batch.contains(" vals=Unit(1)")), "{columns:?}");
    request(&mut writer, &mut reader, "s", "s stop w\n");
    request(&mut writer, &mut reader, "s", "s stop c\n");
    drop(reader);
    drop(writer);
    server.stop();
}

fn assert_typed_sources() {
    let (server, mut writer, mut reader) = start_server(4);
    request(&mut writer, &mut reader, "g", "g load graph begin\nlet rows = input 0 : ((int, List(int), Option(List(int))), ());\noutput \"typed.rows\" = rows | arrange;\ng end-load\n");
    request(&mut writer, &mut reader, "q", "q load copy begin\nlet rows = import \"graph::typed.rows\" : ((int, List(int), Option(List(int))), ());\noutput \"typed.copy\" = rows;\nq end-load\n");
    // Neither a first empty list nor a never-populated sum lane can supply
    // its encoding by example. Both the producer and imported trace use the
    // declared shape, without sentinel rows or altered data.
    request(&mut writer, &mut reader, "f", "f feed graph 0 time=0 tuple(1,list(),inject(0,tuple()))\n");
    request(&mut writer, &mut reader, "t", "t seal graph 0 1\n");
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek copy::typed.copy as corgi\n"),
        vec!["diff=1 key=tuple(1,list(),inject(0,tuple())) val=tuple()"]);
    request(&mut writer, &mut reader, "f2", "f2 feed graph 0 time=1 tuple(2,list(97),inject(1,list()))\n");
    request(&mut writer, &mut reader, "f3", "f3 feed graph 0 time=1 tuple(1,list(),inject(0,tuple())) diff=-1\n");
    request(&mut writer, &mut reader, "t2", "t2 seal graph 0 2\n");
    assert_eq!(request(&mut writer, &mut reader, "p2", "p2 peek copy::typed.copy as corgi\n"),
        vec!["diff=1 key=tuple(2,list(97),inject(1,list())) val=tuple()"]);
    drop(reader);
    drop(writer);
    server.stop();
}

#[test]
fn typed_empty_sources_and_imports_on_four_workers() {
    assert_typed_sources();
}

/// Two consumers join request rows against the same named graph through TCP,
/// while both the requests and graph change.
fn assert_shared_import_requests(workers: usize) {
    let (server, mut writer, mut reader) = start_server(workers);
    request(
        &mut writer,
        &mut reader,
        "g",
        "g load graph begin\nlet e = input 0 : ((int), (int));\noutput \"edges\" = e | arrange;\ng end-load\n",
    );
    for name in ["a", "b"] {
        request(
            &mut writer,
            &mut reader,
            name,
            &format!(
                "{name} load {name} begin\nlet q = input 0 : ((int, int), ());\nlet e = import \"graph::edges\";\noutput \"answer\" = (q | map(((rid, n), ()) -> ((n), (rid)))) | join(e, (_, ((rid), (node))) -> ((rid), (node)));
\n{name} end-load\n"
            ),
        );
    }
    request(
        &mut writer,
        &mut reader,
        "f",
        "f feed graph 0 time=0 begin\n1 val=2\n1 val=3\n2 val=4\nf end-feed\n",
    );
    seal_inputs(&mut writer, &mut reader, &["graph 0", "a 0", "b 0"], 1);
    for name in ["a", "b"] {
        assert!(request(
            &mut writer,
            &mut reader,
            "p",
            &format!("p peek {name}::answer as-of 0 as corgi\n")
        )
        .is_empty());
    }
    // Distinct request identities with equal bindings must not collapse. A
    // missing key completes normally with no output rows for its request id.
    request(
        &mut writer,
        &mut reader,
        "f",
        "f feed a 0 time=1 begin\n10,1\n11,1\n12,99\nf end-feed\n",
    );
    request(&mut writer, &mut reader, "f", "f feed b 0 time=1 20,2\n");
    seal_inputs(&mut writer, &mut reader, &["graph 0", "a 0", "b 0"], 2);
    let answer = |rid, node| format!("diff=1 key=tuple({rid}) val=tuple({node})");
    assert_eq!(
        request(&mut writer, &mut reader, "p", "p peek a::answer as-of 1 as corgi\n"),
        vec![answer(10, 2), answer(10, 3), answer(11, 2), answer(11, 3)]
    );
    assert_eq!(
        request(&mut writer, &mut reader, "p", "p peek b::answer as-of 1 as corgi\n"),
        vec![answer(20, 4)]
    );

    // Retract one request without removing the other equal binding. Graph
    // changes maintain active requests in both independently installed plans.
    request(&mut writer, &mut reader, "f", "f feed a 0 time=2 10,1 diff=-1\n");
    request(
        &mut writer,
        &mut reader,
        "f",
        "f feed graph 0 time=2 begin\n1 val=3 diff=-1\n1 val=5\n2 val=4 diff=-1\n2 val=6\nf end-feed\n",
    );
    seal_inputs(&mut writer, &mut reader, &["graph 0", "a 0", "b 0"], 3);
    assert_eq!(
        request(&mut writer, &mut reader, "p", "p peek a::answer as-of 2 as corgi\n"),
        vec![answer(11, 2), answer(11, 5)]
    );
    assert_eq!(
        request(&mut writer, &mut reader, "p", "p peek b::answer as-of 2 as corgi\n"),
        vec![answer(20, 6)]
    );
    request(
        &mut writer,
        &mut reader,
        "f",
        "f feed a 0 time=3 begin\n11,1 diff=-1\n12,99 diff=-1\nf end-feed\n",
    );
    request(&mut writer, &mut reader, "f", "f feed b 0 time=3 20,2 diff=-1\n");
    seal_inputs(&mut writer, &mut reader, &["graph 0", "a 0", "b 0"], 4);
    for name in ["a", "b"] {
        assert!(request(
            &mut writer,
            &mut reader,
            "p",
            &format!("p peek {name}::answer as-of 3 as corgi\n")
        )
        .is_empty());
    }
    // Reuse an id with a different parameter against the changed graph. No
    // program reinstall and no retained answer from its previous binding.
    request(&mut writer, &mut reader, "f", "f feed a 0 time=4 10,2\n");
    seal_inputs(&mut writer, &mut reader, &["graph 0", "a 0", "b 0"], 5);
    assert_eq!(
        request(&mut writer, &mut reader, "p", "p peek a::answer as-of 4 as corgi\n"),
        vec![answer(10, 6)]
    );
    request(&mut writer, &mut reader, "f", "f feed a 0 time=5 10,2 diff=-1\n");
    seal_inputs(&mut writer, &mut reader, &["graph 0", "a 0", "b 0"], 6);
    assert!(request(&mut writer, &mut reader, "p", "p peek a::answer as-of 5 as corgi\n").is_empty());
    request(&mut writer, &mut reader, "d", "d drop a\n");
    request(&mut writer, &mut reader, "d", "d drop b\n");
    request(&mut writer, &mut reader, "d", "d drop graph\n");
    drop(reader);
    drop(writer);
    server.stop();
}

#[test]
fn requests_follow_shared_graph_changes_on_one_worker() {
    assert_shared_import_requests(1);
}

#[test]
fn requests_follow_shared_graph_changes_on_four_workers() {
    assert_shared_import_requests(4);
}

/// Keep `line`'s frame, if it is one of `reqid`'s: its kind and payload.
fn keep_frame(frames: &mut Vec<(String, Vec<u8>)>, reqid: &str, line: &str, payload: Option<&[u8]>) {
    let header = line.strip_prefix(reqid).and_then(|rest| rest.strip_prefix(" frame "));
    if let (Some(header), Some(payload)) = (header, payload) {
        frames.push((header.split(' ').next().unwrap().to_string(), payload.to_vec()));
    }
}

/// A binary tail starts with a `schema` frame, and its `data` frames, summed through each sealed cut from
/// the replay on, are a binary peek's answer at that cut. Its `progress` frames say how far
/// it has sent. Lists and sums included.
fn assert_tail_frames_sum_to_peeks(workers: usize) {
    use interactive::runtime::frame;
    use std::collections::BTreeMap;
    let (server, mut writer, mut reader) = start_server(workers);
    request(&mut writer, &mut reader, "g", "g load graph begin\nlet rows = input 0 : ((int, List(int)), Option(int));\noutput \"rows\" = rows | arrange;\ng end-load\n");
    request(&mut writer, &mut reader, "f", "f feed graph 0 time=0 begin\ntuple(1,list(2,3)) val=inject(1,7)\ntuple(2,list()) val=inject(0,tuple())\nf end-feed\n");
    request(&mut writer, &mut reader, "t", "t seal graph 0 1\n");
    let mut frames = Vec::new();
    request_observing(&mut writer, &mut reader, "w", "w tail graph::rows as corgi\n", |line, payload| keep_frame(&mut frames, "w", line, payload));
    assert_eq!(frames.first().map(|f| f.0.as_str()), Some("schema"));
    let feeds = ["tuple(3,list(4)) val=inject(1,-1)", "tuple(1,list(2,3)) val=inject(1,7) diff=-1", "tuple(2,list()) val=inject(0,tuple()) diff=2"];
    for (round, feed) in feeds.into_iter().enumerate() {
        let time = round + 1;
        request(&mut writer, &mut reader, "f", &format!("f feed graph 0 time={time} begin\n{feed}\nf end-feed\n"));
        request_observing(&mut writer, &mut reader, "t", &format!("advance seal graph 0 {}\nt spin\n", time + 1), |line, payload| keep_frame(&mut frames, "w", line, payload));
        let mut changes: BTreeMap<(String, String), i64> = BTreeMap::new();
        for (_, payload) in frames.iter().filter(|f| f.0 == "data") {
            for (key, val, diff) in frame::decode(payload).unwrap().0.rows() {
                *changes.entry((key, val)).or_insert(0) += diff;
            }
        }
        changes.retain(|_, diff| *diff != 0);
        let mut peeked = Vec::new();
        request_observing(&mut writer, &mut reader, "p", &format!("p peek graph::rows as-of {time} as corgi\n"), |line, payload| keep_frame(&mut peeked, "p", line, payload));
        let (updates, _) = frame::decode(&peeked.iter().find(|f| f.0 == "data").unwrap().1).unwrap();
        let answer: BTreeMap<(String, String), i64> = updates.rows().into_iter().map(|(k, v, d)| ((k, v), d)).collect();
        assert_eq!(changes, answer, "round {round}, {workers} workers");
        let upper = frames.iter().rev().find(|f| f.0 == "progress").map(|f| u64::from_le_bytes(f.1[..8].try_into().unwrap()));
        assert_eq!(upper, Some(round as u64 + 2), "round {round}: every change before the epoch has been sent");
    }
    request(&mut writer, &mut reader, "s", "s stop w\n");
    drop(reader);
    drop(writer);
    server.stop();
}

#[test]
fn tail_frames_sum_to_peeks_on_one_worker() {
    assert_tail_frames_sum_to_peeks(1);
}

#[test]
fn tail_frames_sum_to_peeks_on_four_workers() {
    assert_tail_frames_sum_to_peeks(4);
}

/// Send `header` and then `payload` as one write, and read through the request's `ok` or `err`.
fn framed(writer: &mut TcpStream, reader: &mut BufReader<TcpStream>, reqid: &str, header: &str, payload: &[u8]) -> Result<(), String> {
    let mut wire = header.as_bytes().to_vec();
    wire.extend_from_slice(payload);
    writer.write_all(&wire).unwrap();
    writer.flush().unwrap();
    loop {
        let mut line = String::new();
        assert_ne!(reader.read_line(&mut line).unwrap(), 0, "server disconnected");
        if let [_, "frame", _, length] = line.trim_end().split(' ').collect::<Vec<_>>()[..] {
            reader.read_exact(&mut vec![0; length.parse().unwrap()]).unwrap();
            continue;
        }
        let Some(rest) = line.trim_end().strip_prefix(reqid).and_then(|line| line.strip_prefix(' ')) else { continue };
        if rest == "ok" || rest.starts_with("ok ") {
            return Ok(());
        } else if let Some(error) = rest.strip_prefix("err ") {
            return Err(error.to_string());
        }
    }
}

/// The shapes an input declares as `declaration`, say `((int), ())`.
fn declared(declaration: &str) -> interactive::server::RowShape {
    let program = interactive::lower::lower_tree(interactive::parse::pipe::parse(&format!("output \"x\" = input 0 : {declaration};")));
    program.root.imports[0].shape.clone().expect("a declared input")
}

/// A feed as a binary frame: the columns of `rows`, read at `shape` (text rows, as `feed … begin`
/// reads them), in the frame format a peek answers with.
fn feed_frame(shape: &interactive::server::RowShape, rows: &[(&str, Option<&str>, i64)]) -> Vec<u8> {
    let mut decoder = interactive::runtime::feed::FeedDecoder::new(&shape.0, &shape.1);
    for (key, val, diff) in rows {
        decoder.push(key, *val, *diff).unwrap();
    }
    interactive::runtime::frame::encode(&decoder.finish(), &[])
}

/// Send `wire`, then `z spin`, and return every line answered before the spin's answer.
fn answers(writer: &mut TcpStream, reader: &mut BufReader<TcpStream>, wire: &str) -> Vec<String> {
    writer.write_all(format!("{wire}z spin\n").as_bytes()).unwrap();
    writer.flush().unwrap();
    let mut lines = Vec::new();
    loop {
        let mut line = String::new();
        assert_ne!(reader.read_line(&mut line).unwrap(), 0, "server disconnected");
        let line = line.trim_end();
        if line.starts_with("z ") {
            assert!(line.starts_with("z ok"), "{line}");
            return lines;
        }
        lines.push(line.to_string());
    }
}

/// A group runs its commands back to back, each row of a batch once however many workers there
/// are, and answers once, on the reqid of its `open`. A refused group runs none of its commands.
fn assert_groups(workers: usize) {
    let (server, mut writer, mut reader) = start_server(workers);
    request(&mut writer, &mut reader, "r0", "r0 load world begin\nlet rows = input \"rows\" : (int, ());\nlet more = input \"more\" : (int, ());\noutput \"both\" = rows + more;\nr0 end-load\n");
    let group = "g1 open\nm1 feed world rows time=0 begin\n0\n1\n2\n3\n4\nm1 end-feed\nm2 feed world more 9 time=0\nm3 seal world rows 1\nm4 seal world more 1\nm5 spin\ng1 shut\n";
    assert_eq!(answers(&mut writer, &mut reader, group), vec!["g1 ok ran 5 commands"]);
    let both = request(&mut writer, &mut reader, "r1", "r1 peek world::both as-of 0\n");
    assert_eq!(both, vec!["0", "1", "2", "3", "4", "9"]);

    // A peek is not a command a group holds: the feed before it never runs.
    let lines = answers(&mut writer, &mut reader, "g2 open\nfeed world rows 7 time=1\npeek world::both\nseal world rows 2\ng2 shut\n");
    assert!(matches!(&lines[..], [line] if line.starts_with("g2 err command 2:")), "{lines:?}");
    seal_inputs(&mut writer, &mut reader, &["world rows", "world more"], 2);
    assert_eq!(request(&mut writer, &mut reader, "r2", "r2 peek world::both as-of 1\n"), both);

    drop(reader);
    drop(writer);
    server.stop();
}

#[test]
fn groups_on_one_worker() {
    assert_groups(1);
}

#[test]
fn groups_on_three_workers() {
    assert_groups(3);
}

/// `feed … frame` stages columns, each row once however many workers there are; a frame of
/// another shape, one cut short, and one too large for a feed are refused, and the session reads
/// on past each.
fn assert_framed_feeds(workers: usize) {
    let (server, mut writer, mut reader) = start_server(workers);
    request(&mut writer, &mut reader, "r0", "r0 load fed begin\nlet rows = input 0 : ((int, List(int)), (int));\noutput \"fed\" = rows;\nr0 end-load\n");
    let shape = declared("((int, List(int)), (int))");
    let rows: Vec<(String, Option<String>, i64)> = (0..10).map(|i| (format!("tuple({i},list({}))", "7,".repeat(i as usize).trim_end_matches(',')), Some(format!("tuple({})", 300 * i - 1000)), i + 1)).collect();
    let rows: Vec<(&str, Option<&str>, i64)> = rows.iter().map(|(k, v, d)| (k.as_str(), v.as_deref(), *d)).collect();
    let bytes = feed_frame(&shape, &rows);
    framed(&mut writer, &mut reader, "r1", &format!("r1 feed fed 0 time=0 frame {}\n", bytes.len()), &bytes).unwrap();
    request(&mut writer, &mut reader, "r2", "r2 seal fed 0 1\n");
    let fed = request(&mut writer, &mut reader, "r3", "r3 peek fed::fed as corgi\n");
    // The same rows fed as text, to another program, are the reference.
    request(&mut writer, &mut reader, "r4", "r4 load text begin\nlet rows = input 0 : ((int, List(int)), (int));\noutput \"text\" = rows;\nr4 end-load\n");
    let body: String = rows.iter().map(|(k, v, d)| format!("{k} val={} diff={d}\n", v.unwrap())).collect();
    request(&mut writer, &mut reader, "r5", &format!("r5 feed text 0 time=0 begin\n{body}r5 end-feed\n"));
    request(&mut writer, &mut reader, "r6", "r6 seal text 0 1\n");
    let text = request(&mut writer, &mut reader, "r7", "r7 peek text::text as corgi\n");
    assert_eq!(fed.len(), 10);
    assert_eq!(fed, text);

    // Another shape: the value is not a one-field tuple.
    let other = feed_frame(&declared("((int, List(int)), int)"), &[("tuple(1,list())", Some("5"), 1)]);
    let error = framed(&mut writer, &mut reader, "r8", &format!("r8 feed fed 0 time=1 frame {}\n", other.len()), &other).unwrap_err();
    assert!(error.contains("shape"), "{error}");
    // Cut short: its header counts the bytes sent, but they are not a whole frame.
    let short = &bytes[..bytes.len() - 8];
    assert!(framed(&mut writer, &mut reader, "r9", &format!("r9 feed fed 0 time=1 frame {}\n", short.len()), short).is_err());
    // More bytes than a feed may hold, and an input with no declared shape: read past, refused.
    let too_large = framed(&mut writer, &mut reader, "r10", &format!("r10 feed fed 0 time=1 frame {}\n", 17 << 20), &vec![0; 17 << 20]).unwrap_err();
    assert!(too_large.contains("exceeds"), "{too_large}");
    assert!(framed(&mut writer, &mut reader, "r11", &format!("r11 feed fed 3 time=1 frame {}\n", bytes.len()), &bytes).is_err());
    // The session reads on: nothing refused reached the program.
    request(&mut writer, &mut reader, "r12", "r12 seal fed 0 2\n");
    assert_eq!(request(&mut writer, &mut reader, "r13", "r13 peek fed::fed as corgi\n"), fed);

    drop(reader);
    drop(writer);
    server.stop();
}

#[test]
fn framed_feeds_on_one_worker() {
    assert_framed_feeds(1);
}

#[test]
fn framed_feeds_on_three_workers() {
    assert_framed_feeds(3);
}

#[test]
fn planning_reads_live_exports_without_installing() {
    let (server, mut writer, mut reader) = start_server(2);
    request(&mut writer, &mut reader, "r0", "r0 load provider begin\noutput \"rows\" = input 0 : (int, int);\nr0 end-load\n");
    request(&mut writer, &mut reader, "s", "s seal provider 0 8\n");
    request(&mut writer, &mut reader, "t", "t trim provider::rows 7\n");
    let before = request(&mut writer, &mut reader, "before", "before list\n");
    let command = "p plan preview begin\noutput \"out\" = import \"provider::rows\" | distinct;\np end-plan\n";
    let plan = request(&mut writer, &mut reader, "p", command);
    assert!(plan.iter().any(|line| line.contains("Imported histories advance to G = (7, [])")));
    assert!(plan.iter().any(|line| line.contains("reduce(")));
    assert!(!plan.iter().any(|line| line.contains("Unresolved import")));
    assert_eq!(plan, request(&mut writer, &mut reader, "p", command));
    assert_eq!(before, request(&mut writer, &mut reader, "after", "after list\n"));
    request(&mut writer, &mut reader, "r3", "r3 drop provider\n");
    let missing = request(&mut writer, &mut reader, "p", command);
    assert!(missing.iter().any(|line| line.contains("Unresolved import: \"provider::rows\"")));
    server.stop();
}

#[test]
fn diagnostics_plan_uses_source_shapes_and_reports_the_legacy_knob() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ddir_server"))
        .env("DDIR_BIND", "127.0.0.1:0").env("DDIR_WS_BIND", "127.0.0.1:0")
        .env("DDIR_WORKERS", "2").env("DDIR_DIAGNOSTICS", "0")
        .env("DDIR_PROJECTION", "1").env_remove("DDIR_PASSES")
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn().unwrap();
    child.stdin.take().unwrap().write_all(b"p plan preview begin\noutput \"out\" = import \"diag.time\";\np end-plan\nexit\n").unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let warning = String::from_utf8(output.stderr).unwrap();
    assert!(warning.contains("DDIR_PROJECTION is ignored; use DDIR_PASSES=projection"), "{warning}");
    let report = String::from_utf8(output.stdout).unwrap();
    assert!(!report.contains("Unresolved import") && !report.contains("incomplete"), "{report}");
    assert!(report.contains("projection switched off"), "{report}");
    let stages = report.lines().filter(|line| line.starts_with("p data === ")).count();
    assert!(stages > 0, "{report}");
    assert!(report.contains(&format!("p ok planned \"preview\" ({stages} stages)")), "{report}");
}

/// A refused load of a program already installed does not redirect feeds to it: the workers
/// resolve an input's name against the program installed.
#[test]
fn refused_loads_do_not_redirect_named_inputs() {
    let (server, mut writer, mut reader) = start_server(2);
    let program = |first: &str, second: &str| format!("let x = input {first:?} : ((int), ());\nlet y = input {second:?} : ((int), ());\noutput \"a\" = x;\noutput \"b\" = y;");
    request(&mut writer, &mut reader, "l1", &format!("l1 load g begin\n{}\nl1 end-load\n", program("a", "b")));
    let refused = framed(&mut writer, &mut reader, "l2", &format!("l2 load g begin\n{}\nl2 end-load\n", program("b", "a")), &[]);
    assert!(matches!(&refused, Err(e) if e.contains("already installed")), "{refused:?}");
    request(&mut writer, &mut reader, "f", "f feed g a time=0 7\n");
    seal_inputs(&mut writer, &mut reader, &["g a", "g b"], 1);
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek g::a\n"), ["tuple(7)"]);
    assert!(request(&mut writer, &mut reader, "q", "q peek g::b\n").is_empty());
    drop(reader);
    drop(writer);
    server.stop();
}

/// A load binds an input of its program to another program's output: the input reads the output,
/// as an import does, and follows its changes.
#[test]
fn loads_bind_inputs_to_published_outputs() {
    let (server, mut writer, mut reader) = start_server(2);
    request(&mut writer, &mut reader, "g", "g load g begin\nlet e = input \"e\" : ((int), (int));\noutput \"edges\" = e | arrange;\ng end-load\n");
    request(&mut writer, &mut reader, "q", "q load q edges=g::edges begin\nlet edges = input \"edges\" : ((int), (int));\nlet roots = input \"roots\" : ((int), ());\noutput \"hop\" = roots | join(edges, (_, ((), d)) -> ((d), ()));\nq end-load\n");
    request(&mut writer, &mut reader, "f", "f feed g e time=0 begin\n1 val=2\n1 val=3\n4 val=5\nf end-feed\n");
    request(&mut writer, &mut reader, "r", "r feed q roots time=0 1\n");
    seal_inputs(&mut writer, &mut reader, &["g e", "q roots"], 1);
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek q::hop\n"), ["tuple(tuple(2))", "tuple(tuple(3))"]);
    request(&mut writer, &mut reader, "f", "f feed g::e time=1 1 val=3 diff=-1\n");
    seal_inputs(&mut writer, &mut reader, &["g e", "q roots"], 2);
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek q::hop\n"), ["tuple(tuple(2))"]);
    drop(reader);
    drop(writer);
    server.stop();
}

/// A timed batch as a binary frame: the columns of `rows`, each `(key, val, diff, time)`, read at `shape`,
/// with one time lane, as a binary tail's frames have.
fn timed_frame(shape: &interactive::server::RowShape, rows: &[(&str, Option<&str>, i64, u64)]) -> Vec<u8> {
    let mut decoder = interactive::runtime::feed::FeedDecoder::new(&shape.0, &shape.1);
    for (key, val, diff, _) in rows {
        decoder.push(key, *val, *diff).unwrap();
    }
    interactive::runtime::frame::encode(&decoder.finish(), &[rows.iter().map(|row| row.3).collect()])
}

/// The rows of the `data` frames among `frames`, sorted, and the upper of the last `progress`
/// frame among them: a tail may say its progress in several steps.
fn heard(frames: &mut Vec<(String, Vec<u8>)>) -> (Vec<String>, Option<u64>) {
    let mut rows: Vec<String> = frames.iter().filter(|f| f.0 == "data").flat_map(|f| frame_rows(&f.1)).collect();
    rows.sort();
    let progress = frames.iter().rev().find(|f| f.0 == "progress").map(|f| u64::from_le_bytes(f.1[..8].try_into().unwrap()));
    frames.clear();
    (rows, progress)
}

/// A program's inputs each take a timed batch, updates at their own times, and each moves alone
/// with `seal`. No `tick` is sent. A binary tail of its output sends each change at its time,
/// and `progress` as the least of the inputs' times moves; a peek reads what is complete.
fn assert_timed_feeds(workers: usize) {
    let (server, mut writer, mut reader) = start_server(workers);
    let loaded = framed(&mut writer, &mut reader, "l", "l load p begin\nlet a = input \"a\" : ((int), (int));\nlet b = input \"b\" : ((int), (int));\noutput \"both\" = a + b;\nl end-load\n", &[]);
    assert_eq!(loaded, Ok(()));
    let shape = declared("((int), (int))");
    let mut frames = Vec::new();
    request_observing(&mut writer, &mut reader, "w", "w tail p::both as corgi\n", |line, payload| keep_frame(&mut frames, "w", line, payload));
    let a = timed_frame(&shape, &[("3", Some("30"), 1, 2), ("1", Some("10"), 1, 0), ("2", Some("20"), 1, 1), ("1", Some("10"), -1, 2)]);
    let b = timed_frame(&shape, &[("1", Some("100"), 1, 0), ("2", Some("200"), 1, 3)]);
    framed(&mut writer, &mut reader, "f", &format!("f feed p a frame {}\n", a.len()), &a).unwrap();
    framed(&mut writer, &mut reader, "f", &format!("f feed p::b frame {}\n", b.len()), &b).unwrap();
    let mut seal = |writer: &mut TcpStream, reader: &mut BufReader<TcpStream>, command: &str| {
        request_observing(writer, reader, "s", &format!("a seal {command}\ns spin\n"), |line, payload| keep_frame(&mut frames, "w", line, payload));
        heard(&mut frames)
    };

    // Input `b` is still at 0, so nothing is complete.
    assert!(seal(&mut writer, &mut reader, "p a 2").0.is_empty());
    assert!(framed(&mut writer, &mut reader, "p", "p peek p::both\n", &[]).unwrap_err().contains("no closed time"));
    // Time 0 is complete once `b` passes it. The changes come before the `spin` after it is answered.
    assert_eq!(seal(&mut writer, &mut reader, "p::b 1"), (vec!["time=0 diff=1 key=tuple(1) val=tuple(10)".into(), "time=0 diff=1 key=tuple(1) val=tuple(100)".into()], Some(1)));
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek p::both\n"), ["tuple(1) val=tuple(10)", "tuple(1) val=tuple(100)"]);
    // `a` is at 2 and `b` at 4: time 1 is complete.
    assert_eq!(seal(&mut writer, &mut reader, "p b 4"), (vec!["time=1 diff=1 key=tuple(2) val=tuple(20)".into()], Some(2)));
    // `a` is at 5 and `b` at 4: times 2 and 3 are complete.
    let (rows, progress) = seal(&mut writer, &mut reader, "p a 5");
    assert_eq!(rows, ["time=2 diff=-1 key=tuple(1) val=tuple(10)", "time=2 diff=1 key=tuple(3) val=tuple(30)", "time=3 diff=1 key=tuple(2) val=tuple(200)"]);
    assert_eq!(progress, Some(4));
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek p::both\n"), ["tuple(1) val=tuple(100)", "tuple(2) val=tuple(20)", "tuple(2) val=tuple(200)", "tuple(3) val=tuple(30)"]);

    // A timed batch with a time before its input, a seal backwards, and a row of text before its
    // input are refused, and nothing of them is fed.
    let late = timed_frame(&shape, &[("7", Some("70"), 1, 9), ("8", Some("80"), 1, 4)]);
    let error = framed(&mut writer, &mut reader, "f", &format!("f feed p a frame {}\n", late.len()), &late).unwrap_err();
    assert!(error.contains("cannot feed at time 4"), "{error}");
    let error = framed(&mut writer, &mut reader, "a", "a seal p a 3\n", &[]).unwrap_err();
    assert!(error.contains("cannot seal input"), "{error}");
    assert!(framed(&mut writer, &mut reader, "f", "f feed p a 7 val=70 time=4\n", &[]).is_err());
    assert!(framed(&mut writer, &mut reader, "a", "a seal p a soon\n", &[]).unwrap_err().contains("invalid point"));
    // A row of text at a time is a timed batch of one row.
    request(&mut writer, &mut reader, "f", "f feed p b 5 val=500 time=6\n");
    assert!(seal(&mut writer, &mut reader, "p b 7").0.is_empty(), "`a` is at 5");
    assert_eq!(seal(&mut writer, &mut reader, "p a 7"), (vec!["time=6 diff=1 key=tuple(5) val=tuple(500)".into()], Some(7)));

    request(&mut writer, &mut reader, "s", "s stop w\n");
    drop(reader);
    drop(writer);
    server.stop();
}

#[test]
fn timed_batches_feed_inputs_that_move_alone_on_one_worker() {
    assert_timed_feeds(1);
}

#[test]
fn timed_batches_feed_inputs_that_move_alone_on_three_workers() {
    assert_timed_feeds(3);
}

/// The frames a binary tail sends carry row times: fed unchanged to another program's input, they
/// give it the first program's contents at the first program's times. A tail started late
/// replays the trace from the read frontier set by `trim`.
fn assert_tail_frames_feed_on(workers: usize) {
    let (server, mut writer, mut reader) = start_server(workers);
    for name in ["p", "q"] {
        request(&mut writer, &mut reader, "l", &format!("l load {name} begin\noutput \"rows\" = input 0 : ((int, List(int)), Option(int));\nl end-load\n"));
    }
    let shape = declared("((int, List(int)), Option(int))");
    let first = timed_frame(&shape, &[("tuple(1,list(2,3))", Some("inject(1,7)"), 1, 0), ("tuple(2,list())", Some("inject(0,tuple())"), 2, 0), ("tuple(3,list(4))", Some("inject(1,-1)"), 1, 2)]);
    framed(&mut writer, &mut reader, "f", &format!("f feed p 0 frame {}\n", first.len()), &first).unwrap();
    request(&mut writer, &mut reader, "s", "a seal p 0 1\ns spin\n");

    // The tail's replay is time 0, and what follows is each later change. Each frame goes to `q`
    // as it came.
    let mut frames = Vec::new();
    let mut fed = 0;
    let mut pass_on = |writer: &mut TcpStream, reader: &mut BufReader<TcpStream>, frames: &mut Vec<(String, Vec<u8>)>| {
        for (_, payload) in frames.iter().filter(|f| f.0 == "data") {
            framed(writer, reader, "f", &format!("f feed q 0 frame {}\n", payload.len()), payload).unwrap();
            fed += 1;
        }
        heard(frames)
    };
    request_observing(&mut writer, &mut reader, "w", "w tail p::rows as corgi\n", |line, payload| keep_frame(&mut frames, "w", line, payload));
    let (rows, progress) = pass_on(&mut writer, &mut reader, &mut frames);
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(rows.iter().all(|row| row.starts_with("time=0 ")), "a frame says its times, zero included: {rows:?}");
    assert_eq!(progress, Some(1));
    request(&mut writer, &mut reader, "s", "a seal q 0 1\ns spin\n");
    let at_zero = request(&mut writer, &mut reader, "p", "p peek p::rows\n");
    assert_eq!(at_zero.len(), 2);
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek q::rows\n"), at_zero);

    let second = timed_frame(&shape, &[("tuple(1,list(2,3))", Some("inject(1,7)"), -1, 4), ("tuple(5,list())", Some("inject(1,0)"), 1, 6)]);
    framed(&mut writer, &mut reader, "f", &format!("f feed p 0 frame {}\n", second.len()), &second).unwrap();
    request_observing(&mut writer, &mut reader, "s", "a seal p 0 5\ns spin\n", |line, payload| keep_frame(&mut frames, "w", line, payload));
    let (rows, progress) = pass_on(&mut writer, &mut reader, &mut frames);
    assert_eq!(rows.len(), 2, "times 2 and 4: {rows:?}");
    assert_eq!(progress, Some(5));
    // `q` is fed times 2 and 4 while it stands at 1, and is complete through 4 once it passes it.
    request(&mut writer, &mut reader, "s", "a seal q 0 3\ns spin\n");
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek q::rows\n").len(), 3, "time 2");
    request(&mut writer, &mut reader, "s", "a seal q 0 5\ns spin\n");
    let through_four = request(&mut writer, &mut reader, "p", "p peek p::rows\n");
    assert_eq!(through_four.len(), 2);
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek q::rows\n"), through_four);

    // Give up times before 4. A new tail replays those changes at 4.
    request(&mut writer, &mut reader, "cut", "cut trim p::rows 4\n");
    let mut late = Vec::new();
    request_observing(&mut writer, &mut reader, "v", "v tail p::rows as corgi\n", |line, payload| keep_frame(&mut late, "v", line, payload));
    let (rows, progress) = heard(&mut late);
    assert!(!rows.is_empty() && rows.iter().all(|row| row.starts_with("time=4 ")), "{rows:?}");
    assert_eq!(progress, Some(5));

    // Everything `q`'s input holds is taken back from its own output, at the input's time.
    request(&mut writer, &mut reader, "r", "r feed q 0 time=now from q::rows diff=-1\n");
    request(&mut writer, &mut reader, "s", "a seal q 0 6\ns spin\n");
    assert!(request(&mut writer, &mut reader, "p", "p peek q::rows\n").is_empty());
    assert!(fed >= 2);
    request(&mut writer, &mut reader, "s", "s stop w\n");
    request(&mut writer, &mut reader, "s", "s stop v\n");
    drop(reader);
    drop(writer);
    server.stop();
}

#[test]
fn tail_frames_feed_another_program_on_one_worker() {
    assert_tail_frames_feed_on(1);
}

#[test]
fn tail_frames_feed_another_program_on_three_workers() {
    assert_tail_frames_feed_on(3);
}

/// A bound consumer imports from the producer’s read frontier and follows its updates.
#[test]
fn a_bound_consumer_follows_its_producer() {
    let (server, mut writer, mut reader) = start_server(2);
    request(&mut writer, &mut reader, "l", "l load p begin\noutput \"rows\" = input 0 : ((int), (int));\nl end-load\n");
    let shape = declared("((int), (int))");
    let first = timed_frame(&shape, &[("1", Some("10"), 1, 0), ("2", Some("20"), 1, 1), ("3", Some("30"), 1, 5)]);
    framed(&mut writer, &mut reader, "f", &format!("f feed p 0 frame {}\n", first.len()), &first).unwrap();
    request(&mut writer, &mut reader, "s", "a seal p 0 3\ns spin\n");

    request(&mut writer, &mut reader, "t", "t trim p::rows 2\n");
    let mut answer = Vec::new();
    request_observing(&mut writer, &mut reader, "c", "c load c rows=p::rows begin\nlet rows = input \"rows\" : ((int), (int));\noutput \"tens\" = rows | map((k, (v)) -> (k, ((v, 10) mul)));\nc end-load\n", |line, _| answer.push(line.to_string()));
    assert!(answer.iter().any(|line| line.starts_with("c ok installed \"c\"")), "{answer:?}");
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek c::tens\n"), ["tuple(1) val=tuple(100)", "tuple(2) val=tuple(200)"]);

    let mut frames = Vec::new();
    request_observing(&mut writer, &mut reader, "w", "w tail c::tens as corgi\n", |line, payload| keep_frame(&mut frames, "w", line, payload));
    let (rows, progress) = heard(&mut frames);
    assert_eq!(rows, ["time=2 diff=1 key=tuple(1) val=tuple(100)", "time=2 diff=1 key=tuple(2) val=tuple(200)"], "read from the producer's compaction frontier");
    assert_eq!(progress, Some(3));
    let second = timed_frame(&shape, &[("1", Some("10"), -1, 7)]);
    framed(&mut writer, &mut reader, "f", &format!("f feed p 0 frame {}\n", second.len()), &second).unwrap();
    request_observing(&mut writer, &mut reader, "s", "a seal p 0 9\ns spin\n", |line, payload| keep_frame(&mut frames, "w", line, payload));
    assert_eq!(heard(&mut frames), (vec!["time=5 diff=1 key=tuple(3) val=tuple(300)".into(), "time=7 diff=-1 key=tuple(1) val=tuple(100)".into()], Some(9)));
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek c::tens\n"), ["tuple(2) val=tuple(200)", "tuple(3) val=tuple(300)"]);
    // The producer cannot be dropped while the consumer reads it, as for any import.
    assert!(framed(&mut writer, &mut reader, "d", "d drop p\n", &[]).is_err());
    request(&mut writer, &mut reader, "s", "s stop w\n");
    drop(reader);
    drop(writer);
    server.stop();
}

#[test]
fn trim_waits_for_the_export_and_refuses_uncommanded_progress() {
    let programs = concat!(env!("CARGO_MANIFEST_DIR"), "/../examples/server/programs");
    for workers in [1, 3] {
        let (server, mut writer, mut reader) = start_server(workers);
        request(&mut writer, &mut reader, "l", &format!("l load prod from {programs}/producer.ddp\n"));
        request(&mut writer, &mut reader, "f", "f feed prod 0 1,2 time=0\n");

        let error = framed(&mut writer, &mut reader, "t", "t trim prod::edges 3\n", &[]).unwrap_err();
        assert!(error.contains("commanded write frontier is 0"), "{error}");
        let error = framed(&mut writer, &mut reader, "t", "s seal prod 0 4\nt trim prod::edges 9\n", &[]).unwrap_err();
        assert!(error.contains("commanded write frontier is 4"), "{error}");
        assert_eq!(request(&mut writer, &mut reader, "p", "p peek prod::edges as-of 0\n"), ["1 val=2"]);

        // Send the seal, trim and joining import together, without an intervening peek or spin.
        request(&mut writer, &mut reader, "l", &format!("l load fresh from {programs}/producer.ddp\n"));
        let install = format!("f feed fresh 0 1,2 time=0\ns seal fresh 0 4\nt trim fresh::edges 3\nl load reach from {programs}/reach_import.ddp edges=fresh::edges\n");
        let mut replies = Vec::new();
        request_observing(&mut writer, &mut reader, "l", &install, |line, _| replies.push(line.to_string()));
        for id in ["f", "s", "t", "l"] {
            assert!(replies.iter().any(|line| line.starts_with(&format!("{id} ok"))), "{replies:?}");
        }
        request(&mut writer, &mut reader, "f", "f feed reach roots 1 time=3\n");
        request(&mut writer, &mut reader, "s", "s seal reach roots 4\n");
        assert_eq!(request(&mut writer, &mut reader, "p", "p peek reach::result as-of 3\n"), ["1", "2"]);
        server.stop();
    }
}

/// The end of stdin stops the server, even while a TCP session is open.
#[test]
fn end_of_stdin_stops_the_server() {
    for workers in [1, 3] {
        let (server, mut writer, mut reader) = start_server(workers);
        request(&mut writer, &mut reader, "l", "l load p begin\noutput \"o\" = input 0 : (int, ());\nl end-load\n");
        server.close_stdin();
    }
}

/// Two outputs with one name are refused at load, and the server keeps serving.
#[test]
fn duplicate_output_names_are_refused() {
    let (server, mut writer, mut reader) = start_server(3);
    let error = framed(&mut writer, &mut reader, "d", "d load p begin\noutput \"a\" = input 0 : (int, ());\noutput \"a\" = input 1 : (int, ());\nd end-load\n", &[]).unwrap_err();
    assert!(error.contains("output `a` defined twice"), "{error}");
    request(&mut writer, &mut reader, "l", "l load p begin\noutput \"a\" = input 0 : (int, ());\nl end-load\n");
    request(&mut writer, &mut reader, "f", "f feed p 0 7 time=0\n");
    request(&mut writer, &mut reader, "s", "s seal p 0 1\n");
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek p::a\n"), ["7"]);
    server.stop();
}

/// Each spin has to reply before the client can issue the next seal.
/// start_server installs a read deadline, so waiting for that seal fails the test.
#[test]
fn union_waits_only_for_commanded_progress() {
    let (server, mut writer, mut reader) = start_server(3);
    request(&mut writer, &mut reader, "l", "l load u begin\nlet a = input \"a\" : (int, ());\nlet b = input \"b\" : (int, ());\noutput \"both\" = a + b;\nl end-load\n");
    request_observing(&mut writer, &mut reader, "f", "f feed u a 42 time=7\n", |line, _| {
        if line.starts_with("f ok") { assert!(line.ends_with("at t=7"), "{line}"); }
    });
    request(&mut writer, &mut reader, "a", "a seal u a 10\n");
    request(&mut writer, &mut reader, "s", "s spin\n");
    assert!(framed(&mut writer, &mut reader, "p", "p peek u::both as-of 0\n", &[]).unwrap_err().contains("not closed"));
    request(&mut writer, &mut reader, "b", "b seal u::b 5\n");
    request(&mut writer, &mut reader, "s", "s spin\n");
    assert!(request_observing(&mut writer, &mut reader, "p", "p peek u::both\n", |line, _| {
        if line.starts_with("p ok") { assert_eq!(line, "p ok as-of=4"); }
    }).is_empty());
    assert!(request(&mut writer, &mut reader, "p", "p peek u::both as-of 4\n").is_empty());
    request(&mut writer, &mut reader, "l", "l load copy begin\noutput \"rows\" = input 0 : (int, ());\nl end-load\n");
    // A late importer starts at the producer's read floor, with its new input still open.
    request(&mut writer, &mut reader, "l", "l load src begin\noutput \"rows\" = input 0 : (int, ());\nl end-load\n");
    request(&mut writer, &mut reader, "s", "s seal src 0 5\n");
    request(&mut writer, &mut reader, "t", "t trim src::rows 4\n");
    request(&mut writer, &mut reader, "l", "l load late begin\noutput \"rows\" = import \"src::rows\" + input 0 : (int, ());\nl end-load\n");
    assert!(framed(&mut writer, &mut reader, "p", "p peek late::rows\n", &[]).unwrap_err().contains("no closed time"));
    request(&mut writer, &mut reader, "s", "s seal late 0 5\n");
    assert!(request_observing(&mut writer, &mut reader, "p", "p peek late::rows\n", |line, _| {
        if line.starts_with("p ok") { assert_eq!(line, "p ok as-of=4"); }
    }).is_empty());
    request(&mut writer, &mut reader, "d", "d drop late\n");
    // No intervening spin: copying a trace must read the same cut on every shard.
    request(&mut writer, &mut reader, "copy", "b seal u b 8\ncopy feed copy 0 time=now from u::both\n");
    request(&mut writer, &mut reader, "c", "c seal copy 0 1\n");
    request(&mut writer, &mut reader, "s", "s spin\n");
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek u::both 42 as-of 7\n"), ["42"]);
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek copy::rows as-of 0\n"), ["42"]);
    assert!(request(&mut writer, &mut reader, "p", "p peek u::both as-of 2\n").is_empty());
    request(&mut writer, &mut reader, "t", "t trim u::both 4\n");
    assert!(framed(&mut writer, &mut reader, "p", "p peek u::both as-of 2\n", &[]).unwrap_err().contains("read floor"));
    assert!(request(&mut writer, &mut reader, "p", "p peek u::both as-of 4\n").is_empty());
    assert!(framed(&mut writer, &mut reader, "t", "t trim u::both 3\n", &[]).is_err());
    // A finished input releases its capability; the other input can still write at 8.
    request(&mut writer, &mut reader, "a", "a seal u a []\n");
    request(&mut writer, &mut reader, "a", "a seal u a []\n");
    assert!(framed(&mut writer, &mut reader, "a", "a seal u a 11\n", &[]).is_err());
    assert!(framed(&mut writer, &mut reader, "f", "f feed u a 43 time=11\n", &[]).is_err());
    request(&mut writer, &mut reader, "f", "f feed u b time=12 begin\n43\n44\nend-feed\n");
    request(&mut writer, &mut reader, "b", "b seal u b 12\n");
    request(&mut writer, &mut reader, "s", "s spin\n");
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek u::both as-of 11\n"), ["42"]);
    request(&mut writer, &mut reader, "w", "w tail u::both as corgi\n");
    let mut terminal = false;
    request_observing(&mut writer, &mut reader, "s", "b seal u b []\ns spin\n", |header, body| {
        if header == "w frame progress 0" && body == Some(&[][..]) { terminal = true; }
    });
    assert!(terminal, "spin delivers the empty frontier to the tail");
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek u::both\n"), ["42", "43", "44"]);
    request(&mut writer, &mut reader, "t", "t trim u::both []\n");
    assert!(framed(&mut writer, &mut reader, "p", "p peek u::both\n", &[]).is_err());
    assert!(framed(&mut writer, &mut reader, "p", "p peek u::both as-of 12\n", &[]).is_err());
    assert!(framed(&mut writer, &mut reader, "t", "t trim u::both 12\n", &[]).is_err());
    assert!(framed(&mut writer, &mut reader, "w2", "w2 tail u::both\n", &[]).is_err());
    drop(reader);
    drop(writer);
    server.stop();
}

/// Each worker participates in the lookup, but a request contributes its weight only once.
#[test]
fn keyed_peek_matches_the_snapshot_at_product_times() {
    let (server, mut writer, mut reader) = start_server(3);
    let mut run = |command: &str| request(&mut writer, &mut reader, "r", &format!("r {command}\n"));
    run("load p width=2 begin\noutput \"rows\" = input 0 : (int, int);\nr end-load");
    run("feed p 0 begin\n7 val=10 diff=2 time=[1,0]\n7 val=11 time=[1,0]\n7 val=10 diff=-2 time=[1,1]\n7 val=20 diff=-1 time=[0,1]\n8 val=30 time=[0,0]\nend-feed");
    run("seal p 0 [[2,0],[0,2]]");
    for point in ["[1,0]", "[0,1]", "[1,1]"] {
        let snapshot = run(&format!("peek p::rows as-of {point}"));
        for key in ["7", "8", "9"] {
            let expected: Vec<_> = snapshot.iter().filter(|row| row.split_whitespace().next() == Some(key)).cloned().collect();
            assert_eq!(run(&format!("peek p::rows {key} as-of {point}")), expected, "key {key} at {point}");
        }
    }
    run("seal p 0 []");
    run("trim p::rows [1,1]");
    assert_eq!(run("peek p::rows 7 as-of [1,1] as corgi"), ["diff=1 key=7 val=11", "diff=-1 key=7 val=20"]);
    assert!(framed(&mut writer, &mut reader, "r", "r peek p::rows 7 as-of [1,0]\n", &[]).unwrap_err().contains("read floor"));
    server.stop();
}

#[test]
fn mixed_time_frame_under_an_antichain() {
    let (server, mut writer, mut reader) = start_server(3);
    request(&mut writer, &mut reader, "l", "l load p width=2 begin\noutput \"rows\" = input 0 : ((int), (int));\nl end-load\n");
    request(&mut writer, &mut reader, "s", "s seal p 0 [[2,0],[0,2]]\n");
    assert!(framed(&mut writer, &mut reader, "n", "n feed p 0 time=now 1 val=9\n", &[]).is_err());
    assert!(framed(&mut writer, &mut reader, "s", "s seal p 0 [[3,0],[1,1]]\n", &[]).is_err());
    let shape = declared("((int), (int))");
    let mut rows = interactive::runtime::feed::FeedDecoder::new(&shape.0, &shape.1);
    rows.push("7", Some("10"), 2).unwrap();
    rows.push("8", Some("20"), -1).unwrap();
    let rows = rows.finish();
    for (ys, valid) in [(vec![0, 1], false), (vec![0, 2], true)] {
        let bytes = interactive::runtime::frame::encode(&rows, &[vec![2, 0], ys]);
        let cmd = format!("f feed p 0 frame {}\n", bytes.len());
        assert_eq!(framed(&mut writer, &mut reader, "f", &cmd, &bytes).is_ok(), valid);
    }
    // The dominated second row exceeds the input width only on worker 1.
    // Every worker must reject the first row too, even though the batch minimum fits.
    let wide = interactive::runtime::frame::encode(&rows, &[vec![2, 2], vec![0, 0], vec![0, 1]]);
    let cmd = format!("f feed p 0 frame {}\n", wide.len());
    assert!(framed(&mut writer, &mut reader, "f", &cmd, &wide).unwrap_err().contains("width"));
    // An untimed header permits each text row to name its own point.
    request(&mut writer, &mut reader, "f", "f feed p 0 begin\n9 val=30 time=[2,2]\n9 time=[3,3] val=30 diff=-1\nend-feed\n");
    assert!(request(&mut writer, &mut reader, "p", "p peek p::rows as-of [1,1]\n").is_empty());
    request(&mut writer, &mut reader, "s", "s seal p 0 [[3,0],[0,3]]\n");
    request(&mut writer, &mut reader, "s", "s spin\n");
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek p::rows as-of [2,0]\n"), ["tuple(7) val=tuple(10) diff=2"]);
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek p::rows as-of [0,2]\n"), ["tuple(8) val=tuple(20) diff=-1"]);
    request(&mut writer, &mut reader, "s", "s seal p 0 []\n");
    request(&mut writer, &mut reader, "s", "s spin\n");
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek p::rows as-of [2,2]\n").len(), 3);
    drop(reader);
    drop(writer);
    server.stop();
}

#[test]
fn source_loads_stamp_the_destination_independently_of_the_source_cut() {
    let (server, mut writer, mut reader) = start_server(3);
    let file = std::env::temp_dir().join(format!("ddir-source-time-{}.txt", std::process::id()));
    std::fs::write(&file, "41\n42\n").unwrap();
    request(&mut writer, &mut reader, "L", "L load src begin\noutput \"rows\" = input 0 : (int, ());\nL end-load\n");
    request(&mut writer, &mut reader, "F", &format!("F feed src 0 time=8 from {}\n", file.display()));
    request(&mut writer, &mut reader, "F", "F feed src 0 time=12 43\n");
    request(&mut writer, &mut reader, "S", "S seal src 0 9\n");
    assert!(request(&mut writer, &mut reader, "P", "P peek src::rows as-of 7\n").is_empty());
    assert_eq!(request(&mut writer, &mut reader, "P", "P peek src::rows as-of 8\n"), ["41", "42"]);

    request(&mut writer, &mut reader, "L", "L load dst width=2 begin\noutput \"rows\" = input 0 : (int, ());\nL end-load\n");
    request(&mut writer, &mut reader, "S", "S seal dst 0 [[2,0],[0,2]]\n");
    assert!(framed(&mut writer, &mut reader, "bad", "bad feed dst 0 time=[1,1] from src::rows\n", &[]).is_err());
    assert!(framed(&mut writer, &mut reader, "now", "now feed dst 0 time=now from src::rows\n", &[]).is_err());
    request(&mut writer, &mut reader, "F", "F feed dst 0 time=[2,0] from src::rows diff=2\n");
    request(&mut writer, &mut reader, "F", &format!("F feed dst 0 time=[0,2] from {}\n", file.display()));
    request(&mut writer, &mut reader, "S", "S seal dst 0 []\n");
    assert_eq!(request(&mut writer, &mut reader, "P", "P peek dst::rows as-of [2,0]\n"), ["41 diff=2", "42 diff=2"]);
    assert_eq!(request(&mut writer, &mut reader, "P", "P peek dst::rows as-of [0,2]\n"), ["41", "42"]);
    assert_eq!(request(&mut writer, &mut reader, "P", "P peek dst::rows as-of [2,2]\n"), ["41 diff=3", "42 diff=3"]);

    std::fs::remove_file(file).unwrap();
    drop(reader);
    drop(writer);
    server.stop();
}
