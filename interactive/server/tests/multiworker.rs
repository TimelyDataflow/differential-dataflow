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
    let (updates, lanes) = interactive::corgi::frame::decode(bytes).unwrap();
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

fn assert_commands_replay() {
    let (server, mut writer, mut reader) = start_server(4);

    request(
        &mut writer,
        &mut reader,
        "r0",
        "r0 load world begin\nlet rows = input 0 : ((int), (int));\nexport \"rows\" = rows;\nexport \"minimum\" = rows | min;\nr0 end-load\n",
    );
    request(
        &mut writer,
        &mut reader,
        "r1",
        "r1 feed world 0 begin\n7 val=9\n7 val=3\nr1 end-feed\n",
    );
    request(&mut writer, &mut reader, "r3", "r3 tick\n");
    let rows = request(&mut writer, &mut reader, "r4", "r4 peek rows as corgi\n");
    assert_eq!(
        rows,
        vec![
            "diff=1 key=tuple(7) val=tuple(3)",
            "diff=1 key=tuple(7) val=tuple(9)",
        ]
    );
    // The text form is one line of the answer's columns, as corgi shows them.
    let text = request(&mut writer, &mut reader, "r4", "r4 peek rows\n");
    assert!(matches!(&text[..], [line] if line.starts_with("keys=") && line.ends_with("diffs=[1, 1]")), "{text:?}");
    let minimum = request(&mut writer, &mut reader, "r5", "r5 peek minimum as corgi\n");
    assert_eq!(
        minimum,
        vec!["diff=1 key=tuple(7) val=tuple(3)"]
    );

    request(&mut writer, &mut reader, "r6", "r6 tail rows as corgi\n");
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
    let mut tail_update = Vec::new();
    request_observing(&mut writer, &mut reader, "r8", "r8 tick\n", |line, payload| {
        if let (true, Some(payload)) = (line.starts_with("r6 frame data "), payload) {
            tail_update.extend(frame_rows(payload));
        }
    });
    assert_eq!(tail_update, vec!["time=1 diff=1 key=tuple(8) val=tuple(10)"]);

    // Server-side sourcing: every worker feeds its shard of the recipe, so the
    // union is the source exactly once however many workers there are. Its own
    // program, because a recipe's rows carry a unit value and `world`'s carry
    // an integer, and one input holds one shape.
    request(&mut writer, &mut reader, "r9", "r9 stop r6\n");
    request(
        &mut writer,
        &mut reader,
        "r10",
        "r10 load counted begin\nlet rows = input 0 : ((int), ());\nexport \"counted\" = rows;\nr10 end-load\n",
    );
    request(&mut writer, &mut reader, "r11", "r11 feed counted 0 from iota:5\n");
    request(&mut writer, &mut reader, "r12", "r12 tick\n");
    let rows = request(&mut writer, &mut reader, "r13", "r13 peek counted as corgi\n");
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

fn assert_typed_sources() {
    let (server, mut writer, mut reader) = start_server(4);
    request(&mut writer, &mut reader, "g", "g load graph begin\nlet rows = input 0 : ((int, List(int), Option(List(int))), ());\nexport \"typed.rows\" = rows | arrange;\ng end-load\n");
    request(&mut writer, &mut reader, "q", "q load copy begin\nlet rows = import \"typed.rows\" : ((int, List(int), Option(List(int))), ());\nexport \"typed.copy\" = rows;\nq end-load\n");
    // Neither a first empty list nor a never-populated sum lane can supply
    // its encoding by example. Both the producer and imported trace use the
    // declared shape, without sentinel rows or altered data.
    request(&mut writer, &mut reader, "f", "f feed graph 0 tuple(1,list(),inject(0,tuple()))\n");
    request(&mut writer, &mut reader, "t", "t tick\n");
    assert_eq!(request(&mut writer, &mut reader, "p", "p peek typed.copy as corgi\n"),
        vec!["diff=1 key=tuple(1,list(),inject(0,tuple())) val=tuple()"]);
    request(&mut writer, &mut reader, "f2", "f2 feed graph 0 tuple(2,list(97),inject(1,list()))\n");
    request(&mut writer, &mut reader, "f3", "f3 feed graph 0 tuple(1,list(),inject(0,tuple())) diff=-1\n");
    request(&mut writer, &mut reader, "t2", "t2 tick\n");
    assert_eq!(request(&mut writer, &mut reader, "p2", "p2 peek typed.copy as corgi\n"),
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
        "g load graph begin\nlet e = input 0 : ((int), (int));\nexport \"edges\" = e | arrange;\ng end-load\n",
    );
    for name in ["a", "b"] {
        request(
            &mut writer,
            &mut reader,
            name,
            &format!(
                "{name} load {name} begin\nlet q = input 0 : ((int, int), ());\nlet e = import \"edges\";\nexport \"{name}.answer\" = (q | map(((rid, n), ()) -> ((n), (rid)))) | join(e, (_, (rid), (node)) -> ((rid), (node)));
\n{name} end-load\n"
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
            &format!("p peek {name}.answer as corgi\n")
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
    let answer = |rid, node| format!("diff=1 key=tuple({rid}) val=tuple({node})");
    assert_eq!(
        request(&mut writer, &mut reader, "p", "p peek a.answer as corgi\n"),
        vec![answer(10, 2), answer(10, 3), answer(11, 2), answer(11, 3)]
    );
    assert_eq!(
        request(&mut writer, &mut reader, "p", "p peek b.answer as corgi\n"),
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
        request(&mut writer, &mut reader, "p", "p peek a.answer as corgi\n"),
        vec![answer(11, 2), answer(11, 5)]
    );
    assert_eq!(
        request(&mut writer, &mut reader, "p", "p peek b.answer as corgi\n"),
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
            &format!("p peek {name}.answer as corgi\n")
        )
        .is_empty());
    }
    // Reuse an id with a different parameter against the changed graph. No
    // program reinstall and no retained answer from its previous binding.
    request(&mut writer, &mut reader, "f", "f feed a 0 10,2\n");
    request(&mut writer, &mut reader, "t", "t tick\n");
    assert_eq!(
        request(&mut writer, &mut reader, "p", "p peek a.answer as corgi\n"),
        vec![answer(10, 6)]
    );
    request(&mut writer, &mut reader, "f", "f feed a 0 10,2 diff=-1\n");
    request(&mut writer, &mut reader, "t", "t tick\n");
    assert!(request(&mut writer, &mut reader, "p", "p peek a.answer as corgi\n").is_empty());
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

/// A binary tail starts with a `schema` frame, and its `data` frames, summed over every tick from
/// the replay on, are a binary peek's answer after each tick. Its `progress` frames say how far
/// it has sent. Lists and sums included.
fn assert_tail_frames_sum_to_peeks(workers: usize) {
    use interactive::corgi::frame;
    use std::collections::BTreeMap;
    let (server, mut writer, mut reader) = start_server(workers);
    request(&mut writer, &mut reader, "g", "g load graph begin\nlet rows = input 0 : ((int, List(int)), Option(int));\nexport \"rows\" = rows | arrange;\ng end-load\n");
    request(&mut writer, &mut reader, "f", "f feed graph 0 begin\ntuple(1,list(2,3)) val=inject(1,7)\ntuple(2,list()) val=inject(0,tuple())\nf end-feed\n");
    request(&mut writer, &mut reader, "t", "t tick\n");
    let mut frames = Vec::new();
    request_observing(&mut writer, &mut reader, "w", "w tail rows as corgi\n", |line, payload| keep_frame(&mut frames, "w", line, payload));
    assert_eq!(frames.first().map(|f| f.0.as_str()), Some("schema"));
    let feeds = ["tuple(3,list(4)) val=inject(1,-1)", "tuple(1,list(2,3)) val=inject(1,7) diff=-1", "tuple(2,list()) val=inject(0,tuple()) diff=2"];
    for (round, feed) in feeds.into_iter().enumerate() {
        request(&mut writer, &mut reader, "f", &format!("f feed graph 0 begin\n{feed}\nf end-feed\n"));
        request_observing(&mut writer, &mut reader, "t", "t tick\n", |line, payload| keep_frame(&mut frames, "w", line, payload));
        let mut changes: BTreeMap<(String, String), i64> = BTreeMap::new();
        for (_, payload) in frames.iter().filter(|f| f.0 == "data") {
            for (key, val, diff) in frame::decode(payload).unwrap().0.rows() {
                *changes.entry((key, val)).or_insert(0) += diff;
            }
        }
        changes.retain(|_, diff| *diff != 0);
        let mut peeked = Vec::new();
        request_observing(&mut writer, &mut reader, "p", "p peek rows as corgi\n", |line, payload| keep_frame(&mut peeked, "p", line, payload));
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
    let program = interactive::lower::lower_tree(interactive::parse::pipe::parse(&format!("export \"x\" = input 0 : {declaration};")));
    program.root.imports[0].shape.clone().expect("a declared input")
}

/// A feed as a binary frame: the columns of `rows`, read at `shape` (text rows, as `feed … begin`
/// reads them), in the frame format a peek answers with.
fn feed_frame(shape: &interactive::server::RowShape, rows: &[(&str, Option<&str>, i64)]) -> Vec<u8> {
    let mut decoder = interactive::corgi::feed::FeedDecoder::new(&shape.0, &shape.1);
    for (key, val, diff) in rows {
        decoder.push(key, *val, *diff).unwrap();
    }
    interactive::corgi::frame::encode(&decoder.finish(), &[])
}

/// `feed … frame` stages columns, each row once however many workers there are; a frame of
/// another shape, one cut short, and one too large for a feed are refused, and the session reads
/// on past each.
fn assert_framed_feeds(workers: usize) {
    let (server, mut writer, mut reader) = start_server(workers);
    request(&mut writer, &mut reader, "r0", "r0 load fed begin\nlet rows = input 0 : ((int, List(int)), (int));\nexport \"fed\" = rows;\nr0 end-load\n");
    let shape = declared("((int, List(int)), (int))");
    let rows: Vec<(String, Option<String>, i64)> = (0..10).map(|i| (format!("tuple({i},list({}))", "7,".repeat(i as usize).trim_end_matches(',')), Some(format!("tuple({})", 300 * i - 1000)), i + 1)).collect();
    let rows: Vec<(&str, Option<&str>, i64)> = rows.iter().map(|(k, v, d)| (k.as_str(), v.as_deref(), *d)).collect();
    let bytes = feed_frame(&shape, &rows);
    framed(&mut writer, &mut reader, "r1", &format!("r1 feed fed 0 frame {}\n", bytes.len()), &bytes).unwrap();
    request(&mut writer, &mut reader, "r2", "r2 tick\n");
    let fed = request(&mut writer, &mut reader, "r3", "r3 peek fed as corgi\n");
    // The same rows fed as text, to another program, are the reference.
    request(&mut writer, &mut reader, "r4", "r4 load text begin\nlet rows = input 0 : ((int, List(int)), (int));\nexport \"text\" = rows;\nr4 end-load\n");
    let body: String = rows.iter().map(|(k, v, d)| format!("{k} val={} diff={d}\n", v.unwrap())).collect();
    request(&mut writer, &mut reader, "r5", &format!("r5 feed text 0 begin\n{body}r5 end-feed\n"));
    request(&mut writer, &mut reader, "r6", "r6 tick\n");
    let text = request(&mut writer, &mut reader, "r7", "r7 peek text as corgi\n");
    assert_eq!(fed.len(), 10);
    assert_eq!(fed, text);

    // Another shape: the value is not a one-field tuple.
    let other = feed_frame(&declared("((int, List(int)), int)"), &[("tuple(1,list())", Some("5"), 1)]);
    let error = framed(&mut writer, &mut reader, "r8", &format!("r8 feed fed 0 frame {}\n", other.len()), &other).unwrap_err();
    assert!(error.contains("shape"), "{error}");
    // Cut short: its header counts the bytes sent, but they are not a whole frame.
    let short = &bytes[..bytes.len() - 8];
    assert!(framed(&mut writer, &mut reader, "r9", &format!("r9 feed fed 0 frame {}\n", short.len()), short).is_err());
    // More bytes than a feed may hold, and an input with no declared shape: read past, refused.
    let too_large = framed(&mut writer, &mut reader, "r10", &format!("r10 feed fed 0 frame {}\n", 17 << 20), &vec![0; 17 << 20]).unwrap_err();
    assert!(too_large.contains("exceeds"), "{too_large}");
    assert!(framed(&mut writer, &mut reader, "r11", &format!("r11 feed fed 3 frame {}\n", bytes.len()), &bytes).is_err());
    // The session reads on: nothing refused reached the program.
    request(&mut writer, &mut reader, "r12", "r12 tick\n");
    assert_eq!(request(&mut writer, &mut reader, "r13", "r13 peek fed as corgi\n"), fed);

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
