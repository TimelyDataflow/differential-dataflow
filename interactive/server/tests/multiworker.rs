use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct ServerProcess(Child);

impl ServerProcess {
    fn stop(mut self) {
        let stdin = self.0.stdin.as_mut().expect("server stdin is piped");
        stdin.write_all(b"exit\n").unwrap();
        stdin.flush().unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
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
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
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
            reader.read_line(&mut line).unwrap(),
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

fn assert_backend(backend: &str) {
    let listeners: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect();
    drop(listeners);

    let child = Command::new(env!("CARGO_BIN_EXE_ddir_server"))
        .env("DDIR_WORKERS", "4")
        .env("DDIR_BACKEND", backend)
        // The retired polling/tick knob must not reintroduce wall-clock
        // progress if it remains in an old deployment environment.
        .env("DDIR_TICK_MS", "1")
        .env("DDIR_BIND", format!("127.0.0.1:{}", ports[0]))
        .env("DDIR_WS_BIND", format!("127.0.0.1:{}", ports[1]))
        .env("DDIR_DIAG_PORT", ports[2].to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let server = ServerProcess(child);

    let deadline = Instant::now() + Duration::from_secs(10);
    let stream = loop {
        match TcpStream::connect(("127.0.0.1", ports[0])) {
            Ok(stream) => break stream,
            Err(error) => {
                assert!(Instant::now() < deadline, "server did not listen: {error}");
                thread::sleep(Duration::from_millis(10));
            }
        }
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);

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
