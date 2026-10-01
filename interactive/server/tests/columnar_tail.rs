//! `tail <export> as corgi`: binary frames of Corgi columns. A client decodes
//! them and must see exactly what text `tail` shows, and after each `progress`
//! frame, what `peek` shows.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::Duration;

use interactive::corgi::container::CorgiContainer;
use timely::dataflow::channels::ContainerBytes;

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_server(workers: usize, columnar_exports: bool) -> (Server, TcpStream, BufReader<TcpStream>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ddir_server"))
        .env("DDIR_WORKERS", workers.to_string())
        .env("DDIR_BACKEND", "corgi")
        .env("DDIR_COLUMNAR_EXPORTS", if columnar_exports { "1" } else { "0" })
        .env("DDIR_BIND", "127.0.0.1:0")
        .env("DDIR_WS_BIND", "127.0.0.1:0")
        .env("DDIR_DIAG_PORT", "0")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().unwrap();
    let (ready_tx, ready_rx) = sync_channel(1);
    thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if let Some(address) = line.strip_prefix("ddir_server: tcp listening on ") {
                let _ = ready_tx.send(address.to_string());
            }
        }
    });
    let address = ready_rx.recv_timeout(Duration::from_secs(10)).expect("server listening");
    let stream = TcpStream::connect(address).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let writer = stream.try_clone().unwrap();
    (Server(child), writer, BufReader::new(stream))
}

/// Everything a session receives: text lines by reqid, and each subscription's
/// decoded updates and progress.
#[derive(Default)]
struct Client {
    /// Text `tail` lines, `time=.. diff=.. key=.. val=..`, by reqid.
    text: BTreeMap<String, Vec<String>>,
    /// Decoded columnar updates in the same format, by reqid.
    columns: BTreeMap<String, Vec<String>>,
    /// The latest progress upper, by reqid.
    upper: BTreeMap<String, u64>,
    /// Whether a schema frame arrived, by reqid.
    schema: BTreeMap<String, String>,
}

impl Client {
    /// Send `command` and read until `<reqid> ok`, returning its `data` lines.
    fn request(&mut self, writer: &mut TcpStream, reader: &mut BufReader<TcpStream>, reqid: &str, command: &str) -> Vec<String> {
        writer.write_all(format!("{reqid} {command}\n").as_bytes()).unwrap();
        writer.flush().unwrap();
        let mut data = Vec::new();
        loop {
            let mut line = String::new();
            let n = reader.read_line(&mut line).unwrap();
            assert_ne!(n, 0, "server disconnected");
            READ.with(|r| *r.borrow_mut() += n);
            let line = line.trim_end_matches('\n');
            let (id, rest) = line.split_once(' ').unwrap();
            if let Some(frame) = rest.strip_prefix("frame ") {
                let (kind, n) = frame.split_once(' ').unwrap();
                let mut bytes = vec![0u8; n.parse().unwrap()];
                reader.read_exact(&mut bytes).unwrap();
                READ.with(|r| *r.borrow_mut() += bytes.len());
                self.frame(id, kind, bytes);
                continue;
            }
            if id != reqid {
                if let Some(update) = rest.strip_prefix("data ") {
                    self.text.entry(id.to_string()).or_default().push(update.to_string());
                }
                continue;
            }
            if let Some(body) = rest.strip_prefix("data ") {
                data.push(body.to_string());
            } else if rest == "ok" || rest.starts_with("ok ") {
                return data;
            } else if rest.starts_with("err") {
                panic!("{reqid} {command}: {rest}");
            }
        }
    }

    fn frame(&mut self, id: &str, kind: &str, bytes: Vec<u8>) {
        match kind {
            "schema" => { self.schema.insert(id.to_string(), String::from_utf8(bytes).unwrap()); }
            "progress" => { self.upper.insert(id.to_string(), u64::from_le_bytes(bytes[..8].try_into().unwrap())); }
            "data" => {
                let payload = timely::bytes::arc::BytesMut::from(bytes[8..].to_vec()).freeze();
                let container = <CorgiContainer<u64, i64> as ContainerBytes>::from_bytes(payload);
                let out = self.columns.entry(id.to_string()).or_default();
                for ((key, val), time, diff) in container.into_updates() {
                    out.push(format!("time={time} diff={diff} key={key:?} val={val:?}"));
                }
            }
            other => panic!("unknown frame {other}"),
        }
    }
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

/// The accumulated contents a stream of updates describes, as `peek` prints them.
fn accumulate(updates: &[String]) -> Vec<String> {
    let mut sums: BTreeMap<String, i64> = BTreeMap::new();
    for u in updates {
        let rest = u.split_once(' ').unwrap().1; // drop time=
        let (diff, kv) = rest.split_once(' ').unwrap();
        *sums.entry(kv.to_string()).or_default() += diff.strip_prefix("diff=").unwrap().parse::<i64>().unwrap();
    }
    sorted(sums.into_iter().filter(|(_, d)| *d != 0).map(|(kv, d)| format!("diff={d} {kv}")).collect())
}

fn check(workers: usize, columnar_exports: bool) {
    let (_server, mut writer, mut reader) = start_server(workers, columnar_exports);
    let mut c = Client::default();
    c.request(&mut writer, &mut reader, "p", "load world begin\ntype Three = A u64 | B u64 | C u64;\nlet rows = input 0;\nexport \"rows\" = rows;\nexport \"lists\" = rows | collect;\nexport \"tagged\" = rows | map($0 ; variant(Three, 2, $1[0]));\np end-load");
    let exports = ["rows", "lists", "tagged"];
    let mut next = 0;
    for (epoch, step) in [(0, 0), (1, 1), (2, 2), (3, 3)] {
        // Retract some earlier rows, add new ones.
        let mut body = String::new();
        for i in 0..24 {
            let (k, v) = (i % 5, i * 3 + step);
            body.push_str(&format!("{k} val={v}\n"));
            if step > 0 && i % 4 == 0 {
                body.push_str(&format!("{k} val={} diff=-1\n", i * 3 + step - 1));
            }
        }
        c.request(&mut writer, &mut reader, "f", &format!("feed world 0 begin\n{body}f end-feed"));
        if epoch == 1 {
            // Subscribe mid-stream: the first frames are the compacted replay.
            for name in exports {
                // The text tail's replay arrives as its request's own `data` lines.
                let replay = c.request(&mut writer, &mut reader, &format!("t{name}"), &format!("tail {name}"));
                c.text.entry(format!("t{name}")).or_default().extend(replay);
                c.request(&mut writer, &mut reader, &format!("c{name}"), &format!("tail {name} as corgi"));
            }
        }
        c.request(&mut writer, &mut reader, "k", "tick");
        next += 1;
        if epoch >= 1 {
            for name in exports {
                let (t, col) = (format!("t{name}"), format!("c{name}"));
                assert!(c.schema[&col].contains(&format!("export={name}")), "{:?}", c.schema);
                assert!(c.upper[&col] >= next as u64, "{name}: progress {} after tick {next}", c.upper[&col]);
                let text = sorted(c.text.get(&t).cloned().unwrap_or_default());
                let cols = sorted(c.columns.get(&col).cloned().unwrap_or_default());
                assert!(!cols.is_empty(), "{name}: no columnar updates");
                // The same batches, so the same updates, time by time.
                assert_eq!(cols, text, "{name} at {workers} workers: columnar and text updates differ");
                assert_eq!(accumulate(&cols), accumulate(&text), "{name} at {workers} workers: columns and text tail disagree");
                let peek = sorted(c.request(&mut writer, &mut reader, "q", &format!("peek {name}")));
                assert_eq!(accumulate(&cols), peek, "{name} at {workers} workers: columns and peek disagree");
            }
        }
    }
    // Retractions and nested values really were exercised.
    assert!(c.columns["crows"].iter().any(|u| u.contains("diff=-1")));
    assert!(c.columns["clists"].iter().any(|u| u.contains("List(")) || c.columns["clists"].iter().any(|u| u.contains('[')));
    c.request(&mut writer, &mut reader, "s", "stop crows");
}

#[test]
fn columnar_tail_matches_text_tail_and_peek_on_one_worker() {
    check(1, true);
}

#[test]
fn columnar_tail_matches_text_tail_and_peek_on_four_workers() {
    check(4, true);
}

/// A row trace (columnar exports off) has no columnar form to send: `as corgi`
/// is refused, and text tail still works.
#[test]
fn columnar_tail_of_a_row_trace_is_refused() {
    let (_server, mut writer, mut reader) = start_server(1, false);
    let mut c = Client::default();
    c.request(&mut writer, &mut reader, "p", "load world begin\nlet rows = input 0;\nexport \"rows\" = rows;\np end-load");
    writer.write_all(b"c tail rows as corgi\n").unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(line.starts_with("c err") && line.contains("columnar export"), "{line}");
    c.request(&mut writer, &mut reader, "t", "tail rows");
}

/// Not a test: bytes on the wire and client time to receive and decode (for
/// columns, decode includes rebuilding and formatting each row, as the checks
/// above do), text `tail` against `tail as corgi`, for the tour (inspects removed) on 50k nodes /
/// 100k edges with 500 edges replaced per tick. Run with
/// `cargo test --release -p ddir-server --test columnar_tail -- --ignored --nocapture`.
#[test]
#[ignore]
fn columnar_tail_wire_measurement() {
    let program: String = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../examples/programs/tour.ddp"))
        .unwrap()
        .lines()
        .map(|line| match line.find(" | inspect(") {
            Some(i) => format!("{};", &line[..i]),
            None => line.to_string(),
        })
        .collect::<Vec<_>>()
        .join("\n");
    for columnar in [false, true] {
        let (_server, mut writer, mut reader) = start_server(4, true);
        let mut c = Client::default();
        c.request(&mut writer, &mut reader, "p", &format!("load tour begin\n{program}\np end-load"));
        c.request(&mut writer, &mut reader, "e", "feed tour 0 from random:nodes=50000,edges=100000,seed=1,churn=500");
        c.request(&mut writer, &mut reader, "r", "feed tour 1 0");
        c.request(&mut writer, &mut reader, "k", "tick");
        let command = if columnar { "tail scored as corgi" } else { "tail scored" };
        let bytes_before = counted(&mut reader);
        let start = std::time::Instant::now();
        let replay = c.request(&mut writer, &mut reader, "s", command);
        let replay_time = start.elapsed();
        let replay_bytes = counted(&mut reader) - bytes_before;
        let rows = if columnar { c.columns.values().map(Vec::len).sum::<usize>() } else { replay.len() };
        let mut tick_time = Duration::ZERO;
        let before = counted(&mut reader);
        let mut churn_rows = 0;
        for i in 0..10 {
            let seen = c.text.values().map(Vec::len).sum::<usize>() + c.columns.values().map(Vec::len).sum::<usize>();
            let start = std::time::Instant::now();
            c.request(&mut writer, &mut reader, &format!("k{i}"), "tick");
            tick_time += start.elapsed();
            churn_rows += c.text.values().map(Vec::len).sum::<usize>() + c.columns.values().map(Vec::len).sum::<usize>() - seen;
        }
        let churn_bytes = counted(&mut reader) - before;
        println!(
            "{}: replay {} rows, {} bytes, {:.1} ms; ten churn ticks {} rows, {} bytes, {:.1} ms (tick + receive + decode)",
            if columnar { "as corgi" } else { "text" },
            rows, replay_bytes, replay_time.as_secs_f64() * 1e3,
            churn_rows, churn_bytes, tick_time.as_secs_f64() * 1e3,
        );
    }
}

/// Bytes read from the server so far, as tallied by `Client::request`.
fn counted(_reader: &mut BufReader<TcpStream>) -> usize {
    READ.with(|r| *r.borrow())
}

thread_local! {
    static READ: std::cell::RefCell<usize> = const { std::cell::RefCell::new(0) };
}
