//! The examples in `examples/`: each folder's session `NAME/NAME.txt`, piped into a server's stdin
//! from `interactive/`, must print its recorded transcript `NAME/NAME.out`, at one worker and at
//! three. `UPDATE_EXAMPLES=1` records the one-worker transcripts instead.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn interactive() -> PathBuf { Path::new(env!("CARGO_MANIFEST_DIR")).join("..") }

/// The server's stdout for one session. An install's operator count is left out, so a change
/// to the optimizer does not change an example's transcript.
fn transcript(session: &Path, workers: usize) -> String {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ddir_server"))
        .current_dir(interactive())
        .env("DDIR_WORKERS", workers.to_string())
        .env("DDIR_BIND", "127.0.0.1:0")
        .env("DDIR_WS_BIND", "127.0.0.1:0")
        .stdin(std::fs::File::open(session).unwrap())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("{}: the server did not exit", session.display());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut out = String::new();
    child.stdout.take().unwrap().read_to_string(&mut out).unwrap();
    out.lines()
        .map(|line| match line.find(" ok installed ") {
            Some(_) => line.rsplit_once(" (").map_or(line, |(head, _)| head).to_string(),
            None => line.to_string(),
        })
        .collect::<Vec<_>>()
        .join("\n") + "\n"
}

/// The folders of `examples/` that hold a session. `aoc2023` has its own runner.
const EXAMPLES: &[&str] = &["tour", "tables", "graphs", "apps"];

#[test]
fn examples_print_their_recorded_transcripts() {
    let update = std::env::var("UPDATE_EXAMPLES").as_deref() == Ok("1");
    for name in EXAMPLES {
        let session = interactive().join("examples").join(name).join(format!("{name}.txt"));
        let recorded = session.with_extension("out");
        let got = transcript(&session, 1);
        if update {
            std::fs::write(&recorded, &got).unwrap();
        } else {
            let want = std::fs::read_to_string(&recorded)
                .unwrap_or_else(|e| panic!("{}: {e}", recorded.display()));
            assert_eq!(got, want, "{} disagrees with {}", session.display(), recorded.display());
        }
        assert_eq!(transcript(&session, 3), got, "{} at three workers", session.display());
    }
}
