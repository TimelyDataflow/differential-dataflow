//! The recipes in `examples/recipes`: each `NAME.txt` session, piped into a server's stdin from
//! `interactive/`, must print its recorded transcript `NAME.out`, at one worker and at three.
//! `UPDATE_RECIPES=1` records the one-worker transcripts instead.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn interactive() -> PathBuf { Path::new(env!("CARGO_MANIFEST_DIR")).join("..") }

/// The server's stdout for one session. An install's operator count is left out, so a change
/// to the optimizer does not change a recipe's transcript.
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

#[test]
fn recipes_print_their_recorded_transcripts() {
    let dir = interactive().join("examples/recipes");
    let mut sessions: Vec<PathBuf> = std::fs::read_dir(&dir).unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "txt"))
        .collect();
    sessions.sort();
    assert!(!sessions.is_empty(), "no recipes in {}", dir.display());
    let update = std::env::var("UPDATE_RECIPES").as_deref() == Ok("1");
    for session in &sessions {
        let recorded = session.with_extension("out");
        let got = transcript(session, 1);
        if update {
            std::fs::write(&recorded, &got).unwrap();
        } else {
            let want = std::fs::read_to_string(&recorded)
                .unwrap_or_else(|e| panic!("{}: {e}", recorded.display()));
            assert_eq!(got, want, "{} disagrees with {}", session.display(), recorded.display());
        }
        assert_eq!(transcript(session, 3), got, "{} at three workers", session.display());
    }
}
