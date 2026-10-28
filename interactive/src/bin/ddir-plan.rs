//! Print preparation stages without installing a dataflow.

use std::collections::BTreeMap;
use std::io::{self, Write};

fn run() -> Result<(), String> {
    let files: Vec<_> = std::env::args().skip(1).collect();
    if files.is_empty() { return Err("usage: ddir-plan file.ddp [more.ddp ...]".into()); }
    let mut catalog = BTreeMap::new();
    let mut incomplete = false;
    let mut out = io::stdout().lock();
    for (i, file) in files.iter().enumerate() {
        let text = std::fs::read_to_string(file).map_err(|e| format!("{file}: {e}"))?;
        let report = interactive::plan::report(&text, &|name| catalog.get(name).cloned()).map_err(|e| format!("{file}: {e}"))?;
        writeln!(out, "Program {}\n{}", i + 1, report.print()).map_err(|e| e.to_string())?;
        incomplete |= !report.unresolved.is_empty() || report.stages.iter().any(|s| !s.problems.is_empty());
        for (name, shape) in report.exports {
            if catalog.insert(name.clone(), shape).is_some() { return Err(format!("duplicate export {name:?}")); }
        }
    }
    if incomplete { Err("planning incomplete; see unresolved imports and shape problems above".into()) }
    else { Ok(()) }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("ddir-plan: {error}");
        std::process::exit(1);
    }
}
