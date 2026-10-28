//! Print preparation stages without installing a dataflow.

use std::collections::BTreeMap;
use std::io::{self, Write};

fn run() -> Result<(), String> {
    let mut options = interactive::plan::PrintOptions::default();
    let mut files = Vec::new();
    let mut emit_ddp = false;
    let mut check_roundtrip = false;
    let mut stage = interactive::plan::ddp::DdpStage::AsOf;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--lambda-source" => options.lambda_source = true,
            "--check-lambdas" => options.check_lambdas = true,
            "--emit-ddp" => emit_ddp = true,
            "--emit-ddp-stage" => {
                let name = args.next().ok_or("--emit-ddp-stage requires parsed, lowered, optimized, projected, or as-of")?;
                stage = interactive::plan::ddp::DdpStage::ALL.into_iter().find(|stage| stage.name() == name)
                    .ok_or_else(|| format!("unknown DDP stage {name:?}"))?;
                emit_ddp = true;
            }
            "--check-roundtrip" => check_roundtrip = true,
            _ if arg.starts_with("--") => return Err(format!("unknown option {arg}")),
            _ => files.push(arg),
        }
    }
    if files.is_empty() { return Err("usage: ddir-plan [--emit-ddp | --emit-ddp-stage STAGE | --lambda-source] [--check-lambdas] [--check-roundtrip] file.ddp [more.ddp ...]".into()); }
    if emit_ddp && options.lambda_source { return Err("--emit-ddp always checks source against the compiled graph".into()); }
    if check_roundtrip && !emit_ddp { return Err("--check-roundtrip requires --emit-ddp".into()); }
    let mut catalog = BTreeMap::new();
    let mut incomplete = false;
    let mut out = io::stdout().lock();
    for (i, file) in files.iter().enumerate() {
        let text = std::fs::read_to_string(file).map_err(|e| format!("{file}: {e}"))?;
        let report = interactive::plan::report(&text, &|name| catalog.get(name).cloned()).map_err(|e| format!("{file}: {e}"))?;
        if emit_ddp {
            // Earlier files supply imports. Only the requested (last) program is emitted.
            if i + 1 == files.len() {
                let printed = report.emit_ddp_at(stage)?;
                write!(out, "{}", printed.text).map_err(|e| e.to_string())?;
                eprintln!("Lambdas: source={}, graph={}, graph_changed={}", printed.lambdas.source, printed.lambdas.graph, printed.lambdas.graph_changed);
                if options.check_lambdas { incomplete |= printed.lambdas.graph_changed != 0; }
                if check_roundtrip {
                    let reparsed = interactive::plan::report(&printed.text, &|name| catalog.get(name).cloned())?;
                    let differences = report.compare_final(&reparsed);
                    let stable = reparsed.emit_ddp_at(stage)?.text == printed.text;
                    eprintln!("Round trip: plan={}, text={}", if differences.is_empty() { "same" } else { "different" }, if stable { "stable" } else { "changed" });
                    for difference in &differences { eprintln!("Mismatch: {difference}"); }
                    incomplete |= !differences.is_empty() || !stable;
                }
            }
        } else {
            let printed = report.print_with(options);
            if options.check_lambdas {
                incomplete |= printed.lambdas.failed != 0 || printed.lambdas.checked != printed.lambdas.printed;
            }
            writeln!(out, "Program {}\n{}", i + 1, printed.text).map_err(|e| e.to_string())?;
        }
        incomplete |= !report.unresolved.is_empty() || report.stages.iter().any(|s| !s.problems.is_empty());
        for (name, shape) in report.exports {
            if catalog.insert(name.clone(), shape).is_some() { return Err(format!("duplicate export {name:?}")); }
        }
    }
    if incomplete { Err("planning incomplete; see imports, shape problems, and lambda checks above".into()) }
    else { Ok(()) }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("ddir-plan: {error}");
        std::process::exit(1);
    }
}
