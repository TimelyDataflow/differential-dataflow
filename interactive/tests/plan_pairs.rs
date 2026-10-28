//! Reviewed whole DDP programs and stage diffs. UPDATE_PLAN_PAIRS=1 accepts snapshots, never invalid IR.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use interactive::plan::{self, Report};
use interactive::plan::ddp::DdpStage;
use interactive::scope_ir::{Item, Node, Ref, RowShape, Scope, Source};

#[path = "plan_pairs/manifest.rs"]
mod manifest;

struct Case {
    name: &'static str,
    input: &'static str,
    providers: &'static [&'static str],
    request: Option<&'static str>,
    explain: bool,
    problem: Option<&'static str>,
    functions: &'static str,
}

impl Case {
    const fn new(name: &'static str, input: &'static str, providers: &'static [&'static str]) -> Self {
        Self { name, input, providers, request: None, explain: false, problem: None, functions: "none" }
    }
    const fn request(mut self, name: &'static str) -> Self { self.request = Some(name); self }
    const fn explain(mut self) -> Self { self.explain = true; self }
    const fn problem(mut self, problem: &'static str) -> Self { self.problem = Some(problem); self }
    const fn functions(mut self, functions: &'static str) -> Self { self.functions = functions; self }
}

/// The only registered function in this executable. Shape inference evaluates it on empty columns.
struct Identity(corgi::Shape);
impl corgi::HostKernel for Identity {
    fn name(&self) -> &str { "plan_pair_identity" }
    fn input(&self) -> &corgi::Shape { &self.0 }
    fn output(&self) -> &corgi::Shape { &corgi::Shape::Int }
    fn eval(&self, input: corgi::Value) -> Result<corgi::Value, String> {
        Ok(input.into_prod("plan_pair_identity")?.remove(0))
    }
}

/// Check the tree independently of its printed text, even when accepting snapshots.
fn check_scope(scope: &Scope, parent: Option<(&Scope, usize)>, labels: bool, origins: &mut BTreeSet<String>, path: &str) {
    if labels {
        assert_eq!(scope.labels.len(), scope.items.len(), "{path}: one label per item");
        assert!(scope.labels.iter().all(|label| !label.is_empty()), "{path}: empty label");
    } else {
        // The explanation builder currently emits no statement labels. Do not invent origins.
        assert!(scope.labels.is_empty(), "{path}: explain label support changed; tighten this check");
    }
    let check_ref = |r: &Ref, limit: usize| check_ref(scope, r, limit);
    for import in &scope.imports {
        match (&import.from, parent) {
            (Source::Parent(r), Some((parent, limit))) => self::check_ref(parent, r, limit),
            (Source::Input(_) | Source::Trace(_), None) => {}
            _ => panic!("{path}: import crosses the wrong scope boundary"),
        }
    }
    for (i, item) in scope.items.iter().enumerate() {
        if labels { origins.insert(format!("{path}/{}", scope.labels[i])); }
        match item {
            Item::Op(node) => match node {
                Node::Linear { input, .. } | Node::Arrange(input)
                | Node::Reduce { input, .. } | Node::Inspect { input, .. } => check_ref(input, i),
                Node::Concat(refs) => for r in refs { check_ref(r, i); },
                Node::Join { left, right, .. } => { check_ref(left, i); check_ref(right, i); }
                Node::HalfJoin { stream, arrangement, .. } => { check_ref(stream, i); check_ref(arrangement, i); }
            },
            Item::Sub(child) => {
                if labels { assert_eq!(scope.labels[i], child.name, "{path}: child label"); }
                check_scope(child, Some((scope, i)), labels, origins, &format!("{path}/{}", child.name));
            }
        }
    }
    let mut bound = BTreeSet::new();
    for bind in &scope.binds {
        assert!(bind.var < scope.vars.len(), "{path}: invalid variable");
        assert!(bound.insert(bind.var), "{path}: variable bound twice");
        check_ref(&bind.value, scope.items.len());
    }
    assert_eq!(bound.len(), scope.vars.len(), "{path}: unbound variable");
    let mut exports = BTreeSet::new();
    for export in &scope.exports {
        assert!(exports.insert(&export.name), "{path}: duplicate export");
        check_ref(&export.value, scope.items.len());
    }
}

fn check_ref(scope: &Scope, r: &Ref, limit: usize) {
    match r {
        Ref::Local(i) => assert!(*i < limit && matches!(scope.items[*i], Item::Op(_)), "invalid local {r:?}"),
        Ref::Import(i) => assert!(*i < scope.imports.len(), "invalid import {r:?}"),
        Ref::Var(i) => assert!(*i < scope.vars.len(), "invalid variable {r:?}"),
        Ref::ChildExport(i, e) => {
            assert!(*i < limit, "forward child reference {r:?}");
            let Item::Sub(child) = &scope.items[*i] else { panic!("not a child: {r:?}") };
            assert!(*e < child.exports.len(), "invalid child export {r:?}");
        }
    }
}

fn check_report(case: &Case, report: &Report) {
    assert_eq!(report.stages.len(), 4, "{}: preparation stages changed", case.name);
    assert!(report.unresolved.is_empty(), "{}: missing providers: {:?}", case.name, report.unresolved);
    let mut original = BTreeSet::new();
    for (index, stage) in report.stages.iter().enumerate() {
        let mut origins = BTreeSet::new();
        check_scope(&stage.program.root, None, !case.explain, &mut origins, "root");
        if index == 0 { original = origins; }
        else { assert!(origins.is_subset(&original), "{}: {} invented an origin", case.name, stage.name); }
        match case.problem {
            None => {
                assert!(stage.problems.is_empty(), "{}: {}: {:?}", case.name, stage.name, stage.problems);
                assert!(stage.shapes.exports.iter().all(Option::is_some), "{}: unknown export", case.name);
                assert_eq!(stage.shapes.exports, report.stages[0].shapes.exports, "{}: export shapes changed", case.name);
            }
            Some(expected) => {
                assert!(!stage.problems.is_empty(), "{}: expected rejection disappeared", case.name);
                assert!(stage.problems.iter().all(|p| p.contains(expected)), "{}: unexpected problems: {:?}", case.name, stage.problems);
                assert!(report.exports.is_empty(), "a rejected program must not supply providers");
            }
        }
    }
    let optimized = &report.stages[1].program;
    let mut again = optimized.clone();
    again.optimize();
    assert_eq!(format!("{again:?}"), format!("{optimized:?}"), "{}: optimizer is not idempotent", case.name);
    if case.problem.is_none() {
        for stage in &report.stages[2..] {
            let mut again = stage.program.clone();
            let source = |source: &Source| stage.program.root.imports.iter().zip(&stage.shapes.imports)
                .find(|(import, _)| match (&import.from, source) {
                    (Source::Input(a), Source::Input(b)) => a == b,
                    (Source::Trace(a), Source::Trace(b)) => a == b,
                    _ => false,
                })
                .and_then(|(_, shape)| shape.as_ref().map(|s| (s.key.clone(), s.val.clone())));
            let masks = interactive::projection::pushdown(&mut again, &source).unwrap();
            assert_eq!(masks, 0, "{}: {}: repeated projection count changed", case.name, stage.name);
            assert_eq!(format!("{again:?}"), format!("{:?}", stage.program), "{}: second projection changed the plan", case.name);
        }
    }

}

fn snapshot(path: &Path, actual: &str, update: bool, differences: &mut Vec<String>) {
    let expected = match fs::read_to_string(path) {
        Ok(text) => Some(text),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => panic!("{}: {e}", path.display()),
    };
    if expected.as_deref() == Some(actual) { return; }
    if update {
        fs::write(path, actual).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    } else {
        let line = expected.as_deref().unwrap_or("").lines().zip(actual.lines())
            .position(|(a, b)| a != b).map(|i| i + 1)
            .unwrap_or_else(|| expected.as_deref().unwrap_or("").lines().count().min(actual.lines().count()) + 1);
        differences.push(format!("{} (first difference at line {line})", path.display()));
    }
}

/// Use the same whole programs as --emit-ddp-stage. Python is already needed by the providers.
fn stage_diff(before: (&str, &str), after: (&str, &str)) -> String {
    let mut child = Command::new("python3").arg("-B").arg("-c")
        .arg("import difflib, sys; a, b = sys.stdin.read().split('\\0'); sys.stdout.write(''.join(difflib.unified_diff(a.splitlines(True), b.splitlines(True), fromfile=sys.argv[1], tofile=sys.argv[2])))")
        .args([before.0, after.0]).stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap();
    let mut input = child.stdin.take().unwrap();
    write!(input, "{}\0{}", before.1, after.1).unwrap();
    drop(input);
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap()
}

fn emitted(case: &Case, report: &Report, source: &dyn Fn(&str) -> Option<RowShape>) -> Vec<(String, String)> {
    let mut output = Vec::new();
    let mut previous: Option<(DdpStage, String)> = None;
    for stage in DdpStage::ALL {
        if case.explain && stage == DdpStage::Parsed {
            assert!(report.emit_ddp_at(stage).err().unwrap().contains("supplied as IR"));
            continue;
        }
        if case.problem.is_some() && stage != DdpStage::Parsed {
            let error = report.emit_ddp_at(stage).err().expect("invalid plans must not be emitted");
            if stage == DdpStage::AsOf { output.push((format!("{}.error", case.name), format!("{error}\n"))); }
            continue;
        }
        let text = match report.emit_ddp_at(stage) {
            Ok(printed) => printed.text,
            Err(error) => panic!("{}: {}: {error}", case.name, stage.name()),
        };
        let replanned = plan::report(&text, source).unwrap_or_else(|e| panic!("{}: {}: emitted DDP does not replan: {e}", case.name, stage.name()));
        if case.problem.is_none() {
            assert!(replanned.unresolved.is_empty(), "{}: {}: emitted imports are unresolved", case.name, stage.name());
            assert!(replanned.stages.iter().all(|s| s.problems.is_empty()), "{}: {}: emitted DDP has shape problems", case.name, stage.name());
        }
        if stage == DdpStage::Parsed || (case.explain && stage == DdpStage::Lowered) {
            output.push((format!("{}.{}.ddp", case.name, stage.name()), text.clone()));
        }
        if let Some((before, before_text)) = previous {
            let diff = stage_diff((before.name(), &before_text), (stage.name(), &text));
            output.push((format!("{}.{}--{}.diff", case.name, before.name(), stage.name()), diff));
        }
        if stage == DdpStage::AsOf { output.push((format!("{}.ddp", case.name), text.clone())); }
        previous = Some((stage, text));
    }
    output
}

fn ddp_files(root: &Path, directory: &str) -> BTreeSet<String> {
    fs::read_dir(root.join(directory)).unwrap().map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "ddp"))
        .map(|path| path.strip_prefix(root).unwrap().to_str().unwrap().to_owned()).collect()
}

/// Adding a benchmark input must not silently leave it outside the reviewed corpus.
fn check_coverage(root: &Path) {
    let inputs: BTreeSet<_> = manifest::CASES.iter().map(|case| case.input.to_owned()).collect();
    for directory in ["bench/txn", "examples/server/programs", "bench/ldbc/interactive_v2/oneoff"] {
        assert_eq!(ddp_files(root, directory), inputs.iter().filter(|p| p.starts_with(&format!("{directory}/"))).cloned().collect());
    }
    for n in 1..=7 { assert!(inputs.contains(&format!("bench/ldbc/authored/plans/is{n:02}.ddp"))); }
    for n in 1..=14 { assert!(inputs.contains(&format!("bench/ldbc/interactive_v2/oneoff/ic{n:02}.oneoff.ddp"))); }
}

fn corpus_bytes(path: &Path) -> u64 {
    fs::read_dir(path).unwrap().map(|entry| {
        let path = entry.unwrap().path();
        if path.is_dir() { corpus_bytes(&path) } else { path.metadata().unwrap().len() }
    }).sum()
}

#[test]
fn reviewed_plan_pairs() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let pairs = root.join("tests/plan_pairs");
    let update = std::env::var("UPDATE_PLAN_PAIRS").as_deref() == Ok("1");
    check_coverage(&root);
    let mut differences = Vec::new();
    for (emitter, path) in manifest::GENERATED {
        let output = Command::new("python3").arg("-B").arg(pairs.join("emit_graph.py")).arg(emitter)
            .output().expect("plan pairs require python3 for the benchmark emitters");
        assert!(output.status.success(), "{emitter}: {}", String::from_utf8_lossy(&output.stderr));
        let text = String::from_utf8(output.stdout).expect("the graph emitter writes UTF-8");
        snapshot(&pairs.join(path), &text, update, &mut differences);
    }
    // A stale generated input must fail before any plan can be accepted against it.
    assert!(differences.is_empty(), "generated inputs differ: {differences:?}; run with UPDATE_PLAN_PAIRS=1");

    let mut providers: BTreeMap<&str, BTreeMap<String, RowShape>> = BTreeMap::new();
    let mut outputs = Vec::new();
    for case in manifest::CASES {
        assert!(!providers.contains_key(case.name), "duplicate case {}", case.name);
        assert!(case.name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'));
        let mut catalog = BTreeMap::new();
        for &provider in case.providers {
            let exports = providers.get(provider).unwrap_or_else(|| panic!("{}: provider {provider} must be planned first", case.name));
            assert!(!exports.is_empty(), "provider {provider} has no exports");
            for (name, shape) in exports {
                assert!(catalog.insert(name.clone(), shape.clone()).is_none(), "{}: duplicate trace {name}", case.name);
            }
        }
        if case.functions != "none" {
            assert_eq!(case.functions, "plan_pair_identity(int) -> int; pure identity");
            interactive::lambda::register(std::sync::Arc::new(Identity(corgi::Shape::Prod(vec![corgi::Shape::Int]))));
        }
        let mut text = fs::read_to_string(root.join(case.input)).unwrap();
        if let Some(name) = case.request {
            let original = format!("export \"{name}.answer\"");
            assert_eq!(text.matches(&original).count(), 1, "{}: request export changed", case.name);
            // The replacement in interactive_v2/systems.py, with request id fixed to 1.
            text = text.replace(&original, &format!("export \"{name}x1.answer\""));
        }
        let source = |name: &str| catalog.get(name).cloned();
        let report = if case.explain {
            let program = interactive::lower::lower_tree(interactive::parse::pipe::parse(&text));
            plan::report_program(interactive::explain::explain(&program), &source)
        } else { plan::report(&text, &source) }.unwrap_or_else(|e| panic!("{}: {e}", case.name));
        check_report(case, &report);

        let repeated = if case.explain {
            let program = interactive::lower::lower_tree(interactive::parse::pipe::parse(&text));
            plan::report_program(interactive::explain::explain(&program), &source)
        } else { plan::report(&text, &source) }.unwrap();
        assert!(case.problem.is_some() || report.compare_final(&repeated).is_empty(), "{}: planning changed on repetition", case.name);
        let out = emitted(case, &report, &source);
        assert_eq!(out, emitted(case, &repeated, &source), "{}: emitted bytes changed on repetition", case.name);
        providers.insert(case.name, report.exports);
        outputs.extend(out.into_iter().map(|(name, text)| (pairs.join("expected").join(name), text)));
    }
    // Check all cases before accepting any expected output.
    let expected_names: BTreeSet<_> = outputs.iter().map(|(path, _)| path.file_name().unwrap().to_owned()).collect();
    for entry in fs::read_dir(pairs.join("expected")).unwrap() {
        let entry = entry.unwrap();
        assert_ne!(entry.path().extension().and_then(|s| s.to_str()), Some("txt"), "remove diagnostic snapshots");
        assert!(expected_names.contains(&entry.file_name()), "unlisted expected file: {}; remove it explicitly", entry.path().display());
    }
    for (path, out) in outputs { snapshot(&path, &out, update, &mut differences); }
    assert!(differences.is_empty(), "plan pairs differ:\n{}\nReview with UPDATE_PLAN_PAIRS=1 cargo test -p interactive --test plan_pairs, then inspect the diff.", differences.join("\n"));
    let bytes = corpus_bytes(&pairs);
    assert!(bytes < 1_500_000, "keep the corpus well under 2 MB: {bytes} bytes");
    println!("{}; {}; {}; {}; {}", manifest::FORMAT, manifest::LANGUAGE, manifest::BACKEND, manifest::TRANSFORMS, manifest::ASSUMPTIONS);
    println!("{} plan pairs; {bytes} bytes under tests/plan_pairs", manifest::CASES.len());
}
