//! Read-only planning and diagnostic text. This is not an execution or serialization format.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::panic::{catch_unwind, AssertUnwindSafe};

use corgi::Shape;
use crate::ir::{Lambda, LinearOp, Reducer};
use crate::scope_ir::{Item, Node, Program, Ref, RowShape, Scope, Source};
use crate::shapes::{self, CollShape, ItemShapes, ScopeShapes};

/// The four preparation stages. Imports are assumed to use the producer's arrangement.
/// Projection is deliberately enabled, independent of the process environment.
pub struct Report {
    pub stages: Vec<Stage>,
    /// Missing providers, even when the import declares a shape.
    pub unresolved: Vec<String>,
    /// Inferred exports for planning another program. Empty if preparation is incomplete.
    pub exports: BTreeMap<String, RowShape>,
}

/// One scope tree and its inferred shapes, before rendering.
pub struct Stage {
    pub name: &'static str,
    pub program: Program,
    pub shapes: ScopeShapes,
    pub changes: Vec<String>,
    pub problems: Vec<String>,
    unresolved: Vec<String>,
}

/// Parse, lower, optimize, project, and apply the install-time as-of drop without installing.
/// `source` supplies published trace shapes; a declaration alone does not resolve an import.
/// Lambdas use the same registered functions as the DDP parser. The as-of time is the fixed
/// placeholder `G = (1, [])`, as for installation at epoch 2, extended by zero scope lanes.
/// This reports the projection-enabled path, not the server's default projection-off path.
pub fn report(text: &str, source: &dyn Fn(&str) -> Option<RowShape>) -> Result<Report, String> {
    catch_unwind(AssertUnwindSafe(|| build_report(text, source))).map_err(|panic| {
        if let Some(message) = panic.downcast_ref::<String>() { message.clone() }
        else if let Some(message) = panic.downcast_ref::<&str>() { message.to_string() }
        else { "planning panicked without a text message".into() }
    })?
}

fn build_report(text: &str, source: &dyn Fn(&str) -> Option<RowShape>) -> Result<Report, String> {
    let mut program = crate::lower::lower_tree(crate::parse::pipe::parse(text));
    let mut catalog = BTreeMap::new();
    let mut unresolved = Vec::new();
    let mut problems = Vec::new();
    for import in &program.root.imports {
        match &import.from {
            Source::Trace(name) => {
                let shape = catalog.entry(name.clone()).or_insert_with(|| source(name));
                if let Some(shape) = shape {
                    if import.shape.as_ref().is_some_and(|declared| declared != shape) {
                        return Err(format!("import {} declares {} but its provider has {}", quote(name), row(import.shape.as_ref().unwrap()), row(shape)));
                    }
                } else {
                    unresolved.push(name.clone());
                }
            }
            Source::Input(n) if import.shape.is_none() => problems.push(format!("input {n} has no declared shape")),
            _ => {}
        }
    }
    unresolved.sort();
    unresolved.dedup();
    if !program.root.vars.is_empty() {
        problems.push("feedback at the root cannot be installed; put it in a named scope".into());
    }
    let resolve = |source: &Source| match source {
        Source::Trace(name) => catalog.get(name).cloned().flatten(),
        _ => None,
    };
    let mut stages = vec![stage("as lowered", program.clone(), Vec::new(), &resolve)];
    let mut changes = Vec::new();
    let mut names = BTreeMap::new();
    program.root.optimize_with("root", &mut |path, scope, action, from, to| {
        let entry = names.entry(path.to_string()).or_insert_with(|| (scope.items.len(), Names::new(scope)));
        if entry.0 != scope.items.len() { *entry = (scope.items.len(), Names::new(scope)); }
        let names = &entry.1;
        changes.push(if from == to {
            format!("{action} {path}/{} (name before compaction)", names.items[from])
        } else {
            format!("{action} {path}/{} into {path}/{} (names before compaction)", names.items[from], names.items[to])
        });
    });
    stages.push(stage("after optimizer", program.clone(), changes, &resolve));
    let valid = problems.is_empty() && unresolved.is_empty() && stages[1].problems.is_empty();
    let exports = stages[1].shapes.exports.clone();
    let mut changes = Vec::new();
    if valid {
        let mut candidate = program.clone();
        let masked = crate::projection::pushdown(&mut candidate, &resolve).unwrap_or(0);
        if masked > 0 {
            let checked = stage("", candidate.clone(), Vec::new(), &resolve);
            if checked.problems.is_empty() && checked.shapes.exports == exports {
                changes = changed_statements(&program, &candidate, &resolve, "masked");
                changes.insert(0, format!("masked {masked} top-level fields (including inserted maps)"));
                program = candidate;
            } else {
                changes.push("projection candidate rejected by shape/export checks; kept the optimizer output".into());
            }
        } else {
            changes.push("no fields masked (the pass also returns zero for unsupported or rejected candidates)".into());
        }
    } else {
        changes.push("skipped: unresolved imports or invalid/unknown shapes prevent installation".into());
    }
    stages.push(stage("after projection (off by default; enabled for this report)", program.clone(), changes, &resolve));
    let mut changes = Vec::new();
    if valid {
        let mut candidate = program.clone();
        let inserted = crate::as_of::drop_dead_as_of(&mut candidate, &crate::ir::Time::new(1, Default::default()));
        if inserted > 0 {
            let checked = stage("", candidate.clone(), Vec::new(), &resolve);
            if checked.problems.is_empty() && checked.shapes.exports == exports {
                changes = changed_statements(&program, &candidate, &resolve, "added as-of filter to");
                changes.insert(0, format!("inserted {inserted} filters dropping G where continuing paths reach a strict half join"));
                program = candidate;
            } else {
                changes.push("as-of candidate rejected by shape/export checks; kept the projection output".into());
            }
        }
    } else {
        changes.push("skipped: unresolved imports or invalid/unknown shapes prevent installation".into());
    }
    stages.push(stage("after as-of drop (on by default; G = (1, []), placeholder install epoch 2)", program, changes, &resolve));
    for stage in &mut stages { stage.problems.extend(problems.iter().cloned()); }
    let exports = if valid {
        stages.last().unwrap().program.root.exports.iter().zip(exports).filter_map(|(export, shape)| {
            shape.map(|shape| (export.name.clone(), (shape.key, shape.val)))
        }).collect()
    } else { BTreeMap::new() };
    Ok(Report { stages, unresolved, exports })
}

fn stage(name: &'static str, program: Program, changes: Vec<String>, source: &dyn Fn(&Source) -> Option<RowShape>) -> Stage {
    let (shapes, problems) = shapes::infer(&program, source);
    let unresolved = program.root.imports.iter().filter_map(|import| match &import.from {
        Source::Trace(name) if source(&import.from).is_none() => Some(name.clone()),
        _ => None,
    }).collect();
    Stage { name, program, shapes, changes, problems: problems.conflicts.into_iter().chain(problems.unknown).collect(), unresolved }
}

impl Report {
    /// Print all stages and their changes, with no runtime identities or wall-clock values.
    pub fn print(&self) -> String {
        let mut out = String::from("Not yet parseable DDP. Read-only plan report.\n");
        out.push_str("Assumptions: trace imports use arrangements; projection enabled; as-of drop enabled.\n");
        out.push_str("G = (1, []) is a placeholder as-of time, for install epoch 2; nested lanes are zero.\n");
        out.push_str("Imported histories advance to G; fed inputs start at epoch 2. No frontier checks or installation.\n");
        out.push_str("Lambda text is diagnostic source, not the compiled graph. Composition/masking may make them differ.\n");
        out.push_str("Scalar compilation and other renderer internals are not shown. Type variant names and erased aliases are not retained.\n");
        for name in &self.unresolved { writeln!(out, "Unresolved import: {} (a declared shape is not a provider)", quote(name)).unwrap(); }
        for stage in &self.stages {
            writeln!(out, "\n=== {} ===", stage.name).unwrap();
            if stage.changes.is_empty() { out.push_str("Changes: none\n"); }
            else {
                out.push_str("Changes:\n");
                for change in &stage.changes { writeln!(out, "  - {change}").unwrap(); }
            }
            out.push_str(&stage.print());
        }
        out
    }
}

impl Stage {
    /// Print one stage in dependency order. Generated item suffixes are local ordinals within
    /// a statement, not runtime identities; compaction may renumber them between stages.
    pub fn print(&self) -> String {
        let mut out = String::from("Not yet parseable DDP. Lambdas show source text; composed text may differ from its graph.\n");
        for problem in &self.problems { writeln!(out, "Problem: {problem}").unwrap(); }
        out.push_str("Renderer notes assume arranged trace imports. Other expansions and scalar compilation are not shown.\n");
        let unresolved = self.unresolved.iter().cloned().collect();
        print_scope(&self.program.root, &self.shapes, None, &unresolved, 0, &mut out);
        out
    }
}

fn quote(text: &str) -> String { format!("\"{}\"", text.chars().flat_map(char::escape_default).collect::<String>()) }
fn row((key, val): &RowShape) -> String { format!("({key}, {val})") }
fn shape(shape: Option<CollShape>) -> String {
    shape.map(|s| format!("({}, {}); time = {} integer lanes; diff = int", s.key, s.val, s.time)).unwrap_or_else(|| "unknown".into())
}

struct Names {
    items: Vec<String>,
    imports: Vec<String>,
    vars: Vec<String>,
    exports: Vec<Vec<String>>,
}

impl Names {
    fn new(scope: &Scope) -> Self {
        let mut counts = BTreeMap::new();
        let items = scope.items.iter().enumerate().map(|(i, item)| {
            if let Item::Sub(child) = item { return format!("scope {}", quote(&child.name)); }
            let label = label(scope, i);
            let count = counts.entry(label.clone()).or_insert(0);
            *count += 1;
            format!("{}#{}", quote(&label), count)
        }).collect();
        Names {
            items,
            imports: scope.imports.iter().enumerate().map(|(i, v)| format!("import {}@{i}", quote(&v.name))).collect(),
            vars: scope.vars.iter().map(|v| format!("var {}", quote(&v.name))).collect(),
            exports: scope.items.iter().map(|item| match item {
                Item::Sub(child) => child.exports.iter().map(|e| quote(&e.name)).collect(),
                Item::Op(_) => Vec::new(),
            }).collect(),
        }
    }
    fn of(&self, r: &Ref) -> String {
        match r {
            Ref::Local(i) => self.items[*i].clone(),
            Ref::Import(i) => self.imports[*i].clone(),
            Ref::Var(i) => self.vars[*i].clone(),
            Ref::ChildExport(i, e) => format!("{}::{}", self.items[*i], self.exports[*i][*e]),
        }
    }
}

fn label(scope: &Scope, i: usize) -> String {
    if scope.labels.len() == scope.items.len() { scope.labels[i].clone() }
    else { format!("unlabeled item {i}") }
}

fn lambda(lambda: &Lambda) -> String { format!("lambda {{ {} }}", lambda.src) }

fn linear(op: &LinearOp) -> String {
    match op {
        LinearOp::Project(l) => format!("map({})", lambda(l)),
        LinearOp::Filter(l) => format!("filter({})", lambda(l)),
        LinearOp::Negate => "negate".into(),
        LinearOp::EnterAt(l) => format!("enter_at({})", lambda(l)),
        LinearOp::LiftIter => "lift_iter".into(),
        LinearOp::FlatMap(l) => format!("flatmap({})", lambda(l)),
        LinearOp::Lift => "lift".into(),
        LinearOp::JoinFunction(l) => format!("join_function({})", lambda(l)),
    }
}

fn reducer(reducer: &Reducer) -> String {
    match reducer {
        Reducer::Min => "min".into(),
        Reducer::Distinct => "distinct".into(),
        Reducer::Count => "count".into(),
        Reducer::Collect => "collect".into(),
        Reducer::Expr(l) => format!("reduce({})", lambda(l)),
    }
}

fn node(node: &Node, names: &Names) -> String {
    match node {
        Node::Linear { input, ops } => format!("{} | linear [ {} ]", names.of(input), ops.iter().map(linear).collect::<Vec<_>>().join(" | ")),
        Node::Concat(refs) => format!("concat({})", refs.iter().map(|r| names.of(r)).collect::<Vec<_>>().join(", ")),
        Node::Arrange(input) => format!("{} | arrange", names.of(input)),
        Node::Join { left, right, projection } => format!("{} | join({}, {})", names.of(left), names.of(right), lambda(projection)),
        Node::HalfJoin { stream, arrangement, strict, projection } => format!("{} | half_join({}, strict={strict}, {})", names.of(stream), names.of(arrangement), lambda(projection)),
        Node::Reduce { input, reducer: r } => format!("{} | {}", names.of(input), reducer(r)),
        Node::Inspect { input, label } => format!("{} | inspect({})", names.of(input), quote(label)),
    }
}

#[derive(Clone, Copy)]
enum Form { Rows, Arrangement, Unknown }
impl Form {
    fn text(self) -> &'static str { match self { Self::Rows => "rows", Self::Arrangement => "arrangement", Self::Unknown => "unknown" } }
}

fn form(r: &Ref, imports: &[Form], items: &[Form]) -> Form {
    match r {
        Ref::Local(i) => items[*i], Ref::Import(i) => imports[*i],
        Ref::Var(_) | Ref::ChildExport(..) => Form::Rows,
    }
}

// These are the representation choices in backend::render_scope, not a second renderer.
fn item_form(node: &Node, shapes: &ScopeShapes) -> Form {
    match node {
        Node::Arrange(_) => Form::Arrangement,
        Node::Reduce { input, reducer: Reducer::Distinct } => match shapes.of(input) {
            Some(s) if s.val == Shape::Unit => Form::Arrangement,
            Some(_) => Form::Rows,
            None => Form::Unknown,
        },
        Node::Reduce { .. } => Form::Arrangement,
        _ => Form::Rows,
    }
}

/// Input reads, separating row streams from index reads (including arranging/reusing a value).
fn reads<'a>(node: &'a Node, shapes: &ScopeShapes) -> Vec<(&'a Ref, &'static str)> {
    match node {
        Node::Linear { input, .. } | Node::Inspect { input, .. } => vec![(input, "rows")],
        Node::Concat(refs) => refs.iter().map(|r| (r, "rows")).collect(),
        Node::Arrange(input) => vec![(input, "arrange/reuse")],
        Node::Join { left, right, .. } => vec![(left, "index"), (right, "index")],
        Node::HalfJoin { stream, arrangement, .. } => vec![(stream, "rows"), (arrangement, "index")],
        Node::Reduce { input, reducer: Reducer::Distinct } => vec![(input, match shapes.of(input) {
            Some(s) if s.val == Shape::Unit => "index",
            Some(_) => "rows", None => "unknown distinct read",
        })],
        Node::Reduce { input, .. } => vec![(input, "index")],
    }
}

fn print_scope(scope: &Scope, shapes: &ScopeShapes, parent: Option<(&Names, &[Form])>, unresolved: &BTreeSet<String>, depth: usize, out: &mut String) {
    let names = Names::new(scope);
    let indent = "  ".repeat(depth);
    writeln!(out, "{indent}scope {} {{", quote(&scope.name)).unwrap();
    let sources: Vec<_> = scope.imports.iter().enumerate().map(|(i, import)| {
        match &import.from {
            Source::Input(n) => (format!("input {n}"), Form::Rows),
            Source::Trace(name) if unresolved.contains(name) => (format!("unresolved trace {} (shape, if known, is declared)", quote(name)), Form::Unknown),
            Source::Trace(name) => (format!("trace {}", quote(name)), Form::Arrangement),
            Source::Parent(r) => {
                let (names, forms) = parent.expect("a nested scope has a parent");
                (format!("parent {}", names.of(r)), forms[i])
            }
        }
    }).collect();
    let imports: Vec<_> = sources.iter().map(|(_, form)| *form).collect();
    let mut readers = vec![Vec::new(); imports.len()];
    for (j, item) in scope.items.iter().enumerate() {
        match item {
            Item::Op(node) => for (r, mode) in reads(node, shapes) {
                if let Ref::Import(i) = r { readers[*i].push(format!("{} ({mode})", names.items[j])); }
            },
            Item::Sub(child) => for imp in &child.imports {
                if let Source::Parent(Ref::Import(i)) = &imp.from {
                    readers[*i].push(format!("scope {} import {} (pass through {})", quote(&child.name), quote(&imp.name), imports[*i].text()));
                }
            },
        }
    }
    for bind in &scope.binds {
        if let Ref::Import(i) = &bind.value { readers[*i].push(format!("{} (rows)", names.vars[bind.var])); }
    }
    for export in &scope.exports {
        if let Ref::Import(i) = &export.value {
            readers[*i].push(format!("export {} ({})", quote(&export.name), if depth == 0 { "arrange/reuse" } else { "rows" }));
        }
    }
    for (i, (source, form)) in sources.iter().enumerate() {
        writeln!(out, "{indent}  {} = {source} : {}; arrives as {}", names.imports[i], shape(shapes.imports[i].clone()), form.text()).unwrap();
        writeln!(out, "{indent}    readers: {}", if readers[i].is_empty() { "none".into() } else { readers[i].join(", ") }).unwrap();
    }
    for (i, var) in scope.vars.iter().enumerate() {
        writeln!(out, "{indent}  var {} : {}", quote(&var.name), shape(shapes.vars[i].clone())).unwrap();
    }
    let mut forms = Vec::new();
    for (i, item) in scope.items.iter().enumerate() {
        match item {
            Item::Sub(child) => {
                let ItemShapes::Sub(child_shapes) = &shapes.items[i] else { unreachable!() };
                let entering: Vec<_> = child.imports.iter().map(|imp| match &imp.from {
                    Source::Parent(r) => form(r, &imports, &forms), _ => Form::Unknown,
                }).collect();
                print_scope(child, child_shapes, Some((&names, &entering)), unresolved, depth + 1, out);
                forms.push(Form::Rows);
            }
            Item::Op(op) => {
                let repr = item_form(op, shapes);
                writeln!(out, "{indent}  let {} [statement {}] = {}", names.items[i], quote(&label(scope, i)), node(op, &names)).unwrap();
                writeln!(out, "{indent}    : {}; output {}", shape(shapes.of(&Ref::Local(i))), repr.text()).unwrap();
                for (r, mode) in reads(op, shapes) {
                    let supplied = form(r, &imports, &forms);
                    let conversion = match (mode, supplied) {
                        ("rows", Form::Arrangement) => "flatten arrangement to rows",
                        ("index" | "arrange/reuse", Form::Rows) => "build arrangement",
                        ("index" | "arrange/reuse", Form::Arrangement) => "reuse arrangement",
                        _ => supplied.text(),
                    };
                    writeln!(out, "{indent}    reads {} as {mode}: {conversion}", names.of(r)).unwrap();
                }
                if let Node::Reduce { input, reducer: Reducer::Distinct } = op {
                    match shapes.of(input) {
                        Some(s) if s.val != Shape::Unit => writeln!(out,
                            "{indent}    renderer: rows -> map((k, v) -> ((k, v), ())) -> arrange : (({}, {}), ()) -> distinct -> rows -> map((kv, ()) -> kv); output rows", s.key, s.val).unwrap(),
                        None => writeln!(out, "{indent}    renderer: distinct expansion not shown (unknown value shape)").unwrap(),
                        _ => {}
                    }
                }
                if let Node::Linear { ops, .. } = op {
                    if ops.iter().any(|op| matches!(op, LinearOp::EnterAt(_))) {
                        writeln!(out, "{indent}    renderer: enter_at takes max(current scope lane, 256 * bit_length(argument as u64))").unwrap();
                    }
                }
                forms.push(repr);
            }
        }
    }
    for bind in &scope.binds {
        writeln!(out, "{indent}  set {} = {}; feedback advances scope lane {depth} by 1", names.vars[bind.var], names.of(&bind.value)).unwrap();
    }
    for (i, export) in scope.exports.iter().enumerate() {
        let action = if depth > 0 { "read rows; leave scope and truncate its time coordinate" }
        else { match form(&export.value, &imports, &forms) {
            Form::Arrangement => "publish existing arrangement", Form::Rows => "build export arrangement", Form::Unknown => "export storage not shown",
        }};
        writeln!(out, "{indent}  export {} = {} : {}; renderer: {action}", quote(&export.name), names.of(&export.value), shape(shapes.exports[i].clone())).unwrap();
    }
    writeln!(out, "{indent}}}").unwrap();
}

#[derive(Default)]
struct Statement {
    items: Vec<String>,
    shapes: Vec<String>,
    lambdas: Vec<String>,
}

fn statements(program: &Program, source: &dyn Fn(&Source) -> Option<RowShape>) -> BTreeMap<String, Statement> {
    fn visit(scope: &Scope, shapes: &ScopeShapes, path: &str, result: &mut BTreeMap<String, Statement>) {
        let names = Names::new(scope);
        for (i, item) in scope.items.iter().enumerate() {
            match item {
                Item::Sub(child) => {
                    let ItemShapes::Sub(shapes) = &shapes.items[i] else { unreachable!() };
                    visit(child, shapes, &format!("{path}/{}", child.name), result);
                }
                Item::Op(op) => {
                    let entry = result.entry(format!("{path}/{}", quote(&label(scope, i)))).or_default();
                    entry.items.push(node(op, &names));
                    entry.shapes.push(shape(shapes.of(&Ref::Local(i))));
                    match op {
                        Node::Linear { ops, .. } => entry.lambdas.extend(ops.iter().map(linear)),
                        Node::Join { projection, .. } | Node::HalfJoin { projection, .. } => entry.lambdas.push(lambda(projection)),
                        Node::Reduce { reducer: Reducer::Expr(l), .. } => entry.lambdas.push(lambda(l)),
                        _ => {}
                    }
                }
            }
        }
    }
    let (shapes, _) = shapes::infer(program, source);
    let mut result = BTreeMap::new();
    visit(&program.root, &shapes, "root", &mut result);
    result
}

fn changed_statements(before: &Program, after: &Program, source: &dyn Fn(&Source) -> Option<RowShape>, action: &str) -> Vec<String> {
    let before = statements(before, source);
    let after = statements(after, source);
    let mut changes = Vec::new();
    for (name, next) in &after {
        if let Some(prev) = before.get(name) {
            let delta = next.items.len() as isize - prev.items.len() as isize;
            if delta > 0 { changes.push(format!("added {delta} items to {name}")); }
            if delta < 0 { changes.push(format!("removed {} items from {name}", -delta)); }
            if next.lambdas != prev.lambdas { changes.push(format!("{action} {name}")); }
            else if next.items != prev.items { changes.push(format!("rewired {name}")); }
            else if next.shapes != prev.shapes { changes.push(format!("changed shape of {name}")); }
        } else { changes.push(format!("added {name}")); }
    }
    for name in before.keys() {
        if !after.contains_key(name) { changes.push(format!("removed {name}")); }
    }
    changes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn providers_are_required_and_as_of_filters_are_visible() {
        let provider = report(r#"export "rows" = input 0 : (int, int);"#, &|_| None).unwrap();
        let text = r#"
            let rows = import "rows" : (int, int);
            export "out" = rows | lift | half_join(rows, 1, (k, v, w) -> (k, v));
        "#;
        let missing = report(text, &|_| None).unwrap();
        assert_eq!(missing.unresolved, ["rows"]);
        assert!(missing.exports.is_empty());
        let plan = report(text, &|name| provider.exports.get(name).cloned()).unwrap();
        assert!(plan.unresolved.is_empty());
        assert!(plan.stages[3].changes.iter().any(|s| s.contains("inserted 1 filters")));
        assert!(plan.print().contains("(t0, 1) ne"));
        assert_eq!(plan.print(), report(text, &|name| provider.exports.get(name).cloned()).unwrap().print());
        assert!(report(text, &|_| Some((Shape::Unit, Shape::Unit))).is_err());
    }
}
