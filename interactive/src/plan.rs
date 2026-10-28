//! Read-only planning, diagnostic reports, and final-stage DDP output.

pub mod ddp;
mod compare;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::panic::{catch_unwind, AssertUnwindSafe};

use corgi::Shape;
use crate::ir::{Lambda, LinearOp, Reducer};
use crate::passes::Config;
use crate::scope_ir::{Item, Node, Program, Ref, RowShape, Scope, Source};
use crate::shapes::{self, CollShape, ItemShapes, ScopeShapes};

/// The four preparation stages. Imports are assumed to use the producer's arrangement.
/// A stage whose pass is switched off still appears, unchanged, and says so.
pub struct Report {
    pub stages: Vec<Stage>,
    /// Missing providers, even when the import declares a shape.
    pub unresolved: Vec<String>,
    /// Inferred exports for planning another program. Empty if preparation is incomplete.
    pub exports: BTreeMap<String, RowShape>,
    context: ddp::Context,
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

/// Parse, lower, and run the passes `passes` switches on, as install would, without installing.
/// `source` supplies published and generated trace shapes; a declaration alone does not resolve an import.
/// Lambdas use the same registered functions as the DDP parser. The as-of time is the fixed
/// placeholder `G = (1, [])`, as for installation at epoch 2, extended by zero scope lanes.
pub fn report(text: &str, source: &dyn Fn(&str) -> Option<RowShape>, passes: &Config) -> Result<Report, String> {
    caught_report(|| build_report(crate::lower::lower_tree(crate::parse::pipe::parse(text)), ddp::Context::new(text), source, passes))
}

/// Prepare an already lowered program, such as an explanation rewrite.
/// Parsed syntax is unavailable for this entry point.
pub fn report_program(program: Program, source: &dyn Fn(&str) -> Option<RowShape>, passes: &Config) -> Result<Report, String> {
    caught_report(|| build_report(program, ddp::Context::for_program(), source, passes))
}

fn caught_report(build: impl FnOnce() -> Result<Report, String>) -> Result<Report, String> {
    catch_unwind(AssertUnwindSafe(build)).map_err(|panic| {
        if let Some(message) = panic.downcast_ref::<String>() { message.clone() }
        else if let Some(message) = panic.downcast_ref::<&str>() { message.to_string() }
        else { "planning panicked without a text message".into() }
    })?
}

/// Plan a program and emit its final stage in today's DDP syntax.
pub fn emit_ddp(text: &str, source: &dyn Fn(&str) -> Option<RowShape>, passes: &Config) -> Result<ddp::Emitted, String> {
    report(text, source, passes)?.emit_ddp()
}

fn build_report(mut program: Program, context: ddp::Context, source: &dyn Fn(&str) -> Option<RowShape>, passes: &Config) -> Result<Report, String> {
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
    let mut stages: Vec<Stage> = Vec::new();
    crate::passes::run(&mut program, &resolve, passes, &crate::ir::Time::new(1, Default::default()), &mut |name, program, changes| {
        let previous = stages.last().map(|stage| &stage.program);
        let changes = stage_changes(name, changes, previous, program, &resolve);
        let name = match name {
            "after as-of drop" => "after as-of drop (G = (1, []), placeholder install epoch 2)",
            name => name,
        };
        stages.push(stage(name, program.clone(), changes, &resolve));
    });
    let live_traces: BTreeSet<_> = program.root.imports.iter().filter_map(|i| match &i.from {
        Source::Trace(name) => Some(name.as_str()), _ => None,
    }).collect();
    unresolved.retain(|name| live_traces.contains(name.as_str()));
    let valid = problems.is_empty() && unresolved.is_empty() && stages[1].problems.is_empty();
    let exports = stages[1].shapes.exports.clone();
    for stage in &mut stages { stage.problems.extend(problems.iter().cloned()); }
    let exports = if valid {
        stages.last().unwrap().program.root.exports.iter().zip(exports).filter_map(|(export, shape)| {
            shape.map(|shape| (export.name.clone(), (shape.key, shape.val)))
        }).collect()
    } else { BTreeMap::new() };
    Ok(Report { stages, unresolved, exports, context })
}

fn stage_changes(name: &str, changes: &[crate::passes::Change], before: Option<&Program>, after: &Program, source: &dyn Fn(&Source) -> Option<RowShape>) -> Vec<String> {
    use crate::passes::Change;
    let mut result = Vec::new();
    for change in changes {
        match change {
            Change::Structural { path, action, from, to } if from.label == to.label && from.ordinal == to.ordinal => {
                result.push(format!("{action} {path}/{}#{} (name before compaction)", quote(&from.label), from.ordinal));
            }
            Change::Structural { path, action, from, to } => result.push(format!(
                "{action} {path}/{}#{} into {path}/{}#{} (names before compaction)",
                quote(&from.label), from.ordinal, quote(&to.label), to.ordinal,
            )),
            Change::Masked(masked) => {
                result.push(format!("{masked} field masks and map fusions"));
                result.extend(changed_statements(before.unwrap(), after, source, "masked"));
            }
            Change::AsOf(inserted) => {
                result.push(format!("inserted {inserted} filters dropping G where continuing paths reach a strict half join"));
                result.extend(changed_statements(before.unwrap(), after, source, "added as-of filter to"));
            }
            Change::NoProjection => result.push("no fields masked (the pass also returns zero for unsupported or rejected candidates)".into()),
            Change::Rejected => result.push(if name == "after projection" {
                "projection candidate rejected by shape/export checks; kept the optimizer output"
            } else {
                "as-of candidate rejected by shape/export checks; kept the previous stage"
            }.into()),
            Change::Skipped => result.push("skipped: unresolved imports or invalid/unknown shapes prevent installation".into()),
            Change::Off(pass) => result.push(format!("{pass} switched off")),
        }
    }
    result
}

fn stage(name: &'static str, program: Program, changes: Vec<String>, source: &dyn Fn(&Source) -> Option<RowShape>) -> Stage {
    let (shapes, problems) = shapes::infer(&program, source);
    let unresolved = program.root.imports.iter().filter_map(|import| match &import.from {
        Source::Trace(name) if source(&import.from).is_none() => Some(name.clone()),
        _ => None,
    }).collect();
    Stage { name, program, shapes, changes, problems: problems.conflicts.into_iter().chain(problems.unknown).collect(), unresolved }
}

/// Display options. Evaluation is opt-in because registered words can be expensive.
#[derive(Clone, Copy, Default)]
pub struct PrintOptions {
    pub lambda_source: bool,
    pub check_lambdas: bool,
}

/// Counts refer to operator slots, including all nested scalar bodies in each check.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct LambdaCounts {
    pub printed: usize,
    pub checked: usize,
    pub failed: usize,
    pub fallback: usize,
    pub exact: usize,
    pub sampled: usize,
}

impl LambdaCounts {
    fn add(&mut self, other: Self) {
        self.printed += other.printed; self.checked += other.checked;
        self.failed += other.failed; self.fallback += other.fallback;
        self.exact += other.exact; self.sampled += other.sampled;
    }
    fn line(&self) -> String {
        format!("printed={}, checked={}, failed={}, fallback={}, exact={}, sampled={}", self.printed, self.checked, self.failed, self.fallback, self.exact, self.sampled)
    }
}

pub struct PrintedPlan {
    pub text: String,
    pub lambdas: LambdaCounts,
}

impl Report {
    /// Print all stages and their changes, with no runtime identities or wall-clock values.
    pub fn print(&self) -> String { self.print_with(PrintOptions::default()).text }

    pub fn print_with(&self, options: PrintOptions) -> PrintedPlan {
        let mut out = String::from("Not yet parseable DDP. Read-only plan report.\n");
        out.push_str("Assumptions: trace imports use arrangements; projection enabled; as-of drop enabled.\n");
        out.push_str("G = (1, []) is a placeholder as-of time, for install epoch 2; nested lanes are zero.\n");
        out.push_str("Imported histories advance to G; fed inputs start at epoch 2. No frontier checks or installation.\n");
        out.push_str("Lambda checks compare compiled graphs, then generated rows if the graphs differ. Sampling is not a proof.\n");
        out.push_str("Other renderer internals are not shown. Type variant names and erased aliases are not retained.\n");
        for name in &self.unresolved { writeln!(out, "Unresolved import: {} (a declared shape is not a provider)", quote(name)).unwrap(); }
        let mut lambdas = LambdaCounts::default();
        for stage in &self.stages {
            writeln!(out, "\n=== {} ===", stage.name).unwrap();
            if stage.changes.is_empty() { out.push_str("Changes: none\n"); }
            else {
                out.push_str("Changes:\n");
                for change in &stage.changes { writeln!(out, "  - {change}").unwrap(); }
            }
            let printed = stage.print_with(options);
            out.push_str(&printed.text);
            lambdas.add(printed.lambdas);
        }
        writeln!(out, "Lambda total: {}", lambdas.line()).unwrap();
        PrintedPlan { text: out, lambdas }
    }
}

impl Stage {
    /// Print one stage in dependency order. Generated item suffixes are local ordinals within
    /// a statement, not runtime identities; compaction may renumber them between stages.
    pub fn print(&self) -> String { self.print_with(PrintOptions::default()).text }

    pub fn print_with(&self, options: PrintOptions) -> PrintedPlan {
        let mut out = String::from("Not yet parseable DDP.\n");
        if options.lambda_source { out.push_str("Lambdas show diagnostic source; it may differ from the graph. Checks still use graph output.\n"); }
        else { out.push_str("Lambdas show compiled graphs as DDP expressions; graph fallbacks are marked.\n"); }
        for problem in &self.problems { writeln!(out, "Problem: {problem}").unwrap(); }
        out.push_str("Renderer notes assume arranged trace imports.\n");
        let mut slots = Slots { options, counts: LambdaCounts::default(), notes: Vec::new() };
        let unresolved = self.unresolved.iter().cloned().collect();
        print_scope(&self.program.root, &self.shapes, None, &unresolved, 0, "root", &mut slots, &mut out);
        for note in slots.notes { writeln!(out, "{note}").unwrap(); }
        writeln!(out, "Lambdas: {}", slots.counts.line()).unwrap();
        PrintedPlan { text: out, lambdas: slots.counts }
    }
}

struct Slots {
    options: PrintOptions,
    counts: LambdaCounts,
    notes: Vec<String>,
}

impl Slots {
    fn lambda(&mut self, lambda: &Lambda, input: Option<Shape>, at: &str) -> String {
        self.counts.printed += 1;
        use crate::lambda::print::Check;
        let printed = crate::lambda::print::print(lambda, input.as_ref(), self.options.check_lambdas);
        self.counts.fallback += usize::from(printed.fallback);
        match printed.check {
            Check::Exact => { self.counts.checked += 1; self.counts.exact += 1; }
            Check::Sampled => { self.counts.checked += 1; self.counts.sampled += 1; }
            Check::Failed(error) => {
                self.counts.checked += 1; self.counts.failed += 1;
                self.notes.push(format!("Lambda check FAILED at {at}: {error}"));
            }
            Check::Unchecked(why) if self.options.check_lambdas => self.notes.push(format!("Lambda unchecked at {at}: {why}")),
            Check::Unchecked(_) => {}
        }
        if self.options.lambda_source { return source_lambda(lambda); }
        printed.text
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

fn source_lambda(lambda: &Lambda) -> String { format!("lambda {{ {} }}", lambda.src) }

fn linear(op: &LinearOp) -> String {
    match op {
        LinearOp::Project(l) => format!("map({})", source_lambda(l)),
        LinearOp::Filter(l) => format!("filter({})", source_lambda(l)),
        LinearOp::Negate => "negate".into(),
        LinearOp::EnterAt(l) => format!("enter_at({})", source_lambda(l)),
        LinearOp::LiftIter => "lift_iter".into(),
        LinearOp::FlatMap(l) => format!("flatmap({})", source_lambda(l)),
        LinearOp::Lift => "lift".into(),
        LinearOp::JoinFunction(l) => format!("join_function({})", source_lambda(l)),
    }
}

fn reducer(reducer: &Reducer) -> String {
    match reducer {
        Reducer::Expr(l) => format!("reduce(input -> {})", l.src),
    }
}

fn node(node: &Node, names: &Names) -> String {
    match node {
        Node::Linear { input, ops } => format!("{} | linear [ {} ]", names.of(input), ops.iter().map(linear).collect::<Vec<_>>().join(" | ")),
        Node::Concat(refs) => format!("concat({})", refs.iter().map(|r| names.of(r)).collect::<Vec<_>>().join(", ")),
        Node::Arrange(input) => format!("{} | arrange", names.of(input)),
        Node::Join { left, right, projection } => format!("{} | join({}, {})", names.of(left), names.of(right), source_lambda(projection)),
        Node::HalfJoin { stream, arrangement, strict, projection } => format!("{} | half_join({}, strict={strict}, {})", names.of(stream), names.of(arrangement), source_lambda(projection)),
        Node::Reduce { input, reducer: r } => format!("{} | {}", names.of(input), reducer(r)),
        Node::Inspect { input, label } => format!("{} | inspect({})", names.of(input), quote(label)),
    }
}

// Slot inputs mirror shapes::linear_shapes and the join/reduce typing rules. In particular a
// half join's lambda sees neither the lifted time nor the arrangement's time.
fn printed_node(node: &Node, names: &Names, shapes: &ScopeShapes, depth: usize, at: &str, slots: &mut Slots) -> String {
    match node {
        Node::Linear { input, ops } => {
            let mut input = shapes.of(input).map(|s| (s.key, s.val));
            let mut printed = Vec::new();
            for (i, op) in ops.iter().enumerate() {
                let shape = input.as_ref().map(|(k, v)| Shape::Prod(vec![k.clone(), v.clone()]));
                let name = match op {
                    LinearOp::Project(_) => "map", LinearOp::Filter(_) => "filter", LinearOp::EnterAt(_) => "enter_at",
                    LinearOp::FlatMap(_) => "flatmap", LinearOp::JoinFunction(_) => "join_function",
                    LinearOp::Negate => "negate", LinearOp::Lift => "lift", LinearOp::LiftIter => "lift_iter",
                };
                printed.push(match op {
                    LinearOp::Project(l) | LinearOp::Filter(l) | LinearOp::EnterAt(l) | LinearOp::FlatMap(l) | LinearOp::JoinFunction(l) =>
                        format!("{name}({})", slots.lambda(l, shape, &format!("{at}/{name}[{i}]"))),
                    _ => name.into(),
                });
                input = input.and_then(|(k, v)| shapes::linear_shapes(std::slice::from_ref(op), k, v, depth).ok()?.pop());
            }
            let Node::Linear { input, .. } = node else { unreachable!() };
            format!("{} | linear [ {} ]", names.of(input), printed.join(" | "))
        }
        Node::Join { left, right, projection } => {
            let input = shapes.of(left).zip(shapes.of(right)).map(|(l, r)| Shape::Prod(vec![l.key, l.val, r.val]));
            format!("{} | join({}, {})", names.of(left), names.of(right), slots.lambda(projection, input, &format!("{at}/join")))
        }
        Node::HalfJoin { stream, arrangement, strict, projection } => {
            let input = shapes.of(stream).zip(shapes.of(arrangement)).and_then(|(s, a)| {
                let Shape::Prod(fs) = s.val else { return None };
                (fs.len() == 2).then(|| Shape::Prod(vec![s.key, fs[0].clone(), a.val]))
            });
            format!("{} | half_join({}, strict={strict}, {})", names.of(stream), names.of(arrangement), slots.lambda(projection, input, &format!("{at}/half_join")))
        }
        Node::Reduce { input, reducer: Reducer::Expr(l) } => {
            let input_shape = shapes.of(input).map(|s| Shape::Prod(vec![s.key, Shape::List(Box::new(Shape::Prod(vec![s.val, Shape::Int])))]));
            format!("{} | reduce({})", names.of(input), slots.lambda(l, input_shape, &format!("{at}/reduce")))
        }
        _ => self::node(node, names)
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
fn item_form(node: &Node) -> Form {
    match node {
        Node::Arrange(_) => Form::Arrangement,
        Node::Reduce { .. } => Form::Arrangement,
        _ => Form::Rows,
    }
}

/// Input reads, separating row streams from index reads (including arranging/reusing a value).
fn reads(node: &Node) -> Vec<(&Ref, &'static str)> {
    match node {
        Node::Linear { input, .. } | Node::Inspect { input, .. } => vec![(input, "rows")],
        Node::Concat(refs) => refs.iter().map(|r| (r, "rows")).collect(),
        Node::Arrange(input) => vec![(input, "arrange/reuse")],
        Node::Join { left, right, .. } => vec![(left, "index"), (right, "index")],
        Node::HalfJoin { stream, arrangement, .. } => vec![(stream, "rows"), (arrangement, "index")],
        Node::Reduce { input, .. } => vec![(input, "index")],
    }
}

fn print_scope(scope: &Scope, shapes: &ScopeShapes, parent: Option<(&Names, &[Form])>, unresolved: &BTreeSet<String>, depth: usize, path: &str, slots: &mut Slots, out: &mut String) {
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
            Item::Op(node) => for (r, mode) in reads(node) {
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
                print_scope(child, child_shapes, Some((&names, &entering)), unresolved, depth + 1, &format!("{path}/{}", child.name), slots, out);
                forms.push(Form::Rows);
            }
            Item::Op(op) => {
                let repr = item_form(op);
                writeln!(out, "{indent}  let {} [statement {}] = {}", names.items[i], quote(&label(scope, i)), printed_node(op, &names, shapes, depth, &format!("{path}/{}", names.items[i]), slots)).unwrap();
                writeln!(out, "{indent}    : {}; output {}", shape(shapes.of(&Ref::Local(i))), repr.text()).unwrap();
                for (r, mode) in reads(op) {
                    let supplied = form(r, &imports, &forms);
                    let conversion = match (mode, supplied) {
                        ("rows", Form::Arrangement) => "flatten arrangement to rows",
                        ("index" | "arrange/reuse", Form::Rows) => "build arrangement",
                        ("index" | "arrange/reuse", Form::Arrangement) => "reuse arrangement",
                        _ => supplied.text(),
                    };
                    writeln!(out, "{indent}    reads {} as {mode}: {conversion}", names.of(r)).unwrap();
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
                        Node::Join { projection, .. } | Node::HalfJoin { projection, .. } => entry.lambdas.push(source_lambda(projection)),
                        Node::Reduce { reducer: Reducer::Expr(l), .. } => entry.lambdas.push(source_lambda(l)),
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
        let provider = report(r#"export "rows" = input 0 : (int, int);"#, &|_| None, &Config::all()).unwrap();
        let text = r#"
            let rows = import "rows" : (int, int);
            export "out" = rows | lift | half_join(rows, 1, (k, v, w) -> (k, v));
        "#;
        let missing = report(text, &|_| None, &Config::all()).unwrap();
        assert_eq!(missing.unresolved, ["rows"]);
        assert!(missing.exports.is_empty());
        let plan = report(text, &|name| provider.exports.get(name).cloned(), &Config::all()).unwrap();
        assert!(plan.unresolved.is_empty());
        assert!(plan.stages[3].changes.iter().any(|s| s.contains("inserted 1 filters")));
        assert!(plan.print_with(PrintOptions { lambda_source: true, ..PrintOptions::default() }).text.contains("(t0, 1) ne"));
        assert_eq!(plan.print(), report(text, &|name| provider.exports.get(name).cloned(), &Config::all()).unwrap().print());
        assert!(report(text, &|_| Some((Shape::Unit, Shape::Unit)), &Config::all()).is_err());
    }
}
