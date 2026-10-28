//! Whole-program views in the existing DDP language. Renderer choices remain comments.

use std::collections::{BTreeSet, BTreeMap, HashMap};
use std::fmt::Write;

use corgi::Shape;
use crate::ir::{Lambda, LinearOp, Reducer};
use crate::scope_ir::{Item, Node, Ref, Scope, Source};
use crate::shapes::{self, ItemShapes, ScopeShapes};
use super::{Form, Report};
use crate::parse::{Expr, Stmt};

mod parsed;

pub(crate) struct Context {
    types: Vec<(String, Vec<(String, Shape)>)>,
    scope: corgi::Scope,
    parsed: Option<Vec<Stmt>>,
}

impl Context {
    pub(super) fn for_program() -> Self {
        let mut context = Self::new("");
        context.parsed = None;
        context
    }

    pub(super) fn new(text: &str) -> Self {
        let (types, scope, _) = crate::parse::pipe::print_context(text);
        Self { types, scope, parsed: Some(crate::parse::pipe::parse(text)) }
    }
}

/// The ordered whole-program views. Parsed syntax precedes the four IR stages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DdpStage { Parsed, Lowered, Optimized, Projected, AsOf }

impl DdpStage {
    pub const ALL: [Self; 5] = [Self::Parsed, Self::Lowered, Self::Optimized, Self::Projected, Self::AsOf];

    pub fn name(self) -> &'static str {
        match self {
            Self::Parsed => "parsed", Self::Lowered => "lowered", Self::Optimized => "optimized",
            Self::Projected => "projected", Self::AsOf => "as-of",
        }
    }

    fn ir_index(self) -> Option<usize> {
        match self {
            Self::Parsed => None, Self::Lowered => Some(0), Self::Optimized => Some(1),
            Self::Projected => Some(2), Self::AsOf => Some(3),
        }
    }
}

/// Counts are scalar slots in one emitted view. Source requires exact compiled-graph equality.
#[derive(Debug, Default, Clone, Copy)]
pub struct LambdaCounts {
    pub source: usize,
    pub graph: usize,
    /// Graph spellings that recompile to a different graph. No sampling is used here.
    pub graph_changed: usize,
}

pub struct Emitted {
    pub text: String,
    pub lambdas: LambdaCounts,
}

impl Report {
    /// Write the final stage as DDP. Refuses incomplete plans and unavailable scalar spellings.
    /// Installation-time choices are comments; this is not an executable-plan serialization.
    pub fn emit_ddp(&self) -> Result<Emitted, String> {
        self.emit_ddp_at(DdpStage::AsOf)
    }

    /// Emit all five whole programs, in preparation order.
    pub fn emit_ddp_stages(&self) -> Result<Vec<(DdpStage, Emitted)>, String> {
        DdpStage::ALL.into_iter().map(|stage| Ok((stage, self.emit_ddp_at(stage)?))).collect()
    }

    /// Emit one whole program. The parsed view walks the AST; the others walk the actual IR.
    pub fn emit_ddp_at(&self, view: DdpStage) -> Result<Emitted, String> {
        let mut printer = Printer { context: &self.context, types: self.context.types.clone(), counts: LambdaCounts::default() };
        let mut body = String::new();
        if let Some(index) = view.ir_index() {
            let stage = self.stages.get(index).ok_or("missing preparation stage")?;
            if !self.unresolved.is_empty() || !stage.problems.is_empty() {
                return Err(format!("incomplete plan: unresolved imports {:?}; problems {:?}", self.unresolved, stage.problems));
            }
            printer.scope(&stage.program.root, &stage.shapes, None, 0, &mut body)?;
        } else {
            printer.statements(self.context.parsed.as_deref().ok_or("parsed syntax unavailable for a program supplied as IR")?, 0, &mut body)?;
        }
        let mut text = String::new();
        for i in 0..printer.types.len() { printer.declare_type(i, &mut text)?; }
        text.push_str(&body);
        Ok(Emitted { text, lambdas: printer.counts })
    }
}

struct Printer<'a> {
    context: &'a Context,
    types: Vec<(String, Vec<(String, Shape)>)>,
    counts: LambdaCounts,
}

fn identifier(text: &str) -> String {
    let mut name: String = text.trim_start_matches("export ").chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect();
    if name.is_empty() { name.push('x'); }
    if name.as_bytes()[0].is_ascii_digit() || crate::ir::KEYWORDS.contains(&name.as_str()) { name.insert_str(0, "x_"); }
    name
}

fn fresh(base: &str, used: &mut BTreeSet<String>, next: &mut BTreeMap<String, usize>) -> String {
    let base = identifier(base);
    let mut name = base.clone();
    let n = next.entry(base.clone()).or_insert(2);
    while !used.insert(name.clone()) { name = format!("{base}{n}"); *n += 1; }
    name
}

// IR producers can use export names such as "demand:input0". Nested exports
// need local DDP bindings; only root trace names can remain quoted strings.
fn export_names(scope: &Scope) -> Vec<String> {
    let mut used: BTreeSet<_> = scope.names.iter().map(|(name, _)| name.clone())
        .chain(scope.vars.iter().map(|v| v.name.clone()))
        .chain(scope.imports.iter().map(|i| i.name.clone()))
        .chain(scope.items.iter().filter_map(|i| if let Item::Sub(s) = i { Some(s.name.clone()) } else { None }))
        .chain(scope.exports.iter().filter(|e| identifier(&e.name) == e.name).map(|e| e.name.clone()))
        .collect();
    let mut next = BTreeMap::new();
    scope.exports.iter().map(|e| {
        if e.name.contains("::") || identifier(&e.name) == e.name { e.name.clone() }
        else { fresh(&e.name, &mut used, &mut next) }
    }).collect()
}

struct Names {
    items: Vec<String>,
    imports: Vec<String>,
    vars: Vec<String>,
    exports: Vec<Vec<String>>,
    aliases: HashMap<Ref, String>,
    root: bool,
}

impl Names {
    fn new(scope: &Scope, parent: Option<(&Scope, &Names)>) -> Self {
        let mut used: BTreeSet<_> = scope.names.iter().map(|(n, _)| n.clone())
            .chain(scope.vars.iter().map(|v| v.name.clone()))
            .chain(scope.items.iter().filter_map(|i| if let Item::Sub(s) = i { Some(s.name.clone()) } else { None }))
            .chain(scope.imports.iter().filter(|i| parent.is_some() && !i.name.contains("::")).map(|i| identifier(&i.name))).collect();
        used.extend(export_names(scope));
        let mut next = BTreeMap::new();
        let mut aliases = HashMap::new();
        for (name, r) in &scope.names { aliases.entry(r.clone()).or_insert_with(|| name.clone()); }
        let authored = |r: &Ref| aliases.get(r).cloned();
        let imports: Vec<String> = scope.imports.iter().enumerate().map(|(i, import)| match &import.from {
            Source::Parent(r) => {
                let (scope, names) = parent.expect("nested scope has a parent");
                let name = names.of(r);
                // An enclosing scope is an anchor, not a path to descend through.
                if name.contains("::") { name }
                else if names.root { format!("::{name}") }
                else { format!("{}::{name}", scope.name) }
            }
            _ => authored(&Ref::Import(i)).unwrap_or_else(|| fresh(&import.name, &mut used, &mut next)),
        }).collect();
        let items = scope.items.iter().enumerate().map(|(i, item)| {
            if let Item::Sub(child) = item { return child.name.clone(); }
            if let Some(name) = authored(&Ref::Local(i)) { return name; }
            if let Item::Op(Node::Arrange(input)) = item {
                let base = authored(input).or_else(|| match input {
                    Ref::Import(k) => imports[*k].rsplit("::").next().map(str::to_string),
                    Ref::Var(k) => Some(scope.vars[*k].name.clone()),
                    _ => None,
                });
                if let Some(base) = base { return fresh(&format!("{base}_idx"), &mut used, &mut next); }
            }
            let role = match item {
                Item::Op(Node::Arrange(_)) => "idx",
                Item::Op(Node::Linear { ops, .. }) => match ops.as_slice() {
                    [LinearOp::Project(_)] => "map", [LinearOp::Filter(_)] => "filter", [LinearOp::Negate] => "neg", _ => "rows",
                },
                Item::Op(Node::Join { .. } | Node::HalfJoin { .. }) => "join",
                Item::Op(Node::Reduce { .. }) => "reduce",
                Item::Op(Node::Concat(_)) => "rows",
                Item::Op(Node::Inspect { .. }) => "inspect",
                Item::Sub(_) => unreachable!(),
            };
            fresh(&format!("{}_{role}", scope.labels.get(i).map(String::as_str).unwrap_or("item")), &mut used, &mut next)
        }).collect();
        let exports = scope.items.iter().map(|item| match item {
            Item::Sub(child) => export_names(child).into_iter().map(|name| {
                if name.contains("::") { name } else { format!("{}::{name}", child.name) }
            }).collect(),
            _ => Vec::new(),
        }).collect();
        Self { items, imports, vars: scope.vars.iter().map(|v| v.name.clone()).collect(), exports, aliases, root: parent.is_none() }
    }

    fn of(&self, r: &Ref) -> String {
        if matches!(r, Ref::ChildExport(_, _)) {
            if let Some(name) = self.aliases.get(r) { return name.clone(); }
        }
        self.raw(r)
    }

    fn raw(&self, r: &Ref) -> String {
        match r {
            Ref::Local(i) => self.items[*i].clone(), Ref::Import(i) => self.imports[*i].clone(),
            Ref::Var(i) => self.vars[*i].clone(), Ref::ChildExport(i, e) => self.exports[*i][*e].clone(),
        }
    }
}

impl Printer<'_> {
    /// Declare type `i`, after any sum types first named while spelling it, so a second
    /// print of the emitted program declares them in the same order.
    fn declare_type(&mut self, i: usize, text: &mut String) -> Result<(), String> {
        let named = self.types.len();
        let (name, variants) = self.types[i].clone();
        let variants = variants.iter().map(|(v, s)| Ok(if *s == Shape::Unit { v.clone() } else { format!("{v} {}", self.shape(s)?) })).collect::<Result<Vec<_>, String>>()?;
        for j in named..self.types.len() { self.declare_type(j, text)?; }
        writeln!(text, "type {name} = {};", variants.join(" | ")).unwrap();
        Ok(())
    }

    fn shape(&mut self, shape: &Shape) -> Result<String, String> {
        Ok(match shape {
            Shape::Unit => "()".into(), Shape::Int => "int".into(), Shape::Float => "float".into(),
            Shape::Prod(fields) => format!("({})", fields.iter().map(|s| self.shape(s)).collect::<Result<Vec<_>, _>>()?.join(", ")),
            Shape::List(s) => format!("List({})", self.shape(s)?),
            Shape::Sum(lanes) => {
                if let Some((name, _)) = self.types.iter().find(|(_, v)| v.iter().map(|(_, s)| s).eq(lanes)) { return Ok(name.clone()); }
                let mut n = self.types.len();
                while self.types.iter().any(|(name, _)| *name == format!("Sum{n}")) { n += 1; }
                let name = format!("Sum{n}");
                self.types.push((name.clone(), lanes.iter().enumerate().map(|(i, s)| (format!("V{i}"), s.clone())).collect()));
                name
            }
            Shape::Ref(_) => return Err("reference shapes have no DDP spelling".into()),
        })
    }

    fn row(&mut self, row: &(Shape, Shape)) -> Result<String, String> { self.shape(&Shape::Prod(vec![row.0.clone(), row.1.clone()])) }

    fn lambda(&mut self, lambda: &Lambda, input: Option<Shape>) -> Result<String, String> {
        let input = input.ok_or("lambda input shape is unknown")?;
        let graph = lambda.graph()?;
        corgi::shape_of(graph, &input)?;
        let compiled = corgi::optimize(graph);
        let src = qualify(&lambda.src, &self.context.types);
        if let Some(parsed) = corgi::parse_ml_in(&src, &self.context.scope).ok() {
            if corgi::shape_of(&parsed, &input).is_ok() && corgi::optimize(&parsed) == compiled {
                self.counts.source += 1;
                return Ok(slot_text(&src));
            }
        }
        let printed = crate::lambda::print::print_named(lambda, Some(&input), false, &self.context.types);
        if printed.fallback { return Err(printed.text); }
        self.counts.graph += 1;
        let (pat, body) = printed.text.split_once(" -> ").ok_or("graph printer did not produce a slot")?;
        let parsed = corgi::parse_ml_in(&format!("let {pat} = input in {body}"), &self.context.scope)?;
        if corgi::optimize(&parsed) != compiled { self.counts.graph_changed += 1; }
        Ok(printed.text)
    }

    fn scope(&mut self, scope: &Scope, shapes: &ScopeShapes, parent: Option<(&Scope, &Names, &[Form], &[Form])>, depth: usize, out: &mut String) -> Result<(), String> {
        let names = Names::new(scope, parent.map(|(s, n, _, _)| (s, n)));
        let indent = "    ".repeat(depth);
        writeln!(out, "{indent}-- Time lanes: epoch{}.", if depth == 0 { String::new() } else { format!(", then {depth} scope coordinates; this scope enters at zero") }).unwrap();
        let imports: Vec<_> = scope.imports.iter().map(|imp| match &imp.from {
            Source::Trace(_) => Form::Arrangement,
            Source::Input(_) => Form::Rows,
            Source::Parent(r) => parent.map(|(_, _, imports, forms)| super::form(r, imports, forms)).unwrap_or(Form::Unknown),
        }).collect();
        if depth == 0 && scope.imports.iter().any(|i| matches!(i.from, Source::Trace(_))) {
            writeln!(out, "-- Trace imports use provider arrangements; history advances to G = (1, []), placeholder install epoch 2.").unwrap();
        }
        let mut defined = BTreeSet::new();
        for (i, imp) in scope.imports.iter().enumerate() {
            let name = &names.imports[i];
            match &imp.from {
                Source::Input(_) | Source::Trace(_) => {
                    let source = match &imp.from {
                        Source::Input(n) => format!("input {n}"),
                        Source::Trace(trace) => {
                            format!("import {}", quoted(trace)?)
                        }
                        _ => unreachable!(),
                    };
                    let shape = imp.shape.clone().or_else(|| shapes.imports[i].as_ref().map(|s| (s.key.clone(), s.val.clone()))).ok_or("source shape unknown")?;
                    writeln!(out, "{indent}let {name} = {source} : {};", self.row(&shape)?).unwrap();
                    defined.insert(name.clone());
                }
                Source::Parent(_) => {},
            }
        }
        for var in &scope.vars {
            let shape = var.shape.as_ref().map(|s| self.row(s).map(|s| format!(" : {s}"))).transpose()?.unwrap_or_default();
            writeln!(out, "{indent}var {}{shape};", var.name).unwrap();
            defined.insert(var.name.clone());
        }
        let local_exports = export_names(scope);
        // Schedule aliases at their defining item, preserving authored-then-export order.
        let mut aliases_at = vec![Vec::new(); scope.items.len() + 1];
        let exports = scope.exports.iter().zip(&local_exports)
            .filter(|(_, name)| !names.root && !name.contains("::"))
            .map(|(export, name)| (name, &export.value));
        for (name, r) in scope.names.iter().map(|(n, r)| (n, r)).chain(exports) {
            let at = match r { Ref::Local(i) | Ref::ChildExport(i, _) => i + 1, _ => 0 };
            aliases_at[at].push((name, r));
        }
        aliases(&aliases_at[0], &names, &mut defined, &indent, out);
        let mut forms = Vec::new();
        for (i, item) in scope.items.iter().enumerate() {
            match item {
                Item::Sub(child) => {
                    let ItemShapes::Sub(child_shapes) = &shapes.items[i] else { unreachable!() };
                    writeln!(out, "{indent}{}: {{", child.name).unwrap();
                    self.scope(child, child_shapes, Some((scope, &names, &imports, &forms)), depth + 1, out)?;
                    writeln!(out, "{indent}}}").unwrap();
                    forms.push(Form::Rows);
                }
                Item::Op(node) => {
                    let name = names.items[i].clone();
                    self.node(node, &name, &names, shapes, depth, &indent, out)?;
                    defined.insert(name);
                    forms.push(super::item_form(node));
                }
            }
            aliases(&aliases_at[i + 1], &names, &mut defined, &indent, out);
        }
        for bind in &scope.binds {
            writeln!(out, "{indent}set {} = {};", names.vars[bind.var], names.of(&bind.value)).unwrap();
        }
        for (export, local) in scope.exports.iter().zip(local_exports) {
            let value = names.of(&export.value);
            if depth == 0 {
                let storage = match super::form(&export.value, &imports, &forms) {
                    Form::Arrangement => "publish the existing arrangement", _ => "build an export arrangement",
                };
                writeln!(out, "{indent}-- Renderer: {storage}.").unwrap();
                let shape = export.shape.as_ref().map(|s| self.row(s).map(|s| format!(" : {s}"))).transpose()?.unwrap_or_default();
                writeln!(out, "{indent}export {}{shape} = {value};", quoted(&export.name)?).unwrap();
            } else if !local.contains("::") && !defined.contains(&local) {
                writeln!(out, "{indent}let {local} = {value};").unwrap();
                defined.insert(local);
            }
        }
        Ok(())
    }

    fn node(&mut self, node: &Node, name: &str, names: &Names, shapes: &ScopeShapes, depth: usize, indent: &str, out: &mut String) -> Result<(), String> {
        let expr = match node {
            Node::Linear { input, ops } => {
                let mut row = shapes.of(input).map(|s| (s.key, s.val));
                let mut expr = names.of(input);
                for op in ops {
                    let shape = row.as_ref().map(|(k, v)| Shape::Prod(vec![k.clone(), v.clone()]));
                    let (word, lambda) = match op {
                        LinearOp::Project(l) => ("map", Some(l)), LinearOp::Filter(l) => ("filter", Some(l)),
                        LinearOp::EnterAt(l) => ("enter_at", Some(l)), LinearOp::FlatMap(l) => ("flatmap", Some(l)),
                        LinearOp::JoinFunction(l) => ("join_function", Some(l)), LinearOp::Negate => ("negate", None),
                        LinearOp::Lift => ("lift", None), LinearOp::LiftIter => ("lift_iter", None),
                    };
                    expr += &format!(" | {word}");
                    if let Some(l) = lambda { expr += &format!("({})", self.lambda(l, shape)?); }
                    row = row.and_then(|(k, v)| shapes::linear_shapes(std::slice::from_ref(op), k, v, depth).ok()?.pop());
                }
                expr
            }
            Node::Concat(refs) => refs.iter().map(|r| names.of(r)).collect::<Vec<_>>().join(" + "),
            Node::Arrange(r) => format!("{} | arrange", names.of(r)),
            Node::Join { left, right, projection } => {
                let input = shapes.of(left).zip(shapes.of(right)).map(|(l, r)| Shape::Prod(vec![l.key, l.val, r.val]));
                format!("{} | join({}, {})", names.of(left), names.of(right), self.lambda(projection, input)?)
            }
            Node::HalfJoin { stream, arrangement, strict, projection } => {
                let input = shapes.of(stream).zip(shapes.of(arrangement)).and_then(|(s, a)| {
                    let Shape::Prod(fs) = s.val else { return None };
                    Some(Shape::Prod(vec![s.key, fs[0].clone(), a.val]))
                });
                format!("{} | half_join({}, {}, {})", names.of(stream), names.of(arrangement), u8::from(*strict), self.lambda(projection, input)?)
            }
            Node::Reduce { input, reducer } => {
                let word = match reducer {
                    Reducer::Expr(l) => {
                        let input = shapes.of(input).map(|s| Shape::Prod(vec![s.key, Shape::List(Box::new(Shape::Prod(vec![s.val, Shape::Int])))]));
                        format!("reduce({})", self.lambda(l, input)?)
                    }
                };
                format!("{} | {word}", names.of(input))
            }
            Node::Inspect { input, label } => {
                if identifier(label) != *label { return Err(format!("inspect label {label:?} is not a DDP identifier")); }
                format!("{} | inspect({label})", names.of(input))
            }
        };
        writeln!(out, "{indent}let {name} = {expr};").unwrap();
        Ok(())
    }
}

fn aliases(aliases: &[(&String, &Ref)], names: &Names, defined: &mut BTreeSet<String>, indent: &str, out: &mut String) {
    for &(name, r) in aliases {
        if defined.insert(name.clone()) { writeln!(out, "{indent}let {name} = {};", names.raw(r)).unwrap(); }
    }
}

fn quoted(text: &str) -> Result<String, String> {
    if text.contains('"') { return Err("DDP quoted names cannot contain a quote".into()); }
    Ok(format!("\"{text}\""))
}

fn slot_text(src: &str) -> String {
    if let Some((pat, body)) = src.strip_prefix("let ").and_then(|s| s.split_once(" = input in ")) {
        format!("{pat} -> {body}")
    } else { format!("input -> {src}") }
}

/// Qualify unique declared variants, without touching strings, comments, or local enum names.
fn qualify(src: &str, types: &[(String, Vec<(String, Shape)>)]) -> String {
    let mut out = String::new();
    let mut chars = src.char_indices().peekable();
    let mut inject = false;
    while let Some((start, c)) = chars.next() {
        if c == '"' || c == '#' {
            out.push(c);
            while let Some((_, next)) = chars.next() {
                out.push(next);
                if c == '"' && next == '\\' { if let Some((_, escaped)) = chars.next() { out.push(escaped); } }
                else if (c == '"' && next == '"') || (c == '#' && next == '\n') { break; }
            }
        } else if c.is_ascii_alphabetic() || c == '_' {
            let mut end = start + c.len_utf8();
            while let Some(&(i, next)) = chars.peek() {
                if !(next.is_ascii_alphanumeric() || next == '_' || next == ':') { break; }
                chars.next(); end = i + next.len_utf8();
            }
            let word = &src[start..end];
            let matches: Vec<_> = types.iter().filter(|(_, vs)| vs.iter().any(|(v, _)| v == word)).collect();
            if inject && matches.len() == 1 && !src.contains("enum ") { write!(out, "{}::{word}", matches[0].0).unwrap(); }
            else { out.push_str(word); }
            inject = word == "inject";
        } else { out.push(c); if !c.is_whitespace() { inject = false; } }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(text: &str) -> Report { super::super::report(text, &crate::server::generated_shape, &crate::passes::Config::all()).unwrap() }

    fn roundtrip(text: &str) -> String {
        let before = plan(text);
        let output = before.emit_ddp().unwrap().text;
        let after = plan(&output);
        assert!(before.compare_final(&after).is_empty(), "{:?}\n{output}", before.compare_final(&after));
        assert_eq!(output, after.emit_ddp().unwrap().text);
        output
    }

    #[test]
    fn source_names_types_aliases_and_scope_paths() {
        roundtrip(r#"
            let source = input 0 : (int, ());
            outer: {
                first: { let result = source; }
                second: { let result = outer::first::result; }
                let result = second::result;
            }
            let answer = outer::result;
            let alias = answer;
            export "out" = alias;
        "#);
    }

    #[test]
    fn fused_names_and_generated_names_do_not_collide() {
        let output = roundtrip(r#"
            let x = input 0 : (int, int);
            let result_idx = x | map((k, v) -> (v, k));
            let intermediate = x | filter((k, _) -> (k, 0) gt);
            let result = intermediate | map((k, v) -> (k, v)) | join(result_idx, (k, x, y) -> (k, (x, y)));
            export "out" = result;
        "#);
        assert!(output.contains("let result_idx ="));
        assert!(output.contains("| filter((k, _) -> (k, 0) gt)"));
        assert!(!output.contains("map((k, v) -> (k, v))"));
    }

    #[test]
    fn comparison_checks_edges_sharing_graphs_and_export_names() {
        let source = r#"let a = input 0 : (int, int); let b = input 1 : (int, int);
            let x = a | map((k, v) -> (k, (v, 1) add)); export "out" = x + b;"#;
        let a = plan(source);
        let renamed = plan(&source.replace("let a =", "let renamed =").replace("= a |", "= renamed |"));
        assert!(a.compare_final(&renamed).is_empty());
        for different in [source.replace("(v, 1) add", "(v, 2) add"), source.replace("x + b", "b + x"), source.replace("\"out\"", "\"other\"")] {
            assert!(!a.compare_final(&plan(&different)).is_empty());
        }
    }

    #[test]
    fn unnamed_sum_imports_get_valid_declarations() {
        let source = |_: &str| Some((Shape::Sum(vec![Shape::Int, Shape::Unit]), Shape::Unit));
        let before = super::super::report("export \"out\" = import \"sum\";", &source, &crate::passes::Config::all()).unwrap();
        let text = before.emit_ddp().unwrap().text;
        assert!(text.contains("type Sum0 = V0 int | V1;"));
        let after = super::super::report(&text, &source, &crate::passes::Config::all()).unwrap();
        assert!(before.compare_final(&after).is_empty());
        assert_eq!(text, after.emit_ddp().unwrap().text);
    }

    #[test]
    fn incomplete_plans_are_not_emitted_as_executable_ddp() {
        assert!(plan(r#"export "out" = import "missing" : (int, ());"#).emit_ddp().is_err());
    }

    #[test]
    fn parsed_view_preserves_grouping_paths_and_all_statements() {
        let source = r#"
            let a = input 0 : (int, int);
            let b = input 1 : (int, int);
            let unused = input 2 : (int, int);
            empty: {}
            nested: { let both = (a + b) + (a | negate); }
            let x = nested::both + (b | map((k, v) -> (v, k)));
            export "all" = x | map((k, v) -> (v, k));
            export "other" = a | value((_, v) -> (v, 1) add);
        "#;
        let before = plan(source);
        let parsed = before.emit_ddp_at(DdpStage::Parsed).unwrap();
        let after = plan(&parsed.text);
        assert!(before.compare_final(&after).is_empty(), "{:?}", before.compare_final(&after));
        assert_eq!(parsed.text, after.emit_ddp_at(DdpStage::Parsed).unwrap().text);
        assert!(parsed.text.contains("input 2 : (int, int)"));
        assert!(parsed.text.contains("empty: {\n}"));
        assert!(!parsed.text.contains(" | arrange"));
    }

    #[test]
    fn graph_wins_over_stale_source_and_uses_declared_sums() {
        let report = plan("type Member = User int | Group int; let x = input 0 : (int, Member); export \"out\" = x | map((k, v) -> (k inject Group, v));");
        let mut printer = Printer { context: &report.context, types: report.context.types.clone(), counts: LambdaCounts::default() };
        let mut lambda = Lambda::new("let (k, v) = input in (k inject Group, v)".into(), &report.context.scope);
        lambda.src = "let (k, v) = input in (k, v)".into();
        let text = printer.lambda(&lambda, Some(Shape::Prod(vec![Shape::Int, Shape::Sum(vec![Shape::Int, Shape::Int])]))).unwrap();
        assert!(text.contains("inject Member::Group"), "{text}");
    }

    #[test]
    fn composed_reductions_round_trip_and_repeated_as_of_filters_remain_visible() {
        let before = plan(include_str!("../../bench/txn/maintained.ddp"));
        let output = before.emit_ddp().unwrap();
        assert!(output.text.contains("let best = best_idx | reduce("));
        assert!(!output.text.contains("Added by"));
        assert_eq!(output.lambdas.graph_changed, 0);
        assert!(before.compare_final(&plan(&output.text)).is_empty());
        assert_eq!(output.text, plan(&output.text).emit_ddp().unwrap().text);
        let before = plan(r#"let a = input 0 : (int, ()); let b = input 1 : (int, ());
            export "out" = a | lift | half_join(b, 1, (k, x, _) -> (k, x));"#);
        let output = before.emit_ddp().unwrap().text;
        assert!(output.contains("| lift | filter("), "{output}");
        assert!(!output.contains("Added by"));
        assert!(output.contains("half_join("));
        assert!(!before.compare_final(&plan(&output)).is_empty(), "as-of currently inserts another filter on replanning");
    }
    #[test]
    fn hoisted_names_placement_and_aliases_are_stable() {
        let text = r#"let rows = input 0 : (int, int);
            let reads = rows | filter((k, _) -> (k, 0) ge);
            let occupied = rows | negate;
            let tail = rows | filter((k, _) -> (k, 4) ge);
            child: {
                let rk = ::reads | map((k, v) -> (v, k));
                let occupied = ::reads | map((k, v) -> (k, (v, 1) add));
                let both = rk + occupied;
            }
            export "out" = child::both;
            export "tail" = tail;
            export "occupied" = occupied;"#;
        let output = roundtrip(text);
        assert!(output.contains("let rk = reads | map("), "{output}");
        assert!(output.contains("let reads_map = reads | map("), "{output}");
        assert!(output.find("let rk =").unwrap() < output.find("let occupied =").unwrap(), "{output}");
        let child = output.split("child: {").nth(1).unwrap().split('}').next().unwrap();
        assert!(!child.contains("let rk") && !child.contains("let occupied"), "{output}");
    }


    #[test]
    fn hoisted_joins_follow_the_latest_input() {
        let text = r#"let rows = input 0 : (int, int);
            let early = rows | map((k, v) -> (v, k));
            let late = rows | filter((k, v) -> (k, v) lt);
            let between = rows | map((k, v) -> (k, v neg));
            child: { let pair = ::early | join(::late, (k, a, b) -> (k, (a, b))); }
            export "out" = child::pair;
            export "between" = between;"#;
        let printed = roundtrip(text);
        assert!(printed.find("let late =").unwrap() < printed.find("let pair =").unwrap(), "{printed}");
        assert!(printed.find("let pair =").unwrap() < printed.find("let between =").unwrap(), "{printed}");
        assert!(printed.contains("let pair = ::pair;"), "{printed}");
    }

}
