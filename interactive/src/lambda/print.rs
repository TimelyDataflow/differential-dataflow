//! Print the typed graph that the slot compiler runs. Source text is never an expression input.
//!
//! Patterns check shapes but carry no payload. After checking, we can omit them and name the
//! input fields the live graph reads. Immediate kernels expand to their ordinary binary forms.
//! A check reparses that text at the same shape, then compares compiled graphs or sampled rows.

use std::collections::{BTreeMap, BTreeSet};
use corgi::{ArithOp, BinOp, BitOp, Builder, CmpOp, Graph, NodeRef, NumOp, Op, OpLike, Pred, Red, Scalar, Shape, ShiftOp, TextOp, Value};
use super::{Lambda, normalize::prepare};

type G = Graph<NumOp>;

pub(crate) struct Printed {
    pub text: String,
    pub fallback: bool,
    pub check: Check,
}

pub(crate) enum Check {
    Unchecked(String),
    Exact,
    Sampled,
    Failed(String),
}

/// Read the graph directly. Unknown input shapes prevent compilation and checking.
pub(crate) fn print(lambda: &Lambda, input: Option<&Shape>, check: bool) -> Printed {
    print_named(lambda, input, check, &[])
}

pub(crate) fn print_named(lambda: &Lambda, input: Option<&Shape>, check: bool, types: &[(String, Vec<(String, Shape)>)]) -> Printed {
    print_with(lambda, input, check, types, false)
}

/// Compositions bind the immediate input fields.
pub(crate) fn print_composed(lambda: &Lambda, input: &Shape) -> Printed {
    print_with(lambda, Some(input), false, &[], true)
}

fn print_with(lambda: &Lambda, input: Option<&Shape>, check: bool, types: &[(String, Vec<(String, Shape)>)], shallow: bool) -> Printed {
    let result = (|| {
        let original = lambda.graph()?;
        let Some(input) = input else {
            return Ok(Printed {
                text: format!("graph fallback (input shape unknown) {{\n{}\n}}", graph_text(original)),
                fallback: true, check: Check::Unchecked("input shape unknown".into()),
            });
        };
        corgi::shape_of(original, input)?;
        // Spell the canonical graph: each constant and unit counts rows from the input,
        // so nothing survives only as an anchor the surface syntax cannot write.
        let compiled = corgi::compile(original.as_ref().clone(), input)?.graph().clone();
        let graph = prepare(&compiled, input)?;
        let mut printer = Printer { types: types.to_vec(), shallow, ..Printer::default() };
        match printer.lambda(&graph, input) {
            Ok((pattern, body)) => {
                let declarations = printer.declarations.join(" ");
                let body = format!("{declarations}{body}");
                let text = format!("{pattern} -> {body}");
                let check = if check { check_text(&compiled, input, &text) }
                    else { Check::Unchecked("not requested".into()) };
                Ok(Printed { text, fallback: false, check })
            }
            Err(reason) => Ok(Printed {
                text: format!("graph fallback ({reason}) {{\n{}\n}}", graph_text(&compiled)),
                fallback: true, check: Check::Unchecked("no surface spelling".into()),
            }),
        }
    })();
    result.unwrap_or_else(|error: String| Printed {
        text: format!("graph unavailable ({error})"), fallback: true, check: Check::Failed(error),
    })
}

#[derive(Default)]
struct Printer {
    declarations: Vec<String>,
    types: Vec<(String, Vec<(String, Shape)>)>,
    shallow: bool,
}

impl Printer {
    fn shape(&mut self, shape: &Shape) -> Result<String, String> {
        Ok(match shape {
            Shape::Unit => "()".into(), Shape::Int => "int".into(), Shape::Float => "float".into(),
            Shape::Prod(fs) => tuple(fs.iter().map(|s| self.shape(s)).collect::<Result<Vec<_>, _>>()?),
            Shape::List(elem) => format!("List({})", self.shape(elem)?),
            Shape::Sum(lanes) => self.enum_name(&lanes.iter().cloned().map(Some).collect::<Vec<_>>())?,
            Shape::Ref(_) => return Err("reference shape has no type spelling".into()),
        })
    }

    fn enum_name(&mut self, lanes: &[Option<Shape>]) -> Result<String, String> {
        if let Some((name, _)) = self.types.iter().find(|(_, variants)| {
            variants.len() == lanes.len() && variants.iter().zip(lanes).all(|((_, shape), lane)| lane.as_ref() == Some(shape))
        }) { return Ok(name.clone()); }
        let shapes = lanes.iter().map(|s| s.as_ref().map(|s| self.shape(s)).transpose()).collect::<Result<Vec<_>, _>>()?;
        let mut index = self.declarations.len();
        while self.types.iter().any(|(name, _)| *name == format!("S{index}"))
            || self.declarations.iter().any(|d| d.starts_with(&format!("enum S{index} = "))) { index += 1; }
        let name = format!("S{index}");
        let variants = shapes.iter().enumerate().map(|(i, s)| match s {
            Some(s) => format!("V{i} {s}"), None => format!("V{i}"),
        }).collect::<Vec<_>>().join(" | ");
        self.declarations.push(format!("enum {name} = {variants} in "));
        Ok(name)
    }

    fn lambda(&mut self, graph: &G, input: &Shape) -> Result<(String, String), String> {
        let max_depth = if graph.node_count() > 4096 { 512 } else { 2048 };
        let nodes: Vec<_> = graph.nodes().collect();
        let mut shapes: Vec<Shape> = Vec::new();
        let mut paths: Vec<Option<Vec<usize>>> = Vec::new();
        let mut uses = vec![0usize; nodes.len()];
        let mut read = BTreeSet::new();
        for node in &nodes {
            let (shape, path) = match node {
                NodeRef::Input => (input.clone(), Some(Vec::new())),
                NodeRef::Tuple(fields) => {
                    for &i in *fields { uses[i] += 1; if let Some(p) = &paths[i] { read.insert(p.clone()); } }
                    (Shape::Prod(fields.iter().map(|&i| shapes[i].clone()).collect()), None)
                }
                NodeRef::Op(op, i) => {
                    uses[*i] += 1;
                    let path = if let NumOp::Core(Op::Field(f)) = op {
                        paths[*i].as_ref().map(|p| { let mut p = p.clone(); p.push(*f); p })
                    } else { None };
                    if path.is_none() && !matches!(op, NumOp::Core(Op::Lit(_) | Op::Unit)) {
                        if let Some(p) = &paths[*i] { read.insert(p.clone()); }
                    }
                    (corgi::shape_of_value(&op.eval(Value::empty(&shapes[*i]))?), path)
                }
            };
            shapes.push(shape);
            paths.push(path);
        }
        uses[graph.output()] += 1;
        if let Some(p) = &paths[graph.output()] { read.insert(p.clone()); }
        if self.shallow { read = read.into_iter().map(|p| p.into_iter().take(1).collect()).collect(); }
        let mut names = BTreeMap::new();
        let pattern = pattern(input, &mut Vec::new(), &read, &mut names);
        let mut expressions: Vec<String> = Vec::new();
        let mut depths = Vec::new();
        let mut remaining = uses.clone();
        let mut bindings = String::new();
        let mut next = 0;
        for (index, node) in nodes.iter().enumerate() {
            let mut expression = if let Some(path) = &paths[index] {
                path_name(path, &names).unwrap_or_else(|| "input".into())
            } else {
                match node {
                    NodeRef::Input => unreachable!(),
                    NodeRef::Tuple(fields) => tuple(fields.iter().map(|&i| expressions[i].clone()).collect()),
                    NodeRef::Op(NumOp::Core(Op::Unwrap), i) if uses[*i] == 1 && matches!(nodes[*i], NodeRef::Op(NumOp::Core(Op::MapSum(_)), _)) => {
                        let NodeRef::Op(NumOp::Core(Op::MapSum(arms)), arg) = &nodes[*i] else { unreachable!() };
                        let Shape::Sum(lanes) = &shapes[*arg] else { unreachable!() };
                        let arms = arms.iter().map(|(tag, body)| {
                            let (p, b) = self.lambda(body, &lanes[*tag])?;
                            Ok(format!("{tag} ({p} -> {b})"))
                        }).collect::<Result<Vec<_>, String>>()?;
                        format!("{} match ({})", expressions[*arg], arms.join(", "))
                    }
                    NodeRef::Op(NumOp::Core(Op::Gather), i) if self.shallow && matches!(&nodes[*i], NodeRef::Tuple(fields)
                        if fields.len() == 2 && matches!(&nodes[fields[0]], NodeRef::Op(NumOp::Core(Op::Lit(zero)), _)
                            if self.constant(zero, 0).ok().as_deref() == Some("0"))) => {
                        let NodeRef::Tuple(fields) = &nodes[*i] else { unreachable!() };
                        format!("{} head", expressions[fields[1]])
                    }
                    NodeRef::Op(op, i) => self.op(op, &expressions[*i], &shapes[*i])?,
                }
            };
            let mut depth = match node {
                NodeRef::Input => 0,
                NodeRef::Tuple(fields) => fields.iter().map(|&i| depths[i]).max().unwrap_or(0) + 1,
                NodeRef::Op(_, i) => depths[*i] + 1,
            };
            if paths[index].is_some() { depth = 0; }
            // Keep generated scalar syntax within the parser's recursion budget.
            // Short expressions retain their existing spelling. Naming an intermediate
            // changes no graph edges and also bounds repeated string copying.
            // Field paths, units and constants cost no computation; writing them twice
            // compiles to the same graph. All other shared values keep one explicit binding.
            if (uses[index] > 1 || depth > max_depth) && paths[index].is_none() && !matches!(node, NodeRef::Op(NumOp::Core(Op::Unit | Op::Lit(_)), _)) {
                let name = format!("v{next}"); next += 1;
                bindings += &format!("let {name} = {expression} in ");
                expression = name;
                depth = 0;
            }
            depths.push(depth);
            // The match and shallow-head spellings can inspect a predecessor's input.
            // Other inputs can be released at their last reader.
            if !self.shallow {
                let mut release = |i: usize| {
                    remaining[i] -= 1;
                    if remaining[i] == 0 { expressions[i] = String::new(); }
                };
                match node {
                    NodeRef::Tuple(fields) => for &i in *fields { release(i); },
                    NodeRef::Op(NumOp::Core(Op::MapSum(_)), _) | NodeRef::Input => {},
                    NodeRef::Op(_, i) => release(*i),
                }
            }
            expressions.push(expression);
        }
        Ok((pattern, format!("{bindings}{}", expressions[graph.output()])))
    }

    fn op(&mut self, op: &NumOp, x: &str, shape: &Shape) -> Result<String, String> {
        let pair = |c: String, name: &str, left: bool| if left { format!("({c}, {x}) {name}") } else { format!("({x}, {c}) {name}") };
        let suffix = match op {
            NumOp::Core(core) => match core {
                Op::Field(i) => return Ok(format!("{x}.{i}")),
                Op::Lit(v) => return self.constant(v, 0),
                Op::Unit => return Ok("()".into()),
                Op::Pattern(_) => return Ok(x.into()),
                Op::Weave => return Err("weave has no surface spelling".into()),
                Op::Inject(tag, lanes) => {
                    let mut resolved = lanes.clone();
                    resolved[*tag] = Some(shape.clone());
                    if let Some((name, variants)) = self.types.iter().find(|(_, variants)| {
                        variants.len() == resolved.len() && variants.iter().zip(&resolved).all(|((_, s), lane)| lane.as_ref() == Some(s))
                    }) { format!("inject {name}::{}", variants[*tag].0) }
                    else { format!("inject {}::V{tag}", self.enum_name(lanes)?) }
                }
                Op::Branch(n) => format!("branch {n}"),
                Op::Chunk(n) => format!("chunk {n}"),
                Op::MapList(body) => {
                    let Shape::List(elem) = shape else { return Err("map needs a list".into()) };
                    let (p, b) = self.lambda(body, elem)?;
                    format!("map ({p} -> {b})")
                }
                Op::Fold(body) | Op::FoldScan(body) => {
                    let Shape::Prod(fs) = shape else { return Err("fold needs a pair".into()) };
                    let Shape::List(elem) = &fs[1] else { return Err("fold needs a list".into()) };
                    let (p, b) = self.lambda(body, &Shape::Prod(vec![fs[0].clone(), *elem.clone()]))?;
                    format!("{} ({p} -> {b})", if matches!(core, Op::Fold(_)) { "fold" } else { "foldscan" })
                }
                Op::MapSum(arms) => {
                    let Shape::Sum(lanes) = shape else { return Err("match needs a sum".into()) };
                    arms.iter().map(|(tag, body)| {
                        let (p, b) = self.lambda(body, &lanes[*tag])?;
                        Ok(format!("map_variant {tag} ({p} -> {b})"))
                    }).collect::<Result<Vec<_>, String>>()?.join(" ")
                }
                Op::Unwrap => "unwrap".into(), Op::CapSum => "cap_sum".into(), Op::Enlist => "enlist".into(),
                Op::CapList => "cap_list".into(), Op::Ref => "ref".into(), Op::Clone => "clone".into(),
                Op::Transpose => "transpose".into(), Op::Zip => "zip".into(), Op::Flatten => "flatten".into(),
                Op::Unweave => "unweave".into(), Op::Gather => "gather".into(), Op::Range => "range".into(),
                Op::Iota => "iota".into(), Op::Filter => "filter".into(), Op::Hash => "hash".into(),
                Op::Select => "select".into(), Op::Append => "append".into(), Op::Len => "len".into(), Op::Cut => "cut".into(),
            },
            NumOp::Arith(a) => match a {
                ArithOp::Bin(b) => bin(*b).into(),
                ArithOp::BinImm(b, c) => return Ok(pair(scalar(*c)?, bin(*b), false)),
                ArithOp::ImmBin(c, b) => return Ok(pair(scalar(*c)?, bin(*b), true)),
                ArithOp::Bits(b) => bits(*b).into(),
                ArithOp::BitsImm(b, c) => return Ok(pair(c.to_string(), bits(*b), false)),
                ArithOp::ImmBits(c, b) => return Ok(pair(c.to_string(), bits(*b), true)),
                ArithOp::Shift(s, n) => format!("{} {n}", match s { ShiftOp::ShlB64 => "shl_b64", ShiftOp::ShrB64 => "shr_b64", ShiftOp::RotlB64 => "rotl_b64", ShiftOp::RotrB64 => "rotr_b64" }),
                ArithOp::Neg => "neg".into(), ArithOp::ToFloat => "to_float".into(),
                ArithOp::Reduce(r) => format!("fold_{}", red(*r)), ArithOp::Scan(r) => format!("scan_{}", red(*r)),
            },
            NumOp::Cmp(c) => match c {
                CmpOp::Rel(p) => pred(*p).into(),
                CmpOp::RelImm(p, c) => return Ok(pair(scalar(*c)?, pred(*p), false)),
                CmpOp::ListImm(ne, c) => {
                    let v = Value::Prim(c.clone());
                    let list = Value::List(vec![v.len()].into(), Box::new(v));
                    return Ok(pair(self.constant(&list, 0)?, if *ne { "ne" } else { "eq" }, false));
                }
                CmpOp::Min => "min".into(), CmpOp::Max => "max".into(),
                CmpOp::MinImm(c) => return Ok(pair(scalar(*c)?, "min", false)),
                CmpOp::MaxImm(c) => return Ok(pair(scalar(*c)?, "max", false)),
                CmpOp::SortBy => "sort_by".into(), CmpOp::SortLimit(n) => format!("sort_limit {n}"),
                CmpOp::Adjacent => "adjacent".into(), CmpOp::Find => "find".into(),
            },
            NumOp::Text(TextOp::ParseInt) => "parse_int".into(),
            NumOp::Text(TextOp::Split(byte)) => {
                if *byte >= 128 || *byte == b'"' || *byte == b'\\' { return Err(format!("split byte {byte} has no string spelling")); }
                format!("split \"{}\"", char::from(*byte))
            }
            NumOp::Host(h) => {
                let name = h.0.name();
                if super::lookup(name).is_some_and(|f| std::sync::Arc::ptr_eq(&f, &h.0)) {
                    let arg = if matches!(h.0.input(), Shape::Prod(fs) if fs.len() == 1) { format!("{x}.0") } else { x.into() };
                    return Ok(format!("{arg} call {name}"));
                }
                // Compare the word's graph, including host Arc identity. A replaced registration
                // must never silently print the new function under the old graph's name.
                let scope = super::base_scope();
                if let Some(word) = super::FloatWord::all().find_map(|(word, _)| {
                    let graph = corgi::parse_ml_in(&format!("input call {word}"), &scope).ok()?;
                    // Only a whole one-node word spells this host call. A word that merely
                    // contains the kernel may also compute something before or after it.
                    let mut b = Builder::default();
                    let input = b.input();
                    let output = b.add(op.clone(), vec![input]);
                    (graph == b.finish(output)).then_some(word)
                }) { format!("call {word}") }
                else { return Err(format!("host word {name} is not registered with this identity")); }
            }
        };
        Ok(format!("{x} {suffix}"))
    }

    fn constant(&mut self, value: &Value, row: usize) -> Result<String, String> {
        Ok(match value {
            Value::Prim(_) => match corgi::shape_of_value(value) {
                Shape::Int => value.as_i64("print constant")?[row].to_string(),
                Shape::Float => float(value.as_f64("print constant")?[row])?,
                _ => unreachable!(),
            },
            Value::Unit(_) => "()".into(),
            Value::Prod(fs) => tuple(fs.iter().map(|v| self.constant(v, row)).collect::<Result<Vec<_>, _>>()?),
            Value::List(bounds, values) => {
                let ends = bounds.to_vec();
                let start = if row == 0 { 0 } else { ends[row - 1] };
                let end = ends[row];
                if let Ok(bytes) = values.as_i64("string") {
                    let bytes: Option<Vec<u8>> = bytes[start..end].iter().map(|&b| u8::try_from(b).ok()).collect();
                    if let Some(bytes) = bytes {
                        if let Ok(text) = std::str::from_utf8(&bytes) {
                            // Corgi has no escapes, but DDP scans backslashes as escapes. Lists
                            // spell quotes, backslashes and control bytes without that ambiguity.
                            if text.chars().all(|c| !c.is_control() && c != '"' && c != '\\') { return Ok(format!("\"{text}\"")); }
                        }
                    }
                }
                let elements = (start..end).map(|i| self.constant(values, i)).collect::<Result<Vec<_>, _>>()?;
                let list = format!("[{}]", elements.join(", "));
                if start == end { format!("{list} : List({})", self.shape(&corgi::shape_of_value(values))?) } else { list }
            }
            Value::Sum(_, _) | Value::Ref(_, _) => return Err("sum or reference literal has no constant spelling".into()),
        })
    }
}

fn tuple(fields: Vec<String>) -> String { format!("({})", fields.join(", ")) }
fn bin(op: BinOp) -> &'static str { match op { BinOp::Add => "add", BinOp::Sub => "sub", BinOp::Mul => "mul", BinOp::Div => "div", BinOp::Rem => "rem" } }
fn bits(op: BitOp) -> &'static str { match op { BitOp::AddB64 => "add_b64", BitOp::SubB64 => "sub_b64", BitOp::MulB64 => "mul_b64", BitOp::And => "and", BitOp::Or => "or", BitOp::Xor => "xor" } }
fn pred(op: Pred) -> &'static str { match op { Pred::Eq => "eq", Pred::Ne => "ne", Pred::Lt => "lt", Pred::Le => "le", Pred::Gt => "gt", Pred::Ge => "ge" } }
fn red(op: Red) -> &'static str { match op { Red::Add => "add", Red::Mul => "mul", Red::Min => "min", Red::Max => "max", Red::All => "all", Red::Any => "any" } }
fn float(x: f64) -> Result<String, String> {
    if !x.is_finite() { return Err(format!("float bits 0x{:016x} have no literal spelling", x.to_bits())); }
    let mut s = x.to_string();
    if !s.contains(['.', 'e', 'E']) { s += ".0"; }
    Ok(s)
}
fn scalar(s: Scalar) -> Result<String, String> {
    match s {
        Scalar::Int(i) => Ok(i.to_string()),
        Scalar::Float(key) => float(f64::from_bits(if key >> 63 == 1 { key ^ (1 << 63) } else { !key })),
    }
}

fn pattern(shape: &Shape, path: &mut Vec<usize>, read: &BTreeSet<Vec<usize>>, names: &mut BTreeMap<Vec<usize>, String>) -> String {
    if read.contains(path) {
        if *shape == Shape::Unit { names.insert(path.clone(), "()".into()); return "()".into(); }
        let n = names.len();
        let name = if n < 26 { char::from(b'a' + n as u8).to_string() } else { format!("a{n}") };
        names.insert(path.clone(), name.clone());
        return name;
    }
    if !read.iter().any(|p| p.starts_with(path)) { return "_".into(); }
    if let Shape::Prod(fields) = shape {
        return tuple(fields.iter().enumerate().map(|(i, s)| {
            path.push(i); let p = pattern(s, path, read, names); path.pop(); p
        }).collect());
    }
    "_".into()
}

fn path_name(path: &[usize], names: &BTreeMap<Vec<usize>, String>) -> Option<String> {
    (0..=path.len()).rev().find_map(|n| names.get(&path[..n]).map(|name| {
        format!("{name}{}", path[n..].iter().map(|i| format!(".{i}")).collect::<String>())
    }))
}

/// This form is deliberately diagnostic. In particular, it does not claim that a host name
/// identifies its implementation or that a reference literal can be read back by ML.
fn graph_text(graph: &G) -> String {
    let mut lines = Vec::new();
    for (i, node) in graph.nodes().enumerate() {
        let expression = match node {
            NodeRef::Input => "input".into(),
            NodeRef::Tuple(fields) => tuple(fields.iter().map(|f| format!("%{f}")).collect()),
            NodeRef::Op(op, input) => {
                let x = format!("%{input}");
                match op {
                    NumOp::Core(Op::Lit(value)) => format!("constant {}", value_text(value)),
                    NumOp::Core(Op::Pattern(p)) => format!("{x} check {p}"),
                    NumOp::Core(Op::Weave) => format!("{x} weave"),
                    NumOp::Core(Op::Inject(tag, lanes)) => format!("{x} inject lane {tag} of [{}]", lanes.iter().map(|s| s.as_ref().map(ToString::to_string).unwrap_or_else(|| "payload shape".into())).collect::<Vec<_>>().join(", ")),
                    NumOp::Core(Op::MapList(body) | Op::Fold(body) | Op::FoldScan(body)) => {
                        let name = match op { NumOp::Core(Op::MapList(_)) => "map", NumOp::Core(Op::Fold(_)) => "fold", _ => "foldscan" };
                        format!("{x} {name} {{\n{}\n}}", indent(&graph_text(body)))
                    }
                    NumOp::Core(Op::MapSum(arms)) => format!("{x} map sum {{\n{}\n}}", arms.iter().map(|(tag, body)| format!("  lane {tag} {{\n{}\n  }}", indent(&indent(&graph_text(body))))).collect::<Vec<_>>().join("\n")),
                    NumOp::Host(h) => format!("{x} host {} : {} -> {} (implementation identity required)", h.0.name(), h.0.input(), h.0.output()),
                    NumOp::Arith(ArithOp::BinImm(b, c)) => format!("({x}, {}) {}", graph_scalar(*c), bin(*b)),
                    NumOp::Arith(ArithOp::ImmBin(c, b)) => format!("({}, {x}) {}", graph_scalar(*c), bin(*b)),
                    NumOp::Cmp(CmpOp::RelImm(p, c)) => format!("({x}, {}) {}", graph_scalar(*c), pred(*p)),
                    NumOp::Cmp(CmpOp::MinImm(c)) => format!("({x}, {}) min", graph_scalar(*c)),
                    NumOp::Cmp(CmpOp::MaxImm(c)) => format!("({x}, {}) max", graph_scalar(*c)),
                    NumOp::Cmp(CmpOp::ListImm(ne, c)) => format!("({x}, constant list {}) {}", value_text(&Value::Prim(c.clone())), if *ne { "ne" } else { "eq" }),
                    NumOp::Text(TextOp::Split(byte)) => format!("{x} split byte {byte}"),
                    _ => Printer::default().op(op, &x, &Shape::Unit).expect("all remaining kernels have a shape-independent surface spelling"),
                }
            }
        };
        lines.push(format!("%{i} = {expression}"));
    }
    lines.push(format!("return %{}", graph.output()));
    lines.join("\n")
}

fn graph_scalar(s: Scalar) -> String { scalar(s).unwrap_or_else(|bits| bits) }

fn indent(text: &str) -> String { text.lines().map(|s| format!("  {s}")).collect::<Vec<_>>().join("\n") }

fn value_text(value: &Value) -> String {
    fn row(v: &Value, i: usize) -> String {
        match v {
            Value::Prim(_) => match corgi::shape_of_value(v) {
                Shape::Int => v.as_i64("constant").unwrap()[i].to_string(),
                _ => { let f = v.as_f64("constant").unwrap()[i]; float(f).unwrap_or_else(|_| format!("float_bits(0x{:016x})", f.to_bits())) }
            },
            Value::Unit(_) => "()".into(),
            Value::Prod(fs) => tuple(fs.iter().map(|v| row(v, i)).collect()),
            Value::List(bounds, values) => {
                let ends = bounds.to_vec();
                let start = if i == 0 { 0 } else { ends[i - 1] };
                format!("[{}]", (start..ends[i]).map(|j| row(values, j)).collect::<Vec<_>>().join(", "))
            }
            Value::Sum(tags, lanes) => format!("lane {} ({})", tags.tag_at(i), row(&lanes[tags.tag_at(i)], tags.offset_at(i))),
            Value::Ref(arena, rows) => format!("reference row {} in {}", rows[i], value_text(arena)),
        }
    }
    format!("{} [{}]", corgi::shape_of_value(value), (0..value.len()).map(|i| row(value, i)).collect::<Vec<_>>().join(", "))
}

fn check_text(original: &G, shape: &Shape, text: &str) -> Check {
    let check = || -> Result<Check, String> {
        // Use the real DDP slot reader, including its lambda boundaries, comments and
        // registered-word scope. Typing below supplies the original slot's input shape.
        let statements = crate::parse::pipe::parse(&format!("let printed = input 0 | map({text});"));
        let [crate::parse::Stmt::Let(_, crate::parse::Expr::Map(_, lambda))] = statements.as_slice() else {
            return Err("printed text did not read back as one DDP lambda".into());
        };
        let parsed = lambda.graph()?.as_ref();
        let expected = corgi::shape_of(original, shape)?;
        let actual = corgi::shape_of(parsed, shape).map_err(|e| format!("printed text did not type at {shape}: {e}"))?;
        if expected != actual { return Err(format!("output shape changed from {expected} to {actual}")); }
        let parsed = corgi::compile(parsed.clone(), shape)?;
        let parsed = parsed.graph();
        if corgi::compile(original.clone(), shape)?.graph() == parsed { return Ok(Check::Exact); }
        let mut seed = 0x5eed_d1ff_c0de_2026;
        for rows in [0, 1, 17, 64] {
            let input = generated(shape, rows, &mut seed);
            let a = corgi::eval_graph(original, input.clone());
            let b = corgi::eval_graph(&parsed, input);
            if a.len() != b.len() { return Err(format!("row count differs on the {rows}-row batch")); }
            for row in 0..a.len() {
                if corgi::arrange::compare_at(&a, row, &b, row) != std::cmp::Ordering::Equal {
                    return Err(format!("output differs at row {row} of the {rows}-row batch (seed 0x5eedd1ffc0de2026)"));
                }
            }
        }
        Ok(Check::Sampled)
    };
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(check)) {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Check::Failed(error),
        Err(panic) => {
            let message = panic.downcast_ref::<String>().map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied()).unwrap_or("no message");
            Check::Failed(format!("DDP parsing or evaluation panicked: {message}"))
        }
    }
}

pub(crate) fn generated(shape: &Shape, rows: usize, seed: &mut u64) -> Value {
    fn next(seed: &mut u64) -> u64 {
        *seed ^= *seed << 13; *seed ^= *seed >> 7; *seed ^= *seed << 17; *seed
    }
    match shape {
        Shape::Int => Value::int((0..rows).map(|_| (next(seed) % 12) as i64 - 3)),
        Shape::Float => Value::f64((0..rows).map(|_| ((next(seed) % 25) as f64 - 12.0) / 4.0).collect()),
        Shape::Unit => Value::Unit(rows),
        Shape::Prod(fs) => Value::Prod(fs.iter().map(|s| generated(s, rows, seed)).collect()),
        Shape::List(elem) => {
            let mut total = 0;
            let ends = (0..rows).map(|_| { total += (next(seed) % 5) as usize; total }).collect::<Vec<_>>();
            Value::List(ends.into(), Box::new(generated(elem, total, seed)))
        }
        Shape::Sum(lanes) => {
            let tags: Vec<_> = (0..rows).map(|_| (next(seed) % lanes.len() as u64) as usize).collect();
            let values = lanes.iter().enumerate().map(|(t, s)| generated(s, tags.iter().filter(|&&i| i == t).count(), seed)).collect();
            Value::sum(tags, values)
        }
        Shape::Ref(inner) => Value::Ref(std::sync::Arc::new(generated(inner, rows, seed)), std::sync::Arc::new((0..rows).collect())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn checked(src: &str, shape: Shape) -> String {
        let lambda = Lambda::new(src.into(), &super::super::base_scope());
        checked_lambda(&lambda, shape)
    }

    fn checked_lambda(lambda: &Lambda, shape: Shape) -> String {
        let printed = print(lambda, Some(&shape), true);
        match printed.check {
            Check::Exact | Check::Sampled => {}
            Check::Failed(e) | Check::Unchecked(e) => panic!("{}: {e}", printed.text),
        }
        assert!(!printed.fallback, "{}", printed.text);
        assert_eq!(printed.text, print(lambda, Some(&shape), true).text);
        printed.text
    }

    fn graph_lambda(graph: G) -> Lambda { Lambda { src: "this is deliberately not the graph".into(), graph: Ok(Arc::new(graph)), added_by: None, scope: None } }
    fn ints(n: usize) -> Shape { Shape::Prod(vec![Shape::Int; n]) }
    fn list(s: Shape) -> Shape { Shape::List(Box::new(s)) }

    #[test]
    fn authoritative_mask_and_composition() {
        let shape = Shape::Prod(vec![ints(3), ints(2)]);
        let identity = Lambda::new("input".into(), &corgi::Scope::default());
        let mut composed = Lambda::compose("arg", &[(&identity, "arg".into())], |names| {
            let x = &names[0]; format!("(({x}.0.0, {x}.0.1, {x}.0.2), ((), {x}.1.1))")
        });
        composed.src = "let arg = input in ()".into();
        assert_eq!(checked_lambda(&composed, shape.clone()), "((a, b, c), (_, d)) -> ((a, b, c), ((), d))");
        let mut b = Builder::default(); let x = b.input();
        let key = b.add(Op::Field(0), vec![x]);
        let val = b.add(Op::Field(1), vec![x]);
        let kept = b.add(Op::Field(1), vec![val]);
        let unit = b.add(Op::Unit, vec![x]);
        let val = b.tuple(vec![unit, kept]);
        let out = b.tuple(vec![key, val]);
        assert_eq!(checked_lambda(&graph_lambda(b.finish(out)), shape), "(a, (_, b)) -> (a, ((), b))");
    }

    #[test]
    fn shared_nodes_and_singletons() {
        let text = checked("let (x, y) = input in let sum = (x, y) add in (sum, sum neg)", ints(2));
        assert_eq!(text, "(a, b) -> let v0 = (a, b) add in (v0, v0 neg)");
        assert_eq!(checked("let (x) = input in (x)", ints(1)), "(a) -> (a)");
    }

    #[test]
    fn operators_and_immediates_keep_operand_order() {
        checked("(3, input) sub", Shape::Int);
        checked("(1.25, input) div", Shape::Float);
        for (word, ne) in [("eq", false), ("ne", true)] {
            let graph = corgi::optimize(&corgi::parse_ml(&format!("(input, [1, 2, 3]) {word}")).unwrap());
            assert!(graph.nodes().any(|node| matches!(node,
                NodeRef::Op(NumOp::Cmp(CmpOp::ListImm(flag, _)), _) if *flag == ne)));
            assert!(graph_text(&graph).contains(&format!(") {word}")));
            let text = checked_lambda(&graph_lambda(graph), list(Shape::Int));
            assert!(text.ends_with(word), "{text}");
        }
    }

    #[test]
    fn constants_are_complete_and_strings_have_no_fake_escapes() {
        for src in ["-9223372036854775808", "-0.0", "[1.0, -0.0]", "[] : List(float)", "[(1, [2, 3]), (4, [])]", "[34, 92, 10, 255]", r#""\""#] {
            checked(src, Shape::Int);
        }
        let long = format!("\"{}\"", "a".repeat(100));
        assert!(checked(&long, Shape::Int).contains(&long));
    }

    struct Host { shape: Shape, name: &'static str }
    impl corgi::HostKernel for Host {
        fn name(&self) -> &str { self.name }
        fn input(&self) -> &Shape { &self.shape }
        fn output(&self) -> &Shape { &self.shape }
        fn eval(&self, input: Value) -> Result<Value, String> { Ok(input) }
    }

    #[test]
    fn registered_words_use_the_actual_kernel_identity() {
        for (shape, name, text, input) in [
            (ints(1), "printer_unary", "input call printer_unary", Shape::Int),
        ] {
            let f: super::super::Function = Arc::new(Host { shape, name });
            super::super::register(Arc::clone(&f));
            let mut scope = super::super::base_scope(); scope.word(name, super::super::function_word(&f)).unwrap();
            let l = Lambda::new(text.into(), &scope);
            assert!(checked_lambda(&l, input.clone()).contains(&format!("call {name}")));
            super::super::register(Arc::new(Host { shape: f.input().clone(), name }));
            let printed = print(&l, Some(&input), true);
            assert!(printed.fallback);
            assert!(printed.text.contains("implementation identity required"));
            assert!(matches!(printed.check, Check::Unchecked(_)));
        }
    }

    #[test]
    fn unsupported_nodes_have_readable_graphs() {
        let mut b = Builder::default(); let x = b.input(); let out = b.add(Op::Weave, vec![x]);
        let l = graph_lambda(b.finish(out));
        let shape = Shape::Prod(vec![list(Shape::Int), list(Shape::Int), list(Shape::Int)]);
        let printed = print(&l, Some(&shape), true);
        assert!(printed.fallback);
        assert!(printed.text.contains("%1 = %0 weave\nreturn %1"));
        assert!(!printed.text.contains("Node"));
        let mut b = Builder::default(); let x = b.input();
        let out = b.add(CmpOp::RelImm(Pred::Gt, Scalar::Float(super::super::float_key(f64::INFINITY))), vec![x]);
        let printed = print(&graph_lambda(b.finish(out)), Some(&Shape::Float), true);
        assert!(printed.fallback);
        assert!(printed.text.contains("0x7ff0000000000000"));
        assert!(printed.text.contains(" gt"));
        let unknown = print(&l, None, true);
        assert!(unknown.fallback);
        assert!(matches!(unknown.check, Check::Unchecked(_)));
    }

    #[test]
    fn checker_rejects_parse_shape_and_value_errors() {
        let graph = corgi::optimize(&corgi::parse_ml("(input, 1) add").unwrap());
        for text in ["broken syntax", "input -> ()", "input -> (input, 2) add"] {
            assert!(matches!(check_text(&graph, &Shape::Int, text), Check::Failed(_)), "{text}");
        }
        assert!(matches!(check_text(&graph, &Shape::Int, "input -> (input, 1) add"), Check::Exact));
    }
}
