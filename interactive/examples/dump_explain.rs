//! Print the original and explain-rewritten programs as `.ddp`-ish source.
//!
//! Usage: `dump_explain <program> <arity>`.
//!
//! Goal is readability, not strict parseability — the IR loses original
//! names so we invent `nID` names. Scope/EndScope become `scope_N: { ... }`
//! blocks; Variable + Bind become `var nID = body;` at the Bind point.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use interactive::ir::{Id, LinearOp, Node, Program};
use interactive::parse::{Condition, FieldExpr, Projection, Reducer};
use interactive::{explain, lower, parse};

fn main() {
    let path = std::env::args().nth(1).expect("usage: dump_explain <program> <arity>");
    let arity: usize = std::env::args().nth(2).unwrap_or("2".into()).parse().unwrap();

    let source = interactive::load_program(&path);
    let stmts = if path.ends_with(".ddp") {
        parse::pipe::parse(&source)
    } else {
        parse::applicative::parse(&source)
    };
    let (n_inputs, imports) = interactive::survey_sources(&stmts);
    let original = lower::lower(stmts);

    println!("-- ====================================================");
    println!("-- ORIGINAL ({} nodes)", original.nodes.len());
    println!("-- ====================================================");
    print_ddp(&original);

    let input_arities = vec![(arity, 0usize); n_inputs];
    let import_arities: BTreeMap<String, (usize, usize)> = imports
        .iter()
        .map(|n| (n.clone(), (arity, 0usize)))
        .collect();
    let rewritten = explain::explain(&original, &input_arities, &import_arities);

    println!();
    println!("-- ====================================================");
    println!("-- AFTER explain rewrite ({} nodes)", rewritten.nodes.len());
    println!("-- ====================================================");
    print_ddp(&rewritten);
}

/// Walk a Program's nodes in id order, emitting `.ddp`-ish let / var /
/// scope statements. Generated names are `nID`.
fn print_ddp(p: &Program) {
    // Pre-scan: which Variables are bound to which values?
    let var_body: BTreeMap<Id, Id> = p.nodes.iter().filter_map(|(_, n)| {
        if let Node::Bind { variable, value } = n {
            Some((*variable, *value))
        } else {
            None
        }
    }).collect();
    // Mark which nodes appear inside a Reduce's Arrange-then-Reduce pair,
    // so we can fold them into a single `| distinct` / `| min` / `| count`.
    let mut reduce_arrange_inputs: BTreeSet<Id> = BTreeSet::new();
    for (_, node) in &p.nodes {
        if let Node::Reduce { input, .. } = node {
            reduce_arrange_inputs.insert(*input);
        }
    }
    // Same for Join's Arrange-Arrange pair.
    let mut join_arrange_inputs: BTreeSet<Id> = BTreeSet::new();
    for (_, node) in &p.nodes {
        if let Node::Join { left, right, .. } = node {
            join_arrange_inputs.insert(*left);
            join_arrange_inputs.insert(*right);
        }
    }

    let mut indent: usize = 0;
    for (&id, node) in &p.nodes {
        let pad = "    ".repeat(indent);
        match node {
            Node::Scope => {
                println!("{}scope_{}: {{", pad, id);
                indent += 1;
            }
            Node::EndScope => {
                indent = indent.saturating_sub(1);
                let pad = "    ".repeat(indent);
                println!("{}}}", pad);
            }
            Node::Input(i) => {
                println!("{}let n{} = input {};", pad, id, i);
            }
            Node::Import { name } => {
                println!("{}let n{} = import {:?};", pad, id, name);
            }
            Node::Linear { input, ops } => {
                println!("{}let n{} = n{} | {};", pad, id, input, fmt_linear_ops(ops));
            }
            Node::Concat(ids) => {
                let names: Vec<String> = ids.iter().map(|i| format!("n{}", i)).collect();
                println!("{}let n{} = {};", pad, id, names.join(" + "));
            }
            Node::Arrange(input) => {
                // Fold into the Reduce / Join that wraps it: skip emitting
                // a separate name. (Otherwise just say `| arrange`.)
                if reduce_arrange_inputs.contains(&id) || join_arrange_inputs.contains(&id) {
                    println!("{}-- (n{}: arrange of n{} — folded into next op)", pad, id, input);
                } else {
                    println!("{}let n{} = n{} | arrange;", pad, id, input);
                }
            }
            Node::Join { left, right, projection } => {
                println!(
                    "{}let n{} = n{} | join(n{}, {});",
                    pad, id, left, right, fmt_projection(projection)
                );
            }
            Node::Reduce { input, reducer } => {
                let op = match reducer {
                    Reducer::Min => "min",
                    Reducer::Distinct => "distinct",
                    Reducer::Count => "count",
                };
                // `input` is the wrapping Arrange; show the un-arranged source.
                let source = match p.nodes.get(input) {
                    Some(Node::Arrange(inner)) => *inner,
                    _ => *input,
                };
                println!("{}let n{} = n{} | {};", pad, id, source, op);
            }
            Node::Variable => {
                // Defer to the matching Bind. Note the placeholder.
                match var_body.get(&id) {
                    Some(body_id) => println!("{}-- (n{}: Variable, bound to n{} below)", pad, id, body_id),
                    None => println!("{}-- (n{}: Variable, never bound)", pad, id),
                }
            }
            Node::Inspect { input, label } => {
                println!("{}let n{} = n{} | inspect({});", pad, id, input, label);
            }
            Node::Leave(inner, level) => {
                println!("{}let n{} = leave(n{}, level={});", pad, id, inner, level);
            }
            Node::Bind { variable, value } => {
                println!("{}var n{} = n{};", pad, variable, value);
            }
        }
    }
    let pad = "    ".repeat(indent);
    for (name, id) in &p.export {
        if name == "result" {
            println!("{}result n{};", pad, id);
        } else {
            println!("{}export {:?} = n{};", pad, name, id);
        }
    }
}

fn fmt_linear_ops(ops: &[LinearOp]) -> String {
    ops.iter().map(fmt_linear_op).collect::<Vec<_>>().join(" | ")
}

fn fmt_linear_op(op: &LinearOp) -> String {
    match op {
        LinearOp::Project(p) => format!("key({})", fmt_projection_body(p)),
        LinearOp::Filter(c) => format!("filter({})", fmt_condition(c)),
        LinearOp::Negate => "negate".into(),
        LinearOp::EnterAt(f) => format!("enter_at({})", fmt_field(f)),
        LinearOp::LiftIter => "lift_iter".into(),
    }
}

fn fmt_projection(p: &Projection) -> String {
    format!("({})", fmt_projection_body(p))
}

fn fmt_projection_body(p: &Projection) -> String {
    let key: Vec<String> = p.key.iter().map(fmt_field).collect();
    let val: Vec<String> = p.val.iter().map(fmt_field).collect();
    format!("{} ; {}", key.join(", "), val.join(", "))
}

fn fmt_field(f: &FieldExpr) -> String {
    match f {
        FieldExpr::Pos(i) => format!("$ {}", i),
        FieldExpr::Index(r, c) => format!("${}[{}]", r, c),
        FieldExpr::Const(v) => format!("{}", v),
        FieldExpr::Neg(inner) => format!("-{}", fmt_field(inner)),
        FieldExpr::Sub(a, b) => format!("({} - {})", fmt_field(a), fmt_field(b)),
    }
}

fn fmt_condition(c: &Condition) -> String {
    match c {
        Condition::Eq(a, b) => format!("{} == {}", fmt_field(a), fmt_field(b)),
        Condition::Ne(a, b) => format!("{} != {}", fmt_field(a), fmt_field(b)),
        Condition::Lt(a, b) => format!("{} < {}", fmt_field(a), fmt_field(b)),
        Condition::Le(a, b) => format!("{} <= {}", fmt_field(a), fmt_field(b)),
        Condition::Gt(a, b) => format!("{} > {}", fmt_field(a), fmt_field(b)),
        Condition::Ge(a, b) => format!("{} >= {}", fmt_field(a), fmt_field(b)),
        Condition::And(a, b) => format!("({}) && ({})", fmt_condition(a), fmt_condition(b)),
    }
}
