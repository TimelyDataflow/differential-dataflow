//! Print a program's scope-tree IR, before and after the explanation rewrite.
//!
//! Usage: `dump_explain <program> <arity>`.
//!
//! Output is the structural tree dump (readability, not parseability):
//! per scope, imports and vars first, items in order with `Sub`s nested,
//! then binds and exports.

use interactive::{lower, parse, scope_ir};

fn main() {
    let path = std::env::args().nth(1).expect("usage: dump_explain <program> <arity>");
    let arity: usize = std::env::args().nth(2).unwrap_or("2".into()).parse().unwrap();

    let source = interactive::load_program(&path);
    let stmts = if path.ends_with(".ddp") {
        parse::pipe::parse(&source)
    } else {
        parse::applicative::parse(&source)
    };
    let original = lower::lower_tree(stmts);

    println!("-- ====================================================");
    println!("-- ORIGINAL ({} ops)", original.op_count());
    println!("-- ====================================================");
    original.dump();

    let source_shapes: Vec<(usize, usize)> = original.root.imports.iter().map(|imp| match &imp.from {
        scope_ir::Source::Input(_) | scope_ir::Source::Trace(_) => (arity, 0usize),
        scope_ir::Source::Parent(_) => unreachable!("root scope cannot import from a parent"),
    }).collect();
    let rewritten = interactive::explain::explain(&original, &source_shapes);

    println!();
    println!("-- ====================================================");
    println!("-- EXPLAIN ({} ops)", rewritten.op_count());
    println!("-- ====================================================");
    rewritten.dump();
}
