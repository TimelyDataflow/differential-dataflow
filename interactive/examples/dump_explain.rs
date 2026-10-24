//! Print a program's scope-tree IR, before and after the explanation rewrite.
//!
//! Usage: `dump_explain <program>`.
//!
//! Output is the structural tree dump (readability, not parseability):
//! per scope, imports and vars first, items in order with `Sub`s nested,
//! then binds and exports.

use interactive::{lower, parse};

fn main() {
    let path = std::env::args().nth(1).expect("usage: dump_explain <program>");

    let source = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let original = lower::lower_tree(parse::pipe::parse(&source));

    println!("-- ====================================================");
    println!("-- ORIGINAL ({} ops)", original.op_count());
    println!("-- ====================================================");
    original.dump();

    let rewritten = interactive::explain::explain(&original);

    println!();
    println!("-- ====================================================");
    println!("-- EXPLAIN ({} ops)", rewritten.op_count());
    println!("-- ====================================================");
    rewritten.dump();
}
