pub mod parse;
pub mod ir;
pub mod lower;
pub mod explain;
pub mod folded;
pub mod scope_ir;
pub mod explain_tree;

use std::collections::BTreeSet;

use parse::{Stmt, Expr};

/// Survey a program's external sources: the count of positional inputs (one
/// more than the largest `input N` index, zero if none appear) and the set of
/// names referenced by `import "name"`. Two kinds because `import` does not yet
/// subsume `input` — see `ir::Node::Import`; this returns one number when that
/// cutover happens.
pub fn survey_sources(stmts: &[Stmt]) -> (usize, BTreeSet<String>) {
    let mut positional = 0usize;
    let mut imports = BTreeSet::new();
    walk_stmts(stmts, &mut positional, &mut imports);
    (positional, imports)
}

fn walk_stmts(stmts: &[Stmt], positional: &mut usize, imports: &mut BTreeSet<String>) {
    for stmt in stmts {
        match stmt {
            Stmt::Let(_, expr) | Stmt::Var(_, expr) | Stmt::Export(_, expr) => walk_expr(expr, positional, imports),
            Stmt::Scope(_, body) => walk_stmts(body, positional, imports),
        }
    }
}

fn walk_expr(expr: &Expr, positional: &mut usize, imports: &mut BTreeSet<String>) {
    match expr {
        Expr::Input(n) => { *positional = (*positional).max(n + 1); },
        Expr::Import(name) => { imports.insert(name.clone()); },
        Expr::Map(e, _) | Expr::Negate(e) | Expr::Arrange(e)
            | Expr::EnterAt(e, _) | Expr::LiftIter(e) | Expr::Filter(e, _)
            | Expr::Reduce(e, _) | Expr::Inspect(e, _) => walk_expr(e, positional, imports),
        Expr::Join(l, r, _) => { walk_expr(l, positional, imports); walk_expr(r, positional, imports); },
        Expr::Concat(es) => { for e in es { walk_expr(e, positional, imports); } },
        Expr::Name(_) | Expr::Qualified(_, _) => {},
    }
}

/// Load a program source file.
pub fn load_program(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("Cannot read {}: {}", path, e))
}

/// Deterministic hash: maps an index to a pseudorandom u64 (splitmix64).
pub fn hash_u64(index: u64) -> u64 {
    let mut x = index.wrapping_mul(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

/// Generate one row: arity fields, each hash-derived, magnitude < nodes.
pub fn gen_row<R: ir::RowLike>(edge_index: u64, nodes: u64, arity: usize) -> (R, R) {
    let mut key = R::new();
    for col in 0..arity {
        let h = hash_u64(edge_index.wrapping_mul(31).wrapping_add(col as u64));
        key.push((h % nodes) as i64);
    }
    (key, R::new())
}
