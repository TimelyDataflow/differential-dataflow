pub mod parse;
pub mod ir;
pub mod lower;

use parse::{Stmt, Expr};

/// Count the number of distinct inputs referenced in a program.
pub fn count_inputs(stmts: &[Stmt]) -> usize {
    let mut max_input = 0usize;
    for stmt in stmts {
        match stmt {
            Stmt::Let(_, expr) | Stmt::Var(_, expr) | Stmt::Result(expr) => {
                max_input = max_input.max(count_inputs_expr(expr));
            },
            Stmt::Scope(_, body) => {
                max_input = max_input.max(count_inputs(body));
            },
        }
    }
    max_input
}

fn count_inputs_expr(expr: &Expr) -> usize {
    match expr {
        Expr::Input(n) => n + 1,
        Expr::Map(e, _) | Expr::Negate(e) | Expr::Arrange(e)
            | Expr::EnterAt(e, _) | Expr::Filter(e, _) | Expr::Reduce(e, _)
            | Expr::Inspect(e, _) => count_inputs_expr(e),
        Expr::Join(l, r, _) => count_inputs_expr(l).max(count_inputs_expr(r)),
        Expr::Concat(es) => es.iter().map(|e| count_inputs_expr(e)).max().unwrap_or(0),
        Expr::Name(_) | Expr::Qualified(_, _) => 0,
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
