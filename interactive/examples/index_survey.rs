//! Which indexes do importers want? For every `.ddp` program named on the command line,
//! find each arrangement (an `arrange`, a join input, or a reduce input) that is a linear
//! chain over an imported trace, and report the key it is arranged by, as field positions
//! of the trace's record. Aggregated across programs, this is the demand for indexes.
//!
//! ```text
//! python3 server/bench/ldbc/suite.py --emit --output /tmp/snb
//! cargo run --release --example index_survey -- /tmp/snb/*.ddp
//! ```

use std::collections::BTreeMap;

use interactive::ir::{LinearOp, Term};
use interactive::scope_ir::{Item, Node, Ref, Scope, Source};
use interactive::{lower, parse};

/// A value derived from an imported trace by linear ops: the trace, and the current key and
/// value as terms over the trace's original row (`Var(0)` its record, `Var(1)` its unit value).
#[derive(Clone, Debug)]
struct Derived {
    trace: String,
    key: Term,
    val: Term,
    filtered: bool,
}

/// Substitute the current row (`key`, `val`) into `t`, simplifying projections of tuples.
/// `None` when the result is not a plain field selection (computed keys are reported as such).
fn subst(t: &Term, key: &Term, val: &Term) -> Option<Term> {
    Some(match t {
        Term::Var(0) => key.clone(),
        Term::Var(1) => val.clone(),
        Term::Tuple(xs) => {
            let mut out = Vec::new();
            for x in xs {
                match x {
                    // A spread of a known tuple splices its fields.
                    Term::Spread(inner) => match subst(inner, key, val)? {
                        Term::Tuple(fs) => out.extend(fs),
                        other => out.push(Term::Spread(Box::new(other))),
                    },
                    other => out.push(subst(other, key, val)?),
                }
            }
            Term::Tuple(out)
        }
        Term::Proj(inner, i) => match subst(inner, key, val)? {
            Term::Tuple(fs) if fs.iter().all(|f| !matches!(f, Term::Spread(_))) => fs.get(*i)?.clone(),
            // `(…spread(record)…)[i]` with the spread first: the record's field `i`.
            Term::Tuple(fs) if matches!(fs.first(), Some(Term::Spread(_))) && fs.len() == 1 => {
                let Term::Spread(r) = &fs[0] else { unreachable!() };
                Term::Proj(r.clone(), *i)
            }
            other => Term::Proj(Box::new(other), *i),
        },
        _ => return None,
    })
}

/// The key as field positions of the original record, or `None` if it is computed.
fn fields(key: &Term) -> Option<Vec<usize>> {
    let field = |t: &Term| match t {
        Term::Proj(r, i) if matches!(**r, Term::Var(0)) => Some(vec![*i]),
        Term::Spread(r) if matches!(**r, Term::Var(0)) => Some(vec![usize::MAX]), // the whole record
        Term::Var(0) => Some(vec![usize::MAX]),
        _ => None,
    };
    match key {
        Term::Tuple(fs) => {
            let mut out = Vec::new();
            for f in fs { out.extend(field(f)?); }
            Some(out)
        }
        other => field(other),
    }
}

#[derive(Default)]
struct Demand {
    /// (trace, key fields or "computed", filtered) -> (uses, programs)
    uses: BTreeMap<(String, String, bool), (usize, Vec<String>)>,
}

fn survey(s: &Scope, imports: Vec<Option<Derived>>, program: &str, demand: &mut Demand) {
    let mut items: Vec<Option<Derived>> = Vec::new();
    let resolve = |seen: &[Option<Derived>], r: &Ref| -> Option<Derived> {
        match r {
            Ref::Local(i) => seen[*i].clone(),
            Ref::Import(k) => imports[*k].clone(),
            _ => None,
        }
    };
    let record = |d: Option<Derived>, into: &mut Demand| {
        if let Some(d) = d {
            let key = fields(&d.key).map(|f| {
                f.iter().map(|i| if *i == usize::MAX { "*".to_string() } else { i.to_string() }).collect::<Vec<_>>().join(",")
            }).unwrap_or_else(|| "computed".into());
            let entry = into.uses.entry((d.trace, key, d.filtered)).or_default();
            entry.0 += 1;
            if !entry.1.contains(&program.to_string()) { entry.1.push(program.to_string()); }
        }
    };
    for item in &s.items {
        let derived = match item {
            Item::Op(Node::Linear { input, ops }) => resolve(&items, input).and_then(|mut d| {
                for op in ops {
                    match op {
                        LinearOp::Project(p) => {
                            let (k, v) = (subst(&p.key, &d.key, &d.val)?, subst(&p.val, &d.key, &d.val)?);
                            d.key = k;
                            d.val = v;
                        }
                        LinearOp::Filter(_) => d.filtered = true,
                        LinearOp::Negate => {}
                        _ => return None,
                    }
                }
                Some(d)
            }),
            // An arrangement holds the same rows, so its consumers' re-keyings still count.
            // (The use is counted at the join or reduce that reads it.)
            Item::Op(Node::Arrange(input)) => resolve(&items, input),
            Item::Op(Node::Join { left, right, .. }) => {
                record(resolve(&items, left), demand);
                record(resolve(&items, right), demand);
                None
            }
            Item::Op(Node::Reduce { input, reducer }) => {
                let d = resolve(&items, input);
                record(d.clone(), demand);
                // `distinct` of whole records is the same relation, as a set.
                d.filter(|d| matches!(reducer, interactive::ir::Reducer::Distinct) && fields(&d.key) == Some(vec![usize::MAX]))
                    .map(|d| Derived { trace: format!("{}|distinct", d.trace), key: Term::Var(0), val: Term::Var(1), filtered: d.filtered })
            }
            // A union of whole-record views of one trace (e.g. both directions of an edge) is a
            // relation of its own: name it, and index it by its own field positions.
            Item::Op(Node::Concat(refs)) => {
                let ds: Option<Vec<Derived>> = refs.iter().map(|r| resolve(&items, r)).collect();
                ds.filter(|ds| ds.iter().all(|d| d.trace == ds[0].trace)).map(|ds| Derived {
                    trace: format!("{}+", ds[0].trace),
                    key: Term::Var(0),
                    val: Term::Var(1),
                    filtered: ds.iter().any(|d| d.filtered),
                })
            }
            Item::Op(_) => None,
            Item::Sub(child) => {
                let child_imports = child.imports.iter().map(|imp| match &imp.from {
                    Source::Parent(r) => resolve(&items, r),
                    _ => None,
                }).collect();
                survey(child, child_imports, program, demand);
                None
            }
        };
        items.push(derived);
    }
}

fn main() {
    let mut demand = Demand::default();
    for path in std::env::args().skip(1) {
        let src = std::fs::read_to_string(&path).expect("read");
        let mut program = lower::lower_tree(parse::pipe::parse(&src));
        program.optimize();
        let name = std::path::Path::new(&path).file_stem().unwrap().to_string_lossy().to_string();
        let imports = program.root.imports.iter().map(|imp| match &imp.from {
            Source::Trace(t) => Some(Derived { trace: t.clone(), key: Term::Var(0), val: Term::Var(1), filtered: false }),
            _ => None,
        }).collect();
        survey(&program.root, imports, &name, &mut demand);
    }
    println!("trace\tkey\tfiltered\tuses\tprograms\tnames");
    for ((trace, key, filtered), (uses, programs)) in &demand.uses {
        println!("{trace}\t{key}\t{}\t{uses}\t{}\t{}", if *filtered { "yes" } else { "" }, programs.len(), programs.join(" "));
    }
}
