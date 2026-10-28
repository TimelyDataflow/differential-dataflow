//! Print the parsed collection AST, before lowering inserts arrangements or sorts statements.

use super::*;

impl Printer<'_> {
    pub(super) fn statements(&mut self, statements: &[Stmt], depth: usize, out: &mut String) -> Result<(), String> {
        let indent = "    ".repeat(depth);
        for stmt in statements {
            match stmt {
                Stmt::Let(name, expr) | Stmt::Set(name, expr) => {
                    let word = if matches!(stmt, Stmt::Let(..)) { "let" } else { "set" };
                    writeln!(out, "{indent}{word} {name} = {};", self.expression(expr)?).unwrap();
                }
                Stmt::Var(name, shape) => {
                    let shape = self.declaration(shape.as_ref())?;
                    writeln!(out, "{indent}var {name}{shape};").unwrap();
                }
                Stmt::Export(name, shape, expr) => {
                    let shape = self.declaration(shape.as_ref())?;
                    writeln!(out, "{indent}export {}{shape} = {};", quoted(name)?, self.expression(expr)?).unwrap();
                }
                Stmt::Scope(name, body) => {
                    writeln!(out, "{indent}{name}: {{").unwrap();
                    self.statements(body, depth + 1, out)?;
                    writeln!(out, "{indent}}}").unwrap();
                }
            }
        }
        Ok(())
    }

    fn declaration(&mut self, shape: Option<&(Shape, Shape)>) -> Result<String, String> {
        shape.map(|s| self.row(s).map(|s| format!(" : {s}"))).transpose().map(Option::unwrap_or_default)
    }

    fn parsed_lambda(&mut self, lambda: &Lambda) -> Result<String, String> {
        let source = qualify(&lambda.src, &self.context.types);
        let parsed = corgi::parse_ml_in(&source, &self.context.scope)?;
        if corgi::optimize(&parsed) != corgi::optimize(lambda.graph()?) {
            return Err("parsed lambda source does not reproduce its graph".into());
        }
        self.counts.source += 1;
        Ok(slot_text(&source))
    }

    fn atom(&mut self, expr: &Expr) -> Result<String, String> {
        let text = self.expression(expr)?;
        Ok(match expr {
            Expr::Input(_) | Expr::Import(_) | Expr::TypedSource(..) | Expr::Name(_) | Expr::Path(..) => text,
            _ => format!("({text})"),
        })
    }

    fn expression(&mut self, expr: &Expr) -> Result<String, String> {
        Ok(match expr {
            Expr::Input(n) => format!("input {n}"),
            Expr::Import(name) => format!("import {}", quoted(name)?),
            Expr::TypedSource(source, key, val) => format!("{} : {}", self.expression(source)?, self.row(&(key.clone(), val.clone()))?),
            Expr::Name(name) => name.clone(),
            Expr::Path(parts, absolute) => format!("{}{}", if *absolute { "::" } else { "" }, parts.join("::")),
            Expr::ResolvedPath { .. } => return Err("parsed view unexpectedly contains a lowered path".into()),
            Expr::Map(input, l) | Expr::Filter(input, l) | Expr::EnterAt(input, l)
            | Expr::FlatMap(input, l) | Expr::JoinFunction(input, l) => {
                let word = match expr {
                    Expr::Map(..) => "map", Expr::Filter(..) => "filter", Expr::EnterAt(..) => "enter_at",
                    Expr::FlatMap(..) => "flatmap", _ => "join_function",
                };
                format!("{} | {word}({})", self.expression(input)?, self.parsed_lambda(l)?)
            }
            Expr::Join(left, right, l) => format!("{} | join({}, {})", self.expression(left)?, self.expression(right)?, self.parsed_lambda(l)?),
            Expr::HalfJoin(stream, arrangement, strict, l) => format!("{} | half_join({}, {}, {})", self.expression(stream)?, self.expression(arrangement)?, u8::from(*strict), self.parsed_lambda(l)?),
            Expr::Reduce(input, reducer) => {
                let lambda = if matches!(reducer, crate::parse::Reducer::Distinct) {
                    self.parsed_lambda(&crate::reducers::unit_distinct())?
                } else { self.parsed_lambda(&reducer.lambda())? };
                let input = self.expression(input)?;
                if matches!(reducer, crate::parse::Reducer::Distinct) {
                    // Parsed syntax has no inferred shapes. Key by the whole record,
                    // which also preserves unit-valued distinct's meaning.
                    format!("{input} | map((k, v) -> ((k, v), ())) | reduce({lambda}) | map((kv, ()) -> kv)")
                } else {
                    format!("{input} | reduce({}{lambda})", super::limit_text(reducer.limit()))
                }
            }
            Expr::Negate(input) | Expr::Lift(input) | Expr::LiftIter(input) | Expr::Arrange(input) => {
                let word = match expr { Expr::Negate(_) => "negate", Expr::Lift(_) => "lift", Expr::LiftIter(_) => "lift_iter", _ => "arrange" };
                format!("{} | {word}", self.expression(input)?)
            }
            Expr::Inspect(input, label) => format!("{} | inspect({label})", self.expression(input)?),
            Expr::Concat(parts) => parts.iter().map(|part| self.atom(part)).collect::<Result<Vec<_>, _>>()?.join(" + "),
        })
    }
}
