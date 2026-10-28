//! Reduce sugar and composition. Every reducer denotes a lambda over `(key, vals)`;
//! `vals` is nonempty, ordered, and holds `(value, nonzero diff)` pairs.

use crate::ir::{Lambda, Reducer};
use crate::lambda::Projection;

/// Distinct after its input has been keyed by the whole record.
pub(crate) fn unit_distinct() -> Lambda {
    Lambda::new("let (_, vals) = input in vals map (((), _) -> ((), 1))".into(), &corgi::Scope::default())
}

/// Finish lowering distinct once the tree's shapes are known. A unit-valued input
/// is already keyed by its whole record. Otherwise write the record key and its
/// inverse into the IR, where arrangements, projections and readers can see them.
pub(crate) fn expand_distinct(
    scope: &mut crate::scope_ir::Scope,
    shapes: &crate::shapes::ScopeShapes,
    pending: &std::collections::HashMap<Vec<String>, Vec<usize>>,
    path: &mut Vec<String>,
) {
    use crate::scope_ir::{Item, Node, Ref, Source};
    use crate::ir::LinearOp;
    let sites = pending.get(path).map_or(&[][..], Vec::as_slice);
    let mut sites = sites.iter().copied().peekable();
    let mut remap = Vec::new();
    let mut items = Vec::new();
    let mut labels = Vec::new();
    let remap_ref = |r: &mut Ref, remap: &[usize]| {
        if let Ref::Local(i) | Ref::ChildExport(i, _) = r { *i = remap[*i]; }
    };
    for (i, mut item) in std::mem::take(&mut scope.items).into_iter().enumerate() {
        let distinct = sites.peek() == Some(&i);
        if distinct { sites.next(); }
        let by_record = distinct && match &item {
            Item::Op(Node::Reduce { input, .. }) => !shapes.of(input).is_some_and(|s| s.val == corgi::Shape::Unit),
            _ => false,
        };
        match &mut item {
            Item::Op(node) => node.for_each_ref(|r| remap_ref(r, &remap)),
            Item::Sub(child) => {
                let crate::shapes::ItemShapes::Sub(shapes) = &shapes.items[i] else { unreachable!() };
                path.push(child.name.clone());
                expand_distinct(child, shapes, pending, path);
                path.pop();
                for import in &mut child.imports {
                    if let Source::Parent(r) = &mut import.from { remap_ref(r, &remap); }
                }
            }
        }
        let mut push = |node| {
            let r = Ref::Local(items.len());
            items.push(Item::Op(node));
            labels.push(scope.labels[i].clone());
            r
        };
        if distinct {
            let Item::Op(Node::Reduce { input, .. }) = item else { unreachable!() };
            let input = if by_record {
                push(Node::Linear { input, ops: vec![LinearOp::Project(Lambda::new("let (k, v) = input in ((k, v), ())".into(), &corgi::Scope::default()))] })
            } else { input };
            // Both paths now have unit values: either they were known to be unit,
            // or the map above keyed the collection by its whole record.
            let reducer = Reducer::Expr(unit_distinct());
            let input = push(Node::Arrange(input));
            let output = push(Node::Reduce { input, reducer });
            if by_record {
                push(Node::Linear { input: output, ops: vec![LinearOp::Project(Lambda::new("let (kv, ()) = input in kv".into(), &corgi::Scope::default()))] });
            }
        } else {
            items.push(item);
            labels.push(scope.labels[i].clone());
        }
        remap.push(items.len() - 1);
    }
    for bind in &mut scope.binds { remap_ref(&mut bind.value, &remap); }
    for export in &mut scope.exports { remap_ref(&mut export.value, &remap); }
    for (_, value) in &mut scope.names { remap_ref(value, &remap); }
    scope.items = items;
    scope.labels = labels;
}

impl crate::parse::Reducer {
    /// The definitions of the four sugars. Empty keys are suppressed by reduce itself.
    pub(crate) fn lambda(&self) -> Lambda {
        let body = match self {
            Self::Min => "let first = vals head in (first.0, 1) enlist",
            Self::Distinct => "vals map ((v, _) -> (v, 1))",
            Self::Count => "let diffs = vals map ((_, d) -> d) in let n = diffs fold_add in (n, (n, 1)) enlist filter",
            Self::Collect => "let lists = vals map ((v, d) -> (v, (0, d) range) cap_list map ((v, _) -> v)) in let flat = lists flatten in (flat.1, 1) enlist",
            Self::Expr(lambda) => return lambda.clone(),
        };
        Lambda::new(format!("let (_, vals) = input in {body}"), &corgi::Scope::default())
    }
}

impl Reducer {
    pub(crate) fn lambda(&self) -> Lambda {
        let Self::Expr(lambda) = self;
        lambda.clone()
    }

    /// Apply a projection to each output value, preserving its diff and the grouping key.
    pub(crate) fn project(&self, projection: &Projection, key: &corgi::Shape, val: &corgi::Shape) -> Self {
        let base = self.lambda();
        let mut composed = Lambda::compose("arg", &[(&base, "arg".into())], |names| {
            format!("{} map ((_value, _diff) -> ({}, _diff))", names[0], projection.text("_value"))
        });
        // Source is only a spelling. Keep the composed graph, and accept the graph
        // printer's spelling only when reparsing gives that exact optimized graph.
        let input = corgi::Shape::Prod(vec![key.clone(), corgi::Shape::List(Box::new(corgi::Shape::Prod(vec![val.clone(), corgi::Shape::Int])))]);
        let spelling = (|| {
            let printed = crate::lambda::print::print_composed(&composed, &input);
            if printed.fallback { return None; }
            let (pattern, body) = printed.text.split_once(" -> ")?;
            let src = format!("let {pattern} = input in {body}");
            let mut words = crate::lambda::base_scope();
            for f in crate::lambda::registered() { words.word(f.name(), crate::lambda::function_word(&f)).ok()?; }
            let parsed = corgi::parse_ml_in(&src, &words).ok()?;
            let graph = composed.graph().ok()?;
            if corgi::shape_of(&parsed, &input).ok()? != corgi::shape_of(graph, &input).ok()?
                || corgi::optimize(&parsed) != corgi::optimize(graph) { return None; }
            Some(src)
        })();
        if let Some(src) = spelling { composed.src = src; }
        Self::Expr(composed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::Reducer as Sugar;
    use corgi::{Bounds, Shape, Value};

    #[test]
    fn composed_spelling_preserves_the_exact_graph() {
        let base = Sugar::Min.lambda();
        let projection = Projection::Tuple(vec![Projection::Unit, Projection::Field(vec![1])]);
        let mechanical = Lambda::compose("arg", &[(&base, "arg".into())], |names| {
            format!("{} map ((_value, _diff) -> ({}, _diff))", names[0], projection.text("_value"))
        });
        let Reducer::Expr(readable) = Reducer::Expr(base).project(&projection, &Shape::Int, &Shape::Prod(vec![Shape::Int, Shape::Int]));
        assert!(!readable.src.contains("_call0"), "{}", readable.src);
        assert!(readable.src.contains("head"));
        assert_eq!(readable.graph().unwrap(), mechanical.graph().unwrap());
        let parsed = Lambda::new(readable.src.clone(), &corgi::Scope::default());
        assert_eq!(corgi::optimize(readable.graph().unwrap()), corgi::optimize(parsed.graph().unwrap()));
        // A shallow spelling would drop this authored nested pattern check.
        let base = Lambda::new("let ((k, _), vals) = input in (vals head.0, 1) enlist".into(), &corgi::Scope::default());
        let Reducer::Expr(kept) = Reducer::Expr(base).project(&projection, &Shape::Prod(vec![Shape::Int, Shape::Int]), &Shape::Prod(vec![Shape::Int, Shape::Int]));
        assert!(kept.src.contains("_call0"));
    }

    #[test]
    fn definitions_and_projections_round_trip() {
        let shape = Shape::Prod(vec![Shape::Int, Shape::Int]);
        for sugar in [Sugar::Min, Sugar::Distinct, Sugar::Count, Sugar::Collect] {
            let lambda = sugar.lambda();
            crate::lambda::compile_reduce(&lambda, &Shape::Int, &shape).unwrap();
            let projection = if matches!(sugar, Sugar::Min | Sugar::Distinct) {
                Projection::Tuple(vec![Projection::Unit, Projection::Field(vec![1])])
            } else { Projection::Unit };
            let Reducer::Expr(lambda) = Reducer::Expr(sugar.lambda()).project(&projection, &Shape::Int, &shape);
            let reparsed = Lambda::new(lambda.src.clone(), &corgi::Scope::default());
            let running = crate::lambda::compile_reduce(&lambda, &Shape::Int, &shape).unwrap();
            let printed = crate::lambda::compile_reduce(&reparsed, &Shape::Int, &shape).unwrap();
            assert_eq!(running, printed, "{sugar:?}: {}", lambda.src);
            // The spelling emitted by ddir-plan is already valid DDP. Replanning it
            // must not add a mask to the output that was narrowed in the expression.
            let text = format!("export \"out\" = input 0 : (int, (int, int)) | reduce(input -> {});", lambda.src);
            let mut program = crate::lower::lower_tree(crate::parse::pipe::parse(&text));
            program.optimize();
            assert_eq!(crate::projection::pushdown(&mut program, &|_| None).unwrap(), 0);
        }
        // Nonempty input with zero total count emits nothing; a negative total is retained.
        let lambda = crate::lambda::compile_reduce(&Sugar::Count.lambda(), &Shape::Int, &Shape::Int).unwrap();
        let input = Value::Prod(vec![Value::i64(vec![1, 2]), Value::List(Bounds::offsets(vec![2, 3]), Box::new(Value::Prod(vec![
            Value::i64(vec![10, 20, 30]), Value::i64(vec![1, -1, -2]),
        ])))]);
        let (bounds, values) = corgi::eval_graph(&lambda, input).into_list("count").unwrap();
        assert_eq!(bounds.to_vec(), vec![0, 1]);
        assert_eq!(values, Value::Prod(vec![Value::i64(vec![-2]), Value::i64(vec![1])]));
    }

}
