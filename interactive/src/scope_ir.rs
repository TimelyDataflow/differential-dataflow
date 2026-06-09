//! Scope-tree IR: a tree of scopes, each owning its IR, with cross-scope flow
//! as explicit import/export edges and feedback variables first-class.
//! `lower::lower_tree` (AST -> tree) builds it; the backends' `render_tree`
//! (tree -> dataflow) walk it.
//!
//! # Why a tree
//!
//! A scope boundary is a real semantic barrier: an operator cannot be hoisted
//! across it, and an arrangement cannot be shared across it freely. The flat
//! IR encodes boundaries *positionally* (`Scope`/`EndScope` markers in a node
//! list), so every consumer that cares reconstructs them by analysis — and
//! that reconstruction is where the explanation rewrite's level errors lived.
//! Here the boundary is structural: a `Scope` owns its items, and nothing
//! crosses except through an explicit import or export.
//!
//! The guiding principle throughout: make explicit anything a consumer would
//! otherwise have to analyze the IR to recover. Feedback variables are a
//! first-class list (the renderer never scans for them), `items` is in
//! dependency order (rendering is a straight walk, no topological pass), and
//! cross-scope references are edges (no reconstruction of who-refers-to-whom).
//!
//! # Two axes of a scope
//!
//! Structurally, every `{ .. }` renders as a timely region (`enter_region` /
//! `leave_region`): real dataflow nesting, timestamp *type* unchanged.
//! Temporally, an *iterating* scope additionally pushes a `PointStamp`
//! coordinate (`enter_dynamic` / `leave_dynamic`): a timestamp *value*, the
//! iteration counter. A scope iterates iff it has `vars`; there is no
//! separate kind field to fall out of sync with that. When acyclic scopes
//! earn distinct (region-only) rendering, `Scope` becomes an enum so each
//! variant carries only its valid fields.
//!
//! # Cross-scope names
//!
//! Names are local to a scope; the front-end's spanning names (`edges`,
//! `scope::field`) are resolved by lowering into local handles plus edges.
//! Identity is the *definition an edge points at*, not string equality: two
//! imports targeting the same definition are the same entity, so facts
//! learned at a definition propagate to every importer along its edges.

use crate::ir::LinearOp;
use crate::parse::{Projection, Reducer};

pub type ItemId = usize; // index into `Scope::items`
pub type ImportId = usize; // index into `Scope::imports`
pub type VarId = usize; // index into `Scope::vars`

/// A reference, resolved *within its own scope*. Nothing here can name an item
/// in another scope's interior — cross-scope flow goes through imports/exports.
#[derive(Clone, Debug)]
pub enum Ref {
    /// An earlier `Op` item in this scope.
    Local(ItemId),
    /// A value entered via this scope's imports.
    Import(ImportId),
    /// A feedback variable of this scope.
    Var(VarId),
    /// Export `usize` of a `Sub` item (a child scope) in this scope.
    ChildExport(ItemId, usize),
}

/// Where an import's value comes from. Inner scopes import from the parent; the
/// root imports the program's external sources — unifying `Input`/`Import`.
#[derive(Clone, Debug)]
pub enum Source {
    /// A reference in the parent scope.
    Parent(Ref),
    /// A positional program input (root only).
    Input(usize),
    /// A named external trace (root only).
    Trace(String),
}

/// A value brought into a scope — `enter_region` (+ `enter_dynamic` if iterating).
#[derive(Clone, Debug)]
pub struct Import {
    pub name: String,
    pub from: Source,
}

/// A value surrendered up — `leave_region` (+ `leave_dynamic` if iterating).
#[derive(Clone, Debug)]
pub struct Export {
    pub name: String,
    pub value: Ref,
}

/// A feedback variable, identified by its index in `Scope::vars`. Carries its
/// source name (readability, cross-scope name visibility). Its shape is *not*
/// stored — the shape pass derives every node/var shape when a consumer needs
/// it, so storing it here would cache a derivable (and currently unknown) value.
#[derive(Clone, Debug)]
pub struct Var {
    pub name: String,
}

/// Closes a feedback loop: `var <- value`.
#[derive(Clone, Debug)]
pub struct Bind {
    pub var: VarId,
    pub value: Ref,
}

/// A pure operator. Sources (`Input`/`Import`) are imports; structure
/// (`Scope`/`Variable`/`Leave`/`Bind`) is the scope itself — neither is a node.
#[derive(Clone, Debug)]
pub enum Node {
    Linear { input: Ref, ops: Vec<LinearOp> },
    Concat(Vec<Ref>),
    Arrange(Ref),
    Join { left: Ref, right: Ref, projection: Projection },
    Reduce { input: Ref, reducer: Reducer },
    Inspect { input: Ref, label: String },
}

/// An item in a scope's body: a pure operator or a nested child scope. The
/// `items` vec is in dependency order, so rendering is a straight walk — no
/// topological re-analysis. `Ref::Local`/`ChildExport` index into it.
#[derive(Clone, Debug)]
pub enum Item {
    Op(Node),
    Sub(Scope),
}

/// A scope owns its IR. It iterates iff it has `vars` (no `kind` field yet; see
/// the design doc). Children are owned, inline in `items`.
#[derive(Clone, Debug, Default)]
pub struct Scope {
    /// The source name of the `{ .. }` block ("root" for the program root).
    /// Names the timely region at render time and labels the scope in viz.
    pub name: String,
    pub imports: Vec<Import>,
    pub vars: Vec<Var>,
    pub items: Vec<Item>,
    pub binds: Vec<Bind>,
    pub exports: Vec<Export>,
}

/// A program is its root scope: root imports are the external sources, root
/// exports are the program's outputs.
#[derive(Clone, Debug)]
pub struct Program {
    pub root: Scope,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Express a reach-shaped program by hand: it exercises items (Op + Sub),
    // every `Ref` variant, and both relevant `Source` variants — proving the
    // shape can represent a real scoped, iterative program.
    #[test]
    fn expresses_a_scoped_iterative_program() {
        let noproj = Projection { key: vec![], val: vec![] };
        let inner = Scope {
            name: "reach".into(),
            imports: vec![
                Import { name: "edges".into(), from: Source::Parent(Ref::Import(0)) },
                Import { name: "roots".into(), from: Source::Parent(Ref::Import(1)) },
            ],
            vars: vec![Var { name: "reach".into() }],
            items: vec![
                // proposals = reach JOIN edges  — references Var + Import
                Item::Op(Node::Join { left: Ref::Var(0), right: Ref::Import(0), projection: noproj.clone() }),
                // body = roots + proposals       — references Import + Local
                Item::Op(Node::Concat(vec![Ref::Import(1), Ref::Local(0)])),
            ],
            binds: vec![Bind { var: 0, value: Ref::Local(1) }], // reach <- body
            exports: vec![Export { name: "reach".into(), value: Ref::Var(0) }],
        };
        let root = Scope {
            name: "root".into(),
            imports: vec![
                Import { name: "in0".into(), from: Source::Input(0) },
                Import { name: "in1".into(), from: Source::Input(1) },
            ],
            items: vec![Item::Sub(inner)], // item 0 = the reach sub-scope
            exports: vec![Export { name: "result".into(), value: Ref::ChildExport(0, 0) }],
            ..Scope::default()
        };
        let prog = Program { root };

        assert_eq!(prog.root.items.len(), 1);
        assert!(matches!(prog.root.items[0], Item::Sub(_)));
        let Item::Sub(reach) = &prog.root.items[0] else { unreachable!() };
        assert_eq!(reach.vars.len(), 1);
        assert!(matches!(prog.root.exports[0].value, Ref::ChildExport(0, 0)));
        assert!(matches!(prog.root.imports[0].from, Source::Input(0)));
        assert!(matches!(reach.binds[0].value, Ref::Local(1)));
    }
}
