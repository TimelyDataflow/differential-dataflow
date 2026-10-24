//! Inputs and generated sources: a program's positional inputs, the `random:` and `iota:`
//! recipes, the `clock`, and installing them.

use super::*;

/// A container of corgi columns at an epoch, as an input takes them.
pub(super) type InputContainer = crate::runtime::container::CorgiContainer<OuterTime, Diff>;

/// The number of rows an input converts to columns at once, and a generator emits at once.
pub(super) const COLUMN_BATCH: usize = 1 << 16;

/// An installed program's positional `input N`, or a generated source's: an input of corgi
/// containers at one shape, the input's declared one or its source's.
pub(super) struct ServerInput {
    handle: timely::dataflow::operators::core::input::Handle<OuterTime, timely::container::CapacityContainerBuilder<InputContainer>>,
    /// The shape of every update the input holds.
    pub(super) shape: RowShape,
}

impl ServerInput {
    pub(super) fn new(
        handle: timely::dataflow::operators::core::input::Handle<OuterTime, timely::container::CapacityContainerBuilder<InputContainer>>,
        shape: RowShape,
    ) -> Self {
        ServerInput { handle, shape }
    }

    /// Add `recipe`'s rows at `indices`, each with multiplicity `diff` at `time`, as columns.
    pub(super) fn generate(&mut self, recipe: Recipe, indices: impl Iterator<Item = u64>, time: OuterTime, diff: Diff) {
        let mut indices = indices.peekable();
        let mut batch = Vec::with_capacity(COLUMN_BATCH);
        while indices.peek().is_some() {
            batch.clear();
            batch.extend(indices.by_ref().take(COLUMN_BATCH));
            self.give(recipe.container(&batch, time, diff));
        }
    }

    /// Add fed `columns` at `time`. Their shapes must be the input's, as its declaration says.
    pub(super) fn give_columns(&mut self, columns: Updates, time: OuterTime) -> Result<(), String> {
        let shape = (corgi::shape_of_value(&columns.keys), corgi::shape_of_value(&columns.vals));
        if shape != self.shape {
            return Err(format!("a feed of shape {} reached an input of shape {}", fmt_shape(&shape), fmt_shape(&self.shape)));
        }
        let Updates { keys, vals, diffs } = columns;
        let n = diffs.len();
        let times = crate::runtime::col_times::ColTimes::from_raw_lanes(vec![vec![time; n]], n);
        self.give(InputContainer { keys, vals, times, diffs });
        Ok(())
    }

    /// Add the updates of `container`, each at `time` whatever time it carries.
    pub(super) fn give_at(&mut self, container: HostContainer, time: OuterTime) {
        let HostContainer { keys, vals, times, diffs } = container;
        let n = times.len();
        let times = crate::runtime::col_times::ColTimes::from_raw_lanes(vec![vec![time; n]], n);
        self.give(InputContainer { keys, vals, times, diffs });
    }

    /// Send a container, which must have the input's shape.
    fn give(&mut self, mut container: InputContainer) {
        if !container.times.is_empty() {
            let shape = (corgi::shape_of_value(&container.keys), corgi::shape_of_value(&container.vals));
            assert!(shape == self.shape, "a container of shape {} reached an input of shape {}", fmt_shape(&shape), fmt_shape(&self.shape));
            self.handle.send_batch(&mut container);
        }
    }

    pub(super) fn advance_to(&mut self, time: OuterTime) {
        self.handle.advance_to(time);
    }

    pub(super) fn flush(&mut self) {
        self.handle.flush();
    }
}

/// A generated, content-addressed source. Importing such a name installs the
/// generator on demand, and two imports of the same recipe share one source.
#[derive(Clone, Copy)]
pub(super) enum Recipe {
    /// `random:nodes=N,edges=E[,arity=A][,seed=S][,churn=C]` — a deterministic
    /// random graph: a window of `E` rows of `A` fields, every field in `0..N`.
    /// Each tick replaces `C` rows (default zero).
    Random {
        nodes: u64,
        edges: u64,
        arity: usize,
        seed: u64,
        churn: u64,
    },
    /// `iota:N` — the rows `(0) .. (N-1)`, each a one-field `Tuple`. The minimal
    /// index source from which richer generators are derived in-language (with
    /// `hash`).
    Iota { n: u64 },
}

impl Recipe {
    /// Parse a recipe name, or `None` if it isn't one (then it's an ordinary
    /// trace lookup). Unknown keys, missing required keys, or non-numbers reject.
    pub(super) fn parse(name: &str) -> Option<Recipe> {
        if let Some(params) = name.strip_prefix("random:") {
            let (mut nodes, mut edges, mut arity, mut seed, mut churn) =
                (None, None, 2usize, 0u64, 0u64);
            for kv in params.split(',') {
                let (k, v) = kv.split_once('=')?;
                match k.trim() {
                    "nodes" => nodes = Some(v.trim().parse().ok()?),
                    "edges" => edges = Some(v.trim().parse().ok()?),
                    "arity" => arity = v.trim().parse().ok()?,
                    "seed" => seed = v.trim().parse().ok()?,
                    "churn" => churn = v.trim().parse().ok()?,
                    _ => return None,
                }
            }
            Some(Recipe::Random {
                nodes: nodes?,
                edges: edges?,
                arity,
                seed,
                churn,
            })
        } else if let Some(n) = name.strip_prefix("iota:") {
            Some(Recipe::Iota {
                n: n.trim().parse().ok()?,
            })
        } else {
            None
        }
    }

    /// The canonical name: fixed key order, defaults filled — so reorderings and
    /// omitted defaults address the same source.
    fn canonical(&self) -> String {
        match self {
            Recipe::Random {
                nodes,
                edges,
                arity,
                seed,
                churn,
            } => format!(
                "random:nodes={},edges={},arity={},seed={},churn={}",
                nodes, edges, arity, seed, churn
            ),
            Recipe::Iota { n } => format!("iota:{}", n),
        }
    }

    /// The number of rows the source contains.
    pub(super) fn rows_len(&self) -> u64 {
        match self {
            Recipe::Random { edges, .. } => *edges,
            Recipe::Iota { n } => *n,
        }
    }

    /// The shape of every row the source contains: `arity` integers for `random`, one for
    /// `iota`, and a unit value for both.
    pub(super) fn shape(&self) -> RowShape {
        use corgi::Shape;
        let key = match self {
            Recipe::Random { arity: 0, .. } => Shape::Unit,
            Recipe::Random { arity, .. } => Shape::Prod(vec![Shape::Int; *arity]),
            Recipe::Iota { .. } => Shape::Prod(vec![Shape::Int]),
        };
        (key, Shape::Unit)
    }

    /// The generated row at index `e`, as a feed writes it: what [`Recipe::container`] must hold,
    /// formed one row at a time.
    #[cfg(test)]
    fn row(&self, e: u64) -> (String, String) {
        let fields: Vec<String> = match self {
            Recipe::Random { nodes, arity, seed, .. } => (0..*arity).map(|col| gen_field_seeded(*seed, e, *nodes, col).to_string()).collect(),
            Recipe::Iota { .. } => vec![e.to_string()],
        };
        (format!("tuple({})", fields.join(",")), "tuple()".into())
    }

    /// The generated rows at `indices`, as columns of [`Recipe::shape`], each with multiplicity
    /// `diff` at `time`.
    fn container(&self, indices: &[u64], time: OuterTime, diff: Diff) -> InputContainer {
        use corgi::Value as CValue;
        let n = indices.len();
        let keys = match self {
            Recipe::Random { arity: 0, .. } => CValue::Unit(n),
            Recipe::Random { nodes, arity, seed, .. } => CValue::Prod(
                (0..*arity)
                    .map(|col| CValue::i64(indices.iter().map(|&e| gen_field_seeded(*seed, e, *nodes, col)).collect()))
                    .collect(),
            ),
            Recipe::Iota { .. } => CValue::Prod(vec![CValue::i64(indices.iter().map(|&e| e as i64).collect())]),
        };
        InputContainer {
            keys,
            vals: CValue::Unit(n),
            times: crate::runtime::col_times::ColTimes::from_raw_lanes(vec![vec![time; n]], n),
            diffs: vec![diff; n],
        }
    }
}

/// `clock` rows for these epochs, each with its multiplicity: `((t), ())`.
pub(super) fn clock_rows(rows: &[(OuterTime, Diff)]) -> Updates {
    Updates {
        keys: corgi::Value::Prod(vec![corgi::Value::int(rows.iter().map(|&(t, _)| t as i64))]),
        vals: corgi::Value::Unit(rows.len()),
        diffs: rows.iter().map(|&(_, diff)| diff).collect(),
    }
}

/// The shape of the `clock` row.
fn clock_shape() -> RowShape {
    (corgi::Shape::Prod(vec![corgi::Shape::Int]), corgi::Shape::Unit)
}

/// Deterministic hash: maps an index to a pseudorandom u64 (splitmix64).
fn hash_u64(index: u64) -> u64 {
    let mut x = index.wrapping_mul(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

/// Field `col` of the `random:` source's row `edge_index`: a hash-derived `Int` of magnitude below
/// `nodes`. Distinct seeds give distinct graphs.
pub fn gen_field_seeded(seed: u64, edge_index: u64, nodes: u64, col: usize) -> i64 {
    let base = edge_index.wrapping_mul(31).wrapping_add(seed.wrapping_mul(0x9e3779b97f4a7c15));
    (hash_u64(base.wrapping_add(col as u64)) % nodes) as i64
}

/// The shape of a generated source's rows, by its canonical name.
pub(super) fn generated_shape(name: &str) -> Option<RowShape> {
    if name == "clock" { Some(clock_shape()) } else { Recipe::parse(name).map(|r| r.shape()) }
}

/// Map a source name to its canonical form: a recipe canonicalizes, any other
/// name is returned unchanged. Used everywhere a source is looked up, so
/// generated sources are shared by content regardless of how they're spelled.
pub(super) fn canonical_source_name(name: &str) -> String {
    Recipe::parse(name)
        .map(|r| r.canonical())
        .unwrap_or_else(|| name.to_string())
}

impl Server {
    /// Install a generated source under its canonical `name`: a one-input
    /// dataflow pre-filled with the recipe's rows at time 0 and published as a
    /// trace. Content-addressed, so a later importer of the same recipe shares
    /// it; not writable (see `feed`); dropped like any program once unused.
    pub(super) fn install_generated(&mut self, worker: &mut Worker, name: &str, recipe: Recipe) {
        let (index, peers) = (worker.index(), worker.peers());
        let (mut input, dataflow_id, probe) = self.install_source(worker, name, recipe.shape());

        // Each worker emits its shard (e % peers == index) at time 0, so the
        // union is the full source exactly once.
        input.generate(recipe, (0..recipe.rows_len()).filter(|e| (*e as usize) % peers == index), 0, 1);
        input.advance_to(self.epoch);
        input.flush();

        let mut inputs = HashMap::new();
        inputs.insert(0usize, input);
        self.programs.insert(
            name.to_string(),
            Installed {
                inputs,
                imports: Vec::new(),
                exports: vec![name.to_string()],
                dataflow_id,
                probe,
                origin: Origin::Generated,
                generators: HashMap::from([(0usize, (recipe, 0u64))]),
            },
        );
    }

    /// Install the `clock` source: a single row holding the current epoch, which
    /// advances by one each `tick` (an O(1) change, not an O(n) regeneration).
    /// Produced on worker 0 only; `tick` advances it (see [`Server::tick`]).
    pub(super) fn install_clock(&mut self, worker: &mut Worker) {
        let w0 = worker.index() == 0;
        let (mut input, dataflow_id, probe) = self.install_source(worker, "clock", clock_shape());

        if w0 {
            input.give_columns(clock_rows(&[(self.epoch, 1)]), self.epoch).expect("clock rows have the clock's shape");
        }
        input.advance_to(self.epoch);
        input.flush();

        let mut inputs = HashMap::new();
        inputs.insert(0usize, input);
        self.programs.insert(
            "clock".to_string(),
            Installed {
                inputs,
                imports: Vec::new(),
                exports: vec!["clock".to_string()],
                dataflow_id,
                probe,
                origin: Origin::Clock,
                generators: HashMap::new(),
            },
        );
    }

    /// Build a source's dataflow, one input arranged and published as trace `name` of `shape`, as
    /// corgi chunks. Returns the input, the dataflow's id, and the probe on its arrangement.
    pub(super) fn install_source(&mut self, worker: &mut Worker, name: &str, shape: RowShape) -> (ServerInput, usize, ProbeHandle<HostTime>) {
        let probe = ProbeHandle::new();
        let dataflow_id = worker.next_dataflow_index();
        self.note_dataflow(dataflow_id, name);
        let (handle, trace) = host_dataflow(worker, |outer, scope| {
            use timely::dataflow::operators::core::Input as _;
            use timely::dataflow::operators::Probe;
            use differential_dataflow::AsCollection;
            let (handle, stream) = outer.new_input::<InputContainer>();
            let arranged = crate::backend::corgi::arrange_export(stream.as_collection().enter(scope));
            arranged.stream.probe_with(&probe);
            (handle, arranged.trace)
        });
        self.traces.insert(name.to_string(), trace);
        let input = ServerInput::new(handle, shape.clone());
        self.shapes.insert(name.to_string(), shape);
        (input, dataflow_id, probe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A recipe's columns hold the rows it generates, at its shape.
    #[test]
    fn recipe_columns_are_its_rows() {
        let recipes = ["random:nodes=10,edges=50,arity=0", "random:nodes=10,edges=50", "random:nodes=7,edges=50,arity=3,seed=5", "iota:50"];
        for name in recipes {
            let recipe = Recipe::parse(name).unwrap();
            let indices: Vec<u64> = (0..recipe.rows_len()).filter(|e| e % 3 != 1).collect();
            let container = recipe.container(&indices, 4, -1);
            let (k, v) = recipe.shape();
            assert_eq!((corgi::shape_of_value(&container.keys), corgi::shape_of_value(&container.vals)), (k, v), "{name}");
            let rows: Vec<_> = indices.iter().map(|&e| (recipe.row(e), 4, -1)).collect();
            assert_eq!(container.into_updates(), rows, "{name}");
        }
    }
}
