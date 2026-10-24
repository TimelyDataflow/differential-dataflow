//! Feeding and loading inputs: rows as text or columns, bulk loads from recipes, files and
//! traces, and the checks they share.

use super::*;

impl Server {
    /// Stage rows written as text into positional input `input` of installed program `prog`, at
    /// `time` (default the current epoch), which must not be in the closed past. Each line of
    /// `rows` is one row, `<key> [val=<v>] [diff=<d>]`, its values read at the input's declared
    /// shapes as a feed reads them ([`crate::runtime::feed`]); blank lines and `#` comments are
    /// skipped. Nothing is staged if any row is not of those shapes. The rows take effect once a
    /// `tick` advances the input past `time`.
    pub fn feed(&mut self, prog: &str, input: usize, rows: &str, time: Option<OuterTime>) -> Result<(), String> {
        self.validate_feed(prog, input, time.unwrap_or(self.epoch))?;
        let (key, val) = &self.programs[&canonical_source_name(prog)].inputs[&input].shape;
        let mut decoder = crate::runtime::feed::FeedDecoder::new(key, val);
        for (number, line) in rows.lines().map(str::trim).enumerate() {
            if !line.is_empty() && !line.starts_with('#') {
                decoder.push_line(line).map_err(|error| format!("row {}: {error}", number + 1))?;
            }
        }
        self.feed_columns(prog, input, decoder.finish(), time)
    }

    /// Stage rows already read into columns at `time` (default the current epoch), each with
    /// its own multiplicity. Their shapes must be the input's declared shapes; nothing is staged
    /// otherwise.
    pub fn feed_columns(&mut self, prog: &str, input: usize, columns: Updates, time: Option<OuterTime>) -> Result<(), String> {
        let time = time.unwrap_or(self.epoch);
        self.validate_feed(prog, input, time)?;
        self.programs
            .get_mut(&canonical_source_name(prog))
            .expect("feed target was prevalidated")
            .inputs
            .get_mut(&input)
            .expect("feed input was prevalidated")
            .give_columns(columns, time)
    }

    /// Bulk-load rows into positional `input` of `prog` at the current epoch.
    ///
    /// `source` is a recipe (`random:…`, `iota:N` — the same names an
    /// `import` accepts), a published trace, or the path of a text file with one
    /// row per line of whitespace-separated integers, the fields of the input's
    /// key (its value is unit), read into columns at the input's declared shape.
    /// Collective: every worker must call this, and each feeds only its shard
    /// (`row % peers == index`) through its own handle, so the union is the
    /// source exactly once and the exchange places each row on its key's owner.
    /// Each row enters with its multiplicity times `diff`.
    ///
    /// A trace is read as its consolidated contents before the current epoch,
    /// each worker reading its own share of the arrangement directly, with no
    /// dataflow and no exchange (the input's own exchange routes the rows).
    /// `feed bank 0 from retire diff=-1` is the maintenance idiom: a program
    /// exports the rows it no longer needs, and the server deletes them from its
    /// input without a client round trip.
    ///
    /// A `random:` recipe with `churn=C` keeps churning: every later `tick`
    /// retracts the next `C` rows of the window and adds `C` fresh ones, the
    /// standing-change regime a program is benchmarked under. Returns the
    /// number of rows in the source (across all workers), or for a trace the
    /// number in this worker's share.
    ///
    /// A file is validated in full on every worker before any of its rows is
    /// applied: a malformed line fails the load on every worker, so a source
    /// is fed whole or not at all, and the workers agree because they read the
    /// same file (the source must be visible, and identical, to all of them).
    pub fn load(
        &mut self,
        worker: &Worker,
        prog: &str,
        input: usize,
        source: &str,
        diff: Diff,
    ) -> Result<u64, String> {
        let time = self.epoch;
        self.validate_feed(prog, input, time)?;
        if self.loads_from_trace(source) {
            return self.load_trace(prog, input, source, diff);
        }
        let (index, peers) = (worker.index(), worker.peers());
        let mine = |e: u64| (e as usize) % peers == index;
        let recipe = Recipe::parse(source);
        let target = &self.programs[&canonical_source_name(prog)].inputs[&input];
        if let Some(recipe) = recipe {
            if recipe.shape() != target.shape {
                return Err(format!(
                    "program {:?} declares input {} as {} but {:?} generates {}",
                    prog, input, fmt_shape(&target.shape), source, fmt_shape(&recipe.shape())
                ));
            }
        }
        // A recipe's rows are generated into the input below; a file's are read here.
        let (total, rows) = match recipe {
            Some(recipe) => (recipe.rows_len(), None),
            None => {
                let text = std::fs::read_to_string(source)
                    .map_err(|e| format!("load: cannot read {:?}: {}", source, e))?;
                // Every line is read, whichever worker's shard it falls in: a
                // malformed line must fail the load everywhere, or the other
                // workers would apply their shards and the client would see
                // "loaded" over partial data. Only this worker's lines are
                // kept; the others are read into a scratch decoder, emptied
                // as it fills, so that holding them does not grow with the file.
                use crate::runtime::feed::FeedDecoder;
                let (key, val) = &target.shape;
                let (mut rows, mut others) = (FeedDecoder::new(key, val), FeedDecoder::new(key, val));
                let mut total = 0;
                for (e, line) in text.lines().filter(|l| !l.trim().is_empty()).enumerate() {
                    let fields = line.split_whitespace().collect::<Vec<_>>().join(",");
                    let decoder = if mine(e as u64) { &mut rows } else { &mut others };
                    decoder.push(&fields, None, diff).map_err(|error| format!("load: line {} of {:?}: {error}", e + 1, source))?;
                    if others.len() >= COLUMN_BATCH {
                        others = FeedDecoder::new(key, val);
                    }
                    total += 1;
                }
                (total, Some(rows.finish()))
            }
        };
        let installed = self
            .programs
            .get_mut(&canonical_source_name(prog))
            .expect("load target was prevalidated");
        if let Some(recipe @ Recipe::Random { churn: 1.., .. }) = recipe {
            if diff != 1 {
                return Err(format!("load: a churning recipe {:?} must be fed with diff=1", source));
            }
            installed.generators.insert(input, (recipe, 0));
        }
        let handle = installed.inputs.get_mut(&input).expect("load input was prevalidated");
        if let Some(recipe) = recipe {
            handle.generate(recipe, (0..recipe.rows_len()).filter(|e| mine(*e)), time, diff);
        }
        if let Some(rows) = rows {
            handle.give_columns(rows, time)?;
        }
        Ok(total)
    }

    /// Whether [`Server::load`] reads `source` as a published trace (and so counts only this
    /// worker's share of its rows), rather than as a recipe or a file.
    pub fn loads_from_trace(&self, source: &str) -> bool {
        Recipe::parse(source).is_none() && self.is_published(&canonical_source_name(source))
    }

    /// [`Server::load`] from published trace `source`: this worker's share of its contents
    /// before the current epoch, scaled by `diff`, into its own handle for `prog`'s `input`.
    fn load_trace(&mut self, prog: &str, input: usize, source: &str, diff: Diff) -> Result<u64, String> {
        let name = canonical_source_name(source);
        let declared = &self.programs[&canonical_source_name(prog)].inputs[&input].shape;
        let shape = self.shapes[&name].clone();
        if *declared != shape {
            return Err(format!(
                "program {:?} declares input {} as {} but trace {:?} is published as {}",
                prog, input, fmt_shape(declared), name, fmt_shape(&shape)
            ));
        }
        let mut updates = self.snapshot_local(&name)?;
        // Every check above reads state all workers share, so they agree on it. Only scaling
        // reads this worker's rows; an overflow there cannot be refused on every worker alike.
        for d in updates.diffs.iter_mut() {
            *d = d.checked_mul(diff).unwrap_or_else(|| {
                panic!("feed from {name:?}: multiplicity {d} overflows when scaled by {diff}")
            });
        }
        let rows = updates.len() as u64;
        self.programs
            .get_mut(&canonical_source_name(prog))
            .expect("load target was prevalidated")
            .inputs
            .get_mut(&input)
            .expect("load input was prevalidated")
            .give_columns(updates, self.epoch)?;
        Ok(rows)
    }

    /// Check everything about an input target that can fail without changing
    /// its handle. Both singular and batched feeds validate before applying.
    fn validate_feed(&self, prog: &str, input: usize, time: OuterTime) -> Result<(), String> {
        if time < self.epoch {
            return Err(format!(
                "cannot feed at time {} < current epoch {}",
                time, self.epoch
            ));
        }
        let prog = canonical_source_name(prog);
        let installed = self
            .programs
            .get(&prog)
            .ok_or_else(|| format!("no program {:?}", prog))?;
        if installed.origin != Origin::Program {
            let kind = if installed.origin == Origin::Clock {
                "clock"
            } else {
                "generated"
            };
            return Err(format!(
                "{:?} is a {} source and is not writable",
                prog, kind
            ));
        }
        if !installed.inputs.contains_key(&input) {
            return Err(format!("program {:?} has no input {}", prog, input));
        }
        Ok(())
    }
}
