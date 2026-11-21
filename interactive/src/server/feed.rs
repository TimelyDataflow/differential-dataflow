//! Feeding and loading inputs: rows as text or columns, bulk loads from files and
//! traces, and the checks they share.

use super::*;

impl Server {
    /// Stage rows written as text into the input labelled `input` of installed program `prog`, at
    /// `time`, which must not be in the closed past. Each line of
    /// `rows` is one row, `<key> [val=<v>] [diff=<d>]`, its values read at the input's declared
    /// shapes as a feed reads them ([`crate::runtime::feed`]); blank lines and `#` comments are
    /// skipped. Nothing is staged if any row is not of those shapes. The rows take effect once the
    /// output's write frontier advances past `time`.
    pub fn feed(&mut self, prog: &str, input: impl Into<InputId>, rows: &str, time: OuterTime) -> Result<(), String> {
        self.stage_rows(prog, &input.into(), rows, &host_time(time))
    }

    /// Convenience for an open input with a single write capability.
    pub fn feed_now(&mut self, prog: &str, input: impl Into<InputId>, rows: &str) -> Result<(), String> {
        let input = input.into();
        let time = self.now(prog, &input)?.clone();
        self.stage_rows(prog, &input, rows, &time)
    }

    /// Feed rows at a point of the target program's declared root width.
    pub fn feed_at(&mut self, prog: &str, input: impl Into<InputId>, rows: &str, time: &HostTime) -> Result<(), String> {
        self.stage_rows(prog, &input.into(), rows, time)
    }

    fn stage_rows(&mut self, prog: &str, input: &InputId, rows: &str, time: &HostTime) -> Result<(), String> {
        self.validate_feed(prog, input, time)?;
        let (key, val) = &self.programs[prog].inputs[input].shape;
        let mut decoder = crate::runtime::feed::FeedDecoder::new(key, val);
        for (number, line) in rows.lines().map(str::trim).enumerate() {
            if !line.is_empty() && !line.starts_with('#') {
                decoder.push_line(line).map_err(|error| format!("row {}: {error}", number + 1))?;
            }
        }
        self.feed_columns_at(prog, input, decoder.finish(), time)
    }

    /// Stage rows already read into columns at `time`, each with
    /// its own multiplicity. Their shapes must be the input's declared shapes; nothing is staged
    /// otherwise.
    pub fn feed_columns(&mut self, prog: &str, input: impl Into<InputId>, columns: Updates, time: OuterTime) -> Result<(), String> {
        self.feed_columns_at(prog, &input.into(), columns, &host_time(time))
    }

    /// Feed columns at a point; validates both the shape and the input's own frontier.
    pub fn feed_columns_at(&mut self, prog: &str, input: &InputId, columns: Updates, time: &HostTime) -> Result<(), String> {
        self.validate_feed(prog, input, time)?;
        self.programs
            .get_mut(prog)
            .expect("feed target was prevalidated")
            .inputs
            .get_mut(input)
            .expect("feed input was prevalidated")
            .give_columns_at(columns, time)
    }

    /// Stage a timed batch: rows already read into columns, each at its own time in `times`, the first
    /// root coordinate. Every time must be at or after the input's frontier; nothing is staged
    /// otherwise, nor if the shapes are not the input's declared ones. The rows of a time take
    /// effect once the input moves past it, by [`Server::seal`].
    pub fn feed_timed(&mut self, prog: &str, input: impl Into<InputId>, columns: Updates, times: Vec<OuterTime>) -> Result<(), String> {
        let n = times.len();
        let Updates { keys, vals, diffs } = columns;
        let times = crate::runtime::col_times::ColTimes::from_raw_lanes(vec![times], n);
        self.feed_container(prog, &input.into(), HostContainer { keys, vals, times, diffs })
    }

    /// Admit one container with arbitrary row times under the input’s full capability set.
    /// Validate all columns and times before handing anything to Timely.
    pub fn feed_container(&mut self, prog: &str, input: &InputId, container: HostContainer) -> Result<(), String> {
        let handle = self.writable_input(prog, input)?;
        let n = container.diffs.len();
        if container.times.len() != n || container.keys.len() != n || container.vals.len() != n {
            return Err("feed: key, value, time and diff columns must have equal lengths".into());
        }
        if (corgi::shape_of_value(&container.keys), corgi::shape_of_value(&container.vals)) != handle.shape {
            return Err("feed: container has the wrong shape for the input".into());
        }
        if handle.frontier().next().is_none() { return Err(format!("input {prog:?}/{input} is sealed to []")); }
        if container.times.raw_lanes().iter().skip(handle.width).flatten().any(|&coord| coord != 0) {
            return Err(format!("feed: a row time exceeds the input width {}", handle.width));
        }
        let frontier: Antichain<_> = handle.frontier().cloned().collect();
        if let Some(i) = container.times.beyond(frontier.borrow()).iter().position(|valid| !valid) {
            return Err(format!("cannot feed at time {}: input {prog:?}/{input} has frontier {}", progress::coordinates(&container.times.get(i)), progress::frontier_text(frontier.iter())));
        }
        self.programs.get_mut(prog).unwrap().inputs.get_mut(input).unwrap().give(container);
        Ok(())
    }

    /// Bulk-load at the destination input's sole frontier point (`time=now`).
    pub fn load(&mut self, worker: &Worker, prog: &str, input: impl Into<InputId>, source: &str, diff: Diff) -> Result<u64, String> {
        let input = input.into();
        let time = self.now(prog, &input)?.clone();
        self.load_at(worker, prog, &input, source, diff, &time)
    }

    /// Bulk-load rows into the input labelled `input` of `prog` at the destination time `time`.
    ///
    /// `source` is a published trace, or the path of a text file with one
    /// row per line of whitespace-separated integers, the fields of the input's
    /// key (its value is unit), read into columns at the input's declared shape.
    /// Collective: every worker must call this, and each feeds only its shard
    /// (`row % peers == index`) through its own handle, so the union is the
    /// source exactly once and the exchange places each row on its key's owner.
    /// Each row enters with its multiplicity times `diff`.
    ///
    /// A trace is read at its latest complete scalar point,
    /// each worker reading its own share of the arrangement directly, with no
    /// dataflow and no exchange (the input's own exchange routes the rows).
    /// Call [`Server::spin`] or [`Server::wait_for_frontier`] after sealing a
    /// source so that every shard has reached the cut before the load.
    /// `feed bank 0 time=now from retire diff=-1` is the maintenance idiom: a program
    /// exports the rows it no longer needs, and the server deletes them from its
    /// input without a client round trip.
    ///
    /// A file is validated in full on every worker before any of its rows is
    /// applied: a malformed line fails the load on every worker, so a source
    /// is fed whole or not at all, and the workers agree because they read the
    /// same file (the source must be visible, and identical, to all of them).
    pub fn load_at(
        &mut self,
        worker: &Worker,
        prog: &str,
        input: &InputId,
        source: &str,
        diff: Diff,
        time: &HostTime,
    ) -> Result<u64, String> {
        self.validate_feed(prog, input, time)?;
        if self.loads_from_trace(source) {
            return self.load_trace(prog, input, source, diff, time);
        }
        let (index, peers) = (worker.index(), worker.peers());
        let mine = |e: u64| (e as usize) % peers == index;
        let target = &self.programs[prog].inputs[input];
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
        self.feed_columns_at(prog, input, rows.finish(), time)?;
        Ok(total)
    }

    /// Whether [`Server::load`] reads `source` as a published trace (and so counts only this
    /// worker's share of its rows), rather than as a file.
    pub fn loads_from_trace(&self, source: &str) -> bool {
        self.is_published(source)
    }

    /// [`Server::load`] from published trace `source`: this worker's share of its contents
    /// at its latest readable point, scaled by `diff`, into its own handle for `prog`'s `input`.
    fn load_trace(&mut self, prog: &str, input: &InputId, source: &str, diff: Diff, time: &HostTime) -> Result<u64, String> {
        let name = source;
        let declared = &self.programs[prog].inputs[input].shape;
        let shape = self.shapes[name].clone();
        if *declared != shape {
            return Err(format!(
                "program {:?} declares input {} as {} but trace {:?} is published as {}",
                prog, input, fmt_shape(declared), name, fmt_shape(&shape)
            ));
        }
        let mut updates = self.snapshot_local(name)?;
        // Every check above reads state all workers share, so they agree on it. Only scaling
        // reads this worker's rows; an overflow there cannot be refused on every worker alike.
        for d in updates.diffs.iter_mut() {
            *d = d.checked_mul(diff).unwrap_or_else(|| {
                panic!("feed from {name:?}: multiplicity {d} overflows when scaled by {diff}")
            });
        }
        let rows = updates.len() as u64;
        self.programs
            .get_mut(prog)
            .expect("load target was prevalidated")
            .inputs
            .get_mut(input)
            .expect("load input was prevalidated")
            .give_columns_at(updates, time)?;
        Ok(rows)
    }

    /// Check the global time bounds before any worker submits its local shard.
    pub fn validate_batch(&self, prog: &str, input: &InputId, least: &[HostTime], width: usize) -> Result<(), String> {
        let handle = self.writable_input(prog, input)?;
        if width > handle.width { return Err(format!("feed: a row time exceeds the input width {}", handle.width)); }
        if handle.frontier().next().is_none() { return Err(format!("input {prog:?}/{input} is sealed to []")); }
        for time in least.iter() {
            self.validate_feed(prog, input, time)?;
        }
        Ok(())
    }

    /// Check whether this writable input accepts the point before staging any rows.
    pub(super) fn validate_feed(&self, prog: &str, input: &InputId, time: &HostTime) -> Result<(), String> {
        let handle = self.writable_input(prog, input)?;
        if time.len() > handle.width || !handle.frontier().any(|t| t.less_equal(time)) {
            return Err(format!("cannot feed at time {}: input {prog:?}/{input} has width {} and frontier {}", progress::coordinates(time), handle.width, progress::frontier_text(handle.frontier())));
        }
        Ok(())
    }

    fn writable_input(&self, prog: &str, input: &InputId) -> Result<&ServerInput, String> {
        let installed = self
            .programs
            .get(prog)
            .ok_or_else(|| format!("no program {:?}", prog))?;
        if installed.origin != Origin::Program {
            return Err(format!("{prog:?} is a diagnostics source and is not writable"));
        }
        installed.inputs.get(input).ok_or_else(|| no_input(prog, input))
    }
}
