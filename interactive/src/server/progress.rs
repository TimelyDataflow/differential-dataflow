//! Per-input write capabilities, per-export progress and read floors.

use super::*;

/// A time as a client writes it: its first coordinate alone when the others are zero.
pub(super) fn coordinates(time: &HostTime) -> String {
    if time.len() <= 1 { time.get(0).copied().unwrap_or(0).to_string() } else { format!("[{}]", time.iter().map(u64::to_string).collect::<Vec<_>>().join(",")) }
}

pub(super) fn frontier_text<'a>(times: impl Iterator<Item = &'a HostTime>) -> String {
    let points: Vec<_> = times.collect();
    match points.as_slice() {
        [t] => coordinates(t),
        ts => format!("[{}]", ts.iter().map(|t| format!("[{}]", t.iter().map(u64::to_string).collect::<Vec<_>>().join(","))).collect::<Vec<_>>().join(",")),
    }
}

pub(super) fn caught_up(probe: &ProbeHandle<HostTime>, point: &HostTime) -> bool {
    probe.with_frontier(|frontier| frontier.iter().all(|t| point.less_equal(t)))
}

impl Server {
    fn input(&self, prog: &str, input: &InputId) -> Result<&ServerInput, String> {
        let installed = self.programs.get(prog).ok_or_else(|| format!("no program {prog:?}"))?;
        installed.inputs.get(input).ok_or_else(|| no_input(prog, input))
    }

    /// All minimal times at which the input can still write. Empty means finished.
    pub fn write_frontier(&self, prog: &str, input: impl Into<InputId>) -> Result<Antichain<HostTime>, String> {
        Ok(self.input(prog, &input.into())?.frontier().cloned().collect())
    }

    /// The time chosen by time=now. Incomparable capabilities require an explicit time.
    pub fn input_time(&self, prog: &str, input: impl Into<InputId>) -> Result<&HostTime, String> {
        self.now(prog, &input.into())
    }

    pub(super) fn now(&self, prog: &str, input: &InputId) -> Result<&HostTime, String> {
        let handle = self.input(prog, input)?;
        handle.time().ok_or_else(|| if handle.frontier().next().is_none() {
            format!("input {prog:?}/{input} is sealed to []")
        } else {
            format!("input {prog:?}/{input} has multiple frontier elements; supply an explicit time")
        })
    }

    /// Advance one input to a singleton frontier.
    pub fn advance_input(&mut self, prog: &str, input: impl Into<InputId>, point: &HostTime) -> Result<(), String> {
        self.seal(prog, input, &Antichain::from_elem(point.clone()))
    }

    /// Lower bound on the write frontier after all preceding commands take effect.
    pub fn target(&self, trace: &str) -> Result<Antichain<HostTime>, String> {
        let name = trace.to_string();
        let output = self.outputs.get(&name).ok_or_else(|| format!("no trace {trace:?}"))?;
        let mut frontier = Antichain::new();
        self.collect_frontier(&output.owner, &mut std::collections::HashSet::new(), &mut frontier);
        Ok(frontier)
    }

    /// Advance one input to a reachable antichain. An empty frontier finishes it permanently.
    pub fn seal(&mut self, prog: &str, input: impl Into<InputId>, frontier: &Antichain<HostTime>) -> Result<(), String> {
        let input: InputId = input.into();
        let installed = self.programs.get_mut(prog).ok_or_else(|| format!("no program {prog:?}"))?;
        if installed.origin != Origin::Program { return Err(format!("source {prog:?} is not writable")); }
        let handle = installed.inputs.get_mut(&input).ok_or_else(|| no_input(prog, &input))?;
        if frontier.iter().any(|point| point.len() > handle.width || !handle.frontier().any(|t| t.less_equal(point))) {
            return Err(format!("cannot seal input {prog:?}/{input} to {} from {} at width {}", frontier_text(frontier.iter()), frontier_text(handle.frontier()), handle.width));
        }
        handle.advance_to_frontier(frontier);
        Ok(())
    }

    /// Drive each export to or beyond the meet of its input and import frontiers.
    pub fn spin(&self, worker: &mut Worker) {
        for name in self.outputs.keys() {
            let target = self.target(name).expect("published output");
            self.wait_for_frontier(worker, name, &target).expect("input frontiers fit the output width");
        }
    }

    /// Wait for this export alone to reach a point frontier. Advance all of its producers
    /// first: this does not advance imports or unrelated inputs. Collective across workers.
    pub fn wait_for(&self, worker: &mut Worker, trace: &str, point: &HostTime) -> Result<(), String> {
        let output = self.output(trace, point)?;
        while !caught_up(&output.probe, point) { worker.step_or_park(None); }
        Ok(())
    }

    /// Wait for an output to reach an antichain frontier. A widened import may remain
    /// at [s, 0] while a local input is at [s-1, e]; neither point subsumes the other.
    pub fn wait_for_frontier(&self, worker: &mut Worker, trace: &str, frontier: &Antichain<HostTime>) -> Result<(), String> {
        let output = self.outputs.get(trace).ok_or_else(|| format!("no trace {trace:?}"))?;
        if frontier.elements().iter().any(|t| t.len() > output.width) { return Err("frontier exceeds trace width".into()); }
        while output.probe.with_frontier(|f| f.iter().any(|t| !frontier.less_equal(t))) { worker.step_or_park(None); }
        Ok(())
    }

    // A conservative target derived from commanded capabilities, never worker-local probes.
    // Imports form a DAG (producers exist before consumers); the set avoids revisiting diamonds.
    pub(super) fn collect_frontier(&self, prog: &str, seen: &mut std::collections::HashSet<String>, frontier: &mut Antichain<HostTime>) {
        if !seen.insert(prog.to_string()) { return; }
        let installed = &self.programs[prog];
        for time in installed.inputs.values().flat_map(|input| input.frontier()) { frontier.insert(time.clone()); }
        for trace in &installed.imports { self.collect_frontier(&self.outputs[trace].owner, seen, frontier); }
    }

    /// Discard distinctions before this point in one published trace. Collective and monotone.
    /// This low-level logical hold can move ahead of writes. Physical compaction is limited
    /// to batches already in the trace. Readers and importers retain their own holds.
    pub fn compact(&mut self, trace: &str, since: &HostTime) -> Result<(), String> {
        self.output(trace, since)?;
        self.compact_to(trace, Antichain::from_elem(since.clone()))
    }

    /// Advance the published read floor after this output reaches the point. Its inputs and
    /// imports must already have promised enough progress. None releases this handle immediately.
    pub fn trim(&mut self, worker: &mut Worker, trace: &str, since: Option<&HostTime>) -> Result<(), String> {
        if let Some(point) = since {
            let target = self.target(trace)?;
            if !target.iter().all(|t| point.less_equal(t)) {
                return Err(format!("cannot trim trace {trace:?} to {}: commanded write frontier is {}; seal its contributing inputs first",
                    coordinates(point), frontier_text(target.iter())));
            }
            self.wait_for(worker, trace, point)?;
            self.compact(trace, point)
        } else {
            self.compact_to(trace, Antichain::new())
        }
    }

    fn compact_to(&mut self, trace: &str, frontier: Antichain<HostTime>) -> Result<(), String> {
        let name = trace.to_string();
        let output = self.outputs.get_mut(&name).ok_or_else(|| format!("no trace {trace:?}"))?;
        if !PartialOrder::less_equal(&output.since, &frontier) { return Err("compaction must advance monotonically".into()); }
        let trace = self.traces.get_mut(&name).unwrap();
        trace.set_logical_compaction(frontier.borrow());
        // A new join starts by replaying the committed batches. Its physical hold must
        // never pass their upper, even when a Rust caller moves its logical hold ahead.
        let mut physical = Antichain::new();
        if let [floor] = frontier.elements() {
            trace.read_upper(&mut physical);
            physical.insert(floor.clone());
        }
        trace.set_physical_compaction(physical.borrow());
        output.since = frontier;
        Ok(())
    }

    /// The declared width and readable history are independent of the stored point's length.
    pub fn trace_width(&self, trace: &str) -> Option<usize> {
        self.outputs.get(trace).map(|o| o.width)
    }

    pub fn trace_since(&self, trace: &str) -> Option<&HostTime> {
        self.outputs.get(trace).and_then(|o| o.since.elements().first())
    }

    pub(super) fn output(&self, name: &str, point: &HostTime) -> Result<&Output, String> {
        let output = self.outputs.get(name).ok_or_else(|| format!("no trace {name:?}"))?;
        if point.len() > output.width { return Err(format!("point {} exceeds trace {name:?}'s width {}", coordinates(point), output.width)); }
        Ok(output)
    }
}
