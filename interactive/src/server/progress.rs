//! Per-input point capabilities and per-export progress. Program clocks are convenience only.

use super::*;

pub(super) fn caught_up(probe: &ProbeHandle<HostTime>, point: &HostTime) -> bool {
    probe.with_frontier(|frontier| frontier.iter().all(|t| point.less_equal(t)))
}

impl Server {
    /// The default point for this program's next bulk advance.
    pub fn now(&self, prog: &str) -> Result<&HostTime, String> {
        Ok(&self.programs.get(&canonical_source_name(prog)).ok_or_else(|| format!("no program {prog:?}"))?.now)
    }

    /// This input's earliest accepted point; other inputs may have different frontiers.
    pub fn input_frontier(&self, prog: &str, input: usize) -> Result<&HostTime, String> {
        let installed = self.programs.get(&canonical_source_name(prog)).ok_or_else(|| format!("no program {prog:?}"))?;
        Ok(installed.inputs.get(&input).ok_or_else(|| format!("program {prog:?} has no input {input}"))?.time())
    }

    /// Advance just one positional input. Collective across workers; does not wait.
    /// Points are trimmed: omitted coordinates are zero, not inferred from the clock.
    pub fn advance_input(&mut self, prog: &str, input: usize, point: &HostTime) -> Result<(), String> {
        self.validate_feed(prog, input, point)?;
        self.programs.get_mut(&canonical_source_name(prog)).unwrap().inputs.get_mut(&input).unwrap().advance_to_point(point);
        Ok(())
    }

    /// Compare and advance one input's singleton write frontier. Failure changes nothing.
    pub fn advance_write(&mut self, prog: &str, input: usize, lower: &HostTime, upper: &HostTime) -> Result<(), String> {
        self.validate_feed(prog, input, lower)?;
        let current = self.input_frontier(prog, input)?;
        if current != lower {
            return Err(format!("write frontier mismatch: expected {lower:?}, current {current:?}"));
        }
        self.advance_input(prog, input, upper)
    }

    /// This worker's observed output frontier (may lag issued advances).
    pub fn trace_upper(&self, trace: &str) -> Option<Vec<HostTime>> {
        self.outputs.get(&canonical_source_name(trace)).map(|o| o.probe.with_frontier(|f| f.to_vec()))
    }

    /// Wait for this export alone to reach a point frontier. Advance all of its producers
    /// first: this does not advance imports or unrelated inputs. Collective across workers.
    pub fn wait_for(&self, worker: &mut Worker, trace: &str, point: &HostTime) -> Result<(), String> {
        let output = self.output(trace, point)?;
        while !caught_up(&output.probe, point) { worker.step_or_park(None); }
        Ok(())
    }

    /// Advance every input of one program, then wait for each of its outputs. Each input
    /// must accept the point; a rejected advance changes nothing. Waits for the frontier
    /// of local and imported inputs; imports are advanced separately. Keeps one system
    /// epoch of history, all other coordinates.
    pub fn advance_to(&mut self, worker: &mut Worker, prog: &str, point: &HostTime) -> Result<(), String> {
        let name = canonical_source_name(prog);
        let installed = self.programs.get(&name).ok_or_else(|| format!("no program {prog:?}"))?;
        if point.len() > installed.root_width || !installed.now.less_equal(point) {
            return Err(format!("invalid advance from {} to {}", format_point(&installed.now, installed.root_width), format_point(point, installed.root_width)));
        }
        let inputs: Vec<_> = installed.inputs.keys().copied().collect();
        let exports = installed.exports.clone();
        let manual = installed.manual;
        for &input in &inputs {
            let frontier = installed.inputs[&input].time();
            if !frontier.less_equal(point) {
                return Err(format!("cannot advance input {name:?}/{input} from frontier {} to requested target {}", format_point(frontier, installed.root_width), format_point(point, installed.root_width)));
            }
            self.validate_feed(&name, input, point)?;
        }
        for input in inputs { self.advance_input(&name, input, point)?; }
        self.programs.get_mut(&name).unwrap().now = point.clone();
        let mut frontier = Antichain::new();
        let installed = &self.programs[&name];
        for input in installed.inputs.values() { frontier.insert(input.time().clone()); }
        let mut seen = std::collections::HashSet::new();
        for trace in &installed.imports { self.collect_frontier(trace, &mut seen, &mut frontier); }
        for export in exports {
            self.wait_for_frontier(worker, &export, &frontier)?;
            if !manual {
                let since = self.outputs[&export].since.join(&host_time(point.get(0).copied().unwrap_or(0).saturating_sub(1)));
                self.compact(&export, &since)?;
            }
        }
        Ok(())
    }

    /// Wait for an output to reach an antichain frontier. A widened import may remain
    /// at [s, 0] while a local input is at [s-1, e]; neither point subsumes the other.
    pub fn wait_for_frontier(&self, worker: &mut Worker, trace: &str, frontier: &Antichain<HostTime>) -> Result<(), String> {
        let output = self.outputs.get(&canonical_source_name(trace)).ok_or_else(|| format!("no trace {trace:?}"))?;
        if frontier.elements().iter().any(|t| t.len() > output.width) { return Err("frontier exceeds trace width".into()); }
        while output.probe.with_frontier(|f| f.iter().any(|t| !frontier.less_equal(t))) { worker.step_or_park(None); }
        Ok(())
    }

    // A conservative target derived from commanded capabilities, never worker-local probes.
    // Imports form a DAG (producers exist before consumers); the set avoids revisiting diamonds.
    pub(super) fn collect_frontier(&self, trace: &str, seen: &mut std::collections::HashSet<String>, frontier: &mut Antichain<HostTime>) {
        if !seen.insert(trace.to_string()) { return; }
        let output = &self.outputs[trace];
        let installed = &self.programs[&output.owner];
        for input in &output.inputs { frontier.insert(installed.inputs[input].time().clone()); }
        for trace in &output.imports { self.collect_frontier(trace, seen, frontier); }
    }

    pub(super) fn read_frontier(&self, name: &str) -> Antichain<HostTime> {
        let mut frontier = Antichain::new();
        self.collect_frontier(&canonical_source_name(name), &mut std::collections::HashSet::new(), &mut frontier);
        frontier
    }

    /// Program tick: advance coordinate zero by one using per-input advances and output waits.
    pub fn tick_program(&mut self, worker: &mut Worker, prog: &str) -> Result<(), String> {
        let mut next = self.now(prog)?.to_vec();
        if next.is_empty() { next.push(0); }
        next[0] = next[0].checked_add(1).ok_or("epoch overflow")?;
        self.advance_to(worker, prog, &HostTime::new(next.into()))
    }

    /// Discard distinctions before this point in one published trace. Collective and monotone.
    /// Readers/importers retain their own compaction holds. Reads below this floor are refused.
    pub fn compact(&mut self, trace: &str, since: &HostTime) -> Result<(), String> {
        let name = canonical_source_name(trace);
        let output = self.output(&name, since)?;
        if !output.since.less_equal(since) { return Err("compaction must advance monotonically".into()); }
        let frontier = Antichain::from_elem(since.clone());
        let trace = self.traces.get_mut(&name).unwrap();
        trace.set_logical_compaction(frontier.borrow());
        trace.set_physical_compaction(frontier.borrow());
        self.outputs.get_mut(&name).unwrap().since = since.clone();
        Ok(())
    }

    /// The declared width and readable history are independent of the stored point's length.
    pub fn trace_width(&self, trace: &str) -> Option<usize> {
        self.outputs.get(&canonical_source_name(trace)).map(|o| o.width)
    }

    /// Whether the trace's owner uses explicit clocks and retention.
    pub fn trace_manual(&self, trace: &str) -> Option<bool> {
        self.outputs.get(&canonical_source_name(trace)).map(|o| self.programs[&o.owner].manual)
    }

    pub fn trace_since(&self, trace: &str) -> Option<&HostTime> {
        self.outputs.get(&canonical_source_name(trace)).map(|o| &o.since)
    }

    pub(super) fn output(&self, name: &str, point: &HostTime) -> Result<&Output, String> {
        let output = self.outputs.get(&canonical_source_name(name)).ok_or_else(|| format!("no trace {name:?}"))?;
        if point.len() > output.width { return Err(format!("point {} exceeds trace {name:?}'s width {}", format_point(point, output.width), output.width)); }
        Ok(output)
    }
}
