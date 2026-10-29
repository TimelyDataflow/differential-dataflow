//! Reading published traces: snapshots, keyed peeks, and export taps' change streams.

use super::*;
use timely::PartialOrder;

impl Server {
    /// This worker's share of trace `name`, consolidated as of the current epoch, read from its
    /// batches without a dataflow. Unlike [`Server::snapshot`], nothing moves between workers.
    pub(crate) fn snapshot_local(&self, name: &str) -> Result<Updates, String> {
        use differential_dataflow::trace::TraceReader;
        let name = canonical_source_name(name);
        let point = self.latest(&name)?;
        let Some(point) = point else { return Ok(Updates::consolidate(Vec::new(), &self.shapes[&name])); };
        self.readable(&name, &point)?;
        let output = &self.outputs[&name];
        if output.probe.less_equal(&point) { return Err(format!("trace {name:?} is not complete at {point:?}")); }
        let mut trace = self.traces[&name].clone();
        let upper = output.probe.with_frontier(|f| f.to_owned());
        let batches = trace.batches_through(upper.borrow()).ok_or_else(|| format!("trace {name:?} has no readable cut"))?;
        let parts = crate::backend::corgi::batch_containers(batches)
            .into_iter()
            .map(|HostContainer { keys, vals, times, diffs }| {
                let part = Updates { keys, vals, diffs };
                let before: Vec<usize> = (0..times.len()).filter(|&i| times.get(i).less_equal(&point)).collect();
                if before.len() == part.len() { part } else { part.select(&before) }
            })
            .collect();
        Ok(Updates::consolidate(parts, &self.shapes[&name]))
    }

    /// Return the consolidated closed-past contents of a trace on worker 0, as columns.
    ///
    /// Builds a transient dataflow that imports the trace, **sends every
    /// container whole to worker 0**, and consolidates them as of the current
    /// epoch — so the result is the complete contents even when the trace is
    /// sharded across workers, not each worker's slice, in key-then-value order.
    /// The other workers get no updates. The dataflow is dropped as soon as it
    /// has drained. Filtering and rendering are the caller's.
    pub fn snapshot(
        &mut self,
        worker: &mut Worker,
        name: &str,
    ) -> Result<Updates, String> {
        let name = canonical_source_name(name);
        match self.latest(&name)? {
            Some(point) => self.snapshot_as_of(worker, &name, &point),
            None => Ok(Updates::consolidate(Vec::new(), &self.shapes[&name])),
        }
    }

    /// Accumulate updates <= `point`. Waits only for this export to close the point;
    /// callers must advance its upstream inputs first, on every worker.
    pub fn snapshot_as_of(&mut self, worker: &mut Worker, name: &str, point: &HostTime) -> Result<Updates, String> {
        self.readable(name, point)?;
        use timely::dataflow::operators::Probe;
        use timely::dataflow::operators::generic::Operator;

        let name = canonical_source_name(name);

        let mut published = self.published(&name).ok_or_else(|| format!("no trace {:?}", name))?;
        let as_of = self.outputs[&name].since.clone();
        let shape = self.shapes[&name].clone();
        let parts: Rc<RefCell<Vec<Updates>>> = Rc::new(RefCell::new(Vec::new()));
        let parts_in = parts.clone();
        let mut probe = ProbeHandle::new();
        let id = worker.next_dataflow_index();
        host_dataflow(worker, |scope| {
            let point = point.clone();
            // The dataflow is dropped once drained, which releases the import.
            let (containers, _shutdown) = published.import_containers(scope, "SnapshotImport", as_of);
            containers
                .inner
                .unary::<timely::container::CapacityContainerBuilder<HostContainer>, _, _, _>(crate::runtime::exchange::GatherPact, "Snapshot", move |_, _| {
                    move |input, _output| {
                        input.for_each(|_cap, data| {
                            let HostContainer { keys, vals, times, diffs } = std::mem::take(data);
                            let part = Updates { keys, vals, diffs };
                            let before: Vec<usize> = (0..times.len()).filter(|&i| times.get(i).less_equal(&point)).collect();
                            parts_in.borrow_mut().push(if before.len() == part.len() { part } else { part.select(&before) });
                        });
                    }
                })
                .probe_with(&mut probe);
        });
        while probe.less_equal(point) {
            // Remote data or progress wakes idle workers; do not busy-poll.
            worker.step_or_park(None);
        }
        worker.drop_dataflow(id);
        let parts = std::mem::take(&mut *parts.borrow_mut());
        Ok(Updates::consolidate(parts, &shape))
    }

    fn latest(&self, name: &str) -> Result<Option<HostTime>, String> {
        let output = self.outputs.get(name).ok_or_else(|| format!("no trace {name:?}"))?;
        let epoch = self.programs[&output.owner].now.get(0).copied().unwrap_or(0);
        if epoch == 0 { return Ok(None); }
        let mut point = vec![u64::MAX; output.width];
        point[0] = epoch - 1;
        Ok(Some(HostTime::new(point.into())))
    }

    fn readable(&self, name: &str, point: &HostTime) -> Result<(), String> {
        let output = self.output(name, point)?;
        if !output.since.less_equal(point) { return Err(format!("point {point:?} is before trace {name:?}'s read floor {:?}", output.since)); }
        Ok(())
    }

    /// [`Server::snapshot`], with only the updates whose key is `key`, if one is given: text read
    /// at the trace's key shape, as a feed reads a key.
    pub fn peek(&mut self, worker: &mut Worker, name: &str, key: Option<&str>) -> Result<Updates, String> {
        let updates = self.snapshot(worker, name)?;
        self.filter_key(name, key, updates)
    }

    /// A keyed peek at an explicit point, with the same progress and history rules as a snapshot.
    pub fn peek_as_of(&mut self, worker: &mut Worker, name: &str, key: Option<&str>, point: &HostTime) -> Result<Updates, String> {
        let updates = self.snapshot_as_of(worker, name, point)?;
        self.filter_key(name, key, updates)
    }

    fn filter_key(&self, name: &str, key: Option<&str>, updates: Updates) -> Result<Updates, String> {
        let Some(text) = key else { return Ok(updates) };
        let (kshape, _) = self.shapes[&canonical_source_name(name)].clone();
        let mut decoder = crate::runtime::feed::FeedDecoder::new(&kshape, &corgi::Shape::Unit);
        decoder.push(text, None, 1)?;
        let wanted = decoder.finish().keys;
        let rows: Vec<usize> = (0..updates.len()).collect();
        let equal = corgi::arrange::equal_idx(&updates.keys, &wanted, &rows, &vec![0; rows.len()]);
        let keep: Vec<usize> = rows.into_iter().filter(|&row| equal[row] == 0).collect();
        Ok(updates.select(&keep))
    }

    /// The changes to export `name` that this worker's share of its arrangement has produced
    /// since the last call, one container per chunk (with [`Server::set_export_taps`]). Each worker
    /// drains its own.
    pub fn take_changes(&mut self, name: &str) -> Vec<HostContainer> {
        match self.taps.get(name) {
            Some(tap) => crate::backend::corgi::batch_containers(std::mem::take(&mut *tap.borrow_mut())),
            None => Vec::new(),
        }
    }
}
