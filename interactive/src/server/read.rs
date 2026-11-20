//! Reading published traces: snapshots, keyed peeks, and export taps' change streams.

use super::*;
use timely::PartialOrder;

impl Server {
    /// This worker's share of a width-one trace, consolidated at its latest readable point, read
    /// from its batches without a dataflow. Uses the same read checks as [`Server::snapshot`].
    pub(crate) fn snapshot_local(&self, name: &str) -> Result<Updates, String> {
        use differential_dataflow::trace::TraceReader;
        let point = self.latest_readable(name)?;
        let Some(point) = point else { return Ok(Updates::consolidate(Vec::new(), &self.shapes[name])); };
        self.readable(name, &point)?;
        let output = &self.outputs[name];
        if output.probe.less_equal(&point) { return Err(format!("trace {name:?} is not complete at {}", progress::coordinates(&point))); }
        let mut trace = self.traces[name].clone();
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
        Ok(Updates::consolidate(parts, &self.shapes[name]))
    }

    /// Return the latest complete, retained scalar snapshot, consolidated on worker 0.
    /// Programs wider than one coordinate must name a point with [`Server::snapshot_as_of`].
    pub fn snapshot(&mut self, worker: &mut Worker, name: &str) -> Result<Updates, String> {
        self.peek(worker, name, None)
    }

    /// Accumulate updates <= `point`. Refuses points not closed by the program's input and
    /// import frontiers. Collective across workers; drains work already issued by the caller.
    pub fn snapshot_as_of(&mut self, worker: &mut Worker, name: &str, point: &HostTime) -> Result<Updates, String> {
        self.peek_as_of(worker, name, None, point)
    }

    /// Read the latest scalar snapshot, optionally looking up one key in the arrangement.
    pub fn peek(&mut self, worker: &mut Worker, name: &str, key: Option<&str>) -> Result<Updates, String> {
        let point = self.latest_readable(name)?;
        self.read_snapshot(worker, name, key, point.as_ref())
    }

    /// Read at an explicit point, with the same progress and history rules for keyed and full reads.
    pub fn peek_as_of(&mut self, worker: &mut Worker, name: &str, key: Option<&str>, point: &HostTime) -> Result<Updates, String> {
        self.read_snapshot(worker, name, key, Some(point))
    }

    fn read_snapshot(&mut self, worker: &mut Worker, name: &str, key: Option<&str>, point: Option<&HostTime>) -> Result<Updates, String> {
        use timely::dataflow::operators::Probe;
        use timely::dataflow::operators::generic::Operator;

        if let Some(point) = point { self.readable(name, point)?; }
        let shape = self.shapes[name].clone();
        // Parse on every worker before building the collective dataflow.
        let request = key.map(|text| {
            let mut decoder = crate::runtime::feed::FeedDecoder::new(&shape.0, &corgi::Shape::Unit);
            decoder.push(text, None, 1).map(|()| decoder.finish())
        }).transpose()?;
        let Some(point) = point else { return Ok(Updates::consolidate(Vec::new(), &shape)); };
        let mut published = self.published(name).expect("validated snapshot trace");
        let as_of = self.trace_since(name).expect("point is readable").clone();
        let parts: Rc<RefCell<Vec<Updates>>> = Rc::new(RefCell::new(Vec::new()));
        let parts_in = parts.clone();
        let mut probe = ProbeHandle::new();
        let id = worker.next_dataflow_index();
        host_dataflow(worker, |scope| {
            let point = point.clone();
            // The dataflow is dropped once drained, which releases the import.
            let containers = if let Some(request) = request {
                published.lookup(scope, request, &point, &shape)
            } else {
                published.import_containers(scope, "SnapshotImport", as_of).0
            };
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
            // `readable` established that no future command is needed to close this point.
            // Drain the import and gather, including remote data and already-issued progress.
            worker.step_or_park(None);
        }
        worker.drop_dataflow(id);
        let parts = std::mem::take(&mut *parts.borrow_mut());
        Ok(Updates::consolidate(parts, &shape))
    }

    /// Most recent complete, retained scalar point under the commanded input frontiers.
    pub fn latest_readable(&self, name: &str) -> Result<Option<HostTime>, String> {
        let output = self.outputs.get(name).ok_or_else(|| format!("no trace {name:?}"))?;
        if output.since.is_empty() { return Err(format!("trace {name:?} has released its history")); }
        if output.width > 1 {
            return Err(format!("trace {name:?} has root width {}; name a point with snapshot_as_of or peek_as_of", output.width));
        }
        let frontier = self.target(name)?;
        let Some(epoch) = frontier.elements().first().map(|t| t.get(0).copied().unwrap_or(0)) else {
            return Ok(Some(host_time(OuterTime::MAX)));
        };
        if epoch == 0 { return Ok(None); }
        // A program that imports other traces starts where they are read from, and has
        // nothing complete to read until its own inputs have moved past that.
        if !output.since.less_equal(&host_time(epoch - 1)) { return Ok(None); }
        Ok(Some(host_time(epoch - 1)))
    }

    fn readable(&self, name: &str, point: &HostTime) -> Result<(), String> {
        let output = self.output(name, point)?;
        if !output.since.less_equal(point) { return Err(format!("point {} is before trace {name:?}'s read floor {}", progress::coordinates(point), progress::frontier_text(output.since.iter()))); }
        // Commanded capabilities are identical across workers. A local probe alone can lag
        // on some workers, causing them to refuse while others enter the collective gather.
        let mut frontier = Antichain::new();
        self.collect_frontier(&output.owner, &mut std::collections::HashSet::new(), &mut frontier);
        if frontier.less_equal(point) {
            return Err(format!("trace {name:?} is not closed at {}: input/import frontier {}; advance its inputs and imports before reading", progress::coordinates(point), progress::frontier_text(frontier.iter())));
        }
        Ok(())
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

impl Published {
    /// Join one request at `point` against this arrangement. A match at time `t` emerges
    /// at the join of `t` and `point`, so the snapshot includes it exactly when `t <= point`.
    fn lookup<'s>(
        &mut self,
        scope: timely::dataflow::Scope<'s, HostTime>,
        request: Updates,
        point: &HostTime,
        shape: &RowShape,
    ) -> differential_dataflow::Collection<'s, HostTime, HostContainer> {
        use crate::backend::{Backend, corgi::{Arr, CorgiBackend}};
        use differential_dataflow::AsCollection;
        use timely::container::CapacityContainerBuilder;
        use timely::dataflow::operators::generic::operator::source;

        let Updates { keys, vals, diffs } = request;
        let row = HostContainer { keys, vals, times: std::iter::once(point).collect(), diffs };
        let stream = source::<_, CapacityContainerBuilder<HostContainer>, _, _>(scope, "PeekKey", |cap, _| {
            let mut request = (scope.worker().index() == 0).then_some((cap, row));
            move |output| {
                if let Some((cap, mut row)) = request.take() {
                    output.session(&cap).give_container(&mut row);
                }
            }
        });
        let keys = CorgiBackend::arrange(stream.as_collection());
        let target = Arr { arranged: self.0.import_named(scope, "PeekImport"), since: point.clone() };
        let project = crate::ir::Lambda::new("let (k, (_, v)) = input in (k, v)".into(), &corgi::Scope::default());
        CorgiBackend::join(keys, target, &project, ((shape.0.clone(), corgi::Shape::Unit), shape.clone()))
    }
}
