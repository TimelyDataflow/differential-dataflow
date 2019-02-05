use std::collections::HashMap;
use std::hash::Hash;

use timely::dataflow::ProbeHandle;

use differential_dataflow::Data;
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::input::InputSession;

use super::{Time, Diff, Plan};


pub type TraceKeyHandle<K, T, R> = TraceAgent<K, (), T, R, OrdKeySpine<K, T, R>>;
pub type TraceValHandle<K, V, T, R> = TraceAgent<K, V, T, R, OrdValSpine<K, V, T, R>>;

pub type KeysOnlyHandle<V> = TraceKeyHandle<Vec<V>, Time, Diff>;
pub type KeysValsHandle<V> = TraceValHandle<Vec<V>, Vec<V>, Time, Diff>;


pub struct Manager<Value: Data> {
    pub inputs: InputManager<Value>,
    pub traces: TraceManager<Value>,
    pub probe: ProbeHandle<Time>,
}

impl<Value: Data+Hash> Manager<Value> {

    pub fn new() -> Self {
        Manager {
            inputs: InputManager::new(),
            traces: TraceManager::new(),
            probe: ProbeHandle::new(),
        }
    }

    pub fn insert_input(
        &mut self,
        name: String,
        input: InputSession<Time, Vec<Value>, Diff>,
        trace: KeysOnlyHandle<Value>)
    {
        self.inputs.sessions.insert(name.clone(), input);
        self.traces.set_unkeyed(&Plan::Source(name), &trace);
    }

    /// Advances inputs and traces to `time`.
    pub fn advance_time(&mut self, time: &Time) {
        self.inputs.advance_time(time);
        self.traces.advance_time(time);
    }

}

pub struct InputManager<Value: Data> {
    pub sessions: HashMap<String, InputSession<Time, Vec<Value>, Diff>>,
}

impl<Value: Data> InputManager<Value> {

    pub fn new() -> Self { Self { sessions: HashMap::new() } }

    pub fn advance_time(&mut self, time: &Time) {
        for session in self.sessions.values_mut() {
            session.advance_to(time.clone());
            session.flush();
        }
    }

}

/// Root handles to maintained collections.
///
/// Manages a map from plan (describing a collection)
/// to various arranged forms of that collection.
pub struct TraceManager<Value: Data> {

    /// Arrangements where the record itself is they key.
    ///
    /// This contains both input collections, which are here cached so that
    /// they can be re-used, intermediate collections that are cached, and
    /// any collections that are explicitly published.
    inputs: HashMap<Plan<Value>, KeysOnlyHandle<Value>>,

    /// Arrangements of collections by key.
    arrangements: HashMap<Plan<Value>, HashMap<Vec<usize>, KeysValsHandle<Value>>>,

}

impl<Value: Data+Hash> TraceManager<Value> {

    pub fn new() -> Self { Self { inputs: HashMap::new(), arrangements: HashMap::new() } }

    /// Advances the frontier of each maintained trace.
    pub fn advance_time(&mut self, time: &Time) {
        use differential_dataflow::trace::TraceReader;

        let frontier = &[time.clone()];
        for trace in self.inputs.values_mut() {
            trace.advance_by(frontier);
        }
        for map in self.arrangements.values_mut() {
            for trace in map.values_mut() {
                trace.advance_by(frontier)
            }
        }
    }

    /// Recover an arrangement by plan and keys, if it is cached.
    pub fn get_unkeyed(&self, plan: &Plan<Value>) -> Option<KeysOnlyHandle<Value>> {
        self.inputs
            .get(plan)
            .map(|x| x.clone())
    }

    /// Installs an unkeyed arrangement for a specified plan.
    pub fn set_unkeyed(&mut self, plan: &Plan<Value>, handle: &KeysOnlyHandle<Value>) {

        println!("Setting unkeyed: {:?}", plan);

        use differential_dataflow::trace::TraceReader;
        let mut handle = handle.clone();
        handle.distinguish_since(&[]);
        self.inputs
            .insert(plan.clone(), handle);
    }

    /// Recover an arrangement by plan and keys, if it is cached.
    pub fn get_keyed(&self, plan: &Plan<Value>, keys: &[usize]) -> Option<KeysValsHandle<Value>> {
        self.arrangements
            .get(plan)
            .and_then(|map| map.get(keys).map(|x| x.clone()))
    }

    /// Installs a keyed arrangement for a specified plan and sequence of keys.
    pub fn set_keyed(&mut self, plan: &Plan<Value>, keys: &[usize], handle: &KeysValsHandle<Value>) {
        use differential_dataflow::trace::TraceReader;
        let mut handle = handle.clone();
        handle.distinguish_since(&[]);
        self.arrangements
            .entry(plan.clone())
            .or_insert(HashMap::new())
            .insert(keys.to_vec(), handle);
    }

}