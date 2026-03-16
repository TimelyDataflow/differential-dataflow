//! An example value type.

use std::time::Duration;
use serde::{Deserialize, Serialize};
use super::{Datum, VectorFrom, Command};

/// A session.
pub struct Session<W: std::io::Write> {
    write: W,
}

impl<W: std::io::Write> Session<W> {
    /// Create a new session.
    pub fn new(write: W) -> Self { Self { write } }
    /// Issue a command.
    pub fn issue<C: Into<Command<Value>>>(&mut self, command: C) {
        let command: Command<Value> = command.into();
        bincode::serialize_into(&mut self.write, &command)
            .expect("bincode: serialization failed");
    }
}

/// An example value type
#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum Value {
    /// boolean
    Bool(bool),
    /// integer
    Usize(usize),
    /// string
    String(String),
    /// vector
    Vector(Vec<Value>),
    /// duration
    Duration(Duration),
}

impl Datum for Value {
    type Expression = usize;
    fn subject_to(data: &[Self], expr: &Self::Expression) -> Self { data[*expr].clone() }
    fn projection(index: usize) -> Self::Expression { index }
}

impl From<usize> for Value { fn from(x: usize) -> Self { Value::Usize(x) } }
impl From<bool> for Value { fn from(x: bool) -> Self { Value::Bool(x) } }
impl From<String> for Value { fn from(x: String) -> Self { Value::String(x) } }
impl From<Duration> for Value { fn from(x: Duration) -> Self { Value::Duration(x) } }

impl<V> From<Vec<V>> for Value where Value: From<V> {
    fn from(x: Vec<V>) -> Self { Value::Vector(x.into_iter().map(|y| y.into()).collect()) }
}


use timely::logging::TimelyEvent;

impl VectorFrom<TimelyEvent> for Value {
    fn vector_from(item: TimelyEvent) -> Vec<Value> {
        match item {
            TimelyEvent::Operates(x) => {
                vec![
                    x.id.into(),
                    x.addr.into(),
                    x.name.into(),
                ]
            },
            TimelyEvent::Channels(x) => {
                vec![
                    x.id.into(),
                    x.scope_addr.into(),
                    x.source.0.into(),
                    x.source.1.into(),
                    x.target.0.into(),
                    x.target.1.into(),
                ]
            },
            TimelyEvent::Schedule(x) => {
                vec![
                    x.id.into(),
                    (x.start_stop == ::timely::logging::StartStop::Start).into(),
                ]
            },
            TimelyEvent::Messages(x) => {
                vec![
                    x.channel.into(),
                    x.is_send.into(),
                    x.source.into(),
                    x.target.into(),
                    x.seq_no.into(),
                    usize::try_from(x.record_count).unwrap().into(),
                ]
            },
            TimelyEvent::Shutdown(x) => { vec![x.id.into()] },
            // TimelyEvent::Park(x) => {
            //     match x {
            //         timely::logging::ParkEvent::ParkUnpark::Park(x) => { vec![true.into(), x.into()] },
            //         timely::logging::ParkEvent::ParkUnpark::Unpark => { vec![false.into(), 0.into()] },
            //     }
            // },
            TimelyEvent::Text(x) => { vec![Value::String(x)] }
            _ => { vec![] },
        }
    }
}

/// Manual `Columnar` implementation for `Value`.
///
/// Because `Value` is recursive (the `Vector` variant contains `Vec<Value>`),
/// the derive macro overflows. We store each `Value` as bincode-serialized bytes,
/// reusing the columnar representation of `Vec<Vec<u8>>` (`Vecs<Vec<u8>>`).
mod value_columnar {
    use super::Value;

    /// Columnar container for `Value`, wrapping `Vecs<Vec<u8>>`.
    /// Each `Value` is bincode-serialized to bytes on push.
    #[derive(Clone, Default)]
    pub struct ValueColumns(columnar::Vecs<Vec<u8>>);

    // Delegate Borrow, using the same Ref and Borrowed types as Vecs<Vec<u8>>.
    impl columnar::Borrow for ValueColumns {
        // Ref is a slice of bytes: Slice<&'a [u8]>
        type Ref<'a> = columnar::Slice<&'a [u8]>;
        type Borrowed<'a> = columnar::Vecs<&'a [u8], &'a [u64]>;
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            columnar::Borrow::borrow(&self.0)
        }
        fn reborrow<'b, 'a: 'b>(item: Self::Borrowed<'a>) -> Self::Borrowed<'b> {
            <columnar::Vecs<Vec<u8>> as columnar::Borrow>::reborrow(item)
        }
        fn reborrow_ref<'b, 'a: 'b>(item: Self::Ref<'a>) -> Self::Ref<'b> {
            <columnar::Vecs<Vec<u8>> as columnar::Borrow>::reborrow_ref(item)
        }
    }

    impl columnar::Len for ValueColumns {
        fn len(&self) -> usize { columnar::Len::len(&self.0) }
    }
    impl columnar::Clear for ValueColumns {
        fn clear(&mut self) { columnar::Clear::clear(&mut self.0); }
    }
    impl columnar::HeapSize for ValueColumns {
        fn heap_size(&self) -> (usize, usize) { columnar::HeapSize::heap_size(&self.0) }
    }

    // Push &Value by serializing to bytes and pushing as Vec<u8>.
    impl<'a> columnar::Push<&'a Value> for ValueColumns {
        fn push(&mut self, item: &'a Value) {
            let bytes = bincode::serialize(item).expect("bincode serialization of Value failed");
            columnar::Push::push(&mut self.0, &bytes);
        }
    }

    // Push the Ref type (Slice<&[u8]>) by re-encoding as bytes for the inner Vecs.
    impl<'a> columnar::Push<columnar::Slice<&'a [u8]>> for ValueColumns {
        fn push(&mut self, item: columnar::Slice<&'a [u8]>) {
            columnar::Push::push(&mut self.0, item);
        }
    }

    impl columnar::Container for ValueColumns {
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
            columnar::Container::extend_from_self(&mut self.0, other, range);
        }
        fn reserve_for<'a, I: Iterator<Item = Self::Borrowed<'a>>>(&mut self, items: I) where I: Clone {
            columnar::Container::reserve_for(&mut self.0, items);
        }
    }

    impl columnar::Columnar for Value {
        fn into_owned<'a>(other: columnar::Ref<'a, Self>) -> Self {
            // Ref is Slice<&'a [u8]>; access the underlying byte slice.
            let bytes: &[u8] = &other.slice[other.lower..other.upper];
            bincode::deserialize(bytes).expect("bincode deserialization of Value failed")
        }
        fn copy_from<'a>(&mut self, other: columnar::Ref<'a, Self>) {
            let bytes: &[u8] = &other.slice[other.lower..other.upper];
            *self = bincode::deserialize(bytes).expect("bincode deserialization of Value failed");
        }
        type Container = ValueColumns;
    }
}

use differential_dataflow::logging::DifferentialEvent;

impl VectorFrom<DifferentialEvent> for Value {
    fn vector_from(item: DifferentialEvent) -> Vec<Value> {
        match item {
            DifferentialEvent::Batch(x) => {
                vec![
                    x.operator.into(),
                    x.length.into(),
                ]
            },
            DifferentialEvent::Merge(x) => {
                vec![
                    x.operator.into(),
                    x.scale.into(),
                    x.length1.into(),
                    x.length2.into(),
                    x.complete.unwrap_or(0).into(),
                    x.complete.is_some().into(),
                ]
            },
            _ => { vec![] },
        }
    }
}
