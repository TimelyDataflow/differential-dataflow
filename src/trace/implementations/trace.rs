//! A navigable collection of (key, val, time, isize) updates.
//! 
//! A `Trace` supports searching by key, within which one can search by val, 

use std::rc::Rc;

use lattice::Lattice;
use trace::layer::{Layer, LayerCursor};
use trace::cursor::CursorList;
use trace::{Trace, Batch};

