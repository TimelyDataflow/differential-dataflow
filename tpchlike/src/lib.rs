#[macro_use]
extern crate abomonation;
extern crate timely;
extern crate differential_dataflow;
extern crate arrayvec;
extern crate regex;

use timely::dataflow::*;

use differential_dataflow::Collection;

pub mod types;
pub mod queries;

pub use types::*;

pub struct Collections<G: Scope> {
    customers: Collection<G, Customer, isize>,
    lineitems: Collection<G, LineItem, isize>,
    nations: Collection<G, Nation, isize>,
    orders: Collection<G, Order, isize>,
    parts: Collection<G, Part, isize>,
    partsupps: Collection<G, PartSupp, isize>,
    regions: Collection<G, Region, isize>,
    suppliers: Collection<G, Supplier, isize>,
    used: [bool; 8],
}

impl<G: Scope> Collections<G> {
    pub fn new(
        customers: Collection<G, Customer, isize>,
        lineitems: Collection<G, LineItem, isize>,
        nations: Collection<G, Nation, isize>,
        orders: Collection<G, Order, isize>,
        parts: Collection<G, Part, isize>,
        partsupps: Collection<G, PartSupp, isize>,
        regions: Collection<G, Region, isize>,
        suppliers: Collection<G, Supplier, isize>,
    ) -> Self {

        Collections {
            customers: customers,
            lineitems: lineitems,
            nations: nations,
            orders: orders,
            parts: parts,
            partsupps: partsupps,
            regions: regions,
            suppliers: suppliers,
            used: [false; 8]
        }
    }

    fn customers(&mut self) -> &Collection<G, Customer, isize> { self.used[0] = true; &self.customers }
    fn lineitems(&mut self) -> &Collection<G, LineItem, isize> { self.used[1] = true; &self.lineitems }
    fn nations(&mut self) -> &Collection<G, Nation, isize> { self.used[2] = true; &self.nations }
    fn orders(&mut self) -> &Collection<G, Order, isize> { self.used[3] = true; &self.orders }
    fn parts(&mut self) -> &Collection<G, Part, isize> { self.used[4] = true; &self.parts }
    fn partsupps(&mut self) -> &Collection<G, PartSupp, isize> { self.used[5] = true; &self.partsupps }
    fn regions(&mut self) -> &Collection<G, Region, isize> { self.used[6] = true; &self.regions }
    fn suppliers(&mut self) -> &Collection<G, Supplier, isize> { self.used[7] = true; &self.suppliers }

    pub fn used(&self) -> [bool; 8] { self.used }
}
