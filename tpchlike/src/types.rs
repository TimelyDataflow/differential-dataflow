//! Types for TPCH-like queries.


pub use self::part::Part;
pub use self::supplier::Supplier;
pub use self::part_supp::PartSupp;
pub use self::customer::Customer;
pub use self::order::Order;
pub use self::line_item::LineItem;
pub use self::nation::Nation;
pub use self::region::Region;

pub type Date = u32;

#[inline(always)]
pub fn create_date(year: u16, month: u8, day: u8) -> u32 {
    (year as u32) << 16 + (month as u32) << 8 + (day as u32)
}

fn parse_date(date: &str) -> Date {
    let delim = "-";
    let mut fields = date.split(&delim);
    let year = fields.next().unwrap().parse().unwrap();
    let month = fields.next().unwrap().parse().unwrap();
    let day = fields.next().unwrap().parse().unwrap();
    create_date(year, month, day)
}

fn copy_from_to(src: &[u8], dst: &mut [u8]) {
    let limit = if src.len() < dst.len() { src.len() } else { dst.len() };
    for index in 0 .. limit {
        dst[index] = src[index];
    }
}

pub mod part {

    use abomonation::Abomonation;
    use super::copy_from_to;

    unsafe_abomonate!(Part : name, comment);

    #[derive(Default,Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct Part {
        pub part_key: usize,
        pub name: String,
        pub mfgr: [u8; 25],
        pub brand: [u8; 10],
        pub typ: [u8; 25],
        pub size: i32,
        pub container: [u8; 10],
        pub retail_price: i64,
        pub comment: String,
    }

    impl<'a> From<&'a str> for Part {
        fn from(text: &'a str) -> Part {

            let mut result: Part = Default::default();
            let delim = "|";
            let mut fields = text.split(&delim);

            result.part_key = fields.next().unwrap().parse().unwrap();
            result.name = fields.next().unwrap().to_owned();
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.mfgr);
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.brand);
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.typ);
            result.size = fields.next().unwrap().parse().unwrap();
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.container);
            result.retail_price = (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64;
            result.comment = fields.next().unwrap().to_owned();

            result
        }
    }
}

pub mod supplier {

    use abomonation::Abomonation;
    use super::copy_from_to;
    
    unsafe_abomonate!(Supplier : address, comment);

    #[derive(Default,Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct Supplier {
        pub supp_key: usize,
        pub name: [u8; 25],
        pub address: String,
        pub nation_key: usize,
        pub phone: [u8; 15],
        pub acctbal: i64,
        pub comment: String,
    }

    impl<'a> From<&'a str> for Supplier {
        fn from(text: &'a str) -> Supplier {

            let mut result: Supplier = Default::default();
            let delim = "|";
            let mut fields = text.split(&delim);

            result.supp_key = fields.next().unwrap().parse().unwrap();
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.name);
            result.address = fields.next().unwrap().to_owned();
            result.nation_key = fields.next().unwrap().parse().unwrap();
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.phone);
            result.acctbal = (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64;
            result.comment = fields.next().unwrap().to_owned();

            result
        }
    }
}

pub mod part_supp {

    use abomonation::Abomonation;
    unsafe_abomonate!(PartSupp : comment);

    #[derive(Default,Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct PartSupp {
        pub part_key: usize,
        pub supp_key: usize,
        pub availqty: i32,
        pub supplycost: i64,
        pub comment: String,
    }

    impl<'a> From<&'a str> for PartSupp {
        fn from(text: &'a str) -> PartSupp {

            let mut result: PartSupp = Default::default();
            let delim = "|";
            let mut fields = text.split(&delim);

            result.part_key = fields.next().unwrap().parse().unwrap();
            result.supp_key = fields.next().unwrap().parse().unwrap();
            result.availqty = fields.next().unwrap().parse().unwrap();
            result.supplycost = (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64;
            result.comment = fields.next().unwrap().to_owned();

            result
        }
    }
}

pub mod customer {

    use abomonation::Abomonation;
    use super::copy_from_to;

    unsafe_abomonate!(Customer : address, comment);

    #[derive(Default,Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct Customer {
        pub cust_key: usize,
        pub name: [u8; 25],
        pub address: String,
        pub nation_key: usize,
        pub phone: [u8; 15],
        pub acctbal: i64,
        pub mktsegment: [u8; 10],
        pub comment: String,
    }

    impl<'a> From<&'a str> for Customer {
        fn from(text: &'a str) -> Customer {

            let mut result: Customer = Default::default();
            let delim = "|";
            let mut fields = text.split(&delim);

            result.cust_key = fields.next().unwrap().parse().unwrap();
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.name);
            result.address = fields.next().unwrap().to_owned();
            result.nation_key = fields.next().unwrap().parse().unwrap();
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.phone);
            result.acctbal = (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64;
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.mktsegment);
            result.comment = fields.next().unwrap().to_owned();

            result
        }
    }
}

pub mod order {

    use abomonation::Abomonation;
    use super::{parse_date, copy_from_to, Date};

    unsafe_abomonate!(Order : comment);

    #[derive(Default,Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct Order {
        pub order_key: usize,
        pub cust_key: usize,
        pub order_status: [u8; 1],
        pub total_price: i64,
        pub order_date: Date,
        pub order_priority: [u8; 15],
        pub clerk: [u8; 15],
        pub ship_priority: i32,
        pub comment: String,
    }

    impl<'a> From<&'a str> for Order {
        fn from(text: &'a str) -> Order {

            let mut result: Order = Default::default();
            let delim = "|";
            let mut fields = text.split(&delim);

            result.order_key = fields.next().unwrap().parse().unwrap();
            result.cust_key = fields.next().unwrap().parse().unwrap();
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.order_status);
            result.total_price = (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64;
            result.order_date = parse_date(&fields.next().unwrap());
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.order_priority);
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.clerk);
            result.ship_priority = fields.next().unwrap().parse().unwrap();
            result.comment = fields.next().unwrap().to_owned();

            result
        }
    }
}

pub mod line_item {

    use abomonation::Abomonation;
    use super::{parse_date, copy_from_to, Date};

    unsafe_abomonate!(LineItem : comment);

    #[derive(Default,Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct LineItem {
        pub order_key: usize,
        pub part_key: usize,
        pub supp_key: usize,
        pub line_number: i32,
        pub quantity: i64,
        pub extended_price: i64,
        pub discount: i64,
        pub tax: i64,
        pub return_flag: [u8; 1],
        pub line_status: [u8; 1],
        pub ship_date: Date,
        pub commit_date: Date,
        pub receipt_date: Date,
        pub ship_instruct: [u8; 25],
        pub ship_mode: [u8; 10],
        pub comment: String,
    }

    impl<'a> From<&'a str> for LineItem {
        fn from(text: &'a str) -> LineItem {

            let mut result: LineItem = Default::default();
            let delim = "|";
            let mut fields = text.split(&delim);

            result.order_key = fields.next().unwrap().parse().unwrap();
            result.part_key = fields.next().unwrap().parse().unwrap();
            result.supp_key = fields.next().unwrap().parse().unwrap();
            result.line_number = fields.next().unwrap().parse().unwrap();
            result.quantity = (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64;
            result.extended_price = (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64;
            result.discount = (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64;
            result.tax = (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64;
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.return_flag);
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.line_status);
            result.ship_date = parse_date(&fields.next().unwrap());
            result.commit_date = parse_date(&fields.next().unwrap());
            result.receipt_date = parse_date(&fields.next().unwrap());
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.ship_instruct);
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.ship_mode);
            result.comment = fields.next().unwrap().to_owned();

            result
        }
    }
}

pub mod nation {

    use abomonation::Abomonation;
    use super::copy_from_to;

    unsafe_abomonate!(Nation : comment);

    #[derive(Default,Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct Nation {
        pub nation_key: usize,
        pub name: [u8; 25],
        pub region_key: usize,
        pub comment: String,
    }

    impl<'a> From<&'a str> for Nation {
        fn from(text: &'a str) -> Nation {

            let mut result: Nation = Default::default();
            let delim = "|";
            let mut fields = text.split(&delim);

            result.nation_key = fields.next().unwrap().parse().unwrap();
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.name);
            result.region_key = fields.next().unwrap().parse().unwrap();
            result.comment = fields.next().unwrap().to_owned();

            result
        }
    }
}

pub mod region {

    use abomonation::Abomonation;
    use super::copy_from_to;

    unsafe_abomonate!(Region : comment);

    #[derive(Default,Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct Region {
        pub region_key: usize,
        pub name: [u8; 25],
        pub comment: String,
    }

    impl<'a> From<&'a str> for Region {
        fn from(text: &'a str) -> Region {

            let mut result: Region = Default::default();
            let delim = "|";
            let mut fields = text.split(&delim);

            result.region_key = fields.next().unwrap().parse().unwrap();
            copy_from_to(fields.next().unwrap().as_bytes(), &mut result.name);
            result.comment = fields.next().unwrap().to_owned();

            result
        }
    }
}