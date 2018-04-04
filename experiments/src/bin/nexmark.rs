extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::input::Input;
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Consolidate;

pub struct Item {
    id: usize,
    description: String,
    name: String,
    category_id: usize,
}

pub struct Category {
    id: usize,
    parent_id: usize,
    name: String,
    description: String,
}

pub struct Bid {
    auction_id: usize,
    bidder_id: usize,
    price: usize,
}

pub struct Person {
    id: usize,
    name: String,
    email: String,
    cc: String,
    city: String,
    state: String,
}

fn main() {


    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args().skip(4), move |worker| {

        let timer = ::std::time::Instant::now();

        let index = worker.index();
        let peers = worker.peers();

        // create a a degree counting differential dataflow
        let (mut views, mut links, probe) = worker.dataflow(|scope| {

            let (person_input, person) = scope.new_collection();
            let (auction_input, auction) = scope.new_collection();
            let (bid_input, bid) = scope.new_collection();

            let (item_input, item) = scope.new_collection();
            let (category_input, category) = scope.new_collection();

            // determine closed auctions, ...
            let closed_auction =
            auction
                .delay(|x| x.expires)
                .map(|x| x.id);

            // and active auctions.
            let active_auction =
            closed_auction
                .negate()
                .concat(&auction.map(|x| x.id));

            // closing prices
            bids.map(|x| (x.auction_id, x.price))
                .semijoin(&active_auction)
                .group(|_auction, source, target| {
                    target.push((source[source.len()-1]));
                })

            // Query 1:
            //
            // SELECT itemid, DOLTOEUR(price), bidderId, bidTime
            // FROM bid;
            bid.map_in_place(|x| x.price *= 0.9);

            // Query 2:
            //
            // SELECT itemid, price
            // FROM bid
            // WHERE itemid = 1007 OR
            //       itemid = 1020 OR
            //       itemid = 2001 OR
            //       itemid = 2019 OR
            //       itemid = 1087;
            bid.filter(|x| x.auction_id == 1007 || x.auction_id == 1020 || x.auction_id == 2001 || x.auction_id == 2019 || x.auction_id == 1087);

            // Query 3:
            //
            // SELECT person.name, person.city, person.state, open_auction.id
            // FROM open_auction, person, item
            // WHERE open_auction.sellerId = person.id AND person.state = ‘OR’
            // AND open_auction.itemid = item.id
            // AND item.categoryId = 10;
            let q3_items = item.filter(|x| x.category_id == 10).map(|x| x.id);
            let q3_person = person.filter(|x| x.state == "OR").map(|x| (x.id, (x.name, x.city, x.state)));
            open_auction
                .map(|x| (x.item_id, (x.seller_id, x.id)))
                .semijoin(&q3_items)
                .map(|(item_id, (seller, auction))| (seller, auction))
                .join_map(&q3_person, |_seller, &auction, data| (data.name.clone(), data.city.clone(), data.state.clone(), auction));

            // Query 4:
            //
            // SELECT C.id, AVG(CA.price)
            // FROM category C, item I, closed_auction CA
            // WHERE C.id = I.categoryId
            //   AND I.id = CA.itemid
            // GROUP BY C.id;
            let closed_prices =
            auction
                .explode(|x| Some(((x.item_id, ()), DiffPair::new(x.price, 1))));

            item.map(|x| (x.id, x.category))
                .semijoin(&closed_prices)
                .map(|x| x.1);

            // Query 5:
            //
            // SELECT bid.itemid
            // FROM bid [RANGE 60 MINUTES PRECEDING]
            // WHERE (SELECT COUNT(bid.itemid)
            //    FROM bid [PARTITION BY bid.itemid
            //      RANGE 60 MINUTES PRECEDING])
            //    >= ALL (SELECT COUNT(bid.itemid)
            //        FROM bid [PARTITION BY bid.itemid
            //          RANGE 60 MINUTES PRECEDING];
            let window =
            bids.map(|x| ((), x.auction))
                .delay(|t| t.inner += 60)
                .negate()
                .concat(&bids);

            window
                .group(|&(), source, target| {
                    let max_value = source[0].0;
                    let max_count = source[0].1.clone();
                    for &(value, count) in source.iter() {
                        if max_count < count {
                            max_count = count;
                            max_value = value.clone();
                        }
                    }
                    target.push((max_value, max_count));
                });

            // Query 6:
            //
            // SELECT AVG(CA.price), CA.sellerId
            // FROM closed auction CA
            //   [PARTITION BY CA.sellerId ROWS 10 PRECEDING];
            unimplemented!();

            // Query 7:
            //
            // SELECT bid.price, bid.itemid
            // FROM bid where bid.price =
            //    (SELECT MAX(bid.price)
            //    FROM bid [FIXEDRANGE
            //      10 MINUTES PRECEDING]);
            bids.map(|x| ((), (x.auction, x.price)))
                .delay(|t| t.inner += 1)
                .negate()
                .concat(&bids);

            // Query 8:
            //
            // SELECT person.id, person.name
            // FROM person [RANGE 12 HOURS PRECEDING],
            //   open auction [RANGE 12 HOURS PRECEDING]
            // WHERE person.id = open auction.sellerId;
        });

    }).unwrap();
}