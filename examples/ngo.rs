#[allow(unused_variables)]
extern crate fnv;
extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use std::io::{BufReader, BufRead};
use std::fs::File;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use differential_dataflow::Data;
use differential_dataflow::operators::*;
use differential_dataflow::operators::join::{JoinBy, JoinUnsigned};
use differential_dataflow::operators::group::{GroupUnsigned};

macro_rules! join {
    ($source:ident : $help:ident : $(($other:ident, $key:expr, $recons:expr, $index:expr)),*) => {{

        // start each prefix with a large identifier and meaningless identifier
        let mut counts = $source.map(|(x,w)| ((x, (1 << 30, 1 << 30)), w));

        $( // for each other relation, determine the count of its extensions, then join against the counts so far.
            let temp = counts.map(|(p,ci)| ($key(p).0, ($key(p).1,ci)));
            counts = $other.map(|(p,_e)| (p,())).group_u(|_k,s,t| t.push(((s.next().unwrap().1, $index),1)))
                           .join_u(&temp).map(|(k,c1,(r,c2))| ($recons(k,r), if c1.0 < c2.0 { c1 } else { c2 }));
        )*

        // we need some help to name an empty collection
        let mut proposals = $help;
        $(
            // expand appropriate prefixes, add to proposals.
            proposals = counts.filter(|&((_p,(_c,i)),_w)| i == $index)
                              .map(|(p,_)| $key(p))
                              .join_u(&$other)
                              .map(|(k,r,e)| ($recons(k,r),e))
                              .concat(&proposals);
        )*

        $(
            // intersection each proposal with all relations
            proposals = proposals.semijoin_by(&$other, |(p,e)| (($key(p).0,e),$key(p).1), |k| k.hashed(), 
                |&(k,e), &r| ($recons(k,r),e));
        )*

        proposals
    }}
}

fn for_each_line<F: FnMut(String)>(filename: &str, mut logic: F) {
    let file = BufReader::new(File::open(filename).unwrap());
    for readline in file.lines() {
        let line = readline.ok().expect("read error");
        logic(line);
    }
}

fn for_each_pair_in<F: FnMut((u32, u32))>(filename: &str, mut logic: F) {
    for_each_line(filename, |string| {
        if !string.starts_with('#') {
            let mut fields = string[..].split(',');
            let a: u32 = fields.next().unwrap().parse().ok().expect("parse error");
            let b: u32 = fields.next().unwrap().parse().ok().expect("parse error");
            logic((a, b))
        }
    });
}

fn for_each_trip_in<F: FnMut((u32, u32, u32))>(filename: &str, mut logic: F) {
    for_each_line(filename, |string| {
        let mut fields = string[..].split(",");
        let a: u32 = fields.next().unwrap().parse().ok().expect("parse error");
        let b: u32 = fields.next().unwrap().parse().ok().expect("parse error");
        let c: u32 = fields.next().unwrap().parse().ok().expect("parse error");
        logic((a, b, c))
    });
}


fn main() {

    timely::execute_from_args(std::env::args(), |root| {

        let (mut c, mut p) = root.scoped::<u64, _, _>(move |outer| {

            // inputs for p, q, and u base facts.
            let (c_input, c) = outer.new_input();
            let (p_input, p) = outer.new_input();

            // let ir4 = rule_3!(p(_x1,z) := p(_x1,_y1) p(_x2,_w1) c(_y2,_w2,z) : (_x1) = (_x2), (_y1,_w1) = (_y2,_w2));

            let c2 = c.map(|((x,y,_z),w)| ((x,y),w));
            let ce = c.map(|((x,y,z),w)| (((x,y),z),w)).filter(|_| false);
            //
            // let trips = join!(p : ce :
            //     (p,  |(x,y)| (x,y), |x, y| (x,y), 0u32),
            //     (c2, |(x,y)| (y,x), |y, x| (x,y), 1u32)
            // );

            // trips.consolidate().map(|_| ((),1)).consolidate().inspect(|x| println!("count: {:?}", x));

            c2.map(|((x,y),w)| ((y,x),w))
              .join(&p.map(|((x,y),w)| ((y,x),w)))
              .map(|((_w,y,x),w)| (((x,y)),w))
              .join(&p);

            // p.join_u(&p).map(|((x,y,z),w)| (((y,z),x),w))
            //           .join(&c2.map(|(x,w)| ((x,()),w)))
            //         //   .map(|_| ((),1))
            //         //   .consolidate()
            //         //   .inspect(|x| println!("trad count: {:?}", x))
            //           ;

            (c_input, p_input)
        });

        // let mut p = root.scoped::<u64, _, _>(move |outer| {
        //
        //     let (p_input, p) = outer.new_input();
        //
        //     let p = p.map(|((x,y),w)| if x < y { ((x,y),w) } else { ((y,x),w) });
        //
        //     let ce = p.map(|((x,y),w)| (((x,y),0),w)).filter(|_| false);
        //
        //     let trips = join!(p : ce :
        //         (p, |(x,y)| (x,y), |p,r| (p,r), 0),
        //         (p, |(x,y)| (y,x), |p,r| (r,p), 1)
        //     );
        //
        //     // trips.consolidate().map(|_| ((),1)).consolidate().inspect(|x| println!("count: {:?}", x));
        //
        //     // p.join_u(&p).map(|((x,y,z),w)| (((y,z),x),w))
        //     //           .join(&p.map(|(x,w)| ((x,()),w)));
        //     //           .map(|_| ((),1))
        //     //           .consolidate()
        //     //           .inspect(|x| println!("trad count: {:?}", x));
        //
        //     p_input
        // });

        // worker 0 loads the data
        if root.index() == 0 {
            for_each_trip_in("/Users/mcsherry/Projects/Datasets/galen/c.txt", |x| c.send((x,1)));
            for_each_pair_in("/Users/mcsherry/Projects/Datasets/galen/p-final.txt", |x| p.send((x,1)));
            // for_each_pair_in("/Users/mcsherry/Projects/Datasets/soc-LiveJournal1.txt", |x| p.send((x,1)));
        }
    });
}
