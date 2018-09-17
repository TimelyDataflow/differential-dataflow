extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::io::{BufRead, BufReader};
use std::fs::File;

use timely::dataflow::Scope;

use differential_dataflow::operators::iterate::Variable;

use differential_dataflow::Collection;
use differential_dataflow::input::Input;
use differential_dataflow::operators::*;
// use differential_dataflow::operators::iterate::CoreVariable;
use differential_dataflow::operators::arrange::ArrangeByKey;

type Iteration = u64;
type Difference = i64;

fn main() {
    if std::env::args().any(|x| x == "optimized") {
        optimized();
    }
    else {
        unoptimized();
    }
}

fn unoptimized() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        let peers = worker.peers();
        let index = worker.index();

        let (mut a, mut d) = worker.dataflow::<(),_,_>(|scope| {

            // let timer = timer.clone();

            let (a_handle, assignment) = scope.new_collection::<_,Difference>();
            let (d_handle, dereference) = scope.new_collection::<_,Difference>();

            let nodes =
            assignment
                .flat_map(|(a,b)| vec![a,b])
                .concat(&dereference.flat_map(|(a,b)| vec![a,b]));

            let dereference = dereference.arrange_by_key();

            let (value_flow, memory_alias, value_alias) =
            scope
                .scoped(|scope| {

                    let nodes = nodes.enter(scope);
                    let assignment = assignment.enter(scope);
                    let dereference = dereference.enter(scope);

                    // let value_flow = CoreVariable::<_,_,Difference,_>::from_args(Iteration::max_value(), 1, nodes.filter(|_| false).map(|n| (n,n)));
                    // let memory_alias = CoreVariable::<_,_,Difference,_>::from_args(Iteration::max_value(), 1, nodes.filter(|_| false).map(|n| (n,n)));
                    // let value_alias = CoreVariable::<_,_,Difference,_>::from_args(Iteration::max_value(), 1, nodes.filter(|_| false).map(|n| (n,n)));

                    let value_flow = Variable::new(scope, Iteration::max_value(), 1);
                    let memory_alias = Variable::new(scope, Iteration::max_value(), 1);
                    // let value_alias = Variable::from(nodes.filter(|_| false).map(|n| (n,n)));

                    let value_flow_arranged = value_flow.arrange_by_key();
                    let memory_alias_arranged = memory_alias.arrange_by_key();

                    // VA(a,b) <- VF(x,a),VF(x,b)
                    // VA(a,b) <- VF(x,a),MA(x,y),VF(y,b)
                    let value_alias_next = value_flow_arranged.join_core(&value_flow_arranged, |_,&a,&b| Some((a,b)));
                    let value_alias_next = value_flow_arranged.join_core(&memory_alias_arranged, |_,&a,&b| Some((b,a)))
                                                              .join_core(&value_flow_arranged, |_,&a,&b| Some((a,b)))
                                                              .concat(&value_alias_next);

                    // value_alias_next.map(|_|()).consolidate().inspect(|x| println!("VA-total: {:?}", x.2));

                    // let value_alias_next =
                    // value_alias_next
                    //     .threshold_total(|x| if x > 0 { 1 as Difference } else { 0 });


                    // VF(a,a) <-
                    // VF(a,b) <- A(a,x),VF(x,b)
                    // VF(a,b) <- A(a,x),MA(x,y),VF(y,b)
                    let value_flow_next =
                    assignment
                        .map(|(a,b)| (b,a))
                        .join_core(&memory_alias_arranged, |_,&a,&b| Some((b,a)))
                        .concat(&assignment.map(|(a,b)| (b,a)))
                        .join_core(&value_flow_arranged, |_,&a,&b| Some((a,b)))
                        .concat(&nodes.map(|n| (n,n)));

                    // value_flow_next.map(|_|()).consolidate().inspect(|x| println!("VF-total: {:?}", x.2));

                    let value_flow_next =
                    value_flow_next
                        .threshold_total(|x| if x > 0 { 1 as Difference } else { 0 });

                    // MA(a,b) <- D(x,a),VA(x,y),D(y,b)
                    let memory_alias_next: Collection<_,_,Difference> =
                    value_alias_next
                        .join_core(&dereference, |_x,&y,&a| Some((y,a)))
                        .join_core(&dereference, |_y,&a,&b| Some((a,b)));

                    // memory_alias_next.map(|_|()).consolidate().inspect(|x| println!("MA-total: {:?}", x.2));

                    let memory_alias_next: Collection<_,_,Difference>  =
                    memory_alias_next
                        .threshold_total(|x| if x > 0 { 1 as Difference } else { 0 });

                    value_flow.set(&value_flow_next);
                    memory_alias.set(&memory_alias_next);
                    // value_alias.set(&value_alias_next);

                    (value_flow_next.leave(), memory_alias_next.leave(), value_alias_next.leave())

                });

                value_flow.map(|_| ()).consolidate().inspect(|x| println!("VF: {:?}", x));
                memory_alias.map(|_| ()).consolidate().inspect(|x| println!("MA: {:?}", x));
                value_alias.map(|_| ()).consolidate().inspect(|x| println!("VA: {:?}", x));

            (a_handle, d_handle)
        });

        if index == 0 { println!("{:?}:\tDataflow assembled", timer.elapsed()); }

        // snag a filename to use for the input graph.
        let filename = std::env::args().nth(1).unwrap();
        let file = BufReader::new(File::open(filename).unwrap());
        for readline in file.lines() {
            let line = readline.ok().expect("read error");
            if !line.starts_with('#') && line.len() > 0 {
                let mut elts = line[..].split_whitespace();
                let src: u32 = elts.next().unwrap().parse().ok().expect("malformed src");
                if (src as usize) % peers == index {
                    let dst: u32 = elts.next().unwrap().parse().ok().expect("malformed dst");
                    let typ: &str = elts.next().unwrap();
                    match typ {
                        // "a" => { a.insert((src, dst)); },
                        "a" => a.update((src,dst), 1),
                        // "d" => { d.insert((src, dst)); },
                        "d" => d.update((src,dst), 1),
                        _ => { },
                    }
                }
            }
        }

        if index == 0 { println!("{:?}:\tData loaded", timer.elapsed()); }

        a.close();
        d.close();
        while worker.step() { }

        if index == 0 { println!("{:?}:\tComputation complete", timer.elapsed()); }
    }).unwrap();
}

fn optimized() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        let peers = worker.peers();
        let index = worker.index();

        let (mut a, mut d) = worker.dataflow::<(),_,_>(|scope| {

            // let timer = timer.clone();

            let (a_handle, assignment) = scope.new_collection();
            let (d_handle, dereference) = scope.new_collection();

            let nodes =
            assignment
                .flat_map(|(a,b)| vec![a,b])
                .concat(&dereference.flat_map(|(a,b)| vec![a,b]));

            let dereference = dereference.arrange_by_key();

            let (value_flow, memory_alias) =
            scope
                .scoped(|scope| {

                    let nodes = nodes.enter(scope);
                    let assignment = assignment.enter(scope);
                    let dereference = dereference.enter(scope);

                    let value_flow = Variable::new(scope, Iteration::max_value(), 1);
                    let memory_alias = Variable::new(scope, Iteration::max_value(), 1);

                    let value_flow_arranged = value_flow.arrange_by_key();
                    let memory_alias_arranged = memory_alias.arrange_by_key();

                    // VF(a,a) <-
                    // VF(a,b) <- A(a,x),VF(x,b)
                    // VF(a,b) <- A(a,x),MA(x,y),VF(y,b)
                    let value_flow_next =
                    assignment
                        .map(|(a,b)| (b,a))
                        .join_core(&memory_alias_arranged, |_,&a,&b| Some((b,a)))
                        .concat(&assignment.map(|(a,b)| (b,a)))
                        .join_core(&value_flow_arranged, |_,&a,&b| Some((a,b)))
                        .concat(&nodes.map(|n| (n,n)))
                        .distinct_total();

                    // VFD(a,b) <- VF(a,x),D(x,b)
                    let value_flow_deref =
                    value_flow
                        .map(|(a,b)| (b,a))
                        .join_core(&dereference, |_x,&a,&b| Some((a,b)))
                        .arrange_by_key();

                    // MA(a,b) <- VFD(x,a),VFD(y,b)
                    // MA(a,b) <- VFD(x,a),MA(x,y),VFD(y,b)
                    let memory_alias_next =
                    value_flow_deref
                        .join_core(&value_flow_deref, |_y,&a,&b| Some((a,b)));

                    let memory_alias_next =
                    memory_alias_arranged
                        .join_core(&value_flow_deref, |_x,&y,&a| Some((y,a)))
                        .join_core(&value_flow_deref, |_y,&a,&b| Some((a,b)))
                        .concat(&memory_alias_next)
                        .distinct_total();

                    value_flow.set(&value_flow_next);
                    memory_alias.set(&memory_alias_next);

                    (value_flow_next.leave(), memory_alias_next.leave())

                });

                value_flow.map(|_| ()).consolidate().inspect(|x| println!("VF: {:?}", x));
                memory_alias.map(|_| ()).consolidate().inspect(|x| println!("MA: {:?}", x));

            (a_handle, d_handle)
        });

        if index == 0 { println!("{:?}:\tDataflow assembled", timer.elapsed()); }

        // snag a filename to use for the input graph.
        let filename = std::env::args().nth(1).unwrap();
        let file = BufReader::new(File::open(filename).unwrap());
        for readline in file.lines() {
            let line = readline.ok().expect("read error");
            if !line.starts_with('#') && line.len() > 0 {
                let mut elts = line[..].split_whitespace();
                let src: u32 = elts.next().unwrap().parse().ok().expect("malformed src");
                if (src as usize) % peers == index {
                    let dst: u32 = elts.next().unwrap().parse().ok().expect("malformed dst");
                    let typ: &str = elts.next().unwrap();
                    match typ {
                        "a" => { a.insert((src, dst)); },
                        "d" => { d.insert((src, dst)); },
                        _ => { },
                    }
                }
            }
        }

        if index == 0 { println!("{:?}:\tData loaded", timer.elapsed()); }

        a.close();
        d.close();
        while worker.step() { }

        if index == 0 { println!("{:?}:\tComputation complete", timer.elapsed()); }

    }).unwrap();
}
