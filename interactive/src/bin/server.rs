extern crate timely;
extern crate interactive;

use timely::synchronization::Sequencer;
use interactive::{Manager, Command, Query, Rule, Plan};

fn main() {

    let mut args = std::env::args();
    args.next();

    timely::execute_from_args(args, |worker| {

        let timer = ::std::time::Instant::now();
        let mut manager = Manager::new();
        let mut sequencer = Some(Sequencer::new(worker, timer));

        if worker.index() == 0 {

            let edges = (0 .. 100).map(|x| vec![x, (x+1)%100]).collect::<Vec<_>>();

            sequencer.as_mut().map(|x| x.push(Command::CreateInput("edges".to_string(), edges)));
            sequencer.as_mut().map(|x| x.push(Command::AdvanceTime(1)));
            sequencer.as_mut().map(|x| x.push(Command::Query(
                Query {
                    rules: vec![Rule {
                        name: "fof".to_string(),
                        plan: Plan::source("edges").join(Plan::source("edges"), vec![(0,1)])
                                                   .project(vec![1,2])
                                                   .inspect("fof"),
                    }]
                }
            )));
            sequencer.as_mut().map(|x| x.push(Command::AdvanceTime(2)));
            sequencer.as_mut().map(|x| x.push(Command::CloseInput("edges".to_string())));
            sequencer.as_mut().map(|x| x.push(Command::Query(
                Query {
                    rules: vec![Rule {
                        name: "fof2".to_string(),
                        plan: Plan::source("fof").join(Plan::source("fof"), vec![(0,1)])
                                                   .project(vec![1,2])
                                                   .inspect("fof2"),
                    }]
                }
            )));
            sequencer.as_mut().map(|x| x.push(Command::Shutdown));
        }

        let mut shutdown = false;

        while !shutdown {

            if let Some(command) = sequencer.as_mut().unwrap().next() {
                println!("{:?}\tExecuting {:?}", timer.elapsed(), command);
                if command == Command::Shutdown {
                    shutdown = true;
                    sequencer = None;
                }
                command.execute(&mut manager, worker);
            }

            worker.step();
        }

    }).expect("Timely computation did not exit cleanly");
}
