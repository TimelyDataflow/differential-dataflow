extern crate timely;
extern crate interactive;

use std::time::{Instant, Duration};

use timely::synchronization::Sequencer;
use interactive::{Manager, Command, Query, Rule, Plan};

use interactive::plan::join::Join;
use interactive::plan::project::Project;

type Value = usize;

fn main() {

    let mut args = std::env::args();
    args.next();

    timely::execute_from_args(args, |worker| {

        let timer = Instant::now();
        let mut manager = Manager::new();
        let mut sequencer = Some(Sequencer::<Command<Value>>::new(worker, Instant::now()));

        if worker.index() == 0 {

            let edges = (0 .. 100).map(|x| vec![x, (x+1)%100]).collect::<Vec<_>>();

            sequencer.as_mut().map(|x| x.push(Command::CreateInput("edges".to_string(), edges)));
            worker.step();
            sequencer.as_mut().map(|x| x.push(Command::AdvanceTime(1)));
            worker.step();
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
            worker.step();
            sequencer.as_mut().map(|x| x.push(Command::AdvanceTime(2)));
            worker.step();
            sequencer.as_mut().map(|x| x.push(Command::CloseInput("edges".to_string())));
            worker.step();
            sequencer.as_mut().map(|x| x.push(Command::Shutdown));
        }

        let mut shutdown = false;

        while !shutdown {

            if let Some(command) = sequencer.as_mut().unwrap().next() {
                println!("{:?}\tExecuting {:#?}", timer.elapsed(), command);
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
