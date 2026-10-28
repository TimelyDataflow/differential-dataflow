//! Each scope item carries the name of the statement it was lowered from, through `optimize`,
//! and rendering logs which operators each label built.
use std::cell::RefCell;
use std::rc::Rc;

use interactive::backend::LabelEventBuilder;
use interactive::scope_ir::{Item, Node, Program, Scope};
use interactive::server::Server;
use interactive::{lower, parse};

const SRC: &str = "
let edges = input 0 : ((int) ; (int));
let pairs = edges | join(edges, (k, a, b) -> (a, b));
let counts = pairs | count;
reach: {
    let step = reach | join(edges, (k, (), b) -> (b, ()));
    set reach = (edges | map((a, _) -> (a, ()))) + step | distinct;
}
export \"counts\" = counts | map((k, n) -> (k, n));
export \"reach\" = reach::reach;
";

fn program() -> Program {
    let mut p = lower::lower_tree(parse::pipe::parse(SRC));
    p.optimize();
    p
}

fn check(s: &Scope) {
    assert_eq!(s.labels.len(), s.items.len(), "one label per item in {:?}", s.name);
    for (item, label) in s.items.iter().zip(&s.labels) {
        if let Item::Sub(child) = item {
            assert_eq!(label, &child.name);
            check(child);
        }
    }
}

#[test]
fn every_item_is_labelled_after_optimize() {
    let p = program();
    check(&p.root);
    let labelled = |name: &str| p.root.items.iter().zip(&p.root.labels).filter(|(_, l)| *l == name).map(|(i, _)| i).collect::<Vec<_>>();
    // `pairs` lowers to arrangements and a join; `edges` is shared and arranged once, by `pairs`.
    assert!(labelled("pairs").iter().any(|i| matches!(i, Item::Op(Node::Join { .. }))));
    assert!(labelled("counts").iter().any(|i| matches!(i, Item::Op(Node::Reduce { .. }))));
    // The export's identity map is gone; its declared export still names the count.
    assert!(labelled("export counts").is_empty());
    assert!(p.root.exports.iter().any(|e| e.name == "counts"));
    let Some(Item::Sub(reach)) = p.root.items.iter().find(|i| matches!(i, Item::Sub(_))) else { panic!("no child scope") };
    assert!(reach.labels.iter().any(|l| l == "step"));
    assert!(reach.labels.iter().any(|l| l == "reach"));
}

#[test]
fn rendering_logs_the_operators_of_each_label() {
    let events = timely::execute_directly(move |worker| {
        let events: Rc<RefCell<Vec<(usize, usize, String)>>> = Rc::default();
        let sink = Rc::clone(&events);
        worker.log_register().unwrap().insert::<LabelEventBuilder, _>("ddir/labels", move |_, data| {
            if let Some(data) = data {
                sink.borrow_mut().extend(data.iter().map(|(_, e)| (e.first, e.end, e.label.clone())));
            }
        });
        let mut server = Server::new();
        server.install(worker, "p", &program()).unwrap();
        let events = events.borrow().clone();
        events
    });
    let labels: Vec<&str> = events.iter().map(|(_, _, l)| l.as_str()).collect();
    for wanted in ["pairs", "counts", "step", "reach", "export counts"] {
        assert!(labels.contains(&wanted), "{wanted:?} not among {labels:?}");
    }
    for (first, end, label) in &events {
        assert!(first <= end, "{label:?}");
    }
    assert!(events.iter().any(|(first, end, label)| label == "pairs" && end > first), "pairs built operators");
}
