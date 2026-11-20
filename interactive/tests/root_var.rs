//! A feedback variable at a program's root carries state from one time to the next.
use interactive::{lower, parse, server::{host_time, Server}};
use timely::progress::Antichain;

/// `seen` and `fresh` after each epoch, the epoch's rows fed at it and the input sealed past it.
fn run(src: &'static str, epochs: Vec<Vec<&'static str>>) -> Vec<(Vec<(String, String, i64)>, Vec<(String, String, i64)>)> {
    timely::execute(timely::Config::process(1), move |worker| {
        let program = lower::lower_tree(parse::pipe::parse(src));
        let mut server = Server::new();
        server.install(worker, "p", &program).unwrap();
        let mut seen = Vec::new();
        for (epoch, feeds) in epochs.iter().enumerate() {
            server.feed("p", 0, &feeds.join("\n"), epoch as u64).unwrap();
            server.seal("p", 0, &Antichain::from_elem(host_time(epoch as u64 + 1))).unwrap();
            server.spin(worker);
            let mut a = server.snapshot(worker, "p::seen").unwrap().rows();
            let mut b = server.snapshot(worker, "p::fresh").unwrap().rows();
            a.sort();
            b.sort();
            seen.push((a, b));
        }
        seen
    }).unwrap().join().pop().unwrap().unwrap()
}

/// `seen` is everything `x` ever held: it takes what is new at each epoch, one epoch later, and
/// keeps it after `x` lets go.
const REMEMBER: &str = r#"
    let x = input 0 : (int, ());
    var seen : (int, ());
    let fresh = x - (x | join(seen, (k, ((), ())) -> (k, ())));
    set seen = seen + fresh;
    output "seen" = seen;
    output "fresh" = fresh;
"#;

fn keys(rows: &[(String, String, i64)]) -> Vec<(String, i64)> {
    rows.iter().map(|(k, _, d)| (k.clone(), *d)).collect()
}

#[test]
fn a_root_variable_is_its_set_value_one_epoch_later() {
    let out = run(REMEMBER, vec![vec!["1", "2"], vec![], vec!["1 diff=-1", "2 diff=-1", "3"], vec![], vec![]]);
    let s = |i: usize| keys(&out[i].0);
    let f = |i: usize| keys(&out[i].1);
    let one = |ks: &[&str]| ks.iter().map(|k| (k.to_string(), 1)).collect::<Vec<_>>();
    // Epoch 0: `seen` starts empty, so 1 and 2 are fresh.
    assert_eq!(s(0), one(&[]));
    assert_eq!(f(0), one(&["1", "2"]));
    // Epoch 1: `seen` holds them, and nothing is fresh.
    assert_eq!(s(1), one(&["1", "2"]));
    assert_eq!(f(1), one(&[]));
    // Epoch 2: `x` drops 1 and 2 and takes 3; `seen` keeps 1 and 2, and 3 is fresh.
    assert_eq!(s(2), one(&["1", "2"]));
    assert_eq!(f(2), one(&["3"]));
    // Epochs 3 and 4: `seen` holds all three, each once, however many epochs pass.
    assert_eq!(s(3), one(&["1", "2", "3"]));
    assert_eq!(s(4), one(&["1", "2", "3"]));
    assert_eq!(f(4), one(&[]));
}
