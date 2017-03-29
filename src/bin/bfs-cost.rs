
// We want a function which takes an existing graph, its distance labeling for each node, and a sequence of edge
// modifications, and which outputs the corresponding sequence of changes to the distance labelings for each node.

// For each depth, for each node, we can determine for which time intervals that depth is the correct answer. By
// accumulating this information over depths, we get larger and larger regions of time for which the node's depth
// is sorted out, and fewer interesting future moments.


type Node = usize;

// The input parameters are: 
//
//    edges: the graph structure in the form of an adjacency list.
//    dists: the proposed distances for each node, as (round, count).
//    changes: the edits we plan to make to the edge collection.

struct PerNode {
	// "join" state
	edges: Vec<Node>,					// edges
	diffs: Vec<(Node, usize, isize)>,	// changes to edges (time, diff).
	// "group" state
	dists: Vec<(usize, usize)>,			// distance proposals (and counts)
	edits: Vec<(usize, usize, isize)>,	// changes to distance proposals.
}

impl PerNode {
	// re-evaluate at all times with this depth
	fn update(&mut self, depth: usize, changes: &mut Vec<(usize, usize, isize)>) {

		let mut history = Vec::new();

		// self.edits is fixed for this round; we want to swing through each time
		self.edits.sort_by(|x,y| x.1.cmp(&y.1));
		let mut count_prev = 0;
		let mut count_this = 0;

		// initialize counts properly

		let mut new_edits = Vec::new();

		for (d, c) in &self.dists[..] {
			// need to determine if the output changes, and what to remember about this input change.
			if d < depth { 
				// change to a prior depth; could conceal/reveal this depth, or a later depth.
				if count_prev > 0 && count_prev + c == 0 {
					if 
				}

				count_prev += c; 
			}
			if d == depth { 




				count_this += c; 
			} 
			if d > depth {

			}
		}

	}
}

fn bfs(state: Vec<PerNode>) {

	let mut group_todo = Vec::new();

	for source in 0 .. edges.len() {
		for &(target, time, diff) in &state[source].diffs {
			if state[source].dists.len() > 0 {
				let distance = state[source].dists[0].0 + 1;
				state[target].edits.push((distance, time, diff));
				// add (target, time) to our todo list.
				while group_todo.len() <= distance { group_todo.push(Vec::new()); }
				group_todo[distance].push((target, time));
			}
		}
	}

	// We've now populated initial proposal changes for each node and initial todo lists for each distance.
	let mut depth = 0; 
	while depth < todo.len() {

		// perform all work in `todo[depth]`.
		let todo = ::std::mem::replace(&mut group_todo[depth], Vec::new());
		todo.sort();
		todo.dedup();

		let mut cursor = 0;
		while cursor < todo.len() {

			let node = todo[cursor];

			// prepare times at which to do work.
			while todo[cursor].0 == node {
				times.push(todo_depth[cursor].1);
				cursor += 1;
			}

			// perform work at indicated times. 
			state[node].update(&times[..], &mut new_times);
		}

		depth + 1;
	}
}

// NOTE: Cleverness in bfs: we don't really need a value, just the set of reachable nodes, as long as we can extract the
// round at which each became reachable. This means that the "state maintenance" becomes very easy if we use compacted
// times in dd, because we are just tracking "reachable yet or not", which should almost just be a counter.

// NOTE: For monotonic operators, it seems like we can put the monotonic quantity in the timestamp, like distances for
// bfs. What happens is that we learn when a quantity first becomes set, as in what "time" it starts to exist. This seems
// to have some positive implications for how state are compacted, and future interesting times. 