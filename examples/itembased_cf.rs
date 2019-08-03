extern crate timely;
extern crate differential_dataflow;
extern crate rand;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{Join,CountTotal,Count};

use rand::{Rng, SeedableRng, StdRng};


fn main() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let mut interactions_input = InputSession::new();

        let probe = worker.dataflow(|scope| {

            let interactions = interactions_input.to_collection(scope);

            // Find all users with more than 5 and less than 500 interactions
            let users_with_enough_interactions = interactions
                .map(|(user, _item)| user)
                .count_total()
                .filter(move |(_user, count): &(u32, isize)| *count > 5 && *count < 500)
                .map(|(user, _count)| user);

            // Remove users with too few or too many interactions
            let remaining_interactions = interactions
                .semijoin(&users_with_enough_interactions);

            let num_interactions_per_item = remaining_interactions
                .map(|(_user, item)| item)
                .count_total();

            // Compute the number of cooccurrences of each item pair
            let cooccurrences = remaining_interactions
                .join_map(&remaining_interactions, |_user, &item_a, &item_b| (item_a, item_b))
                .filter(|&(item_a, item_b)| item_a > item_b)
                .count();

            // Compute the jaccard similarity between item pairs (= number of users that interacted
            // with both items / number of users that interacted with at least one of the items)
            let jaccard_similarities = cooccurrences
                .map(|((item_a, item_b), num_cooc)| (item_a, (item_b, num_cooc)))
                .join_map(&num_interactions_per_item, |&item_a, &(item_b, num_cooc), &occ_a| {
                    (item_b, (item_a, num_cooc, occ_a))
                })
                .join_map(
                    &num_interactions_per_item,
                    |&item_b, &(item_a, num_cooc, occ_a), &occ_b| {
                        ((item_a, item_b), (num_cooc, occ_a, occ_b))
                    },
                )
                // We have to do this calculation in a map due to the lack of a total order for f64
                .map(|((item_a, item_b), (num_cooc, occ_a, occ_b))| {
                    let jaccard = num_cooc as f64 / (occ_a + occ_b - num_cooc) as f64;
                    ((item_a, item_b), jaccard)
                });

            // We threshold the similarity matrix
            let thresholded_similarities = jaccard_similarities
                .filter(|(_item_pair, jaccard)| *jaccard > 0.05);

            thresholded_similarities.probe()
        });

        let num_interactions: usize = std::env::args().nth(1)
            .or_else(|| Some(String::from("10000"))).unwrap().parse().unwrap();

        if worker.index() == 0 {
            println!("Generating {} synthetic interactions...", num_interactions);
        }

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng: StdRng = SeedableRng::from_seed(seed);

        let synthetic_interactions = generate_interactions(num_interactions, &mut rng);

        let num_users = synthetic_interactions.iter().map(|(user, _item)| user).max().unwrap() + 1;
        let users_to_remove = rand::seq::sample_iter(&mut rng, 0..num_users, 20).unwrap();

        let interactions_to_remove: Vec<(u32,u32)> = synthetic_interactions.iter()
            .filter(|(user, _item)| users_to_remove.contains(&user))
            .map(|(user, item)| (*user, *item))
            .collect();

        let timer = worker.timer();


        for (user, item) in synthetic_interactions.iter() {
            if *user as usize % worker.peers() == worker.index() {
                interactions_input.insert((*user, *item));
            }
        }

        interactions_input.advance_to(1);
        interactions_input.flush();

        worker.step_while(|| probe.less_than(interactions_input.time()));

        let initial_model_time = timer.elapsed();
        println!("Model trained in {:?} on worker {:?}", initial_model_time, worker.index());

        if worker.index() == 0 {
            println!("Removing {} interactions...", interactions_to_remove.len());
        }

        for (user, item) in interactions_to_remove.iter() {
            if *user as usize % worker.peers() == worker.index() {
                interactions_input.remove((*user, *item));
            }
        }

        interactions_input.advance_to(2);
        interactions_input.flush();

        worker.step_while(|| probe.less_than(interactions_input.time()));

        let removal_time = timer.elapsed();
        println!(
            "Model updated after {:?} on worker {:?}",
            (removal_time - initial_model_time),
            worker.index(),
        );

    }).unwrap();
}


struct CRP { alpha: f64, discount: f64, weight: f64, weights: Vec<f64> }

impl CRP {
    fn new(alpha: f64, discount: f64) -> Self {
        CRP { alpha, discount, weight: 0.0, weights: Vec::new() }
    }

    fn sample<R>(&mut self, rng: &mut R) -> u32 where R: Rng {
        let mut u = rng.gen::<f64>() * (self.alpha + self.weight);
        for j in 0 .. self.weights.len() {
            if u < self.weights[j] - self.discount {
                self.weights[j] += 1.0;
                self.weight += 1.0;
                return j as u32;
            } else {
                u = u - self.weights[j] - self.discount;
            }
        }

        self.weights.push(1.0);
        self.weight = self.weight + 1.0;

        (self.weights.len() - 1) as u32
    }
}

// Generate synthetic interactions with a skewed distribution
fn generate_interactions<R>(how_many: usize, rng: &mut R) -> Vec<(u32,u32)> where R: Rng {
    let mut interactions = Vec::with_capacity(how_many);

    let mut user_sampler = CRP::new(6000.0, 0.35);
    let mut item_sampler = CRP::new(6000.0, 0.35);

    for _ in 0 .. how_many {
        let user = user_sampler.sample(rng);
        let item = item_sampler.sample(rng);
        interactions.push((user, item));
    }

    interactions
}