//! Unit-value diversion through the existing tactic, checked against row-level answers.
use super::*;
use crate::corgi::chunk::{present_key, recover_key};
use crate::corgi::logic::{shape_of_row, transcode, untranscode};
use crate::ir::Value;
use differential_dataflow::operators::int_proxy::reduce::ProxyReduceTactic;
use differential_dataflow::operators::reduce::ReduceTactic;
use timely::order::Product;
use timely::progress::Antichain;
use timely::PartialOrder;

#[test]
fn distinct_with_unit_and_nonunit_values_matches_partial_order_oracle() {
    type T = Product<u64, u64>;
    let t = T::new;
    // Incomparable updates require a correction at their join. Some later novel
    // records cancel source records at the same time; their raw seeds must survive.
    let rounds = [
        vec![], // No value shape can be inferred from an empty first retire.
        vec![(7, 10, t(0, 2), 1), (7, 10, t(2, 0), 1),
             (8, 10, t(0, 0), -3), (8, 11, t(0, 0), 3)],
        vec![(7, 10, t(0, 2), -1), (8, 10, t(1, 1), 3), (8, 11, t(1, 1), -3)],
        vec![(7, 10, t(3, 1), -1), (7, 11, t(3, 3), 2)],
        vec![(7, 11, t(4, 4), -2)],
    ];
    for key_shape in 0..3 {
        let key = |k| match key_shape {
            0 => Value::Int(k),
            1 => Value::Tuple(vec![Value::Int(k), Value::Int(0)]),
            _ => Value::List(vec![Value::Int(k), Value::Int(0)]),
        };
        let shape = shape_of_row(&key(7)).unwrap();
        for unit in [false, true] {
            let mut tactic = ProxyReduceTactic::new(CorgiReduceBackend::new(Reducer::Distinct));
            let (mut source, mut output, mut original) = (Vec::new(), Vec::new(), Vec::new());
            for novel in &rounds {
                let input = Rc::new(columns_to_batch(
                    present_key(transcode(&novel.iter().map(|r| key(r.0)).collect::<Vec<_>>(), &shape)),
                    if unit { CValue::Unit(novel.len()) }
                    else { CValue::u64(novel.iter().map(|r| r.1).collect()) },
                    novel.iter().map(|r| r.2).collect(),
                    novel.iter().map(|r| r.3).collect(),
                ));
                let lower = Antichain::from_elem(t(0, 0));
                let upper = Antichain::from_elem(t(2, 2));
                let (result, pending) = tactic.retire(source.clone(), output.clone(), vec![input.clone()], &lower, &upper, &lower);
                if let Some(batch) = result.and_then(|s| s.inner) { output.push(batch); }
                source.push(input);
                original.extend_from_slice(novel);
                // Drain pending joins with no novel input.
                let (result, pending) = tactic.retire(source.clone(), output.clone(), vec![], &upper, &Antichain::new(), &pending);
                assert!(pending.is_empty());
                if let Some(batch) = result.and_then(|s| s.inner) { output.push(batch); }
                let mut actual = Vec::new();
                for chunk in chunks_of(&output) {
                    assert!(matches!(chunk.vals(), CValue::Unit(_)));
                    for (i, key) in untranscode(recover_key(chunk.keys()), &shape).into_iter().enumerate() {
                        actual.push((key, chunk.times().get(i), chunk.diffs()[i]));
                    }
                }
                for x in 0..=5 {
                    for y in 0..=5 {
                        for k in [7, 8] {
                            let time = t(x, y);
                            let mut counts = std::collections::BTreeMap::<u64, Diff>::new();
                            for &(rk, value, rt, diff) in &original {
                                if rk == k && rt.less_equal(&time) {
                                    *counts.entry(if unit { 0 } else { value }).or_default() += diff;
                                }
                            }
                            let expected = Diff::from(counts.values().any(|&d| d != 0));
                            let observed: Diff = actual.iter().filter(|r| r.0 == key(k) && r.1.less_equal(&time)).map(|r| r.2).sum();
                            assert_eq!(observed, expected, "key shape {key_shape}, unit {unit}, key {k}, time {time:?}");
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn unit_input_remains_resolvable_for_other_reducers() {
    for reducer in [Reducer::Min, Reducer::Count, Reducer::Collect] {
        let mut tactic = ProxyReduceTactic::new(CorgiReduceBackend::new(reducer.clone()));
        let (mut source, mut output) = (Vec::new(), Vec::new());
        let mut count = 0;
        for (epoch, diff) in [2, -2, -3, 4].into_iter().enumerate() {
            let input = Rc::new(columns_to_batch(CValue::u64(vec![7]), CValue::Unit(1), vec![epoch as u64], vec![diff]));
            let lower = Antichain::from_elem(epoch as u64);
            let upper = Antichain::from_elem(epoch as u64 + 1);
            let (result, pending) = tactic.retire(source.clone(), output.clone(), vec![input.clone()], &lower, &upper, &lower);
            assert!(pending.is_empty());
            source.push(input);
            if let Some(batch) = result.and_then(|s| s.inner) { output.push(batch); }
            let mut actual = Vec::new();
            for chunk in chunks_of(&output) {
                let values = untranscode(chunk.vals().clone(), &corgi::shape_of_value(chunk.vals()));
                actual.extend(values.into_iter().zip(chunk.diffs().iter().copied()));
            }
            differential_dataflow::consolidation::consolidate(&mut actual);
            count += diff;
            let expected = match reducer {
                Reducer::Count if count > 0 => vec![(Value::Tuple(vec![Value::Int(count)]), 1)],
                Reducer::Min if count != 0 => vec![(Value::unit(), 1)],
                Reducer::Collect if count != 0 => vec![(Value::List(vec![Value::unit(); count.max(0) as usize]), 1)],
                _ => vec![],
            };
            assert_eq!(actual, expected, "epoch {epoch}");
        }
    }
}
