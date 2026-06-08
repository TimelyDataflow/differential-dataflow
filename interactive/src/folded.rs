//! Centralized `user_chain` (folded iteration-time) algebra.
//!
//! The reverse-tracing explanation rewrite moves iteration time *into data*: a
//! demand row's value carries some number of user-chain coordinates
//! (innermost-first) plus a trailing query id. Indexing those by hand —
//! `FieldExpr::Index(1, v_pre + …)` scattered across the reverse rules — is
//! what produced SCC explain's off-by-one (comparing a just-left inner scope's
//! coord against an enclosing scope's). This module owns that arithmetic so it
//! exists once, correct, and every rule (and the scope builder's ports) routes
//! through it.

use crate::parse::{Condition, FieldExpr, Projection};

/// The value layout of a *joined* demand row, as the reverse lookups produce it
/// before filtering: in the value row (`$1`),
/// `[V_pre (v_pre)] [user_in (in_len)] [user_out (out_len)] [q]`,
/// with each user-chain innermost-first.
#[derive(Clone, Copy, Debug)]
pub struct Joined {
    /// Non-time value columns carried through (e.g. `V_in`).
    pub v_pre: usize,
    /// user-chain length of the input side.
    pub in_len: usize,
    /// user-chain length of the output (demand) side.
    pub out_len: usize,
}

impl Joined {
    /// Value-row index of input-side user coord `i` (0 = innermost).
    fn user_in(self, i: usize) -> usize {
        self.v_pre + i
    }
    /// Value-row index of output-side user coord `i` (0 = innermost).
    fn user_out(self, i: usize) -> usize {
        self.v_pre + self.in_len + i
    }
    /// Value-row index of the trailing query id.
    fn q(self) -> usize {
        self.v_pre + self.in_len + self.out_len
    }

    /// The soundness filter `user_in.time ≤ user_out.time`, aligned at the
    /// **outer** ends. user_chain is innermost-first, so when the two sides
    /// differ in length (a `Leave` crossing drops the innermost coord) the
    /// *shared* scopes are the outer ones — the last `min(in,out)` coords of
    /// each side. Aligning from index 0 instead is exactly the bug that made
    /// SCC explain unsound. Returns `None` when there's nothing to compare.
    pub fn time_le(self) -> Option<Condition> {
        let n = self.in_len.min(self.out_len);
        let in_off = self.in_len - n;
        let out_off = self.out_len - n;
        let mut acc: Option<Condition> = None;
        for i in 0..n {
            let cond = Condition::Le(
                FieldExpr::Index(1, self.user_in(in_off + i)),
                FieldExpr::Index(1, self.user_out(out_off + i)),
            );
            acc = Some(match acc {
                None => cond,
                Some(prev) => Condition::And(Box::new(prev), Box::new(cond)),
            });
        }
        acc
    }

    /// The projection that strips `user_out` and the innermost `user_in` coords
    /// past `keep_in_len`, leaving `(K[k_out]; V_pre ++ outer keep_in_len ++ [q])`.
    /// The kept coords are the *outer* ones (last `keep_in_len`), matching the
    /// outer alignment in `time_le`.
    pub fn strip(self, k_out: usize, keep_in_len: usize) -> Projection {
        assert!(keep_in_len <= self.in_len, "strip: keep_in_len exceeds in_len");
        let key = (0..k_out).map(|i| FieldExpr::Index(0, i)).collect();
        let mut val: Vec<FieldExpr> = Vec::new();
        for i in 0..self.v_pre {
            val.push(FieldExpr::Index(1, i));
        }
        let drop_in = self.in_len - keep_in_len;
        for i in 0..keep_in_len {
            val.push(FieldExpr::Index(1, self.user_in(drop_in + i)));
        }
        val.push(FieldExpr::Index(1, self.q()));
        Projection { key, val }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn le_indices(c: &Condition) -> (usize, usize) {
        match c {
            Condition::Le(FieldExpr::Index(1, a), FieldExpr::Index(1, b)) => (*a, *b),
            other => panic!("expected a single value-row Le, got {:?}", other),
        }
    }

    #[test]
    fn time_le_aligns_outer_ends_across_a_leave() {
        // Leave crossing: input is one scope deeper (in_len 2) than the demand
        // (out_len 1). The shared scope is the OUTER one, so compare
        // user_in[1] (not user_in[0], the just-left inner scope) vs user_out[0].
        let j = Joined { v_pre: 3, in_len: 2, out_len: 1 };
        // user_in[1] = 3 + 1 = 4 ; user_out[0] = 3 + 2 + 0 = 5
        assert_eq!(le_indices(&j.time_le().unwrap()), (4, 5));
    }

    #[test]
    fn time_le_equal_depth_aligns_from_zero() {
        // Same depth (e.g. a Concat input): align from index 0.
        let j = Joined { v_pre: 2, in_len: 1, out_len: 1 };
        // user_in[0] = 2 ; user_out[0] = 2 + 1 = 3
        assert_eq!(le_indices(&j.time_le().unwrap()), (2, 3));
    }

    #[test]
    fn time_le_empty_when_no_shared_coords() {
        assert!(Joined { v_pre: 1, in_len: 1, out_len: 0 }.time_le().is_none());
    }

    #[test]
    fn strip_keeps_outer_coords_and_q() {
        // in_len 2, keep 1 → drop the innermost input coord, keep the outer one.
        let j = Joined { v_pre: 1, in_len: 2, out_len: 1 };
        let p = j.strip(1, 1);
        let idx = |f: &FieldExpr| match f {
            FieldExpr::Index(r, c) => (*r, *c),
            other => panic!("expected Index, got {:?}", other),
        };
        assert_eq!(p.key.iter().map(idx).collect::<Vec<_>>(), vec![(0, 0)]);
        // val = [V_pre[0]=(1,0), user_in[1]=(1,2) (outer kept coord), q=(1,4)]
        assert_eq!(p.val.iter().map(idx).collect::<Vec<_>>(), vec![(1, 0), (1, 2), (1, 4)]);
    }
}
