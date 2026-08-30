#!/usr/bin/env python3
"""Feasibility proof for the recovery-balanced asymmetric scenario."""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from logistics_game import (
    DEPOT,
    GRANTS,
    MAX_SERVICE_HOPS,
    ROLES,
    TOWN,
    VILLAGE,
    initial_roads,
    scenario_terrain,
)
from logistics_rules import (
    BRIDGE,
    ROAD,
    TERRAFORM,
    action_cost,
    replay_actions,
    shortest_service_path,
)
from run_dem import priority_flood


class LogisticsScenarioTest(unittest.TestCase):
    def test_known_solution_uses_every_role_and_preserves_recovery_margin(self):
        _, terrain, dam = scenario_terrain()
        actions = []
        revision = 0

        def state():
            return replay_actions(
                terrain=terrain,
                initial_infrastructure=initial_roads(),
                depots={DEPOT},
                roles=ROLES,
                grants=GRANTS,
                actions=actions,
                priority_flood=priority_flood,
                locked=dam,
                protected=set(VILLAGE),
            )

        def build(agent, kind, cells):
            nonlocal revision
            revision += 1
            for item, (x, y) in enumerate(cells):
                actions.append(dict(
                    revision=revision, item=item, agent=agent, kind=kind,
                    x=x, y=y, old=0, new=0, cost=1,
                ))
            self.assertEqual(state()["violations"], [])

        def dig(cells):
            nonlocal revision
            before = state()
            revision += 1
            for item, (x, y) in enumerate(cells):
                old = before["terrain"][(x, y)]
                actions.append(dict(
                    revision=revision, item=item, agent=3, kind=TERRAFORM,
                    x=x, y=y, old=old, new=1708,
                    cost=action_cost(TERRAFORM, old, 1708),
                ))
            self.assertEqual(state()["violations"], [])

        build(1, ROAD, [(97, 27), (96, 27), (95, 27),
                        (94, 27), (94, 28), (94, 29)])
        build(2, BRIDGE, [(95, 35)])
        dig([(95, 28), (95, 29), (96, 28)]
            + [(97, y) for y in range(28, 34)]
            + [(98, 33)])
        for y in range(30, 34):
            build(1, ROAD, [(94, y)])
            dig([(95, y)])
        dig([(95, 34), (95, 35), (95, 36)])
        build(1, ROAD, [(94, 36)])
        for y in range(37, 42):
            build(1, ROAD, [(94, y)])
            dig([(95, y)])

        final = state()
        self.assertTrue(all(
            final["water"][cell] == final["terrain"][cell]
            for cell in VILLAGE
        ))
        self.assertEqual(final["spend"][(1, ROAD)], 16)
        self.assertEqual(final["spend"][(2, BRIDGE)], 1)
        self.assertEqual(final["spend"][(3, TERRAFORM)], 773)
        self.assertEqual(GRANTS[(3, TERRAFORM)] - final["spend"][(3, TERRAFORM)], 177)
        self.assertEqual(
            shortest_service_path(final["passable"], {DEPOT}, TOWN),
            MAX_SERVICE_HOPS,
        )


if __name__ == "__main__":
    unittest.main()
