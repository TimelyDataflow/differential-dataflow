#!/usr/bin/env python3
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from logistics_rules import (
    BRIDGE,
    ROAD,
    TERRAFORM,
    connected_cells,
    construction_frontier,
    passable_infrastructure,
    replay_actions,
    shortest_service_path,
)
from run_dem import priority_flood


class LogisticsRulesTest(unittest.TestCase):
    def test_flooded_surface_road_retracts_but_bridge_remains(self):
        terrain = {(x, 0): 0 for x in range(4)}
        infrastructure = {
            (0, 0): ROAD,
            (1, 0): ROAD,
            (2, 0): BRIDGE,
            (3, 0): ROAD,
        }
        water = dict(terrain)
        water[(1, 0)] = 1
        passable = passable_infrastructure(infrastructure, terrain, water)
        self.assertEqual(passable, {(0, 0), (2, 0), (3, 0)})
        self.assertEqual(connected_cells(passable, {(0, 0)}), {(0, 0)})

        water[(1, 0)] = 0
        passable = passable_infrastructure(infrastructure, terrain, water)
        self.assertEqual(
            connected_cells(passable, {(0, 0)}),
            {(0, 0), (1, 0), (2, 0), (3, 0)},
        )

    def test_frontier_and_shortest_service_path(self):
        road = {(0, 1), (1, 1), (2, 1), (2, 2)}
        domain = {(x, y) for x in range(4) for y in range(4)}
        connected = connected_cells(road, {(0, 1)})
        frontier = construction_frontier(connected, domain)
        self.assertIn((3, 1), frontier)
        self.assertIn((1, 2), frontier)
        self.assertEqual(shortest_service_path(road, {(0, 1)}, (2, 2)), 3)
        self.assertIsNone(shortest_service_path(road, {(0, 1)}, (3, 3)))

    def test_revision_replay_enforces_roles_access_and_resources(self):
        terrain = {(x, y): 0 for x in range(5) for y in range(3)}
        actions = [
            dict(revision=1, item=0, agent=1, kind=ROAD,
                 x=1, y=1, old=0, new=0, cost=1),
            dict(revision=2, item=0, agent=2, kind=BRIDGE,
                 x=2, y=1, old=0, new=0, cost=1),
            dict(revision=3, item=0, agent=3, kind=TERRAFORM,
                 x=2, y=2, old=0, new=-2, cost=2),
        ]
        state = replay_actions(
            terrain=terrain,
            initial_infrastructure={(0, 1): ROAD},
            depots={(0, 1)},
            roles={1: ROAD, 2: BRIDGE, 3: TERRAFORM},
            grants={(1, ROAD): 1, (2, BRIDGE): 1, (3, TERRAFORM): 2},
            actions=actions,
            priority_flood=priority_flood,
        )
        self.assertEqual(state["violations"], [])
        self.assertEqual(state["terrain"][(2, 2)], -2)
        self.assertEqual(state["spend"][(1, ROAD)], 1)
        self.assertEqual(state["spend"][(2, BRIDGE)], 1)
        self.assertEqual(state["spend"][(3, TERRAFORM)], 2)

    def test_replay_reports_stale_protected_and_over_budget_work(self):
        terrain = {(x, y): 0 for x in range(3) for y in range(3)}
        actions = [
            dict(revision=1, item=0, agent=3, kind=TERRAFORM,
                 x=1, y=1, old=7, new=3, cost=4),
        ]
        state = replay_actions(
            terrain=terrain,
            initial_infrastructure={(0, 1): ROAD},
            depots={(0, 1)},
            roles={3: TERRAFORM},
            grants={(3, TERRAFORM): 2},
            actions=actions,
            priority_flood=priority_flood,
            protected={(1, 1)},
        )
        joined = "\n".join(state["violations"])
        self.assertIn("protected earthwork", joined)
        self.assertIn("stale height", joined)
        self.assertIn("exceeds grant", joined)


if __name__ == "__main__":
    unittest.main()
