import unittest

from .pathways_rules import (
    BRIDGE,
    ROAD,
    connected_cells,
    replay_history,
    required_kind,
    shortest_route,
)


class PathwaysRulesTest(unittest.TestCase):
    def setUp(self):
        self.terrain = {(x, y): 0 for x in range(3) for y in range(3)}
        self.water = dict(self.terrain)
        self.accum = {cell: 1 for cell in self.terrain}

    def test_distance_only_uses_diagonal(self):
        path, cost = shortest_route(
            self.terrain, self.water, self.accum,
            (0, 0), (2, 2), (1, 0, 0, 0),
        )
        self.assertEqual(path, [(0, 0), (1, 1), (2, 2)])
        self.assertEqual(cost, 74)
        _path, coarse_cost = shortest_route(
            self.terrain,
            self.water,
            self.accum,
            (0, 0),
            (2, 2),
            (1, 0, 0, 0),
            step_lengths=(53, 75),
        )
        self.assertEqual(coarse_cost, 150)

    def test_grade_weight_selects_longer_gentle_route(self):
        self.terrain[(1, 1)] = 100
        self.water = dict(self.terrain)
        direct, _ = shortest_route(
            self.terrain, self.water, self.accum,
            (0, 0), (2, 2), (1, 0, 0, 0),
        )
        gentle, gentle_cost = shortest_route(
            self.terrain, self.water, self.accum,
            (0, 0), (2, 2), (1, 1, 0, 0),
        )
        self.assertIn((1, 1), direct)
        self.assertNotIn((1, 1), gentle)
        self.assertEqual(gentle_cost, 89)

    def test_existing_path_and_road_attract_a_reuse_weighted_route(self):
        terrain = {(x, y): 0 for x in range(5) for y in range(3)}
        water = dict(terrain)
        accum = {cell: 1 for cell in terrain}
        path_use = {(x, 0): 2 for x in range(5)}
        raw, _ = shortest_route(
            terrain,
            water,
            accum,
            (0, 1),
            (4, 1),
            (1, 0, 0, 0, 0),
            path_use,
            {},
        )
        reused, reused_cost = shortest_route(
            terrain,
            water,
            accum,
            (0, 1),
            (4, 1),
            (1, 0, 0, 0, 4),
            path_use,
            {},
        )
        self.assertEqual(raw, [(0, 1), (1, 1), (2, 1), (3, 1), (4, 1)])
        self.assertIn((2, 0), reused)
        self.assertLess(reused_cost, 4 * 26 * (1 + 2 * 4))

        road = {(x, 0): (ROAD, 1) for x in range(5)}
        road_route, road_cost = shortest_route(
            terrain,
            water,
            accum,
            (0, 1),
            (4, 1),
            (1, 0, 0, 0, 4),
            {},
            road,
        )
        self.assertIn((2, 0), road_route)
        self.assertLess(road_cost, reused_cost)

    def test_bridge_requirement_covers_water_and_major_runoff(self):
        self.assertEqual(
            required_kind((1, 1), self.terrain, self.water, self.accum),
            ROAD,
        )
        self.water[(1, 1)] = 1
        self.assertEqual(
            required_kind((1, 1), self.terrain, self.water, self.accum),
            BRIDGE,
        )
        self.water[(1, 1)] = self.terrain[(1, 1)]
        self.accum[(1, 1)] = 1024
        self.assertEqual(
            required_kind((1, 1), self.terrain, self.water, self.accum),
            BRIDGE,
        )

    def test_public_network_allows_diagonal_roads(self):
        infrastructure = {
            (1, 1): (ROAD, 1),
            (2, 2): (ROAD, 1),
        }
        connected = connected_cells(
            infrastructure, {(0, 0)}, self.terrain, self.water
        )
        self.assertEqual(connected, {(0, 0), (1, 1), (2, 2)})

    def test_history_replay_checks_path_before_paving_and_freight(self):
        terrain = {(x, 0): 0 for x in range(3)}
        water = dict(terrain)
        accum = {cell: 1 for cell in terrain}
        briefing = {
            "version": 2,
            "sites": [
                {
                    "id": 1,
                    "label": "Town",
                    "cell": [2, 0],
                    "kind": 0,
                    "amount": 20,
                },
                {
                    "id": 10,
                    "label": "Source",
                    "cell": [0, 0],
                    "kind": 1,
                    "amount": 20,
                },
            ],
            "agents": {
                "1": {
                    "road_grant": 2,
                    "bridge_grant": 0,
                }
            },
            "orthogonal_metres": 1,
            "diagonal_metres": 1,
            "trail_use_required": 2,
            "bridge_accum_threshold": 10,
            "porter_trip_capacity": 5,
            "porter_town_quota": 10,
            "max_live_routes": 1,
        }
        events = [
            {
                "accepted": True,
                "agent": 1,
                "command": "survey 100 10 1 1 0 0 0 1",
            },
            {
                "accepted": True,
                "agent": 1,
                "command": "deliver 1 5 100",
            },
            {
                "accepted": True,
                "agent": 1,
                "command": "deliver 1 5 100",
            },
            {
                "accepted": True,
                "agent": 1,
                "command": "pave 100 10",
                "detail": {
                    "cells": [[1, 0, ROAD], [2, 0, ROAD]],
                },
            },
            {
                "accepted": True,
                "agent": 1,
                "command": "deliver 1 10 100",
            },
            {
                "accepted": True,
                "agent": 1,
                "command": "retire 100",
            },
        ]
        replayed = replay_history(events, briefing, terrain, water, accum)
        self.assertEqual(replayed["route_requests"], {})
        self.assertEqual(
            replayed["infrastructure"],
            {(1, 0): (ROAD, 1), (2, 0): (ROAD, 1)},
        )
        self.assertEqual(
            [value[4] for value in replayed["deliveries"].values()],
            [0, 0, 1],
        )
        self.assertEqual(replayed["path_use"], {cell: 3 for cell in terrain})

        premature = [events[0], events[3]]
        with self.assertRaisesRegex(ValueError, "precedes path establishment"):
            replay_history(premature, briefing, terrain, water, accum)


if __name__ == "__main__":
    unittest.main()
