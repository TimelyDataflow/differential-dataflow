import unittest

from .pathways_rules import (
    BRIDGE,
    ENGINEERED_ROAD,
    ROAD,
    connected_cells,
    engineered_profile,
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

    def test_v3_grade_constrains_engineered_edges_but_retains_exemptions(self):
        terrain = {(x, 0): height for x, height in enumerate((0, 5, 20))}
        water = dict(terrain)
        grade_options = {
            "grade_permille": 100,
            "step_lengths": (10, 14),
        }

        engineered = {(1, 0): (ENGINEERED_ROAD, 1)}
        self.assertEqual(
            connected_cells(
                engineered, {(0, 0)}, terrain, water, **grade_options
            ),
            {(0, 0)},
        )

        bridge_span = {
            (1, 0): (BRIDGE, 1),
            (2, 0): (ENGINEERED_ROAD, 1),
        }
        self.assertEqual(
            connected_cells(
                bridge_span, {(0, 0)}, terrain, water, **grade_options
            ),
            {(0, 0), (1, 0), (2, 0)},
        )

        legacy = {
            (1, 0): (ROAD, 1),
            (2, 0): (ROAD, 1),
        }
        self.assertEqual(
            connected_cells(
                legacy, {(0, 0)}, terrain, water, **grade_options
            ),
            {(0, 0), (1, 0), (2, 0)},
        )

    def test_engineered_profile_is_least_fill_and_rejects_fixed_boundary(self):
        path = [(x, 0) for x in range(4)]
        terrain = {
            (0, 0): 0,
            (1, 0): 0,
            (2, 0): 2,
            (3, 0): 0,
        }
        water = dict(terrain)
        accum = {cell: 1 for cell in terrain}
        profile = engineered_profile(
            path,
            {},
            {(0, 0)},
            terrain,
            water,
            accum,
            10,
            100,
            (10, 14),
        )
        self.assertEqual(
            profile,
            [
                ((1, 0), ENGINEERED_ROAD, 0, 1, 1),
                ((2, 0), ENGINEERED_ROAD, 2, 2, 0),
                ((3, 0), ENGINEERED_ROAD, 0, 1, 1),
            ],
        )

        blocked_terrain = {
            (0, 0): 0,
            (1, 0): 0,
            (2, 0): 3,
            (3, 0): 0,
            (4, 0): 0,
        }
        blocked_water = dict(blocked_terrain)
        blocked_accum = {cell: 1 for cell in blocked_terrain}
        with self.assertRaisesRegex(
            ValueError, "fill-only profile cannot meet fixed boundary"
        ):
            engineered_profile(
                [(x, 0) for x in range(5)],
                {},
                {(0, 0), (4, 0)},
                blocked_terrain,
                blocked_water,
                blocked_accum,
                10,
                100,
                (10, 14),
            )

    def test_bridge_or_embankment_changes_the_same_runoff_crossing(self):
        terrain = {(x, 0): 0 for x in range(3)}
        water = dict(terrain)
        accum = {cell: 1 for cell in terrain}
        accum[(1, 0)] = 10
        args = (
            [(0, 0), (1, 0), (2, 0)],
            {},
            {(0, 0)},
            terrain,
            water,
            accum,
            10,
            1000,
            (10, 14),
        )
        bridge = engineered_profile(*args, "bridge", 2)
        embankment = engineered_profile(*args, "embankment", 2)
        self.assertEqual(bridge[0], ((1, 0), BRIDGE, 0, 0, 0))
        self.assertEqual(
            embankment[0], ((1, 0), ENGINEERED_ROAD, 0, 2, 2)
        )

    def test_v3_replay_scouts_then_role_gates_one_cell_build(self):
        terrain = {(x, 0): 0 for x in range(3)}
        water = dict(terrain)
        accum = {cell: 1 for cell in terrain}
        briefing = {
            "version": 3,
            "sites": [
                {
                    "id": 1,
                    "label": "Hill worksite",
                    "cell": [2, 0],
                    "kind": 0,
                    "amount": 20,
                },
                {
                    "id": 10,
                    "label": "Valley stockpile",
                    "cell": [0, 0],
                    "kind": 1,
                    "amount": 20,
                },
            ],
            "agents": {
                "1": {
                    "road_grant": 2,
                    "bridge_grant": 1,
                    "build_kinds": [BRIDGE],
                },
                "2": {
                    "road_grant": 2,
                    "bridge_grant": 1,
                    "build_kinds": [ENGINEERED_ROAD],
                },
            },
            "orthogonal_metres": 10,
            "diagonal_metres": 14,
            "road_grade_permille": 100,
            "trail_use_required": 2,
            "bridge_accum_threshold": 10,
            "porter_trip_capacity": 5,
            "porter_town_quota": 10,
            "max_live_routes": 1,
            "scout_agent": 1,
            "scout_trip_limit": 2,
            "initial_aggregate": 2,
            "initial_rock": 0,
            "quarry_aggregate": 0,
            "quarry_rock": 0,
            "drainage_embankment_fill": 2,
        }
        history = [
            {
                "accepted": True,
                "agent": 1,
                "command": "survey 100 10 1 1 0 0 0 1",
            },
            {
                "accepted": True,
                "agent": 1,
                "command": "scout 100",
                "detail": {"trip": -1, "route": 100, "cells": 3},
            },
            {
                "accepted": True,
                "agent": 1,
                "command": "scout 100",
                "detail": {"trip": -2, "route": 100, "cells": 3},
            },
        ]
        wrong_role = {
            "accepted": True,
            "agent": 1,
            "command": "build 100 bridge",
            "detail": {
                "engineering": "fill-envelope-v1",
                "alignment": "bridge",
                "revision": 1,
                "cell": [1, 0, ENGINEERED_ROAD, 0, 0, 0],
            },
        }
        with self.assertRaisesRegex(
            ValueError, "role may not build the required surface road"
        ):
            replay_history(
                history + [wrong_role], briefing, terrain, water, accum
            )

        build = dict(wrong_role, agent=2)
        replayed = replay_history(
            history + [build], briefing, terrain, water, accum
        )
        self.assertEqual(
            replayed["infrastructure"],
            {(1, 0): (ENGINEERED_ROAD, 2)},
        )
        self.assertEqual(
            replayed["build_actions"],
            {(1, 0): (2, 100, 1, 0, ENGINEERED_ROAD, 0, 0)},
        )
        self.assertEqual(replayed["path_use"], {cell: 2 for cell in terrain})
        self.assertEqual(
            sorted({key[0] for key in replayed["traversals"]}), [-2, -1]
        )

        bad_detail = dict(build, detail=dict(build["detail"], revision=2))
        with self.assertRaisesRegex(
            ValueError, "recorded build action disagrees with replay"
        ):
            replay_history(
                history + [bad_detail], briefing, terrain, water, accum
            )

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
