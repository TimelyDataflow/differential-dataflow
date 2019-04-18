#!/usr/bin/env python3

import sys
import experiments
from collections import OrderedDict

arg_node = int(sys.argv[sys.argv.index('--node') + 1])
experiments.eprint("node {}".format(arg_node))

def experiment_setup(experiment_name, n, w, **config):
    experiments.ensuredir(experiment_name)
    return "{}/{}_n={}_w={}_{}".format(
        experiments.experdir(experiment_name),
        experiment_name,
        n,
        w,
        "_".join(["{}={}".format(k, str(v)) for k, v in config.items()]))

def graphs_interactive_alt():
    # experiments.run_cmd("cargo build --release --bin graphs-interactive-alt")

    experiment_name = "graphs-interactive-alt"

    experiments.eprint("### {} ###".format(experiment_name))
    experiments.eprint(experiments.experdir(experiment_name))

    for w in [32]:
        for nodes in [10000000]:
            for edges in [32000000]:
                for rate in [2500 * x for x in [2, 4, 8]]:
                    for goal in [5]:
                        for queries in [0, 10, 100, 1000]:
                            for shared in ["no", "shared"]:
                                for bidijkstra in ["no", "bidijkstra"]:
                                    config = OrderedDict([
                                        ("nodes", nodes),
                                        ("edges", edges),
                                        ("rate", rate),
                                        ("goal", goal),
                                        ("queries", queries),
                                        ("shared", shared),
                                        ("bidijkstra", bidijkstra),
                                    ])

                                    n = 1

                                    filename = experiment_setup(experiment_name, n, w, **config)
                                    experiments.eprint("RUNNING {}".format(filename))
                                    commands = [
                                            "DIFFERENTIAL_EFFORT=4 ./target/release/graphs-interactive-alt {} -n {} -p {} -w {}".format(
                                                " ".join(str(x) for x in [nodes, edges, rate, goal, queries, shared, bidijkstra, w]),
                                                n,
                                                p,
                                                w) for p in range(0, n)]
                                    experiments.eprint("commands: {}".format(commands))
                                    processes = [experiments.run_cmd(command, filename, True, node = arg_node) for command in commands]
                                    experiments.waitall(processes)

def graphs_interactive_neu():
    experiments.run_cmd(". ~/eth_proxy.sh; cargo build --release --bin graphs-interactive-neu", node = arg_node)

    experiment_name = "graphs-interactive-neu"

    experiments.eprint("### {} ###".format(experiment_name))
    experiments.eprint(experiments.experdir(experiment_name))

    for w in [32]:
        for nodes in [10000000]:
            for edges in [32000000]:
                for rate in [x * 20000 for x in range(10, 21)]: # + x for x in [-250000, 0, 250000, 500000]]:
                    for goal in [1800,]:
                        for queries in [32]: # [0, 1000, 10000, 100000]:
                            for shared in ["no", "shared"]:
                                config = OrderedDict([
                                    ("nodes", nodes),
                                    ("edges", edges),
                                    ("rate", rate),
                                    ("goal", goal),
                                    ("queries", queries),
                                    ("shared", shared),
                                ])

                                n = 1

                                filename = experiment_setup(experiment_name, n, w, **config)
                                experiments.eprint("RUNNING {}".format(filename))
                                commands = [
                                        "DIFFERENTIAL_EFFORT=4 ./target/release/graphs-interactive-neu {} -n {} -p {} -w {}".format(
                                            " ".join(str(x) for x in [nodes, edges, rate, goal, queries, shared, w]),
                                            n,
                                            p,
                                            w) for p in range(0, n)]
                                experiments.eprint("commands: {}".format(commands))
                                processes = [experiments.run_cmd(command, filename, True, node = arg_node) for command in commands]
                                experiments.waitall(processes)

def graphs_interactive_neu_zwei():
    # experiments.run_cmd("cargo build --release --bin graphs-interactive-neu")
    experiments.run_cmd(". ~/eth_proxy.sh; cargo build --release --bin graphs-interactive-neu-zwei", node = arg_node)

    experiment_name = "graphs-interactive-neu-zwei"

    experiments.eprint("### {} ###".format(experiment_name))
    experiments.eprint(experiments.experdir(experiment_name))

    for w in [32]:
        for nodes in [10000000]:
            for edges in [32000000]:
                for rate in [200000]: # + x for x in [-250000, 0, 250000, 500000]]:
                    for goal in [int((edges / rate) * 2),]:
                        for query in ["1", "2", "3", "4"]: # [0, 1000, 10000, 100000]:
                            config = OrderedDict([
                                ("nodes", nodes),
                                ("edges", edges),
                                ("rate", rate),
                                ("goal", goal),
                                ("query", query),
                            ])

                            n = 1

                            filename = experiment_setup(experiment_name, n, w, **config)
                            experiments.eprint("RUNNING {}".format(filename))
                            commands = [
                                    "DIFFERENTIAL_EFFORT=4 ./target/release/graphs-interactive-neu-zwei {} -n {} -p {} -w {}".format(
                                        " ".join(str(x) for x in [nodes, edges, rate, goal, query, w]),
                                        n,
                                        p,
                                        w) for p in range(0, n)]
                            experiments.eprint("commands: {}".format(commands))
                            processes = [experiments.run_cmd(command, filename, True, node = arg_node) for command in commands]
                            experiments.waitall(processes)
