#!/usr/bin/env python3

import experiments

def experiment_setup(experiment_name, n, w, **config):
    experiments.ensuredir(experiment_name)
    return "{}/{}_n={}_w={}_{}".format(
        experiments.experdir(experiment_name),
        experiment_name,
        n,
        w,
        "_".join(["{}={}".format(k, str(v)) for k, v in config.items()]))

def arrange():
    experiment_name = "arrange"

    experiments.eprint("### {} ###".format(experiment_name))
    experiments.eprint(experiments.experdir(experiment_name))

    for w in reversed([1, 2, 4, 8, 16, 31]):
        for nodes in [10000000]:
            for edges in [32000000]:
                for rate in [2500 * x for x in [2, 4, 8]]:
                    for goal in [5]:
                        for queries in [0, 10, 100, 1000]:
                            for shared in ["no", "shared"]:
                                for bidijkstra in ["no", "bidijkstra"]:
                                    config = {
                                        "nodes": nodes,
                                        "edges": edges,
                                        "rate": rate,
                                        "goal": goal,
                                        "queries": queries,
                                        "shared": shared,
                                        "bidijkstra": bidijkstra,
                                    }

                                    n = 1

                                    filename = experiment_setup(experiment_name, n, w, **config)
                                    experiments.eprint("RUNNING {}".format(filename))
                                    commands = [
                                            "./target/release/arrange {} -n {} -p {} -w {}".format(
                                                " ".join(str(x) for x in [nodes, edges, rate, goal, queries, shared, bidijkstra]),
                                                n,
                                                p,
                                                w) for p in range(0, n)]
                                    experiments.eprint("commands: {}".format(commands))
                                    processes = [experiments.run_cmd(command, filename, True) for command in commands]
                                    experiments.waitall(processes)

