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

def arrange_some_params():
    experiments.run_cmd("cargo build --release --bin arrange")

    experiment_name = "arrange"

    experiments.eprint("### {} ###".format(experiment_name))
    experiments.eprint(experiments.experdir(experiment_name))

    for keys in [1000000]:
        for recs in [1000000]:
            for rate in [1000000]:
                for work in [10]:
                    for comp in ["exchange"]:
                        config = {
                            "keys": keys,
                            "recs": recs,
                            "rate": rate,
                            "work": work,
                            "comp": comp
                        }

                        n = 1
                        w = 4

                        filename = experiment_setup(experiment_name, n, w, **config)
                        experiments.eprint("RUNNING {}".format(filename))
                        commands = [
                                "hwloc-bind node:{}.core:0-9.pu:0 -- ./target/release/arrange {} -n {} -p {} -w {}".format(
                                    p,
                                    " ".join(str(x) for x in config.values()),
                                    n,
                                    p,
                                    w) for p in range(0, n)]
                        processes = [experiments.run_cmd(command, filename, True) for command in commands]
                        experiments.waitall(processes)
