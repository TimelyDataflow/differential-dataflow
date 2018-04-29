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

    for w in reversed([4, 8, 16, 32]):
        for keys in [10000000]:
            for recs in [32000000]:
                for rate in [1000000 * x for x in [2, 4, 8]]:
                    for work in [1, 4, "max"]:
                        for comp in [
                                "exchange",
                                "arrange",
                                "maintain",
                                "selfjoin",
                                "count",
                                "nothing",
                                ]:
                            mode = "closedloop"
                            dmode = "overwrite"
                            dparam = "10"

                            config = {
                                "keys": keys,
                                "recs": recs,
                                "rate": rate,
                                "work": work,
                                "comp": comp,
                                "mode": mode,
                                "dmode": dmode,
                                "dparam": dparam,
                            }

                            n = 1

                            filename = experiment_setup(experiment_name, n, w, **config)
                            experiments.eprint("RUNNING {}".format(filename))
                            commands = [
                                    "./target/release/arrange {} -n {} -p {} -w {}".format(
                                        " ".join(str(x) for x in [keys, recs, rate, work, comp, mode, dmode, dparam]),
                                        n,
                                        p,
                                        w) for p in range(0, n)]
                            experiments.eprint("commands: {}".format(commands))
                            processes = [experiments.run_cmd(command, filename, True) for command in commands]
                            experiments.waitall(processes)

