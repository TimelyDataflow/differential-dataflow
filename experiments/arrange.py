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

def arrange_closed_loop():
    for run in range(0, 10):
        experiment_name = "arrange-closed-loop-{}".format(run)

        experiments.eprint("### {} ###".format(experiment_name))
        experiments.eprint(experiments.experdir(experiment_name))

        for w in reversed([1, 2, 4, 8, 16, 32]):
            for keys in [10000000]:
                for recs in [32000000]:
                    for rate in [10000]: # * x for x in [1]]: #, 2, 4, 8]]:
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
                                dmode = "seconds"
                                dparam = "60"

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
                                        ("./target/release/arrange {} -h hostfile.txt -n {} -p {} -w {}".format(
                                            " ".join(str(x) for x in [keys, recs, rate, work, comp, mode, dmode, dparam]),
                                            n,
                                            p,
                                            w), p) for p in range(0, n)]
                                experiments.eprint("commands: {}".format(commands))
                                processes = [experiments.run_cmd(command, filename, True, node = 3) for command, p in commands]
                                experiments.waitall(processes)

def arrange_open_loop_load_varies():
    experiment_name = "arrange-open-loop-load-varies"

    experiments.eprint("### {} ###".format(experiment_name))
    experiments.eprint(experiments.experdir(experiment_name))

    for n in [1]:
        for w in [1]:
            total_workers = n * w
            for keys in [10000000, 20000000]:
                for recs in [32000000, 64000000]:
                    for rate in [2000000, 1000000, 500000, 250000, 125000, 75000]:
                        for work in [1, 4, "max"]:
                            for comp in [
                                    "exchange",
                                    "arrange",
                                    "maintain",
                                    "selfjoin",
                                    "count",
                                    "nothing",
                                    ]:
                                mode = "openloop"
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

                                filename = experiment_setup(experiment_name, n, w, **config)
                                experiments.eprint("RUNNING {}".format(filename))
                                commands = [
                                        ("./target/release/arrange {} -h hostfile.txt -n {} -p {} -w {}".format(
                                            " ".join(str(x) for x in [keys, recs, rate, work, comp, mode, dmode, dparam]),
                                            n,
                                            p,
                                            w), p) for p in range(0, n)]
                                experiments.eprint("commands: {}".format(commands))
                                processes = [experiments.run_cmd(command, filename, True, node = 4) for command, p in commands]
                                experiments.waitall(processes)

def arrange_open_loop_strong_scaling():
    experiment_name = "arrange-open-loop-strong-scaling"

    experiments.eprint("### {} ###".format(experiment_name))
    experiments.eprint(experiments.experdir(experiment_name))

    for n in [1]:
        for w in [32, 16, 8, 4, 2, 1]:
            total_workers = n * w
            for keys in [10000000, 20000000]:
                for recs in [32000000, 64000000]:
                    for rate in [750000, 1000000, 1250000]:
                        for work in [1, 4, "max"]:
                            for comp in [
                                    "exchange",
                                    "arrange",
                                    "maintain",
                                    "selfjoin",
                                    "count",
                                    "nothing",
                                    ]:
                                mode = "openloop"
                                dmode = "seconds"
                                dparam = "300"

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

                                filename = experiment_setup(experiment_name, n, w, **config)
                                experiments.eprint("RUNNING {}".format(filename))
                                commands = [
                                        ("./target/release/arrange {} -h hostfile.txt -n {} -p {} -w {}".format(
                                            " ".join(str(x) for x in [keys, recs, rate, work, comp, mode, dmode, dparam]),
                                            n,
                                            p,
                                            w), p) for p in range(0, n)]
                                experiments.eprint("commands: {}".format(commands))
                                processes = [experiments.run_cmd(command, filename, True, node = 3) for command, p in commands]
                                experiments.waitall(processes)

def arrange_open_loop_weak_scaling():
    experiment_name = "arrange-open-loop-weak-scaling"

    experiments.eprint("### {} ###".format(experiment_name))
    experiments.eprint(experiments.experdir(experiment_name))

    for n in [1]:
        for w in [32, 16, 8, 4, 2, 1]:
            total_workers = n * w
            for keys in [10000000, 20000000]:
                for recs in [32000000, 64000000]:
                    for rate in [750000 * w, 1000000 * w, 1250000 * w]:
                        for work in [1, 4, "max"]:
                            for comp in [
                                    "exchange",
                                    "arrange",
                                    "maintain",
                                    "selfjoin",
                                    "count",
                                    "nothing",
                                    ]:
                                mode = "openloop"
                                dmode = "overwrite"
                                dparam = "50"

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

                                filename = experiment_setup(experiment_name, n, w, **config)
                                experiments.eprint("RUNNING {}".format(filename))
                                commands = [
                                        ("./target/release/arrange {} -h hostfile.txt -n {} -p {} -w {}".format(
                                            " ".join(str(x) for x in [keys, recs, rate, work, comp, mode, dmode, dparam]),
                                            n,
                                            p,
                                            w), p) for p in range(0, n)]
                                experiments.eprint("commands: {}".format(commands))
                                processes = [experiments.run_cmd(command, filename, True, node = 2) for command, p in commands]
                                experiments.waitall(processes)

