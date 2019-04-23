#!/usr/bin/env python3

import sys
import experiments
from collections import OrderedDict

arg_node = int(sys.argv[sys.argv.index('--node') + 1])

def experiment_setup(experiment_name, n, w, **config):
    experiments.ensuredir(experiment_name)
    return "{}/{}_n={}_w={}_{}".format(
        experiments.experdir(experiment_name),
        experiment_name,
        n,
        w,
        "_".join(["{}={}".format(k, str(v)) for k, v in config.items()]))

def arrange_closed_loop():
    experiments.run_cmd("cargo build --release --bin arrange", node = arg_node)
    for run in range(0, 10):
        experiment_name = "arrange-closed-loop-{}".format(run)

        experiments.eprint("### {} ###".format(experiment_name))
        experiments.eprint(experiments.experdir(experiment_name))

        for w in reversed([1, 2, 4, 8, 16, 32]):
            for keys in [10000000]:
                for recs in [32000000]:
                    for rate in [100000]: # * x for x in [1]]: #, 2, 4, 8]]:
                        for work in [4,]:
                            for comp in [
                                    # "exchange",
                                    "arrange",
                                    "maintain",
                                    # "selfjoin",
                                    "count",
                                    # "nothing",
                                    ]:
                                mode = "closedloop"
                                dmode = "seconds"
                                dparam = "60"

                                thp = "no"
                                zerocopy = "thread"
                                alloc = "jemallocalloc"
                                inputstrategy = "ms"

                                config = OrderedDict([
                                    ("keys", keys),
                                    ("recs", recs),
                                    ("rate", rate),
                                    ("work", work),
                                    ("comp", comp),
                                    ("mode", mode),
                                    ("dmode", dmode),
                                    ("dparam", dparam),
                                    ("alloc", alloc),
                                    ("zerocopy", zerocopy),
                                    ("thp", thp),
                                    ("inputstrategy", inputstrategy),
                                ])

                                n = 1

                                filename = experiment_setup(experiment_name, n, w, **config)
                                experiments.eprint("RUNNING {}".format(filename))
                                commands = [
                                        ("DIFFERENTIAL_EFFORT={} ./target/release/arrange {} -h hostfile.txt -n {} -p {} -w {}".format(
                                            work,
                                            " ".join(str(x) for x in [keys, recs, rate, comp, mode, dmode, dparam, zerocopy, w, alloc, inputstrategy]),
                                            n,
                                            p,
                                            w), p) for p in range(0, n)]
                                experiments.eprint("commands: {}".format(commands))
                                processes = [experiments.run_cmd(command, filename, True, node = arg_node) for command, p in commands]
                                experiments.waitall(processes)

def arrange_open_loop_load_varies():
    experiment_name = "arrange-open-loop-load-varies"
    experiments.run_cmd("cargo build --release --bin arrange", node = arg_node)

    experiments.eprint("### {} ###".format(experiment_name))
    experiments.eprint(experiments.experdir(experiment_name))

    for n in [1]:
        for w in [1]:
            total_workers = n * w
            for factor in [1, 2, 4, 8, 16, 32]:
                rate = int(1000000 / factor)
                keys = int(10000000 / factor)
                for recs in [32000000]:
                    for work in [4]:
                        for comp in [
                                # "exchange",
                                # "arrange",
                                "maintain",
                                # "selfjoin",
                                # "count",
                                # "nothing",
                                ]:
                            mode = "openloop"
                            dmode = "overwrite"
                            dparam = "50"

                            thp = "no"
                            zerocopy = "thread"
                            alloc = "jemallocalloc"
                            inputstrategy = "ms"

                            config = OrderedDict([
                                ("keys", keys),
                                ("recs", recs),
                                ("rate", rate),
                                ("work", work),
                                ("comp", comp),
                                ("mode", mode),
                                ("dmode", dmode),
                                ("dparam", dparam),
                                ("alloc", alloc),
                                ("zerocopy", zerocopy),
                                ("thp", thp),
                                ("inputstrategy", inputstrategy),
                            ])

                            filename = experiment_setup(experiment_name, n, w, **config)
                            experiments.eprint("RUNNING {}".format(filename))
                            commands = [
                                    ("DIFFERENTIAL_EFFORT={} ./target/release/arrange {} -h hostfile.txt -n {} -p {} -w {}".format(
                                        work,
                                        " ".join(str(x) for x in [keys, recs, rate, comp, mode, dmode, dparam, zerocopy, w, alloc, inputstrategy]),
                                        n,
                                        p,
                                        w), p) for p in range(0, n)]
                            experiments.eprint("commands: {}".format(commands))
                            processes = [experiments.run_cmd(command, filename, True, node = arg_node) for command, p in commands]
                            experiments.waitall(processes)

def arrange_open_loop_strong_scaling():
    experiment_name = "arrange-open-loop-strong-scaling"
    experiments.run_cmd("cargo build --release --bin arrange", node = arg_node)

    experiments.eprint("### {} ###".format(experiment_name))
    experiments.eprint(experiments.experdir(experiment_name))

    for n in [1]:
        for w in [32, 16, 8, 4, 2, 1]: # 32
            total_workers = n * w
            for keys in [10000000]: #, 20000000]:
                for recs in [32000000]: # , 64000000]:
                    for rate in [750000, 1000000, 1250000]:
                        for work in [1, 4, "max"]:
                            for comp in [
                                    # "exchange",
                                    # "arrange",
                                    "maintain",
                                    # "selfjoin",
                                    # "count",
                                    # "nothing",
                                    ]:
                                mode = "openloop"
                                dmode = "seconds"
                                dparam = "300"

                                thp = "no"
                                zerocopy = "thread"
                                alloc = "jemallocalloc"
                                inputstrategy = "ms"

                                config = OrderedDict([
                                    ("keys", keys),
                                    ("recs", recs),
                                    ("rate", rate),
                                    ("work", work),
                                    ("comp", comp),
                                    ("mode", mode),
                                    ("dmode", dmode),
                                    ("dparam", dparam),
                                    ("alloc", alloc),
                                    ("zerocopy", zerocopy),
                                    ("thp", thp),
                                    ("inputstrategy", inputstrategy),
                                ])

                                filename = experiment_setup(experiment_name, n, w, **config)
                                experiments.eprint("RUNNING {}".format(filename))
                                commands = [
                                        ("DIFFERENTIAL_EFFORT={} ./target/release/arrange {} -h hostfile.txt -n {} -p {} -w {}".format(
                                            work,
                                            " ".join(str(x) for x in [keys, recs, rate, comp, mode, dmode, dparam, zerocopy, w, alloc, inputstrategy]),
                                            n,
                                            p,
                                            w), p) for p in range(0, n)]
                                experiments.eprint("commands: {}".format(commands))
                                processes = [experiments.run_cmd(command, filename, True, node = arg_node) for command, p in commands]
                                experiments.waitall(processes)

def arrange_open_loop_weak_scaling():
    experiment_name = "arrange-open-loop-weak-scaling"
    experiments.run_cmd("cargo build --release --bin arrange", node = arg_node)

    experiments.eprint("### {} ###".format(experiment_name))
    experiments.eprint(experiments.experdir(experiment_name))

    for n in [1]:
        for w in [32, 16, 8, 4, 2, 1]:
            total_workers = n * w
            for keys in [(10000000 // 32) * total_workers]: # , 20000000]:
                for recs in [(32000000 // 32) * total_workers]: #, 64000000]:
                    for rate in [1000000 * w]:
                        for work in [1, 4, "max"]:
                            for inputstrategy in ["ms"]:
                                for comp in [
                                        # "exchange",
                                        "arrange",
                                        "maintain",
                                        # "selfjoin",
                                        "count",
                                        # "nothing",
                                        ]:
                                    mode = "openloop"
                                    dmode = "overwrite"
                                    dparam = "50"

                                    thp = "no"
                                    zerocopy = "thread"
                                    alloc = "jemallocalloc"

                                    config = OrderedDict([
                                        ("keys", keys),
                                        ("recs", recs),
                                        ("rate", rate),
                                        ("work", work),
                                        ("comp", comp),
                                        ("mode", mode),
                                        ("dmode", dmode),
                                        ("dparam", dparam),
                                        ("alloc", alloc),
                                        ("zerocopy", zerocopy),
                                        ("thp", thp),
                                        ("inputstrategy", inputstrategy),
                                    ])

                                    filename = experiment_setup(experiment_name, n, w, **config)
                                    experiments.eprint("RUNNING {}".format(filename))
                                    commands = [
                                            ("DIFFERENTIAL_EFFORT={} ./target/release/arrange {} -h hostfile.txt -n {} -p {} -w {}".format(
                                                work,
                                                " ".join(str(x) for x in [keys, recs, rate, comp, mode, dmode, dparam, zerocopy, w, alloc, inputstrategy]),
                                                n,
                                                p,
                                                w), p) for p in range(0, n)]
                                    experiments.eprint("commands: {}".format(commands))
                                    processes = [experiments.run_cmd(command, filename, True, node = arg_node) for command, p in commands]
                                    experiments.waitall(processes)

def arrange_install():
    experiment_name = "arrange-install"
    experiments.run_cmd("cargo build --release --bin install", node = arg_node)

    experiments.eprint("### {} ###".format(experiment_name))
    experiments.eprint(experiments.experdir(experiment_name))

    for n in [1]:
        for w in [32]:
            total_workers = n * w
            for keys in [10000000]: # , 20000000]:
                for recs in [32000000]: #, 64000000]:
                    work = 4

                    thp = "no"
                    zerocopy = "thread"
                    alloc = "jemallocalloc"

                    config = OrderedDict([
                        ("keys", keys),
                        ("recs", recs),
                        ("work", work),
                        ("alloc", alloc),
                        ("zerocopy", zerocopy),
                        ("thp", thp),
                    ])

                    filename = experiment_setup(experiment_name, n, w, **config)
                    experiments.eprint("RUNNING {}".format(filename))
                    commands = [
                            ("DIFFERENTIAL_EFFORT={} ./target/release/install {} -h hostfile.txt -n {} -p {} -w {}".format(
                                work,
                                " ".join(str(x) for x in [keys, recs, w]),
                                n,
                                p,
                                w), p) for p in range(0, n)]
                    experiments.eprint("commands: {}".format(commands))
                    processes = [experiments.run_cmd(command, filename, True, node = arg_node) for command, p in commands]
                    experiments.waitall(processes)

