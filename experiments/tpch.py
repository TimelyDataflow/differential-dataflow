#!/usr/bin/env python3

import sys
import experiments
from collections import OrderedDict

arg_node = int(sys.argv[sys.argv.index('--node') + 1])
prefix = sys.argv[sys.argv.index('--prefix') + 1]

experiments.eprint('prefix: {}'.format(prefix))

def experiment_setup(experiment_name, n, w, **config):
    experiments.ensuredir(experiment_name)
    return "{}/{}_n={}_w={}_{}".format(
        experiments.experdir(experiment_name),
        experiment_name,
        n,
        w,
        "_".join(["{}={}".format(k, str(v)) for k, v in config.items()]))

def i_tpchlike_mixing():
    # physical_batch = int(sys.argv[sys.argv.index('--physical_batch') + 1])
    physical_batch = 10000
    experiments.eprint("physical batch {}".format(physical_batch))

    experiments.run_cmd("cd ../tpchlike; . ~/eth_proxy.sh; cargo build --release --bin sosp --features jemalloc", node=arg_node)

    experiment_name = "i-tpchlike-mixing-jemalloc"

    experiments.eprint("### {} ###".format(experiment_name))
    experiments.eprint(experiments.experdir(experiment_name))

    n = 1
    w = 32 
    logical_batch = 1
    concurrent = 10
    seal_inputs = 'dontseal'
    #for arrange in ['false', 'true']: # 'false'
    for arrange in ['false', 'true']:
        # zerocopy = "no"
        # alloc = "jemallocalloc"

        config = OrderedDict([
            ("logicalbatch", logical_batch),
            ("physicalbatch", physical_batch),
            ("concurrent", concurrent),
            ("arrange", arrange),
            ("sealinputs", seal_inputs),
        ])

        filename = experiment_setup(experiment_name, n, w, **config)
        experiments.eprint("RUNNING {}".format(filename))
        commands = [
                ("../tpchlike/target/release/sosp {} -h hostfile.txt -n {} -p {} -w {}".format(
                    " ".join(str(x) for x in [prefix, logical_batch, physical_batch, concurrent, arrange, w, seal_inputs]),
                    n,
                    p,
                    w), p) for p in range(0, n)]
        experiments.eprint("commands: {}".format(commands))
        processes = [experiments.run_cmd(command, filename, True, node = arg_node) for command, p in commands]
        experiments.waitall(processes)

