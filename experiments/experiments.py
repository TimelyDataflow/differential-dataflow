#!/usr/bin/env python3
import sys, os
from executor import execute

is_worktree_clean = execute("cd `git rev-parse --show-toplevel`; git diff-index --quiet HEAD -- src/ Cargo.toml experiments/src/ experiments/Cargo.toml tpchlike/src tpchlike/Cargo.toml", check=False)

if not is_worktree_clean:
    shall = input("Work directory dirty. Continue? (y/N) ").lower() == 'y'

current_commit = ("dirty-" if not is_worktree_clean else "") + execute("git rev-parse HEAD", capture=True)[:10]

def eprint(*args):
    print(*args, file=sys.stderr)

def experdir(name):
    return "results/{}/{}".format(current_commit, name)

def ensuredir(name):
    eprint("making directory: {}".format(experdir(name)))
    execute("mkdir -p {}".format(experdir(name)))

def waitall(processes):
    for p in processes:
        p.wait()

eprint("commit: {}".format(current_commit))

# assumes the experiment code is at this path on the cluster machine(s)
cluster_src_path = "/home/andreal/Src/differential-dataflow/experiments"
cluster_server = "andreal@fdr{}.ethz.ch"

def run_cmd(cmd, redirect=None, background=False, node=""):
    full_cmd = "cd {}; {}".format(cluster_src_path, cmd)
    actual_server = cluster_server.format(node)
    eprint("running on {}: {}".format(actual_server, full_cmd))
    if redirect is not None and os.path.exists(redirect):
        return execute("echo \"skipping {}\"".format(redirect), async=background)
    else:
        return execute("ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -t {} \"{}\"".format(actual_server, full_cmd) +
                        (" > {}".format(redirect) if redirect else ""), async=background)
