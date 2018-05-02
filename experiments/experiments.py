#!/usr/bin/env python3
import sys, os
from executor import execute

is_worktree_clean = execute("cd `git rev-parse --show-toplevel`; git diff-index --quiet HEAD -- src/ Cargo.toml experiments/src/ experiments/Cargo.toml", check=False)

if not is_worktree_clean:
    shall = input("Work directory dirty. Continue? (y/N) ").lower() == 'y'

# current_commit = ("dirty-" if not is_worktree_clean else "") + execute("git rev-parse HEAD", capture=True)
current_commit = "may-1"

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
cluster_server = "andreal@fdr"

def run_cmd(cmd, redirect=None, background=False, node=""):
    full_cmd = "cd {}; {}".format(cluster_src_path, cmd)
    eprint("running on {}{}: {}".format(cluster_server, node, full_cmd))
    if redirect is not None and os.path.exists(redirect):
        return execute("echo \"skipping {}\"".format(redirect), async=background)
    else:
        return execute("ssh -t {}{} \"{}\"".format(cluster_server, node, full_cmd) +
                        (" > {}".format(redirect) if redirect else ""), async=background)
