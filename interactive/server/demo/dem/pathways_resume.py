#!/usr/bin/env python3
"""Rebuild one pathways world by replaying its accepted semantic commands.

The DDIR server is intentionally in-memory. This utility is the crash-recovery
boundary: it stages a fresh server from the recorded briefing, then replays
accepted surveys, deliveries, paving, and route retirement in event order.
"""

import argparse
import json
import os
import shlex
import shutil
import socket
import subprocess
import sys
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from pathways_game import (
    program_hashes,
    setup,
    stop_recorded_server,
    write_json,
)


REPLAYABLE = {"survey", "deliver", "pave", "retire"}


def port_is_open(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


def accepted_commands(events_path):
    commands = []
    with open(events_path) as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            event = json.loads(line)
            if not event.get("accepted"):
                continue
            tokens = shlex.split(event["command"])
            if not tokens or tokens[0] not in REPLAYABLE:
                raise RuntimeError(
                    f"cannot replay accepted event {line_number}: {event['command']}"
                )
            commands.append((int(event["agent"]), tokens))
    return commands


def resume(source_run, destination_run, port, ws_host):
    source_run = os.path.abspath(source_run)
    destination_run = os.path.abspath(destination_run)
    with open(os.path.join(source_run, "briefing.json")) as source:
        briefing = json.load(source)
    if briefing.get("version", 1) < 2:
        raise RuntimeError("semantic replay supports pathways briefing version 2+")
    expected_hashes = briefing.get("program_hashes")
    if expected_hashes is not None and expected_hashes != program_hashes():
        raise RuntimeError(
            "current DDIR programs differ from the recorded world; "
            "recover with the matching revision"
        )
    if port_is_open(int(briefing["port"])):
        raise RuntimeError(
            "source world is still reachable; stop it before creating a replacement"
        )

    if os.path.exists(destination_run):
        raise RuntimeError(f"destination already exists: {destination_run}")
    commands = accepted_commands(os.path.join(source_run, "events.jsonl"))
    with open(os.path.join(source_run, "site_office.md")) as source:
        prior_office = source.read()

    staging_run = destination_run + ".recovering-" + uuid.uuid4().hex[:8]
    client_path = os.path.join(HERE, "pathways_client.py")
    try:
        setup(
            staging_run,
            port,
            ws_host,
            briefing["grid"],
            briefing_template=briefing,
        )
        for agent, tokens in commands:
            subprocess.run(
                [
                    sys.executable,
                    client_path,
                    "--run-dir",
                    staging_run,
                    "--agent",
                    str(agent),
                    *tokens,
                ],
                check=True,
            )

        office_path = os.path.join(staging_run, "site_office.md")
        with open(office_path, "w") as destination:
            destination.write(prior_office.rstrip())
            destination.write(
                "\n\n## Recovery\n\n"
                f"Replayed {len(commands)} accepted commands from "
                f"`{source_run}`.\n"
            )
        with open(os.path.join(staging_run, "briefing.json")) as source:
            recovered_briefing = json.load(source)
        recovered_briefing["run_dir"] = destination_run
        write_json(
            os.path.join(staging_run, "briefing.json"),
            recovered_briefing,
        )
        os.rename(staging_run, destination_run)
    except Exception:
        stop_recorded_server(staging_run)
        shutil.rmtree(staging_run, ignore_errors=True)
        raise
    print(f"recovered {len(commands)} commands into {destination_run}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-run", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--ws-host", default="127.0.0.1")
    args = parser.parse_args()
    resume(args.from_run, args.run_dir, args.port, args.ws_host)


if __name__ == "__main__":
    main()
