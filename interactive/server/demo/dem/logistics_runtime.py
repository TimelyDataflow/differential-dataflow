"""Shared wire/file helpers for the civil-logistics scenario."""

import contextlib
import fcntl
import json
import os
import re
import socket

from run_physics import parse_rows

INT_RE = re.compile(r"Int\((-?\d+)\)")


def parse_weighted_rows(data_lines):
    """Return ``(diff, key_tuple, val_tuple)`` without hiding multiplicity."""
    rows = []
    for line in data_lines:
        diff = int(line.split("diff=", 1)[1].split(" ", 1)[0])
        key_text = line.split("key=Tuple([", 1)[1].split("])", 1)[0]
        val_text = line.split("val=Tuple([", 1)[1].split("])", 1)[0]
        key = tuple(int(v) for v in INT_RE.findall(key_text))
        val = tuple(int(v) for v in INT_RE.findall(val_text))
        rows.append((diff, key, val))
    return rows


def unique_rows(data_lines, label):
    """Require a functional, unit-multiplicity relation and return a dict."""
    out = {}
    for diff, key, val in parse_weighted_rows(data_lines):
        if diff != 1:
            raise RuntimeError(f"{label}: key {key} has diff {diff}, expected 1")
        if key in out:
            raise RuntimeError(f"{label}: duplicate key {key}")
        out[key] = val
    return out


def actions_from_client(client):
    rows = unique_rows(client.cmd("peek actions", collect=True), "actions")
    actions = []
    for (revision, item), value in rows.items():
        if len(value) != 7:
            raise RuntimeError(f"action {(revision, item)} has malformed value {value}")
        agent, kind, x, y, old, new, cost = value
        actions.append({
            "revision": revision,
            "item": item,
            "agent": agent,
            "kind": kind,
            "x": x,
            "y": y,
            "old": old,
            "new": new,
            "cost": cost,
        })
    return sorted(actions, key=lambda a: (a["revision"], a["item"]))


def load_program(client, name, path):
    with open(path) as source:
        lines = source.read().splitlines()
    request_id = f"load-{name}"
    client.send_lines(
        [f"{request_id} load {name} begin"]
        + lines
        + [f"{request_id} end-load"]
    )
    while True:
        tokens = client.read_line().split(" ", 2)
        if tokens[0] == request_id:
            if tokens[1] != "ok":
                detail = tokens[2] if len(tokens) > 2 else ""
                raise RuntimeError(f"load {name}: {tokens[1]} {detail}")
            return


def feed_chunks(client, lines, size=2000):
    for start in range(0, len(lines), size):
        chunk = lines[start:start + size]
        client.send_lines(chunk)
        client.drain_oks(len(chunk))


def ensure_ports_available(ports):
    probes = []
    try:
        for port in ports:
            probe = socket.socket()
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("127.0.0.1", port))
            probes.append(probe)
    except OSError as error:
        raise RuntimeError(f"required port {port} is unavailable: {error}") from error
    finally:
        for probe in probes:
            probe.close()


@contextlib.contextmanager
def run_lock(run_dir):
    path = os.path.join(run_dir, "game.lock")
    with open(path, "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def append_event(run_dir, event):
    path = os.path.join(run_dir, "events.jsonl")
    with open(path, "a") as events:
        events.write(json.dumps(event, sort_keys=True) + "\n")
        events.flush()
        os.fsync(events.fileno())


def read_briefing(run_dir):
    with open(os.path.join(run_dir, "briefing.json")) as source:
        return json.load(source)


def tuple_map(rows):
    """Compatibility helper for a unit-multiplicity peek."""
    return parse_rows(rows)
