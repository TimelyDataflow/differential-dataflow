#!/usr/bin/env python3
"""Rounds of ledger transactions, resolved by DDIR and made durable by a thin driver.

The server runs three kinds of program, all `.ddp`:
  state     state.ddp, standing: the manifest (registrations and spans) and the three indexes the rules read
  gen       requests.ddp, one per round: shapes flat columns of ints into requests
  res       ../../model/resolve.ddp, one per round: the rules, bound to `state` and `gen`

A round, driven from here:
  1. load `gen` and `res`, and feed the round's requests to `gen` as binary frames
  2. tick: the rules decide every request
  3. read the round's change to the manifest, as binary frames
  4. feed the rules' conclusions into `state`, drop `gen` and `res`, and tick
  5. write the frames as one blob, flush, swap the root, flush: the round is durable
Outcomes count as reported when step 5 returns. With `--mode pipelined` step 5 runs on another
thread while the next round resolves, and rounds that queue behind a commit are committed together.

The durable log is a chain of blobs, each naming the one before it, with a root file naming the last.
`--check` kills the server, rebuilds the manifest from the log on a fresh one, derives the indexes
from it with ../../model/ledger.ddp, checks that model's invariants, and compares what it rebuilt.

Every ledger here has one time coordinate, and every batch is empty: there is no client data yet.

  cargo build --release -p ddir-server
  python3 interactive/bench/ledger/fabric.py --dir /tmp/fabric --ledgers 100000 --clocks 1 --spans 0,2560 --check
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import queue
import random
import shutil
import socket
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE.parent / "ldbc"))
from client import decode, encode_frame  # noqa: E402  (the binary frame encoder and decoder)

FLAT = (("prod", ["int"]), ("prod", ["int"] * 5))
RULES = "interactive/model/resolve.ddp"
BIND = "input0=state::own input1=gen::requests input2=state::registered input3=state::follows"
INVARIANTS = ["malformed", "orphaned", "forked", "skipped", "early", "unclocked", "mismatched"]


class Server:
    """A ddir_server process and one session to it."""

    def __init__(self, binary: Path, workers: int):
        ports = []
        for _ in range(2):
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                ports.append(probe.getsockname()[1])
        env = dict(os.environ, DDIR_WORKERS=str(workers), DDIR_BIND=f"127.0.0.1:{ports[0]}",
                   DDIR_WS_BIND=f"127.0.0.1:{ports[1]}", DDIR_MAX_FEED_BYTES=str(1 << 31))
        self.process = subprocess.Popen([str(binary)], cwd=ROOT, env=env, stdin=subprocess.PIPE,
                                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 15
        while True:
            try:
                self.stream = socket.create_connection(("127.0.0.1", ports[0]), timeout=1)
                break
            except OSError:
                if self.process.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("the server did not start")
                time.sleep(0.01)
        self.stream.settimeout(600)
        self.reader = self.stream.makefile("rb")
        self.count = 0

    def ask(self, command: str, body: bytes = b"") -> tuple[list[str], list[bytes]]:
        """Send one command, with a frame's bytes after it if any. Returns its data lines and data frames."""
        self.count += 1
        reqid = f"q{self.count}"
        self.stream.sendall(f"{reqid} {command}\n".encode() + body)
        lines, frames = [], []
        while True:
            raw = self.reader.readline()
            if not raw:
                raise RuntimeError(f"the server went away during {command[:60]!r}")
            line = raw.decode().rstrip("\r\n")
            if not line.startswith(reqid + " "):
                continue
            reply = line[len(reqid) + 1:]
            if reply.startswith("frame "):
                kind, size = reply[6:].split(" ")
                payload = self.reader.read(int(size))
                if kind == "data":
                    frames.append(payload)
            elif reply.startswith("data "):
                lines.append(reply[5:])
            elif reply == "ok" or reply.startswith("ok "):
                return lines, frames
            else:
                raise RuntimeError(f"{command[:80]!r}: {reply}")

    def frames(self, trace: str) -> list[bytes]:
        return self.ask(f"peek {trace} as corgi")[1]

    def rows(self, trace: str) -> list:
        """A trace's rows, decoded, in a canonical order."""
        return sorted(decode([("data", frame) for frame in self.frames(trace)]), key=repr)

    def feed(self, target: str, frame: bytes) -> None:
        self.ask(f"feed {target} frame {len(frame)}", frame)

    def kill(self) -> None:
        self.process.kill()
        self.process.wait()

    def close(self) -> None:
        try:
            self.process.stdin.write(b"exit\n")
            self.process.stdin.flush()
            self.process.wait(timeout=15)
        except Exception:
            self.kill()


def rows_in(frames: list[bytes]) -> int:
    """How many updates a peek's data frames hold: each begins with its row count."""
    return sum(struct.unpack_from("<Q", frame)[0] for frame in frames)


class Store:
    """Immutable blobs named by their hash, and one root replaced only by compare-and-swap.

    `sync` is `none`, `fsync`, or `full`. On macOS fsync hands data to the drive but does not make
    the drive store it; `full` adds F_FULLFSYNC after the blob and after the root, two per commit.
    """

    def __init__(self, directory: Path, sync: str):
        self.blobs, self.root, self.sync = directory / "blobs", directory / "root.json", sync
        self.blobs.mkdir(parents=True, exist_ok=True)
        self.version, self.head = 0, ""
        if self.root.exists():
            saved = json.loads(self.root.read_text())
            self.version, self.head = saved["version"], saved["head"]

    def _sync(self, fd: int, flush: bool) -> None:
        if self.sync == "none":
            return
        os.fsync(fd)
        if flush and self.sync == "full" and hasattr(fcntl, "F_FULLFSYNC"):
            fcntl.fcntl(fd, fcntl.F_FULLFSYNC)

    def commit(self, frames: list[tuple[int, bytes]]) -> dict:
        """Make these frames durable as the next record of the log. Returns where the time went."""
        clock = time.perf_counter()
        record = bytearray(self.head.ljust(64).encode() + struct.pack("<Q", len(frames)))
        for target, frame in frames:
            record += struct.pack("<2Q", target, len(frame)) + frame
        name = hashlib.sha256(record).hexdigest()
        path = self.blobs / name
        with open(path.with_suffix(".tmp"), "wb") as blob:
            blob.write(record)
            blob.flush()
            self._sync(blob.fileno(), False)
        os.replace(path.with_suffix(".tmp"), path)
        written = time.perf_counter()
        fd = os.open(path, os.O_RDONLY)
        self._sync(fd, True)
        os.close(fd)
        flushed = time.perf_counter()
        with open(self.root.with_suffix(".lock"), "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if self.root.exists() and json.loads(self.root.read_text())["version"] != self.version:
                raise RuntimeError("the root moved: another writer holds this log")
            with open(self.root.with_suffix(".tmp"), "w") as root:
                json.dump({"version": self.version + 1, "head": name}, root)
                root.flush()
                self._sync(root.fileno(), False)
            os.replace(self.root.with_suffix(".tmp"), self.root)
            fd = os.open(self.root.parent, os.O_RDONLY)
            self._sync(fd, False)
            os.close(fd)
            fd = os.open(self.root, os.O_RDONLY)
            self._sync(fd, True)
            os.close(fd)
        self.version, self.head = self.version + 1, name
        swapped = time.perf_counter()
        return dict(bytes=len(record), write=written - clock, flush=flushed - written, root=swapped - flushed)

    def log(self) -> list[list[tuple[int, bytes]]]:
        """Every record's frames, oldest record first."""
        records, name = [], self.head
        while name:
            data = (self.blobs / name).read_bytes()
            name, (count,) = data[:64].decode().strip(), struct.unpack_from("<Q", data, 64)
            at, frames = 72, []
            for _ in range(count):
                target, size = struct.unpack_from("<2Q", data, at)
                frames.append((target, data[at + 16:at + 16 + size]))
                at += 16 + size
            records.append(frames)
        return records[::-1]


class Committer:
    """Commits rounds in order on its own thread; rounds that queue during a commit share the next one."""

    def __init__(self, store: Store):
        self.store, self.queue, self.done, self.costs = store, queue.Queue(), {}, {}
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        while (item := self.queue.get()) is not None:
            batch = [item]
            while not self.queue.empty():
                more = self.queue.get()
                if more is None:
                    self.queue.put(None)
                    break
                batch.append(more)
            cost = self.store.commit([frame for _, frames, _ in batch for frame in frames])
            now = time.perf_counter()
            for number, _, event in batch:
                self.done[number], self.costs[number] = now, dict(cost, rounds=len(batch))
                event.set()

    def submit(self, number: int, frames: list[tuple[int, bytes]]) -> threading.Event:
        event = threading.Event()
        self.queue.put((number, frames, event))
        return event

    def finish(self) -> None:
        self.queue.put(None)
        self.thread.join()


class World:
    """The driver's own account of the ledgers, used to write requests that will commit."""

    def __init__(self, ledgers: int, clocks: int, seed: int):
        self.ledgers, self.clocks, self.rng, self.key = ledgers, clocks, random.Random(seed), 0
        self.version, self.upper = [0] * ledgers, [0] * ledgers
        self.clock_version, self.clock_upper = [0] * clocks, [0] * clocks

    def keys(self, count: int) -> range:
        self.key += count
        return range(self.key - count, self.key)

    def creation(self, first: int, count: int) -> tuple[bytes, int]:
        """Create ledgers first..first+count, each in its own transaction, following its clock if there are clocks."""
        keys = [(k,) for k in self.keys(count)]
        if self.clocks:
            vals = [(0, n, 1, n % self.clocks, 1) for n in range(first, first + count)]
        else:
            vals = [(0, n, 0, 0, 0) for n in range(first, first + count)]
        return encode_frame(FLAT, keys, [1] * count, vals), count

    def clock_creation(self) -> tuple[bytes, int]:
        keys = [(k,) for k in self.keys(self.clocks)]
        return encode_frame(FLAT, keys, [1] * self.clocks, [(1, c, 0, 0, 0) for c in range(self.clocks)]), self.clocks

    def round(self, spans: int, width: int, contend: float, tick: bool) -> dict:
        """One round's requests: `spans` appends with (empty) data, `width` to a transaction, and one append per clock."""
        chosen = self.rng.sample(range(self.ledgers), spans)
        floors = list(self.clock_upper)
        appends = []
        for n in chosen:
            lower = max(self.upper[n], floors[n % self.clocks]) if self.clocks else self.upper[n]
            appends.append((0, n, lower, lower + 1, self.version[n]))
            self.version[n], self.upper[n] = self.version[n] + 1, lower + 1
        singles, grouped, rejected = [], [], 0
        if width == 1:
            singles = list(zip(((k,) for k in self.keys(len(appends))), appends))
            # A contender repeats an append under a later key; the rules must reject it.
            for row in self.rng.sample(appends, int(contend * len(appends))):
                singles.append(((self.keys(1)[0],), row))
                rejected += 1
        else:
            for at in range(0, len(appends), width):
                key = (self.keys(1)[0],)
                grouped += [(key, row) for row in appends[at:at + width]]
        ticks = []
        if tick:
            for c in range(self.clocks):
                ticks.append(((self.keys(1)[0],), (1, c, self.clock_upper[c], self.clock_upper[c] + 1, self.clock_version[c])))
                self.clock_version[c], self.clock_upper[c] = self.clock_version[c] + 1, self.clock_upper[c] + 1
        singles += ticks
        frame = lambda rows: encode_frame(FLAT, [k for k, _ in rows], [1] * len(rows), [v for _, v in rows]) if rows else None
        transactions = len(singles) + (len(appends) + width - 1) // width * (width > 1)
        return dict(singles=frame(singles), grouped=frame(grouped), creates=None, spans=len(appends) + len(ticks),
                    data=len(appends), ticks=len(ticks), transactions=transactions, rejected=rejected)


class Fabric:
    """One resolver: a server, its standing state, and the durable log."""

    def __init__(self, args, store: Store):
        self.args, self.store = args, store
        self.server = Server(args.binary, args.workers)
        self.server.ask(f"load state from {HERE.relative_to(ROOT)}/state.ddp")
        self.server.ask("tick")

    def round(self, requests: dict) -> dict:
        """Resolve one round and absorb it. Returns the manifest's change as frames, and the timings."""
        s, mark, t = self.server, time.perf_counter(), {}

        def lap(name: str) -> None:
            nonlocal mark
            now = time.perf_counter()
            t[name], mark = now - mark, now

        s.ask(f"load gen from {HERE.relative_to(ROOT)}/requests.ddp")
        s.ask(f"load res from {RULES} {BIND}")
        lap("load")
        for target, name in enumerate(("singles", "grouped", "creates")):
            if requests.get(name):
                s.feed(f"gen {target}", requests[name])
        lap("feed")
        s.ask("tick")
        lap("resolve")
        registrations, spans = s.frames("res::registrations_add"), s.frames("res::spans_add")
        lap("read")
        s.ask("feed state 0 from res::own_delta")
        s.ask("feed state 1 from res::registered_add")
        s.ask("feed state 2 from res::follows_add")
        if self.args.manifest == "resident":
            s.ask("feed state 3 from res::registrations_add")
            s.ask("feed state 4 from res::spans_add")
        s.ask("drop res")
        s.ask("drop gen")
        s.ask("tick")
        lap("absorb")
        frames = [(3, f) for f in registrations] + [(4, f) for f in spans]
        return dict(t, frames=frames, committed=rows_in(registrations) + rows_in(spans))

    def recover(self) -> dict:
        """On a fresh server: the manifest from the log, the indexes from the manifest, the model's invariants."""
        s, begin = self.server, time.perf_counter()
        for record in self.store.log():
            for target, frame in record:
                s.feed(f"state {target}", frame)
        s.ask("tick")
        s.ask("load check from interactive/model/ledger.ddp input0=state::registrations input1=state::spans")
        s.ask("tick")
        broken = {name: rows_in(s.frames(f"check::{name}")) for name in INVARIANTS}
        for target, name in enumerate(("own", "registered", "follows")):
            s.ask(f"feed state {target} from check::{name}")
        s.ask("drop check")
        s.ask("tick")
        return dict(seconds=time.perf_counter() - begin, broken={k: v for k, v in broken.items() if v})


def percentile(values, q):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


def phase(fabric: Fabric, rounds: list[dict], mode: str, warmup: int) -> dict:
    """Run these rounds, committing each; report the rounds after the warmup."""
    committer = Committer(fabric.store)
    started, events, results = [], [], []
    for number, requests in enumerate(rounds):
        started.append(time.perf_counter())
        result = fabric.round(requests)
        expected = requests["spans"] + (requests.get("created") or 0)
        if result["committed"] != expected:
            raise RuntimeError(f"round {number}: {result['committed']} committed, {expected} expected")
        events.append(committer.submit(number, result.pop("frames")))
        results.append(result)
        if mode == "serial":
            events[-1].wait()
    committer.finish()
    kept = range(warmup, len(rounds))
    window = committer.done[len(rounds) - 1] - started[warmup]
    mean = lambda values: sum(values) / len(kept)
    part = lambda name: 1000 * mean([results[i][name] for i in kept])
    cost = lambda name: 1000 * mean([committer.costs[i][name] / committer.costs[i]["rounds"] for i in kept])
    latency = [1000 * (committer.done[i] - started[i]) for i in kept]
    count = lambda name: sum(rounds[i].get(name) or 0 for i in kept)
    return dict(
        mode=mode, rounds=len(kept), ms_per_round=round(1000 * window / len(kept), 2),
        spans_per_s=round(count("spans") / window), data_spans_per_s=round(count("data") / window),
        creations_per_s=round(count("created") / window), transactions_per_s=round(count("transactions") / window),
        rejected_per_round=count("rejected") // len(kept),
        resolve_ms=round(part("resolve"), 2), absorb_ms=round(part("absorb"), 2),
        load_ms=round(part("load"), 2), feed_ms=round(part("feed"), 2), read_ms=round(part("read"), 2),
        compute_ms=round(sum(part(n) for n in ("load", "feed", "resolve", "read", "absorb")), 2),
        commit_ms=round(sum(cost(n) for n in ("write", "flush", "root")), 2),
        commit_write_ms=round(cost("write"), 2), commit_flush_ms=round(cost("flush"), 2), commit_root_ms=round(cost("root"), 2),
        record_kib=round(mean([committer.costs[i]["bytes"] / committer.costs[i]["rounds"] for i in kept]) / 1024, 1),
        rounds_per_commit=round(len(kept) / len({committer.done[i] for i in kept}), 2),
        latency_p50_ms=round(percentile(latency, 0.5), 1), latency_p95_ms=round(percentile(latency, 0.95), 1))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dir", type=Path, required=True, help="where the durable log goes; must not exist")
    p.add_argument("--binary", type=Path, default=ROOT / "target/release/ddir_server")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--ledgers", type=int, default=1_000_000)
    p.add_argument("--clocks", type=int, default=1, help="how many clocks the ledgers follow; 0 for none")
    p.add_argument("--still", action="store_true", help="do not advance the clocks")
    p.add_argument("--spans", default="0,256,2560,25600", help="appends with data per round; one phase for each")
    p.add_argument("--width", type=int, default=1, help="appends per transaction")
    p.add_argument("--contend", type=float, default=0.0, help="fraction of appends that a second transaction repeats")
    p.add_argument("--rounds", type=int, default=30)
    p.add_argument("--warmup", type=int, default=5)
    p.add_argument("--modes", default="serial,pipelined")
    p.add_argument("--sync", choices=["none", "fsync", "full"], default="full")
    p.add_argument("--manifest", choices=["resident", "none"], default="resident",
                   help="whether the server keeps every registration and span resident, or only the indexes")
    p.add_argument("--create-batch", type=int, default=50_000, help="creations per round while setting up")
    p.add_argument("--check", action="store_true", help="afterwards, kill the server and rebuild from the log")
    p.add_argument("--seed", type=int, default=1)
    args = p.parse_args()
    args.dir.mkdir(parents=True)
    store = Store(args.dir, args.sync)
    fabric = Fabric(args, store)
    world = World(args.ledgers, args.clocks, args.seed)
    try:
        # Creation: the clocks first, since a ledger may only follow a clock registered before its round.
        creating = []
        if args.clocks:
            frame, count = world.clock_creation()
            creating.append(dict(creates=frame, spans=0, created=count, transactions=count))
        for first in range(0, args.ledgers, args.create_batch):
            frame, count = world.creation(first, min(args.create_batch, args.ledgers - first))
            creating.append(dict(creates=frame, spans=0, created=count, transactions=count))
        report = phase(fabric, creating, "pipelined", min(1, len(creating) - 1))
        print(json.dumps(dict(phase="create", ledgers=args.ledgers, clocks=args.clocks, **report)), flush=True)

        for spans in [int(x) for x in args.spans.split(",")]:
            for mode in args.modes.split(","):
                rounds = [world.round(spans, args.width, args.contend, bool(args.clocks) and not args.still)
                          for _ in range(args.rounds)]
                report = phase(fabric, rounds, mode, args.warmup)
                print(json.dumps(dict(phase="append", spans=spans, width=args.width, contend=args.contend,
                                      clocks=args.clocks, manifest=args.manifest, sync=args.sync, **report)), flush=True)

        if args.check:
            before = fabric.server.rows("state::own")
            fabric.server.kill()
            fabric = Fabric(args, Store(args.dir, args.sync))
            recovered = fabric.recover()
            after = fabric.server.rows("state::own")
            # The rebuilt resolver must also go on: one more round of appends, all of which must commit.
            more = world.round(min(1000, args.ledgers), 1, 0.0, bool(args.clocks) and not args.still)
            went_on = fabric.round(more)["committed"] == more["spans"]
            print(json.dumps(dict(phase="check", records=len(fabric.store.log()), ledgers=len(after),
                                  rebuilt_matches=before == after, invariants_broken=recovered["broken"],
                                  recovery_s=round(recovered["seconds"], 2), resolves_after_recovery=went_on)), flush=True)
            if before != after or recovered["broken"] or not went_on:
                raise SystemExit("the rebuilt state differs, or an invariant is broken")
    finally:
        fabric.server.close()


if __name__ == "__main__":
    main()
