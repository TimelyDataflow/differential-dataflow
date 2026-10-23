#!/usr/bin/env python3
"""Closed-loop transaction benchmark for the live DDIR server.

Two ways to run the same workload (see README.md):

`maintained`: one installed program (maintained.ddp) over a log that holds the
store. Each cycle admits one atomic feed, closes its resolution epoch, observes
the rollback decisions, feeds the `retire` trace back with diff=-1, and closes
the maintenance epoch. Cycle throughput includes the deletions and their work.

`oneoff`: a store program (store.ddp) plus, each cycle, a resolver
(resolve.ddp) installed for one epoch: load it, admit the batch, close the
epoch, observe rollbacks, feed its `delta` into the store, and drop it. The
store change takes effect in the next cycle's tick, so its work is counted
there.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import random
import re
import shlex
import socket
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from typing import Iterable


ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
# The LDBC client's frame encoder writes `feed … frame` payloads.
sys.path.insert(0, str(HERE.parent / "ldbc"))
from client import encode_frame  # noqa: E402

# The intents input of every program here: `((txn, is_read, key), (value))`.
INTENTS = (("prod", ["int", "int", "int"]), ("prod", ["int"]))
DEFAULT_BINARY = ROOT / "target/release/ddir_server"


@dataclass(frozen=True)
class Transaction:
    """Reads are (key, expected value), -1 for absent; writes are (key, value)."""

    txn_id: int
    reads: tuple[tuple[int, int], ...]
    writes: tuple[tuple[int, int], ...]

    def rows(self) -> list[str]:
        return [f"{self.txn_id},1,{k} val={e}" for k, e in self.reads] + [
            f"{self.txn_id},0,{k} val={v}" for k, v in self.writes
        ]


@dataclass
class Sample:
    """One cycle's phases. `install_ms` and `drop_ms` are the oneoff resolver's;
    `maintain_ms` is the maintained program's second tick."""

    attempts: int
    committed: int
    install_ms: float
    submit_ms: float
    resolve_ms: float
    observe_ms: float
    retire_submit_ms: float
    drop_ms: float
    maintain_ms: float

    @property
    def decision_ms(self) -> float:
        return self.install_ms + self.submit_ms + self.resolve_ms + self.observe_ms

    @property
    def cycle_ms(self) -> float:
        return self.decision_ms + self.retire_submit_ms + self.drop_ms + self.maintain_ms


class Client:
    def __init__(self, stream: socket.socket, frames: bool = False):
        self.stream = stream
        self.reader = stream.makefile("rb")
        self.next_id = 0
        self.frames = frames
        self.encode_ms = 0.0

    def _id(self) -> str:
        self.next_id += 1
        return f"q{self.next_id}"

    def _exchange(self, reqid: str, payload) -> list[str]:
        self.stream.sendall(payload if isinstance(payload, bytes) else payload.encode())
        data: list[str] = []
        while True:
            raw = self.reader.readline()
            if not raw:
                raise RuntimeError("server disconnected")
            line = raw.decode().rstrip("\r\n")
            prefix = f"{reqid} "
            if not line.startswith(prefix):
                continue
            response = line[len(prefix) :]
            if response.startswith("data "):
                data.append(response[5:])
            elif response == "ok" or response.startswith("ok "):
                return data
            elif response == "err" or response.startswith("err "):
                raise RuntimeError(response)

    def request(self, command: str) -> tuple[list[str], float]:
        reqid = self._id()
        start = time.perf_counter()
        data = self._exchange(reqid, f"{reqid} {command}\n")
        return data, (time.perf_counter() - start) * 1000.0

    def load(self, name: str, program: Path) -> float:
        return self.request(f"load {name} from {program}")[1]

    def drop(self, name: str) -> float:
        return self.request(f"drop {name}")[1]

    def feed(self, prog: str, transactions: Iterable[Transaction]) -> float:
        reqid = self._id()
        encode = time.perf_counter()
        if self.frames:
            keys, vals = [], []
            for txn in transactions:
                for k, e in txn.reads:
                    keys.append((txn.txn_id, 1, k))
                    vals.append((e,))
                for k, v in txn.writes:
                    keys.append((txn.txn_id, 0, k))
                    vals.append((v,))
            frame = encode_frame(INTENTS, keys, [1] * len(keys), vals)
            payload = f"{reqid} feed {prog} 0 frame {len(frame)}\n".encode() + frame
        else:
            body = "\n".join(row for txn in transactions for row in txn.rows())
            payload = f"{reqid} feed {prog} 0 begin\n{body}\n{reqid} end-feed\n"
        self.encode_ms += (time.perf_counter() - encode) * 1000.0
        start = time.perf_counter()
        self._exchange(reqid, payload)
        return (time.perf_counter() - start) * 1000.0

    def tick(self) -> float:
        return self.request("tick")[1]

    def peek(self, trace: str) -> tuple[list[str], float]:
        return self.request(f"peek {trace}")

    def feed_from(self, prog: str, trace: str, diff: int) -> float:
        return self.request(f"feed {prog} 0 from {trace} diff={diff}")[1]

    def close(self) -> None:
        self.reader.close()
        self.stream.close()


class ServerProcess:
    def __init__(self, binary: Path, workers: int, wrap: list[str], frames: bool = False):
        ports = free_ports(3)
        env = os.environ.copy()
        env.update(
            DDIR_WORKERS=str(workers),
            DDIR_DIAGNOSTICS="0",
            DDIR_BIND=f"127.0.0.1:{ports[0]}",
            DDIR_WS_BIND=f"127.0.0.1:{ports[1]}",
            DDIR_DIAG_PORT=str(ports[2]),
        )
        self.process = subprocess.Popen(
            [*wrap, str(binary)],
            cwd=ROOT,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 15.0
        while True:
            try:
                stream = socket.create_connection(("127.0.0.1", ports[0]), timeout=1.0)
                stream.settimeout(60.0)
                self.client = Client(stream, frames)
                break
            except OSError:
                if self.process.poll() is not None:
                    raise RuntimeError(f"server exited with {self.process.returncode}")
                if time.monotonic() >= deadline:
                    raise RuntimeError("server did not begin listening")
                time.sleep(0.01)

    def close(self) -> None:
        self.client.close()
        if self.process.poll() is None and self.process.stdin is not None:
            try:
                self.process.stdin.write(b"exit\n")
                self.process.stdin.flush()
                self.process.wait(timeout=15.0)
            except (BrokenPipeError, subprocess.TimeoutExpired):
                self.process.kill()
                self.process.wait()
        if self.process.returncode not in (None, 0):
            raise RuntimeError(f"server exited with {self.process.returncode}")

    def __enter__(self) -> Client:
        return self.client

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


def free_ports(count: int) -> list[int]:
    sockets: list[socket.socket] = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


COLUMN_RE = re.compile(r"Prim\(\w+\(\[([^\]]*)\]\)\)")
DIFFS_RE = re.compile(r" diffs=\[([^\]]*)\]$")


def ints(text: str) -> list[int]:
    return [int(part) for part in text.split(",") if part.strip()]


def parse_rows(lines: Iterable[str]) -> list[tuple[tuple[int, ...], tuple[int, ...], int]]:
    """A peek's `keys=… vals=… diffs=[…]` containers as (key, val, diff) rows of integers."""
    rows = []
    for line in lines:
        keys, sep, rest = line.partition(" vals=")
        diffs = DIFFS_RE.search(rest)
        if not keys.startswith("keys=") or not sep or diffs is None:
            raise RuntimeError(f"unrecognized peek row: {line}")
        key_cols = [ints(col) for col in COLUMN_RE.findall(keys)]
        val_cols = [ints(col) for col in COLUMN_RE.findall(rest[: diffs.start()])]
        counts = ints(diffs.group(1))
        if any(len(col) != len(counts) for col in key_cols + val_cols):
            raise RuntimeError(f"ragged peek row: {line}")
        for i, diff in enumerate(counts):
            rows.append((tuple(c[i] for c in key_cols), tuple(c[i] for c in val_cols), diff))
    return rows


def parse_rollback(lines: Iterable[str]) -> set[int]:
    result: set[int] = set()
    for key, _, diff in parse_rows(lines):
        if diff != 1:
            raise RuntimeError(f"rollback {key} has multiplicity {diff}")
        result.add(key[0])
    return result


def parse_state(lines: Iterable[str]) -> dict[int, int]:
    """`state` is ((k), (v)) and `store` is ((k), (wid, v)): the value is the last field."""
    result: dict[int, int] = {}
    for key, val, diff in parse_rows(lines):
        if diff != 1 or key[0] in result:
            raise RuntimeError(f"state key {key} has invalid multiplicity {diff}")
        result[key[0]] = val[-1]
    return result


def generate_transactions(
    rng: random.Random,
    first_id: int,
    attempts: int,
    key_count: int,
    replicas: int,
    pattern: str,
    next_key: int,
    state: dict[int, int],
    shape: str = "cas",
) -> tuple[list[Transaction], int]:
    if attempts % replicas:
        raise ValueError("batch size must be divisible by replicas")
    snapshot = state.copy()
    transactions: list[Transaction] = []
    txn_id = first_id
    if shape == "blog":
        # The post's workload: two reads and two writes at random locations, the reads
        # observing the store as of the batch's start, the writes of fresh values.
        for _ in range(attempts):
            r1, r2 = rng.sample(range(key_count), 2) if key_count < 64 else distinct_pair(rng, key_count)
            w1, w2 = rng.sample(range(key_count), 2) if key_count < 64 else distinct_pair(rng, key_count)
            reads = ((r1, snapshot.get(r1, -1)), (r2, snapshot.get(r2, -1)))
            transactions.append(Transaction(txn_id, reads, ((w1, txn_id), (w2, txn_id))))
            txn_id += 1
        return transactions, next_key
    for _ in range(attempts // replicas):
        if pattern == "round-robin":
            key = next_key % key_count
            next_key += 1
        else:
            key = rng.randrange(key_count)
        expected = snapshot.get(key, -1)
        value = expected + 1
        for _ in range(replicas):
            transactions.append(Transaction(txn_id, ((key, expected),), ((key, value),)))
            txn_id += 1
    return transactions, next_key


def distinct_pair(rng: random.Random, n: int) -> tuple[int, int]:
    a = rng.randrange(n)
    while (b := rng.randrange(n)) == a:
        pass
    return a, b


def apply_reference(
    transactions: Iterable[Transaction], state: dict[int, int]
) -> set[int]:
    rollback: set[int] = set()
    for txn in transactions:
        if all(state.get(k, -1) == e for k, e in txn.reads):
            for k, v in txn.writes:
                state[k] = v
        else:
            rollback.add(txn.txn_id)
    return rollback


def check_rollback(client: Client, expected: set[int]) -> float:
    rows, observe_ms = client.peek("rollback")
    actual = parse_rollback(rows)
    if actual != expected:
        missing = sorted(expected - actual)[:10]
        extra = sorted(actual - expected)[:10]
        raise RuntimeError(f"rollback mismatch: missing={missing}, extra={extra}")
    return observe_ms


def run_cycle(
    client: Client,
    mode: str,
    transactions: list[Transaction],
    state: dict[int, int],
    resolver: Path = HERE / "resolve.ddp",
) -> Sample:
    expected_rollback = apply_reference(transactions, state)
    install_ms = drop_ms = maintain_ms = 0.0
    if mode == "maintained":
        submit_ms = client.feed("bank", transactions)
        resolve_ms = client.tick()
        observe_ms = check_rollback(client, expected_rollback)
        retire_submit_ms = client.feed_from("bank", "retire", -1)
        maintain_ms = client.tick()
    else:
        install_ms = client.load("res", resolver)
        submit_ms = client.feed("res", transactions)
        resolve_ms = client.tick()
        observe_ms = check_rollback(client, expected_rollback)
        retire_submit_ms = client.feed_from("store", "delta", 1)
        drop_ms = client.drop("res")
    return Sample(
        attempts=len(transactions),
        committed=len(transactions) - len(expected_rollback),
        install_ms=install_ms,
        submit_ms=submit_ms,
        resolve_ms=resolve_ms,
        observe_ms=observe_ms,
        retire_submit_ms=retire_submit_ms,
        drop_ms=drop_ms,
        maintain_ms=maintain_ms,
    )


def percentile(values: Iterable[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def summarize(mode: str, workers: int, batch: int, samples: list[Sample]) -> dict:
    cycle_seconds = sum(sample.cycle_ms for sample in samples) / 1000.0
    attempts = sum(sample.attempts for sample in samples)
    committed = sum(sample.committed for sample in samples)
    return {
        "mode": mode,
        "workers": workers,
        "batch": batch,
        "attempts": attempts,
        "committed": committed,
        "commit_rate": committed / attempts,
        "attempted_tps": attempts / cycle_seconds,
        "committed_tps": committed / cycle_seconds,
        "decision_p50_ms": percentile((s.decision_ms for s in samples), 0.50),
        "decision_p95_ms": percentile((s.decision_ms for s in samples), 0.95),
        "cycle_p50_ms": percentile((s.cycle_ms for s in samples), 0.50),
        "cycle_p95_ms": percentile((s.cycle_ms for s in samples), 0.95),
        "phase_means_ms": {
            phase: sum(getattr(s, phase) for s in samples) / len(samples)
            for phase in ("install_ms", "submit_ms", "resolve_ms", "observe_ms",
                          "retire_submit_ms", "drop_ms", "maintain_ms")
        },
        "samples": [asdict(sample) for sample in samples],
    }


def comma_ints(value: str) -> list[int]:
    return [int(part) for part in value.split(",")]


def comma_strings(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument("--modes", type=comma_strings, default=["maintained", "oneoff"])
    parser.add_argument("--maintained-program", type=Path, default=HERE / "maintained.ddp")
    parser.add_argument("--resolve-program", type=Path, default=HERE / "resolve.ddp")
    parser.add_argument("--workers", type=comma_ints, default=[1, 2, 4])
    parser.add_argument("--batch-sizes", type=comma_ints, default=[100, 1000, 10000, 100000])
    parser.add_argument("--keys", type=int, default=1_000_000)
    parser.add_argument(
        "--keys-from-batch",
        action="store_true",
        help="use each batch size as that run's key count",
    )
    parser.add_argument("--replicas", type=int, default=1)
    parser.add_argument(
        "--wrap",
        default="",
        help="a command to run the server under, e.g. 'samply record --save-only -o p.json --'",
    )
    parser.add_argument(
        "--shape",
        choices=["cas", "blog"],
        default="cas",
        help="cas: one read and one write of the same key; blog: the post's two reads and two writes",
    )
    parser.add_argument("--key-pattern", choices=["random", "round-robin"], default="random")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--feed",
        choices=["frames", "text"],
        default="frames",
        help="send intents as one binary frame of columns, or as framed text rows",
    )
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()

    if not args.binary.is_file():
        raise SystemExit(f"missing {args.binary}; run `cargo build --release -p ddir-server`")
    if args.keys <= 0 or args.replicas <= 0 or args.rounds <= 0 or args.warmup < 0:
        raise SystemExit("keys, replicas, and rounds must be positive; warmup cannot be negative")
    results: list[dict] = []

    print(
        "mode       workers batch commit% attempted/s committed/s "
        "decision-p50/p95-ms cycle-p50/p95-ms",
        flush=True,
    )
    for mode in args.modes:
        if mode not in ("maintained", "oneoff"):
            raise SystemExit(f"unknown mode {mode!r}")
        for workers in args.workers:
            for batch in args.batch_sizes:
                if batch <= 0 or batch % args.replicas:
                    raise SystemExit("each batch size must be positive and divisible by replicas")
                rng = random.Random(args.seed)
                state: dict[int, int] = {}
                next_txn = 1
                next_key = 0
                samples: list[Sample] = []
                key_count = batch if args.keys_from_batch else args.keys
                with ServerProcess(args.binary.resolve(), workers, shlex.split(args.wrap), args.feed == "frames") as client:
                    if mode == "maintained":
                        client.load("bank", args.maintained_program.resolve())
                    else:
                        client.load("store", HERE / "store.ddp")
                    for round_index in range(args.warmup + args.rounds):
                        transactions, next_key = generate_transactions(
                            rng,
                            next_txn,
                            batch,
                            key_count,
                            args.replicas,
                            args.key_pattern,
                            next_key,
                            state,
                            args.shape,
                        )
                        next_txn += batch
                        sample = run_cycle(client, mode, transactions, state, args.resolve_program.resolve())
                        if round_index >= args.warmup:
                            samples.append(sample)

                    encode_ms = client.encode_ms / (args.warmup + args.rounds)
                    if mode == "maintained":
                        rollback, _ = client.peek("rollback")
                        if rollback:
                            raise RuntimeError("maintenance left rollback rows behind")
                        retire, _ = client.peek("retire")
                        if retire:
                            raise RuntimeError("maintenance left retire rows behind")
                        actual_state, _ = client.peek("state")
                    else:
                        client.tick()  # apply the last cycle's store delta
                        actual_state, _ = client.peek("store")
                    if parse_state(actual_state) != state:
                        raise RuntimeError("final state differs from the serial reference")

                result = summarize(mode, workers, batch, samples)
                result.update(
                    keys=key_count,
                    replicas=args.replicas,
                    shape=args.shape,
                    key_pattern=args.key_pattern,
                    rounds=args.rounds,
                    warmup=args.warmup,
                    feed=args.feed,
                    client_encode_ms=encode_ms,
                )
                results.append(result)
                print(
                    f"{mode:10} {workers:7d} {batch:7d} "
                    f"{100 * result['commit_rate']:7.2f} "
                    f"{result['attempted_tps']:11.0f} {result['committed_tps']:11.0f} "
                    f"{result['decision_p50_ms']:7.1f}/{result['decision_p95_ms']:.1f} "
                    f"{result['cycle_p50_ms']:7.1f}/{result['cycle_p95_ms']:.1f}",
                    flush=True,
                )

    if args.json:
        args.json.write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
