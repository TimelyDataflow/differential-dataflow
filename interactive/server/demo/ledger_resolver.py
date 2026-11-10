#!/usr/bin/env python3
"""A resolver fed by many sessions, pumped from outside: ledger transactions on the stock server.

Nothing here is durable, and no client has data: every batch is empty. The point is the plumbing.

Standing programs on one server:
  state     the manifest and the indexes the rules read (interactive/bench/ledger/state.ddp)
  intake    the transaction requests clients have fed and no round has yet taken
  outcomes  every transaction's result, for clients to read

A pump session runs rounds: load the rules (interactive/model/resolve.ddp) against those programs, tick,
feed the conclusions into `state` and the results into `outcomes`, retract the taken requests from
`intake`, drop the rules, tick.

Client sessions, each on its own connection, feed requests into `intake` and poll `outcomes`:
  owners      each creates its own ledgers, then appends to all of them in one transaction per step
  contenders  all append to one shared ledger; a rejection reports where the ledger stands, and they retry
  timekeeper  creates a clock, then its followers, then advances the clock every step
  straggler   appends slowly to a follower, learning from rejections where the clock has moved it to

Run from the repository root, after `cargo build --release -p ddir-server`:
  python3 interactive/server/demo/ledger_resolver.py
"""
import os, re, socket, subprocess, sys, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
BIN = os.path.join(ROOT, "target", "release", "ddir_server")
PORT = 7961
OWNERS, LEDGERS_EACH, STEPS = 4, 8, 40      # owners, ledgers per owner, appends per ledger
CONTENDERS, WINS = 3, 15                    # contenders on one ledger, commits each must land
FOLLOWERS, TICKS = 200, 40                  # ledgers following the clock, clock advances

TYPES = """type Batch = Empty | Inline List(int) | Blob List(int);
type Clock = Free | Follows (int, int);
type Action = Create ((int, int), List(int), int, List(List(int)), Clock)
            | Append ((int, int), List(List(int)), List(List(int)), Batch, int);
type Failure = Stale (int, List(List(int))) | Exists (int, int) | Unknown (int, int) | Invalid;
type Outcome = Committed | Rejected (int, Failure);
"""
PROGRAMS = {
    "intake": TYPES + """let requests = input 0 : ((int), (List(Action)));
output "requests" = requests;
""",
    "outcomes": TYPES + """let results = input 0 : ((int), (Outcome));
output "results" = results;
""",
}
RULES = ("load res from interactive/model/resolve.ddp input0=state::own input1=intake::requests "
         "input2=state::registered input3=state::follows")


class Session:
    """One TCP connection; every command is sent with an id and waited on."""
    def __init__(self, name):
        self.name, self.count, self.buf = name, 0, b""
        for _ in range(200):
            try:
                self.sock = socket.create_connection(("127.0.0.1", PORT), timeout=30)
                break
            except OSError:
                time.sleep(0.05)
        else:
            raise RuntimeError("server never came up")

    def ask(self, text):
        self.count += 1
        reqid = f"{self.name}{self.count}"
        self.sock.sendall(f"{reqid} {text}\n".encode())
        data = []
        while True:
            while b"\n" not in self.buf:
                chunk = self.sock.recv(1 << 16)
                if not chunk: raise RuntimeError("connection closed")
                self.buf += chunk
            line, self.buf = self.buf.split(b"\n", 1)
            toks = line.decode().split(" ", 2)
            if toks[0] != reqid: continue
            if toks[1] == "data": data.append(toks[2])
            elif toks[1] == "ok": return data
            else: raise RuntimeError(f"{text[:60]!r}: {line.decode()}")


def frontier(t): return f"list(list({t}))"
def create(ledger, clock=None):
    following = f"inject(1,tuple({clock}))" if clock else "inject(0,tuple())"
    return f"inject(0,tuple(tuple({ledger}),list(),1,{frontier(0)},{following}))"
def append(ledger, lower, upper, seqno):
    return f"inject(1,tuple(tuple({ledger}),{frontier(lower)},{frontier(upper)},inject(0,tuple()),{seqno}))"


class Client(Session):
    """Feeds one transaction at a time and waits for its outcome."""
    def __init__(self, number):
        super().__init__(f"c{number}_")
        self.number, self.sent, self.waits = number, 0, []

    def transact(self, actions):
        """Returns None if it committed; (version, frontier) from a stale append's rejection; else ()."""
        key = self.number * 10**9 + self.sent
        self.sent += 1
        begin = time.perf_counter()
        self.ask(f"feed intake 0 {key} val=tuple(list({','.join(actions)}))")
        while True:
            rows = self.ask(f"peek outcomes::results {key}")
            if rows: break
            time.sleep(0.0005)
        self.waits.append(time.perf_counter() - begin)
        if "val=tuple(inject(0," in rows[0]: return None
        found = re.search(r"inject\(0,tuple\((\d+),list\(list\((\d+)\)\)\)\)", rows[0])
        return (int(found.group(1)), int(found.group(2))) if found else ()


def owner(number, done):
    c = Client(number)
    mine = [f"{10 + number},{j}" for j in range(LEDGERS_EACH)]
    assert c.transact([create(l) for l in mine]) is None
    for step in range(STEPS):
        assert c.transact([append(l, step, step + 1, step) for l in mine]) is None
    done[number] = c


def contender(number, done):
    c = Client(number)
    version, frontier, wins, losses = 0, 0, 0, 0
    if number == OWNERS: c.transact([create("1,0")])  # the first contender creates the shared ledger
    while wins < WINS:
        told = c.transact([append("1,0", frontier, frontier + 1, version)])
        if told is None: version, frontier, wins = version + 1, frontier + 1, wins + 1
        elif told == (): time.sleep(0.002)            # the ledger does not exist yet
        else: (version, frontier), losses = told, losses + 1
    c.losses = losses
    done[number] = c


def timekeeper(number, done):
    c = Client(number)
    assert c.transact([create("9,0")]) is None
    # A ledger may follow only a clock registered before its round, so the followers come second.
    assert c.transact([create(f"5,{j}", "9,0") for j in range(FOLLOWERS)]) is None
    for t in range(TICKS):
        assert c.transact([append("9,0", t, t + 1, t)]) is None
    done[number] = c


def straggler(number, done):
    c = Client(number)
    version, frontier, wins, losses = 0, 0, 0, 0
    while wins < 10:
        time.sleep(0.02)                              # slow enough for the clock to get ahead
        told = c.transact([append("5,7", frontier, frontier + 1, version)])
        if told is None: version, frontier, wins = version + 1, frontier + 1, wins + 1
        elif told == (): time.sleep(0.002)            # the ledger does not exist yet
        else: (version, frontier), losses = told, losses + 1
    c.losses, c.final, c.version = losses, frontier, version
    done[number] = c


def pump(stop, rounds):
    s = Session("pump")
    while not stop.is_set():
        s.ask(RULES)
        s.ask("tick")
        s.ask("feed outcomes 0 from res::results")
        for target, name in enumerate(("own_delta", "registered_add", "follows_add", "registrations_add", "spans_add")):
            s.ask(f"feed state {target} from res::{name}")
        s.ask("feed intake 0 from res::taken diff=-1")
        s.ask("drop res")
        s.ask("tick")
        rounds.append(time.perf_counter())


def check(label, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + label + (f"  [{detail}]" if detail else ""), flush=True)
    if not ok: sys.exit(1)


def main():
    env = dict(os.environ, DDIR_BIND=f"127.0.0.1:{PORT}", DDIR_WS_BIND=f"127.0.0.1:{PORT + 1}",
               DDIR_WORKERS=os.environ.get("DDIR_WORKERS", "4"))
    server = subprocess.Popen([BIN], cwd=ROOT, env=env, stdin=subprocess.PIPE,
                              stdout=subprocess.DEVNULL,
                              stderr=open(os.environ["LEDGER_DEMO_LOG"], "w") if "LEDGER_DEMO_LOG" in os.environ else subprocess.DEVNULL)
    try:
        setup = Session("setup")
        setup.ask("load state from interactive/bench/ledger/state.ddp")
        for name, text in PROGRAMS.items():
            setup.ask(f"load {name} begin\n{text}{name} end-load")
        setup.ask("tick")

        stop, rounds, done = threading.Event(), [], {}
        jobs = [(owner, n) for n in range(OWNERS)]
        jobs += [(contender, OWNERS + n) for n in range(CONTENDERS)]
        jobs += [(timekeeper, OWNERS + CONTENDERS), (straggler, OWNERS + CONTENDERS + 1)]
        threads = [threading.Thread(target=job, args=(n, done)) for job, n in jobs]
        pumper = threading.Thread(target=pump, args=(stop, rounds))
        begin = time.perf_counter()
        pumper.start()
        for t in threads: t.start()
        for t in threads: t.join()
        elapsed = time.perf_counter() - begin
        stop.set(); pumper.join()
        check("every client finished", len(done) == len(jobs))

        current = {}
        for row in setup.ask("peek state::own"):
            a, b, v, f = re.match(r"tuple\(tuple\((\d+),(\d+)\)\) val=tuple\((\d+),list\(list\((\d+)\)\)\)", row).groups()
            current[f"{a},{b}"] = (int(v), int(f))
        owned = [current.get(f"{10 + n},{j}") for n in range(OWNERS) for j in range(LEDGERS_EACH)]
        check(f"{OWNERS} owners' {len(owned)} ledgers all reached {STEPS}", owned == [(STEPS, STEPS)] * len(owned))
        shared = CONTENDERS * WINS
        check(f"the shared ledger took exactly {shared} appends", current.get("1,0") == (shared, shared))
        check(f"the clock reached {TICKS}", current.get("9,0") == (TICKS, TICKS))
        idle = [current.get(f"5,{j}") for j in range(FOLLOWERS) if j != 7]
        check(f"{len(idle)} idle followers have no spans of their own", idle == [(0, 0)] * len(idle))
        s = done[OWNERS + CONTENDERS + 1]
        check("the straggler's follower holds its 10 appends", current.get("5,7") == (10, s.final), f"own upper {s.final}")
        # The model's own account of the manifest must agree with the indexes, and find nothing wrong with it.
        setup.ask("load check from interactive/model/ledger.ddp input0=state::registrations input1=state::spans")
        setup.ask("tick")
        check("the model's view of the manifest matches the resolver's index",
              sorted(setup.ask("peek check::own")) == sorted(setup.ask("peek state::own")))
        broken = [name for name in ("malformed", "orphaned", "forked", "skipped", "early", "unclocked", "mismatched")
                  if setup.ask(f"peek check::{name}")]
        check("the model's invariants hold of the manifest", not broken, ", ".join(broken))
        check("no request is left in the intake", setup.ask("peek intake::requests") == [])

        sent = sum(c.sent for c in done.values())
        results = len(setup.ask("peek outcomes::results"))
        check("every transaction has exactly one outcome", results == sent, f"{results} of {sent}")
        lost = sum(getattr(c, "losses", 0) for c in done.values())
        waits = sorted(w for c in done.values() for w in c.waits)
        print(f"{len(jobs)} client sessions, {sent} transactions ({sent - lost} committed, {lost} rejected and retried), "
              f"{len(rounds)} rounds in {elapsed:.2f} s", flush=True)
        print(f"contenders' rejections: {[done[OWNERS + n].losses for n in range(CONTENDERS)]}; "
              f"straggler's: {s.losses}", flush=True)
        print(f"submit-to-outcome: median {1000 * waits[len(waits) // 2]:.1f} ms, "
              f"worst {1000 * waits[-1]:.1f} ms; round: {1000 * elapsed / len(rounds):.1f} ms", flush=True)
    finally:
        server.kill()


if __name__ == "__main__":
    main()
