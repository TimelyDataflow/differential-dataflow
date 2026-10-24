#!/usr/bin/env python3
"""Two-session smoke test over TCP: convention-based races and the size gate.

Sessions are trusted (identity by convention, no ownership gates): two
clients race stamped-by-convention claims on one cell and the program's
min-policy settles it deterministically; any session may bind into or drop
any program; the one intake gate (program size) rejects oversized loads.

Run from the repo root (release binary must be built):
  python3 interactive/server/demo/two_sessions.py
"""

import os
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "..", "..", "..", "target", "release", "ddir_server")
sys.path.insert(0, os.path.join(HERE, "..", "bench", "ldbc"))
from client import decode  # noqa: E402  (the binary frame decoder)
PORT = 7981

WORLD = """load world begin
let claims = input 0 : ((int, int), (int, int));
export "owner" = claims | map((cell, (id, epoch)) -> (cell, (epoch, id))) | min | map((cell, (_, id)) -> (cell, (id)));
export "claims" = claims;
world end-load
"""


class Client:
    def __init__(self, port):
        for _ in range(100):
            try:
                self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
                break
            except OSError:
                time.sleep(0.05)
        else:
            raise RuntimeError("server never came up")
        self.buf = b""

    def send(self, text):
        self.sock.sendall(text.encode() if text.endswith("\n") else (text + "\n").encode())

    def fill(self, until):
        while not until():
            chunk = self.sock.recv(4096)
            if not chunk:
                raise RuntimeError("connection closed while waiting")
            self.buf += chunk

    def expect(self, reqid):
        """Read until the terminal ok/err for `reqid`; return (status, body, data), its data
        lines and its binary frames as `(kind, payload)`."""
        data = []
        while True:
            self.fill(lambda: b"\n" in self.buf)
            line, self.buf = self.buf.split(b"\n", 1)
            toks = line.decode().split(" ", 2)
            if toks[1] == "frame":
                kind, length = toks[2].split(" ")
                self.fill(lambda: len(self.buf) >= int(length))
                payload, self.buf = self.buf[:int(length)], self.buf[int(length):]
                if toks[0] == reqid:
                    data.append((kind, payload))
                continue
            if toks[0] != reqid:
                continue
            if toks[1] in ("ok", "err"):
                return toks[1], toks[2] if len(toks) > 2 else "", data
            if toks[1] == "data":
                data.append(toks[2])


def check(label, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + label + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        sys.exit(1)


def main():
    env = dict(
        os.environ,
        DDIR_BIND=f"127.0.0.1:{PORT}",
        DDIR_WS_BIND=f"127.0.0.1:{PORT + 1}",
        DDIR_MAX_PROGRAM_BYTES="4096",
    )
    server = subprocess.Popen(
        [BIN], env=env, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, text=True,
    )
    try:
        a = Client(PORT)  # calls itself client 1 by convention
        b = Client(PORT)  # calls itself client 2

        a.send("rA " + WORLD)
        status, body, _ = a.expect("rA")
        check("session A loads the world", status == "ok", body)

        # Both race for (5,5) with convention ids in the value: (id, epoch).
        # The program's min over (epoch, id) settles it for client 1.
        a.send("c1 feed world 0 5,5 val=1,0")
        a.expect("c1")
        b.send("c2 feed world 0 5,5 val=2,0")
        b.expect("c2")
        b.send("c3 feed world 0 2,2 val=2,0")
        b.expect("c3")
        b.send("t1 tick")
        b.expect("t1")
        a.send("p1 peek owner as corgi")
        _, _, frames = a.expect("p1")
        owned = {tuple(key): val for key, val, _ in decode(frames)}
        check(
            "same-epoch race on (5,5) goes to the lower convention id",
            owned.get((5, 5)) == [1],
            str(owned),
        )
        check(
            "uncontested (2,2) goes to client 2",
            owned.get((2, 2)) == [2],
            str(owned),
        )

        # Sessions are trusted: B may bind into (and later drop) A's program.
        b.send("b1 bind claims world 0")
        status, body, _ = b.expect("b1")
        check("B binds into A's world (trusted sessions)", status == "ok", body)
        b.send("b2 unbind claims world 0")
        status, body, _ = b.expect("b2")
        check("B unbinds it again", status == "ok", body)

        # The one gate: an oversized program body is swallowed, one error.
        big = "load big begin\n" + ("let x = input 0; -- pad\n" * 300) + "big end-load\n"
        a.send("s1 " + big)
        status, body, _ = a.expect("s1")
        check("oversized program body is rejected", status == "err" and "exceeds" in body, body)

        b.send("d1 drop world")
        status, body, _ = b.expect("d1")
        check("B drops A's world (trusted sessions)", status == "ok", body)

        a.sock.close()
        b.sock.close()
    finally:
        try:
            server.stdin.write("exit\n")
            server.stdin.flush()
        except OSError:
            pass
        code = server.wait(timeout=10)
        check("server exits cleanly", code == 0, str(code))
    print("all checks passed")


if __name__ == "__main__":
    main()
