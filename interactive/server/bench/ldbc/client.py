"""Small, bounded TCP client for a private, real ddir_server process."""
import ast
import os
import re
import socket
import subprocess
import threading
import time


def term(value):
    if type(value) is int:
        return str(value)
    if isinstance(value, str):
        return 'list(' + ','.join(map(str, value.encode())) + ')'
    if isinstance(value, (tuple, list)):
        return 'tuple(' + ','.join(map(term, value)) + ')'
    raise TypeError(value)


def decode(lines):
    def value(text):
        def parse(node):
            if isinstance(node, ast.Constant) and type(node.value) is int:
                return node.value
            if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
                arg = parse(node.operand)
                if type(arg) is int:
                    return -arg
            if isinstance(node, ast.List):
                return [parse(v) for v in node.elts]
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and not node.keywords and len(node.args) == 1:
                arg = parse(node.args[0])
                if node.func.id == 'Int' and type(arg) is int:
                    return arg
                if node.func.id in ('Tuple', 'List') and isinstance(arg, list):
                    return arg
            raise ValueError('unsupported server value: ' + text[:200])
        return parse(ast.parse(text, mode='eval').body)
    result = []
    for line in lines:
        match = re.fullmatch(r'diff=(-?\d+) key=(.+) val=(.+)', line)
        if not match:
            raise ValueError(line)
        result.append([value(match[2]), value(match[3]), int(match[1])])
    return sorted(result, key=lambda r: r[0])


class Server:
    def __init__(self, binary, log, backend, workers, timeout, max_rss_gib):
        self.timeout, self.counter = timeout, 0
        self.stream = self.reader = None
        self.log = log.open('wb')
        self.peak_rss = 0
        self.failure = None
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        env = dict(os.environ, DDIR_BIND=f'127.0.0.1:{port}', DDIR_WS_BIND='127.0.0.1:0',
                   DDIR_DIAGNOSTICS='0', DDIR_WORKERS=str(workers), DDIR_BACKEND=backend)
        self.process = subprocess.Popen([str(binary)], stdin=subprocess.PIPE,
                                        stdout=subprocess.DEVNULL, stderr=self.log, env=env)
        self.stop_monitor = threading.Event()

        def monitor():
            while not self.stop_monitor.wait(1):
                try:
                    ps = subprocess.run(['ps', '-o', 'rss=', '-p', str(self.process.pid)],
                                        capture_output=True, text=True, timeout=2, check=False)
                    rss = int(ps.stdout.strip() or 0) * 1024
                    self.peak_rss = max(self.peak_rss, rss)
                    if rss > max_rss_gib * 1024**3:
                        self.failure = f'server RSS exceeded {max_rss_gib} GiB'
                        self.process.kill()
                        return
                except (OSError, ValueError, subprocess.TimeoutExpired) as error:
                    self.failure = f'RSS monitor failed: {error}'
                    if self.process.poll() is None:
                        self.process.kill()
                    return
        self.monitor = threading.Thread(target=monitor, daemon=True)
        self.monitor.start()
        try:
            deadline = time.monotonic() + timeout
            while True:
                try:
                    self.stream = socket.create_connection(('127.0.0.1', port), timeout=1)
                    break
                except OSError:
                    if self.process.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError(f'server did not start; see {log}') from None
                    time.sleep(0.02)
            self.stream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.reader = self.stream.makefile('rb')
        except BaseException:
            self.close()
            raise

    def commands(self, commands):
        start = time.perf_counter()
        ids, payload = [], []
        for command in commands:
            self.counter += 1
            rid = f'r{self.counter}'
            ids.append(rid)
            payload.append(rid + ' ' + command.replace('{rid}', rid) + '\n')
        wire = ''.join(payload).encode()
        replies = {rid: [] for rid in ids}
        pending = set(ids)
        encoded = time.perf_counter()
        deadline = encoded + self.timeout
        self.stream.settimeout(self.timeout)
        self.stream.sendall(wire)
        received = 0
        while pending:
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                raise TimeoutError('command deadline exceeded')
            self.stream.settimeout(remaining)
            raw = self.reader.readline()
            received += len(raw)
            if not raw:
                raise RuntimeError(self.failure or 'server disconnected; see server log')
            rid, kind, *body = raw.decode().rstrip('\r\n').split(' ', 2)
            if rid not in pending:
                raise RuntimeError(f'unexpected response: {raw[:200]!r}')
            body = body[0] if body else ''
            if kind == 'data':
                replies[rid].append(body)
            elif kind == 'ok':
                pending.remove(rid)
            else:
                raise RuntimeError(body)
        end = time.perf_counter()
        return dict(encode_ms=1000*(encoded-start), wire_ms=1000*(end-encoded),
                    bytes_sent=len(wire), bytes_received=received), [replies[rid] for rid in ids]

    @staticmethod
    def feed(program, index, rows, diff):
        body = '\n'.join(term(row) + f' diff={diff}' for row in rows)
        return f'feed {program} {index} begin\n{body}\n{{rid}} end-feed'

    def close(self):
        self.stop_monitor.set()
        self.monitor.join(timeout=3)
        if self.reader:
            self.reader.close()
        if self.stream:
            self.stream.close()
        if self.process.poll() is None:
            try:
                self.process.stdin.write(b'exit\n')
                self.process.stdin.flush()
                self.process.wait(timeout=5)
            except (BrokenPipeError, subprocess.TimeoutExpired):
                self.process.kill()
                self.process.wait()
        self.process.stdin.close()
        self.log.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
