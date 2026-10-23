"""Small, bounded TCP client for a private, real ddir_server process."""
import os
import socket
import re
import struct
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


def _float(key):
    """The float whose total-order key is `key`: corgi stores a float as that key."""
    bits = key ^ (1 << 63) if key >> 63 else ~key & 0xFFFF_FFFF_FFFF_FFFF
    return struct.unpack('<d', struct.pack('<Q', bits))[0]


def _float_key(value):
    """The total-order key corgi stores the float `value` as: the inverse of `_float`."""
    bits = struct.unpack('<Q', struct.pack('<d', value))[0]
    return ~bits & 0xFFFF_FFFF_FFFF_FFFF if bits >> 63 else bits ^ (1 << 63)


def parse_shape(text):
    """A declared DDIR shape as the frame encoder reads it: `int` and `float` as themselves,
    `()` as `'unit'`, a tuple `(a, b)` (one field: `(a)`) as `('prod', [a, b])`, and `List(a)`
    as `('list', a)`."""
    tokens = text.replace('(', ' ( ').replace(')', ' ) ').replace(',', ' , ').split()

    def shape(at):
        token = tokens[at]
        if token in ('int', 'float'):
            return token, at + 1
        if token == 'List':
            assert tokens[at + 1] == '(', text
            element, at = shape(at + 2)
            assert tokens[at] == ')', text
            return ('list', element), at + 1
        if token == '(':
            if tokens[at + 1] == ')':
                return 'unit', at + 2
            fields, at = [], at + 1
            while True:
                field, at = shape(at)
                fields.append(field)
                if tokens[at] == ')':
                    return ('prod', fields), at + 1
                assert tokens[at] == ',', text
                at += 1
        raise ValueError(f'unsupported shape {text!r} at {token!r}')

    result, at = shape(0)
    assert at == len(tokens), text
    return result


def _encode_column(shape, values, out):
    """Append the column of `values` at `shape` to `out`, in corgi's byte encoding (the inverse
    of `_Words.column`). Ints that all fit in a byte take one byte each; strings in an int list
    are their UTF-8 bytes, as `term` writes them."""
    n = len(values)
    if shape == 'int':
        if all(0 <= v < 256 for v in values):
            out += struct.pack('<3Q', 0, 8, n) + bytes(values) + bytes(-n % 8)
        else:
            out += struct.pack(f'<3Q{n}q', 0, 64, n, *values)
    elif shape == 'float':
        out += struct.pack(f'<3Q{n}Q', 0, 0xF64, n, *map(_float_key, values))
    elif shape == 'unit':
        out += struct.pack('<2Q', 4, n)
    elif shape[0] == 'prod':
        out += struct.pack('<2Q', 1, len(shape[1]))
        for i, field in enumerate(shape[1]):
            _encode_column(field, [value[i] for value in values], out)
    elif shape[0] == 'list':
        items = [value.encode() if isinstance(value, str) else value for value in values]
        ends, end = [], 0
        for item in items:
            end += len(item)
            ends.append(end)
        out += struct.pack(f'<3Q{n}Q', 3, 0, n, *ends)
        if shape[1] == 'int' and all(isinstance(item, bytes) for item in items):
            flat = b''.join(items)
            out += struct.pack('<3Q', 0, 8, len(flat)) + flat + bytes(-len(flat) % 8)
        else:
            _encode_column(shape[1], [element for item in items for element in item], out)
    else:
        raise ValueError(f'unsupported shape {shape!r}')


def declared_inputs(program):
    """The declared shapes of a program's unit-valued inputs, by index, as `Server.feed` takes
    them to send frames: `input N : (shape, ())` becomes `{N: (parse_shape(shape), 'unit')}`."""
    return {int(n): (parse_shape(shape), 'unit')
            for n, shape in re.findall(r'input (\d+) : \((.*)(?:,| ;) \(\)\)', program)}


def encode_frame(shape, keys, diffs, vals=None):
    """A `data` frame of updates, as `feed … frame` reads it: `rows | 0 | keys | vals | diffs`,
    with no time lanes (see `interactive::corgi::frame`). `shape` is the input's declared
    `(key, val)` shapes, parsed; `vals` defaults to units."""
    key_shape, val_shape = shape
    out = bytearray(struct.pack('<2Q', len(diffs), 0))
    _encode_column(key_shape, keys, out)
    _encode_column(val_shape, vals if vals is not None else [()] * len(keys), out)
    out += struct.pack(f'<{len(diffs)}q', *diffs)
    return bytes(out)


class _Words:
    """A cursor over a frame's little-endian 64-bit words."""
    def __init__(self, data):
        self.data, self.at = data, 0

    def words(self, n, kind='Q'):
        values = struct.unpack_from(f'<{n}{kind}', self.data, self.at)
        self.at += 8 * n
        return values

    def word(self):
        return self.words(1)[0]

    def leaf(self):
        """A leaf column, `storage | len | payload`, as a list of its values."""
        storage, n = self.word(), self.word()
        if storage == 8:
            values = list(self.data[self.at:self.at + n])
            self.at += (n + 7) & ~7
            return values
        if storage == 64:
            return list(self.words(n, 'q'))
        if storage == 0xF64:
            return [_float(key) for key in self.words(n)]
        raise ValueError(f'unsupported leaf storage {storage}')

    def column(self):
        """One column in corgi's byte encoding, as a function from row number to value: an
        integer or float, a list for a tuple or a list, `dict(tag, payload)` for a sum."""
        kind = self.word()
        if kind == 0:
            return self.leaf().__getitem__
        if kind == 1:
            fields = [self.column() for _ in range(self.word())]
            return lambda i: [field(i) for field in fields]
        if kind == 2:
            if self.word() == 0:
                tags = self.leaf()
                offsets = self.words(self.word())
            else:
                tag, rows = self.word(), self.word()
                tags, offsets = [tag] * rows, range(rows)
            lanes = [self.column() for _ in range(self.word())]
            return lambda i: dict(tag=tags[i], payload=lanes[tags[i]](offsets[i]))
        if kind == 3:
            if self.word() == 0:
                ends = self.words(self.word())
            else:
                stride, rows = self.word(), self.word()
                ends = [stride * (row + 1) for row in range(rows)]
            values = self.column()
            return lambda i: [values(j) for j in range(ends[i - 1] if i else 0, ends[i])]
        if kind == 4:
            self.word()
            return lambda i: []
        if kind == 5:
            rows = self.words(self.word())
            arena = self.column()
            return lambda i: [] if rows[i] == (1 << 64) - 1 else arena(rows[i])
        raise ValueError(f'unsupported column kind {kind}')


def decode(frames):
    """The rows of a binary peek's `data` frames: `[key, val, diff]`, by key. A frame is
    `rows | lanes | keys | vals | diffs | times` (see `interactive::corgi::frame`)."""
    result = []
    for kind, payload in frames:
        if kind != 'data':
            continue
        words = _Words(payload)
        rows, lanes = words.word(), words.word()
        if lanes:
            raise ValueError('a peek frame has no times')
        keys, vals = words.column(), words.column()
        diffs = words.words(rows, 'q')
        result.extend([keys(i), vals(i), diffs[i]] for i in range(rows))
    return sorted(result, key=lambda r: r[0])


class Server:
    def __init__(self, binary, log, workers, timeout, max_rss_gib):
        self.timeout, self.counter = timeout, 0
        self.stream = self.reader = None
        self.log = log.open('wb')
        self.peak_rss = 0
        self.failure = None
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        env = dict(os.environ, DDIR_BIND=f'127.0.0.1:{port}', DDIR_WS_BIND='127.0.0.1:0',
                   DDIR_DIAGNOSTICS=os.environ.get('DDIR_DIAGNOSTICS', '0'), DDIR_WORKERS=str(workers))
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
            if isinstance(command, tuple):
                # A framed feed: its header line, then exactly its bytes.
                header, frame = command
                payload.append(f'{rid} {header}\n'.encode() + frame)
            else:
                payload.append((rid + ' ' + command.replace('{rid}', rid) + '\n').encode())
        wire = b''.join(payload)
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
            if kind == 'frame':
                # `<rid> frame <kind> <nbytes>`, then exactly that many bytes.
                frame, length = body.split(' ')
                payload = self.reader.read(int(length))
                received += len(payload)
                replies[rid].append((frame, payload))
            elif kind == 'data':
                replies[rid].append(body)
            elif kind == 'ok':
                pending.remove(rid)
            else:
                raise RuntimeError(body)
        end = time.perf_counter()
        return dict(encode_ms=1000*(encoded-start), wire_ms=1000*(end-encoded),
                    bytes_sent=len(wire), bytes_received=received), [replies[rid] for rid in ids]

    @staticmethod
    def feed(program, index, rows, diff, shape=None):
        """Rows with unit values for `program`'s input `index`, each at `diff`: as text, or as one
        binary frame when `shape` (the input's declared shapes, parsed) is given."""
        if shape is not None:
            frame = encode_frame(shape, list(rows), [diff] * len(rows))
            return f'feed {program} {index} frame {len(frame)}', frame
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
