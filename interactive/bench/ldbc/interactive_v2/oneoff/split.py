"""Writes <plan>.standing.ddp and <plan>.oneoff.ddp here.

Split an authored plan into a standing program (binding-independent statements, published)
and a one-off program (the request and everything that depends on it, importing those)."""
import re, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
PLANS = Path(__file__).resolve().parents[2] / 'authored' / 'plans'

def statements(text):
    text = '\n'.join(re.sub(r'--.*$', '', l) for l in text.splitlines())
    out, depth, cur = [], 0, ''
    for ch in text:
        cur += ch
        if ch in '({[': depth += 1
        elif ch in ')}]': depth -= 1
        if depth == 0 and (ch == ';' or (ch == '}' and re.match(r'\s*[a-z_]+:\s*\{', cur))):
            out.append(cur.strip()); cur = ''
    assert not cur.strip(), cur
    return out

def name_of(s):
    m = re.match(r'(let|var)\s+([a-z_0-9]+)', s) or re.match(r'([a-z_]+):\s*\{', s)
    return m.group(2) if m and m.lastindex == 2 else (m.group(1) if m else None)

# plan -> {standing statement name: its exported (key, value) type}, plus text edits for the one-off
SPEC = {
    'ic09': ({'by_author': '(int, (int, int))', 'latest': '(int, (int, int))', 'newest': '(int, int)'}, []),
    'ic10': ({'people': '(int, (List(int), List(int), List(int), List(int), int, int))', 'posts': '(int, int)',
              'likes': '((int, int), ())'}, []),
    'ic12': ({'below_hl': '(int, int)', 'reply': '(int, (int, int))'},
             [('below::below | map(((low, high), ()) -> (high, low))', 'below_hl')]),
    'ic14': ({'weighted': '(int, (int, int))'}, []),
    'ic01': ({}, []), 'ic13': ({}, []),
    'ic02': ({'by_author': '(int, (int, int))', 'latest': '(int, (int, int))', 'newest': '(int, int)'}, []),
    'ic03': ({'sent_from': '(int, (int, int))'}, []),
    'ic04': ({'posts': '(int, (int, int))', 'tags': '(int, (int, List(int)))'}, []),
    'ic05': ({'posted': '((int, int), int)', 'first': '(int, int)', 'lowest': '(int, int)'}, []),
    'ic06': ({}, []), 'ic07': ({}, []), 'ic08': ({}, []), 'ic11': ({}, []),
}

def split(plan):
    text = (PLANS / f'{plan}.ddp').read_text()
    standing_names, edits = SPEC[plan]
    if plan == 'ic12':
        text = text.replace('let intag', 'let below_hl = below::below | map(((low, high), ()) -> (high, low));\nlet intag', 1)
    for a, b in edits:
        text = text.replace(a, b) if plan != 'ic12' else text.replace('join(' + a, 'join(' + b)
    stmts = statements(text)
    imports = [s for s in stmts if re.match(r'let\s+\w+\s*=\s*import', s)]
    request = [s for s in stmts if re.match(r'let\s+request\s*=\s*input', s)]
    exports = [s for s in stmts if s.startswith('export')]
    body = [s for s in stmts if s not in imports + request + exports]
    standing = [s for s in body if name_of(s) in standing_names or (plan == 'ic12' and name_of(s) in ('selves', 'below'))
                or (plan in ('ic14',) and name_of(s) in ('answered', 'talks'))]
    oneoff = [s for s in body if s not in standing]
    p = plan[:2] + str(int(plan[2:]))
    st = imports + standing + [f'export "{p}.s.{n}" = {n} | arrange;' for n in standing_names]
    oo = request + imports + [f'let {n} = import "{p}.s.{n}" : {t};' for n, t in standing_names.items()] + oneoff + exports
    return '\n'.join(st) + '\n', '\n'.join(oo) + '\n'

for plan in SPEC:
    s, o = split(plan)
    if SPEC[plan][0]:
        (HERE / f'{plan}.standing.ddp').write_text(s)
    (HERE / f'{plan}.oneoff.ddp').write_text(o)
    print(plan, 'standing' if SPEC[plan][0] else '', [name_of(x) for x in statements(o)][:40])
