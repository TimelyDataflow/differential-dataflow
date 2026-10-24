"""The graph program with delete cascades computed in the dataflow.

The shared graph program (snb/indexes.py) takes the 17 projected relations as
inputs, so whoever feeds it must retract exactly the rows a delete removes,
cascades included. The driver used to work those out itself, from a copy of the
whole graph. This program instead takes what the update stream says:

- inputs 0..16: rows as inserted (the initial snapshot, then every insert);
- inputs 17..22: explicit delete targets, each with its deletion time:
  people, forums and messages as `(id, time)`, and friendships, memberships and
  likes as `(endpoint, endpoint, time)`.

It derives each live relation as the inserted rows less those the deletes
remove, following the same rules as `snb.updates.State`: deleting a person
deletes their messages, their Walls and Albums, and their edges, and clears the
moderator of their Groups; deleting a forum deletes its posts; deleting a message
deletes the replies below it. Everything after that is the shared graph program,
unchanged, reading the live relations where it read inputs.

This assumes no id is inserted again after its deletion, and no insert names an
entity already deleted; both hold for the BI SF0.1 and SF1 streams.
"""
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from snb.data import SCHEMA  # noqa: E402
from snb.indexes import binders, graph_program, shape  # noqa: E402

DELETES = {  # input index, name, shape of (target..., time)
    17: ('del_person', '(int, int)'), 18: ('del_forum', '(int, int)'), 19: ('del_message', '(int, int)'),
    20: ('del_knows', '(int, int, int)'), 21: ('del_member', '(int, int, int)'), 22: ('del_likes', '(int, int, int)'),
}
DELETE_INPUT = {'delete-person': 17, 'delete-forum': 18, 'delete-post': 19, 'delete-comment': 19,
                'delete-knows': 20, 'delete-member': 21, 'delete-like-post': 22, 'delete-like-comment': 22}


def row(name):
    return f"(({', '.join(binders(SCHEMA[name]))}), ())"


def tup(name):
    return f"({', '.join(binders(SCHEMA[name]))})"


def killed(name, field, dead):
    """Rows of `<name>_in` whose `field` is in the unit-valued collection `dead`."""
    b = binders(SCHEMA[name])
    key = b[list(SCHEMA[name]).index(field)]
    return f"({name}_in | map({row(name)} -> ({key}, {tup(name)})) | join({dead}, (_, r, ()) -> (r, ())))"


def killed_pair(name, dead):
    """Rows of `<name>_in` whose first two fields are an explicit delete target in `dead`."""
    b = binders(SCHEMA[name])
    return f"({name}_in | map({row(name)} -> (({b[0]}, {b[1]}), {tup(name)})) | join({dead}, (_, r, ()) -> (r, ())))"


def prefix(text, n):
    """Whether the byte list `text` starts with `n`."""
    return f'(((0, ({text} len, {len(n)}) min) range, {text}) gather, "{n}") eq'


def cascade_program(export_live=False):
    lines = [f"let {name}_in = input {i} : ({shape(list(s), s)}, ());" for i, (name, s) in enumerate(SCHEMA.items())]
    lines += [f"let {name} = input {i} : ({sh}, ());" for i, (name, sh) in DELETES.items()]
    walls = f"({prefix('title', 'Album ')}, {prefix('title', 'Wall ')}) any"
    group = prefix('title', 'Group ')
    m = binders(SCHEMA['message'])
    msg = row('message')
    lines.append(f'''
-- People deleted, and the forums they moderated: Walls and Albums are deleted, Groups lose their moderator.
let dead_p = del_person | map(((id, _), ()) -> (id, ())) | distinct;
let owned  = forum_in | map(((id, title, created, moderator), ()) -> (moderator, (id, title, created)))
                      | join(dead_p, (moderator, (id, title, created), ()) -> (id, (title, created, moderator)));
let dead_f = ((del_forum | map(((id, _), ()) -> (id, ()))) + (owned | filter((_, (title, _, _)) -> {walls})
                                                                  | map((id, _) -> (id, ())))) | distinct;
let groups = owned | filter((_, (title, _, _)) -> {group});
let kept   = groups - (groups | join(dead_f, (id, v, ()) -> (id, v)));

-- Messages deleted: explicit targets, those of deleted people, the posts of deleted forums,
-- and every reply below any of them.
let seeds = (del_message | map(((id, _), ()) -> (id, ())))
          + (message_in | map({msg} -> ({m[3]}, {m[0]})) | join(dead_p, (_, id, ()) -> (id, ())))
          + (message_in | filter({msg} -> ({m[1]}, 0) eq) | map({msg} -> ({m[9]}, {m[0]})) | join(dead_f, (_, id, ()) -> (id, ())));
let children = message_in | filter({msg} -> ({m[1]}, 1) eq) | map({msg} -> ({m[10]}, {m[0]}));
dead_m: {{
    let kids   = dead_m | join(children, (_, (), child) -> (child, ()));
    var dead_m = (seeds + kids) | distinct;
}}
let dead_msg = dead_m::dead_m;
let gone_knows  = del_knows  | map(((a, b, _), ()) -> ((a, b), ())) | distinct;
let gone_member = del_member | map(((f, p, _), ()) -> ((f, p), ())) | distinct;
let gone_likes  = del_likes  | map(((p, x, _), ()) -> ((p, x), ())) | distinct;
''')
    live = {
        'person': [killed('person', 'id', 'dead_p')],
        'message': [killed('message', 'id', 'dead_msg')],
        'knows': [killed_pair('knows', 'gone_knows'), killed('knows', 'src', 'dead_p'), killed('knows', 'dst', 'dead_p')],
        'member': [killed_pair('member', 'gone_member'), killed('member', 'forum', 'dead_f'), killed('member', 'person', 'dead_p')],
        'likes': [killed_pair('likes', 'gone_likes'), killed('likes', 'person', 'dead_p'), killed('likes', 'message', 'dead_msg')],
        'mtag': [killed('mtag', 'message', 'dead_msg')],
        'ftag': [killed('ftag', 'forum', 'dead_f')],
        **{n: [killed(n, 'person', 'dead_p')] for n in ('interest', 'study', 'work', 'email', 'language')},
    }
    for name in SCHEMA:
        if name == 'forum':
            lines.append(f"let forum = forum_in - (({killed('forum', 'id', 'dead_f')} + (kept | map((id, (title, created, moderator)) -> ((id, title, created, moderator), ())))) | distinct)\n"
                         f"          + (kept | map((id, (title, created, _)) -> ((id, title, created, -1), ())));")
        elif name in live:
            kills = ' + '.join(live[name])
            lines.append(f"let {name} = {name}_in - (({kills}) | distinct);")
        else:
            lines.append(f"let {name} = {name}_in;")
        if export_live:
            lines.append(f'export "live.{name}" = {name} | arrange;')
    rest = [l for l in graph_program().splitlines() if not re.match(r'let \w+ = input \d+ :', l)]
    return '\n'.join(lines) + '\n' + '\n'.join(rest) + '\n'


def delete_row(kind, rows, time):
    """An explicit delete operation as (input index, row) for `cascade_program`."""
    from snb import data
    entity = next(iter(rows))
    key = data.key(entity, rows[entity][0])
    return DELETE_INPUT[kind], (key, time) if isinstance(key, int) else (*key, time)


if __name__ == '__main__':
    print(cascade_program(export_live='--live' in sys.argv))
