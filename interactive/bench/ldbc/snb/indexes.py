"""The arrangements the shared graph program publishes, for plans to import.

- **Primary keys.** A relation with an `id` is published by it: `ldbc.<relation>` is
  `(id, (its other fields))`, one row per key.
- **Secondary indexes.** A foreign key of such a relation is published as a thin index
  to the primary key: `ldbc.<relation>.by_<field>` is `(field, id)`.
- **Relations without a primary key** (friendships, memberships, tags on messages,
  likes, ...) are published by each endpoint queries look up from:
  `ldbc.<relation>.by_<field>` is `(field, (the other fields))`. The first one listed
  is keyed by the relation's first field.
- **Derived.** `ldbc.friend` is the friendship relation in both directions between
  people who exist, `(person, (friend, since))`, and `ldbc.friend.pair` is the same
  pairs as keys, `((person, friend), ())`, for checking a pair. `ldbc.message.root` maps each message
  to the post at the root of its thread, `(message, root)`, and `ldbc.message.by_root`
  is the reverse, `(root, message)`; a thread's forum is its root post's.

Only indexes some authored plan imports are published, plus each keyless relation's
first index, which generated plans read rows from.

Plans generated from snb/queries.py rebuild each source's rows from its primary-key
arrangement (or its first index), so they see exactly the relations of snb/data.py.
"""
from .data import SCHEMA

PRIMARY = ('person', 'message', 'forum', 'tag', 'tagclass', 'place', 'org')

# relation -> fields indexed. For keyed relations: foreign keys (-> id). Otherwise: the
# endpoints, first field first.
INDEXES = {
    'person': ('city',),
    'message': ('creator', 'parent', 'forum'),
    'forum': ('moderator',),
    'tag': ('name', 'class'),
    'tagclass': ('name', 'parent'),
    'place': ('name', 'parent'),
    'org': ('name',),
    'knows': ('src',),
    'member': ('forum', 'person'),
    'interest': ('person', 'tag'),
    'mtag': ('message', 'tag'),
    'ftag': ('forum',),
    'likes': ('person', 'message'),
    'study': ('person',),
    'work': ('person', 'org'),
    'email': ('person',),
    'language': ('person',),
}
assert set(INDEXES) == set(SCHEMA)

SHAPE = {'int': 'int', 'bytes': 'List(int)'}


def shape(names, schema):
    return '(' + ', '.join(SHAPE[schema[n]] for n in names) + ')'


def binders(schema):
    # Lambda binders named after the fields (`type` is not a usable name).
    return ['typ' if n == 'type' else n for n in schema]


def import_rows(name):
    """DDP for a source relation's rows, `((fields...), ())`, from its published arrangement."""
    schema = SCHEMA[name]
    fields = list(schema)
    b = binders(schema)
    if name in PRIMARY:
        rest = fields[1:]
        return (f'(import "ldbc.{name}" : (int, {shape(rest, schema)}) '
                f'| map(({b[0]}, ({", ".join(b[1:])})) -> (({", ".join(b)}), ())))')
    first = INDEXES[name][0]
    assert first == fields[0], name
    rest = fields[1:]
    value = f'({", ".join(b[1:])})' if len(rest) > 1 else b[1]
    return (f'(import "ldbc.{name}.by_{first}" : ({SHAPE[schema[first]]}, {shape(rest, schema) if len(rest) > 1 else SHAPE[schema[rest[0]]]}) '
            f'| map(({b[0]}, {value}) -> (({", ".join(b)}), ())))')


def graph_program():
    """The shared graph program: inputs 0..16 in SCHEMA order, published by key."""
    lines = []
    for i, (name, schema) in enumerate(SCHEMA.items()):
        fields = list(schema)
        b = binders(schema)
        row = f'(({", ".join(b)}), ())'
        lines.append(f'let {name} = input {i} : ({shape(fields, schema)}, ());')
        if name in PRIMARY:
            lines.append(f'export "ldbc.{name}" = {name} | map({row} -> ({b[0]}, ({", ".join(b[1:])}))) | arrange;')
            for f in INDEXES[name]:
                lines.append(f'export "ldbc.{name}.by_{f}" = {name} | map({row} -> ({b[fields.index(f)]}, {b[0]})) | arrange;')
        else:
            for f in INDEXES[name]:
                others = [b[j] for j, n in enumerate(fields) if n != f]
                value = f'({", ".join(others)})' if len(others) > 1 else others[0]
                lines.append(f'export "ldbc.{name}.by_{f}" = {name} | map({row} -> ({b[fields.index(f)]}, {value})) | arrange;')
    lines.append('''-- Friendships in both directions, between people who exist: (person, (friend, since)).
let exists = person | map(((id, _, _, _, _, _, _, _, _, _, _, _), ()) -> (id, ())) | distinct;
let either = (knows | map(((a, b, t), ()) -> (a, (b, t)))) + (knows | map(((a, b, t), ()) -> (b, (a, t))));
let friends = either | join(exists, (a, (b, t), ()) -> (b, (a, t)))
                     | join(exists, (b, (a, t), ()) -> ((a, b, t), ())) | distinct
                     | map(((a, b, t), ()) -> (a, (b, t)));
export "ldbc.friend" = friends | arrange;
export "ldbc.friend.pair" = friends | map((a, (b, _)) -> ((a, b), ())) | arrange;
-- Threads: every post is its own root; a comment's root is its parent's.
let posts   = message | filter(((_, kind, _, _, _, _, _, _, _, _, _, _, _, _), ()) -> (kind, 0) eq)
                      | map(((id, _, _, _, _, _, _, _, _, _, _, _, _, _), ()) -> ((id, id), ()));
let replies = message | filter(((_, kind, _, _, _, _, _, _, _, _, _, _, _, _), ()) -> (kind, 1) eq)
                      | map(((id, _, _, _, _, _, _, _, _, _, parent, _, _, _), ()) -> (parent, id));
thread: {
    let below  = thread | map(((mid, root), ()) -> (mid, root))
                        | join(replies, (_, root, child) -> ((child, root), ()));
    set thread = (posts + below) | distinct;
}
export "ldbc.message.root" = thread::thread | map(((mid, root), ()) -> (mid, root)) | arrange;
export "ldbc.message.by_root" = thread::thread | map(((mid, root), ()) -> (root, mid)) | arrange;''')
    return '\n'.join(lines) + '\n'
