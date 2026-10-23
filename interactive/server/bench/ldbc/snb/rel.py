"""Small named relational notation with DDP emission and Python evaluation.

This is benchmark authoring infrastructure, not another execution backend.
The Python evaluator checks DDP lowering and incremental execution against
snapshot evaluation of the same logical plan. It is not an independent query
specification oracle; query semantics also need hand-checked witnesses.
"""
from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
import operator

from .topk import pipeline


@dataclass(eq=False)
class E:
    op: str
    args: tuple

    def __add__(self, x): return E('+', (self, expr(x)))
    def __radd__(self, x): return expr(x)+self
    def __sub__(self, x): return E('-', (self, expr(x)))
    def __rsub__(self, x): return expr(x)-self
    def __mul__(self, x): return E('*', (self, expr(x)))
    def __rmul__(self, x): return expr(x)*self
    def __neg__(self): return 0-self
    def __eq__(self, x): return E('==', (self, expr(x)))
    def __ne__(self, x): return E('!=', (self, expr(x)))
    def __lt__(self, x): return E('<', (self, expr(x)))
    def __le__(self, x): return E('<=', (self, expr(x)))
    def __gt__(self, x): return E('>', (self, expr(x)))
    def __ge__(self, x): return E('>=', (self, expr(x)))
    def __and__(self, x): return E('&&', (self, expr(x)))
    def __or__(self, x): return E('or', (self, expr(x)))
    def __bool__(self): raise TypeError('use & / | for relational expressions')
    def at(self, index): return E('at', (self, index))


def expr(x): return x if isinstance(x,E) else E('literal',(x,))
def col(name): return E('column',(name,))
def choose(cond, yes, no): return E('if',(expr(cond),expr(yes),expr(no)))
def absolute(x): return choose(x < 0,-x,x)
def call(name, *args): return E(name,tuple(expr(x) for x in args))
def empty(kind): return E('empty',(kind,))


class Columns:
    def __getattr__(self, name): return col(name)
    def __getitem__(self, name): return col(name)
c = Columns()


_FUNCTIONS = {'+':operator.add,'-':operator.sub,'*':operator.mul,'==':operator.eq,
              '!=':operator.ne,'<':operator.lt,'<=':operator.le,'>':operator.gt,'>=':operator.ge,
              '&&':lambda a,b:int(bool(a) and bool(b)),'or':lambda a,b:int(bool(a) or bool(b)),
              'len':len,'tuple':lambda *xs:tuple(xs),'list':lambda *xs:tuple(xs),'append':operator.add,
              'float':float,'fneg':operator.neg,'fadd':operator.add,'fsub':operator.sub,'fmul':operator.mul,'fdiv':operator.truediv,
              'idiv':lambda a,b: (abs(a)//abs(b))*(-1 if (a<0)!=(b<0) else 1) if b else 0}


def value(e, row):
    op,args = e.op,e.args
    if op == 'column': return row[args[0]]
    if op == 'literal':
        x = args[0]
        return tuple(x.encode()) if isinstance(x,str) else x
    if op == 'empty': return ()
    if op == 'at': return value(args[0],row)[args[1]]
    if op == 'if': return value(args[1] if value(args[0],row) else args[2],row)
    vs = [value(a,row) for a in args]
    return _FUNCTIONS[op](*vs)


def product(fields):
    """A corgi tuple, including unit and one-field tuples."""
    return '('+', '.join(fields)+')'


def mask(e, code):
    """Normalize truthiness only when the expression is not already a 0/1 mask."""
    if e.op in ('==','!=','<','<=','>','>=','&&','or'): return code
    return product([code,'0'])+' ne'


def scalar(e, fields):
    """Print an expression inside a corgi lambda with named input fields."""
    op,args = e.op,e.args
    if op == 'column': return fields[args[0]]
    if op == 'literal':
        x = args[0]
        if isinstance(x,str):
            return '['+','.join(str(b) for b in x.encode())+']' if x else scalar(empty('bytes'),fields)
        return str(int(x))
    if op == 'empty':
        shape = {'bytes':'List(int)', 'strings':'List(List(int))',
                 'triples':'List((List(int), int, List(int)))', 'ints':'List(int)'}[args[0]]
        return f'[] : {shape}'
    if op == 'at': return f'{scalar(args[0],fields)} .{args[1]}'
    ts = [scalar(a,fields) for a in args]
    if op == 'tuple': return product(ts)
    if op == 'list':
        if not ts: raise ValueError('an empty list needs its element shape')
        return product(t+' enlist' for t in ts)+' append'
    if op == 'if': return product([mask(args[0],ts[0]),*ts[1:]])+' select'
    if op in ('&&','or'):
        return product(mask(a,t) for a,t in zip(args,ts))+(' all' if op=='&&' else ' any')
    binary = {'+':'add','-':'sub','*':'mul','==':'eq','!=':'ne','<':'lt','<=':'le','>':'gt','>=':'ge',
              'append':'append','idiv':'div','fadd':'add','fsub':'sub','fmul':'mul','fdiv':'div'}
    unary = {'len':'len','float':'to_float','fneg':'neg'}
    if op in binary: return product(ts)+' '+binary[op]
    if op in unary: return ts[0]+' '+unary[op]
    raise ValueError(f'no corgi spelling for expression {op}')


class R:
    def __init__(self, op, fields, *args):
        self.op,self.fields,self.args = op,tuple(fields),args
        if len(set(self.fields)) != len(self.fields):
            raise ValueError(f'duplicate fields {fields}')

    @staticmethod
    def source(name, schema): return R('source',schema,name,dict(schema))
    def select(self, *names, **computed):
        pairs = {name:col(name) for name in names}
        pairs.update({name:expr(e) for name,e in computed.items()})
        return R('select',pairs,self,pairs)
    def rename(self, **mapping): return self.select(**{mapping.get(n,n):col(n) for n in self.fields})
    def where(self, pred): return R('where',self.fields,self,expr(pred))
    def distinct(self): return R('distinct',self.fields,self)
    def union(self, other):
        assert self.fields == other.fields,(self.fields,other.fields)
        return R('union',self.fields,self,other)
    def join(self, other, on=None, prefix=''):
        on = on or {}
        right = {n:prefix+n for n in other.fields if not (n in on.values() and not prefix and n in self.fields)}
        return R('join',(*self.fields,*right.values()),self,other,dict(on),right)
    def semi(self, other, on):
        return R('semi',self.fields,self,other,dict(on))
    def anti(self, other, on):
        return R('anti',self.fields,self,other,dict(on))
    def left(self, other, on, defaults, prefix=''):
        joined = self.join(other,on,prefix)
        missing = self.anti(other,on).select(*self.fields,**defaults)
        assert joined.fields == missing.fields,(joined.fields,missing.fields)
        return joined.union(missing)
    def group(self, keys=(), **aggregates):
        # aggregate := ('sum'|'count'|'min'|'collect', scalar expression)
        return R('group',(*keys,*aggregates),self,tuple(keys),aggregates)
    def rank(self, order, limit=None, groups=('rid',)):
        return R('rank',(*self.fields,'rank'),self,tuple(expr(e) for e in order),limit,tuple(groups))
    def closure(self, next_relation, on, select, keys, best, condition=1):
        """Least fixpoint of seed UNION recursive equijoin, min per key.

        The step sees left fields and right fields under `next_` prefix.
        `best` is the lexicographically ordered value field list.
        """
        assert set(keys)|set(best) == set(self.fields)
        return R('closure',self.fields,self,next_relation,dict(on),{k:expr(v) for k,v in select.items()},tuple(keys),tuple(best),expr(condition))


class SourceRows:
    """Raw snapshot tuples as read-only rows, encoding byte fields only when read.

    A reference lookup need not copy/encode every content string in a million-row
    source just to inspect its IDs. Ordinary encoded tuple sources still work.
    """
    def __init__(self, rows, schema):
        self.rows = rows
        self.layout = {name:(i,kind) for i,(name,kind) in enumerate(schema.items())}

    def __iter__(self):
        return (_SourceRow(self.layout,row) for row in self.rows)


class _SourceRow(Mapping):
    __slots__ = ('layout','row')

    def __init__(self, layout, row):
        self.layout, self.row = layout, row

    def __iter__(self): return iter(self.layout)
    def __len__(self): return len(self.layout)

    def __getitem__(self, name):
        index, kind = self.layout[name]
        value = self.row[index]
        return tuple(value.encode()) if kind == 'bytes' else value


def evaluate(root, data, cache=None):
    # A caller evaluating several exports against one snapshot can share this
    # cache. It must start a fresh cache when the input snapshot changes.
    private = cache is None
    cache = {} if private else cache
    uses = Counter()
    if private:
        pending, seen = [root], set()
        while pending:
            node = pending.pop()
            if id(node) in seen: continue
            seen.add(id(node))
            for child in node.args:
                if isinstance(child,R):
                    uses[id(child)] += 1
                    pending.append(child)
    def ev(r):
        if id(r) in cache: return cache[id(r)]
        op,args = r.op,r.args
        if op == 'source':
            source = data[args[0]]
            rows = list(source) if isinstance(source,SourceRows) else [dict(zip(r.fields,row)) for row in source]
        elif op == 'select': rows = [{n:value(e,row) for n,e in args[1].items()} for row in ev(args[0])]
        elif op == 'where': rows = [row for row in ev(args[0]) if value(args[1],row)]
        elif op == 'distinct': rows = [dict(zip(r.fields,row)) for row in set(tuple(row[n] for n in r.fields) for row in ev(args[0]))]
        elif op == 'union': rows = ev(args[0])+ev(args[1])
        elif op in ('join','semi','anti'):
            left,right,on = args[:3]
            index = defaultdict(list)
            for row in ev(right): index[tuple(row[n] for n in on.values())].append(row)
            rows = []
            for row in ev(left):
                matches = index[tuple(row[n] for n in on)]
                if op == 'join': rows.extend({**row,**{out:other[n] for n,out in args[3].items()}} for other in matches)
                elif bool(matches) == (op=='semi'): rows.append(row)
        elif op == 'group':
            source,keys,aggs = args
            groups = defaultdict(list)
            for row in ev(source): groups[tuple(row[n] for n in keys)].append(row)
            rows = []
            for key,members in groups.items():
                row = dict(zip(keys,key))
                for n,(kind,e) in aggs.items():
                    vs = [value(expr(e),member) for member in members]
                    row[n] = {'sum':sum,'count':len,'min':min,'collect':lambda xs:tuple(sorted(xs))}[kind](vs)
                rows.append(row)
        elif op == 'rank':
            source,order,limit,keys = args
            groups = defaultdict(list)
            for row in ev(source): groups[tuple(row[n] for n in keys)].append(row)
            rows = [{**row,'rank':i} for members in groups.values()
                    for i,row in enumerate(sorted(members,key=lambda row:(tuple(value(e,row) for e in order),tuple(row[n] for n in source.fields)))[:limit])]
        elif op == 'closure':
            seed,nextrel,on,projection,keys,best,condition = args
            state = {}
            index = defaultdict(list)
            for row in ev(nextrel): index[tuple(row[n] for n in on.values())].append(row)
            pending = ev(seed)
            while pending:
                fresh = {}
                for row in pending:
                    key = tuple(row[n] for n in keys)
                    if key not in state or tuple(row[n] for n in best) < tuple(state[key][n] for n in best):
                        state[key] = row
                        fresh[key] = row
                pending = []
                for row in fresh.values():
                    for other in index[tuple(row[n] for n in on)]:
                        environment = {**row, **{'next_'+k:v for k,v in other.items()}}
                        if value(condition,environment):
                            pending.append({n:value(e,environment) for n,e in projection.items()})
            rows = list(state.values())
        else: raise ValueError(op)
        cache[id(r)] = rows
        if private:
            # Every parent is evaluated once. Keep a shared child until all
            # parents have consumed it, then release its intermediate rows.
            for child in args:
                if isinstance(child,R):
                    uses[id(child)] -= 1
                    if not uses[id(child)]: cache.pop(id(child),None)
        return rows
    try:
        return ev(root)
    finally:
        # The recursive function closes over itself and its whole snapshot
        # cache. Break that cycle before the next reference evaluation.
        ev = None


def source_shape(schema):
    return '(' + ', '.join({'int': 'int', 'bytes': 'List(int)'}[kind] for kind in schema.values()) + ')'


class Compiler:
    def __init__(self, imports=False, *, rank_buckets=(1,), rank_hash_key='value'):
        self.lines = []
        self.names = {}
        self.inputs = []
        self.imports = imports
        self.rank_buckets = tuple(rank_buckets)
        self.rank_hash_key = rank_hash_key

    def emit(self, r):
        if id(r) in self.names: return self.names[id(r)]
        op,args = r.op,r.args
        name = 'r'+str(len(self.names))
        self.names[id(r)] = name
        def positions(fields, row='row'): return {n:f'{row}.{i}' for i,n in enumerate(fields)}
        def projection(pairs, refs): return product(scalar(expr(e),refs) for e in pairs)
        def keyed(source, fields, keys, keep=True):
            key = projection([col(n) for n in keys],positions(fields))
            return f'({source} | map((row, ()) -> ({key}, {"row" if keep else "()"})))'
        if op == 'source':
            self.inputs.append((args[0],args[1]))
            if self.imports:
                code = 'input 0' if args[0] == 'request' else f'import "ldbc.{args[0]}"'
            else:
                code = f'input {len(self.inputs)-1}'
            code += f' : ({source_shape(args[1])}, ())'
        elif op == 'select':
            source,pairs = args
            code = f'{self.emit(source)} | map((row, ()) -> ({projection(pairs.values(),positions(source.fields))}, ()))'
        elif op == 'where':
            source,pred = args
            code = f'{self.emit(source)} | filter((row, ()) -> {scalar(pred,positions(source.fields))})'
        elif op == 'distinct': code = f'{self.emit(args[0])} | distinct'
        elif op == 'union': code = f'({self.emit(args[0])}) + ({self.emit(args[1])})'
        elif op in ('join','semi','anti'):
            left,right,on = args[:3]
            l,rhs = self.emit(left),self.emit(right)
            keyed_left = keyed(l,left.fields,on)
            if op == 'join':
                keyed_right = keyed(rhs,right.fields,on.values())
                out = product([*positions(left.fields,'left').values(),
                               *(positions(right.fields,'right')[n] for n in args[3])])
            else:
                keyed_right = f'({keyed(rhs,right.fields,on.values(),False)} | distinct)'
                out = 'left'
            matched = f'{keyed_left} | join({keyed_right}, (_, left, right) -> ({out}, ()))'
            code = f'{l} - ({matched})' if op=='anti' else matched
        elif op == 'group':
            source,keys,aggs = args
            source_name = self.emit(source)
            refs = positions(source.fields)
            group_key = projection([col(n) for n in keys],refs)
            outs = list(positions(keys,'key').values())
            if aggs and all(kind in ('count', 'sum') for kind, _ in aggs.values()):
                group_rows = name+'_keys'
                self.lines.append(f'let {group_rows} = {source_name} | map((row, ()) -> ({group_key}, ()));')
                groups = name+'_groups'
                if any(kind == 'sum' for kind, _ in aggs.values()):
                    self.lines.append(f'let {groups} = {group_rows} | distinct;')
                joined = None
                for i, (kind, e) in enumerate(aggs.values()):
                    aggregate = f'{name}_aggregate_{i}'
                    if kind == 'count':
                        code = f'{group_rows} | count'
                    else:
                        # DD adds integer contributions in the diff field. Zero
                        # time leaves the input timestamp unchanged.
                        weight = scalar(expr(e), refs)
                        code = f'{source_name} | join_function((row, ()) -> (({group_key}, ()), (0), {weight}) enlist) | count'
                    self.lines.append(f'let {aggregate} = {code};')
                    if kind == 'sum':
                        # count omits zero totals, but SQL-style grouping keeps
                        # a zero sum while the input group remains nonempty.
                        missing = f'({groups} - ({groups} | join({aggregate}, (key, _, _) -> (key, ()))))'
                        present = aggregate+'_present'
                        self.lines.append(f'let {present} = {aggregate} + ({missing} | map((key, ()) -> (key, 0)));')
                        aggregate = present
                    stage = f'{name}_joined_{i}'
                    if joined is None:
                        code = f'{aggregate} | map((key, total) -> (key, (total)))'
                    else:
                        values = product([*(f'left.{j}' for j in range(i)), 'right'])
                        code = f'{joined} | join({aggregate}, (key, left, right) -> (key, {values}))'
                    self.lines.append(f'let {stage} = {code};')
                    joined = stage
                fields = product([*outs, *(f'totals.{i}' for i in range(len(aggs)))])
                self.lines.append(f'-- {name}: ({", ".join(r.fields)})\nlet {name} = {joined} | map((key, totals) -> ({fields}, ()));')
                return name
            if len(aggs)==1 and next(iter(aggs.values()))[0]=='min':
                _,e = next(iter(aggs.values()))
                code = f'{source_name} | map((row, ()) -> ({group_key}, ({scalar(expr(e),refs)}))) | min | map((key, (best)) -> ({product([*outs,"best"])}, ()))'
                self.lines.append(f'-- {name}: ({", ".join(r.fields)})\nlet {name} = {code};')
                return name
            vals = projection([e for _,e in aggs.values()],refs)
            for i,(kind,e) in enumerate(aggs.values()):
                # These fold bodies read only their accumulator and item, so they need no capture.
                if kind=='count': out = 'values len'
                elif kind=='sum': out = f'(0, values) fold ((acc, item) -> (acc, item.{i}) add)'
                elif kind=='min': out = f'((0, values) get .{i}, values) fold ((acc, item) -> ((item.{i}, acc) lt, item.{i}, acc) select)'
                elif kind=='collect':
                    # A dedicated value-only collect avoids lists of singleton
                    # tuples and is useful for nested IC1/IC12 results.
                    assert len(aggs)==1
                    code = f'{source_name} | map((row, ()) -> ({group_key}, ({scalar(expr(e),refs)}))) | value((_, (item)) -> item) | collect | map((key, values) -> ({product([*outs,"values"])}, ()))'
                    self.lines.append(f'-- {name}: ({", ".join(r.fields)})\nlet {name} = {code};')
                    return name
                else: raise ValueError(kind)
                outs.append(out)
            code = f'{source_name} | map((row, ()) -> ({group_key}, {vals})) | collect | map((key, values) -> ({product(outs)}, ()))'
        elif op == 'rank':
            source,order,limit,keys = args
            src = self.emit(source)
            refs = positions(source.fields)
            sort = product([*(scalar(e,refs) for e in order),'row'])
            ordered = f'{src} | map((row, ()) -> ({projection([col(n) for n in keys],refs)}, {sort}))'
            out = product([*(f'item.{len(order)}.{i}' for i in range(len(source.fields))),'rank'])
            if limit is None:
                code = ordered+' | collect | flatmap((_, values) -> values)'
                code += f' | map((_, (rank, item)) -> ({out}, ()))'
            else:
                # DD retains all candidates; the lambda copies only the best
                # `limit` rows. Only the last bucket stage assigns ranks.
                initial = name+'_input'
                stages, last = pipeline(initial, name+'_top', self.rank_buckets,
                                        self.rank_hash_key, limit)
                self.lines.append('\n'.join([f'let {initial} = {ordered};', *stages,
                    f'let {name} = {last} | map((_, (rank, item)) -> ({out}, ()));']))
                return name
        elif op == 'closure':
            seed,nextrel,on,select,keys,best,condition = args
            s,n = self.emit(seed),self.emit(nextrel)
            refs = positions(seed.fields,'left')
            refs.update({'next_'+k:v for k,v in positions(nextrel.fields,'right').items()})
            fields = positions(seed.fields)
            k = projection([col(x) for x in keys],fields)
            b = projection([col(x) for x in best],fields)
            restore = {**positions(keys,'key'),**positions(best,'best')}
            # Keep a flat-row recursive variable, with min in its definition.
            # Evaluate the condition on the joined environment before reshaping.
            combined = list(seed.fields)+['next_'+x for x in nextrel.fields]
            joined = product(refs[x] for x in combined)
            step_refs = positions(combined)
            self.lines.append(f'{name}_loop: {{\n let proposals = {keyed("state",seed.fields,on)} | join({keyed(n,nextrel.fields,on.values())}, (_, left, right) -> ({joined}, ())) | filter((row, ()) -> {scalar(condition,step_refs)}) | map((row, ()) -> ({projection(select.values(),step_refs)}, ()));\n var state = ({s} + proposals) | map((row, ()) -> ({k}, {b})) | min | map((key, best) -> ({product(restore[x] for x in seed.fields)}, ()));\n}}')
            code = f'{name}_loop::state'
        else: raise ValueError(op)
        self.lines.append(f'-- {name}: ({", ".join(r.fields)})\nlet {name} = {code};')
        return name

    def compile(self, root):
        return self.compile_many({'result':root})

    def compile_many(self, roots):
        exports=[]
        for export,root in roots.items():
            if not export.replace('_','').replace('.','').isalnum(): raise ValueError('invalid export name')
            name = self.emit(root)
            refs = {n:f'row.{i}' for i,n in enumerate(root.fields)}
            values = [refs[n] for n in root.fields if n not in ('rid','rank')]
            exports.append(f'export "{export}" = {name} | map((row, ()) -> (({refs["rid"]}, {refs["rank"]}), {product(values)})) | arrange;')
        return '\n\n'.join([*self.lines,*exports])+'\n'
