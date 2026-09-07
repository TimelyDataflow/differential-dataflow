"""Small named relational notation with DDP emission and Python evaluation.

This is benchmark authoring infrastructure, not another execution backend.
The Python evaluator checks DDP lowering and incremental execution against
snapshot evaluation of the same logical plan. It is not an independent query
specification oracle; query semantics also need hand-checked witnesses.
"""
from collections import Counter, defaultdict
from dataclasses import dataclass
import operator


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
    funcs = {'+':operator.add,'-':operator.sub,'*':operator.mul,'==':operator.eq,
             '!=':operator.ne,'<':operator.lt,'<=':operator.le,'>':operator.gt,'>=':operator.ge,
             '&&':lambda a,b:int(bool(a) and bool(b)),'or':lambda a,b:int(bool(a) or bool(b)),
             'len':len,'tuple':lambda *xs:tuple(xs),'list':lambda *xs:tuple(xs),'append':operator.add,
             'float':float,'fneg':operator.neg,'fadd':operator.add,'fsub':operator.sub,'fmul':operator.mul,'fdiv':operator.truediv,
             'idiv':lambda a,b: (abs(a)//abs(b))*(-1 if (a<0)!=(b<0) else 1) if b else 0}
    return funcs[op](*vs)


def term(e, fields):
    op,args = e.op,e.args
    if op == 'column': return fields[args[0]]
    if op == 'literal':
        x = args[0]
        if isinstance(x,str):
            return 'list('+','.join(str(b) for b in x.encode())+')' if x else term(empty('bytes'),fields)
        return str(int(x))
    if op == 'empty':
        name = {'bytes':'EmptyBytes','strings':'EmptyStrings','triples':'EmptyTriples','ints':'EmptyBytes'}[args[0]]
        return f'case {name}(list()) {{ {name}(x) => x }}'
    if op == 'at': return f'({term(args[0],fields)})[{args[1]}]'
    ts = [term(a,fields) for a in args]
    if op in ('+','-','*','==','!=','<','<=','>','>=','&&'):
        return '('+f' {op} '.join(ts)+')'
    return op+'('+', '.join(ts)+')'


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


def evaluate(root, data, cache=None):
    # A caller evaluating several exports against one snapshot can share this
    # cache. It must start a fresh cache when the input snapshot changes.
    cache = {} if cache is None else cache
    def ev(r):
        if id(r) in cache: return cache[id(r)]
        op,args = r.op,r.args
        if op == 'source':
            rows = [dict(zip(r.fields,row)) for row in data[args[0]]]
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
                pending = [{n:value(e,{**row,**{'next_'+k:v for k,v in other.items()}}) for n,e in projection.items()}
                           for row in fresh.values() for other in index[tuple(row[n] for n in on)]
                           if value(condition,{**row,**{'next_'+k:v for k,v in other.items()}})]
            rows = list(state.values())
        else: raise ValueError(op)
        cache[id(r)] = rows
        return rows
    return ev(root)


def source_shape(schema):
    return '(' + ', '.join({'int': 'int', 'bytes': 'List(int)'}[kind] for kind in schema.values()) + ')'


class Compiler:
    def __init__(self, imports=False):
        self.lines = ['type ByteString = EmptyBytes List(int);',
                      'type StringSet = EmptyStrings List(List(int));',
                      'type TripleSet = EmptyTriples List((List(int), int, List(int)));']
        self.names = {}
        self.inputs = []
        self.imports = imports

    def emit(self, r):
        if id(r) in self.names: return self.names[id(r)]
        op,args = r.op,r.args
        name = 'r'+str(len(self.names))
        self.names[id(r)] = name
        def positions(fields, slot=0): return {n:f'${slot}[{i}]' for i,n in enumerate(fields)}
        def projection(pairs, refs): return ', '.join(term(expr(e),refs) for e in pairs)
        if op == 'source':
            self.inputs.append((args[0],args[1]))
            if self.imports:
                code = 'input 0' if args[0] == 'request' else f'import "ldbc.{args[0]}"'
            else:
                code = f'input {len(self.inputs)-1}'
            code += f' : ({source_shape(args[1])} ; ())'
        elif op == 'select':
            source,pairs = args
            code = f'{self.emit(source)} | map({projection(pairs.values(),positions(source.fields))} ;)'
        elif op == 'where':
            source,pred = args
            code = f'{self.emit(source)} | filter({term(pred,positions(source.fields))})'
        elif op == 'distinct': code = f'{self.emit(args[0])} | distinct'
        elif op == 'union': code = f'({self.emit(args[0])}) + ({self.emit(args[1])})'
        elif op in ('join','semi','anti'):
            left,right,on = args[:3]
            l,rhs = self.emit(left),self.emit(right)
            lkey = projection([col(n) for n in on],positions(left.fields))
            rkey = projection([col(n) for n in on.values()],positions(right.fields))
            keyed_left = f'({l} | key({lkey} ; $0))'
            if op == 'join':
                keyed_right = f'({rhs} | key({rkey} ; $0))'
                out = [f'$1[{i}]' for i in range(len(left.fields))]+[positions(right.fields,2)[n] for n in args[3]]
            else:
                keyed_right = f'({rhs} | key({rkey} ;) | distinct)'
                out = [f'$1[{i}]' for i in range(len(left.fields))]
            matched = f'{keyed_left} | join({keyed_right}, ({", ".join(out)} ;))'
            code = f'{l} - ({matched})' if op=='anti' else matched
        elif op == 'group':
            source,keys,aggs = args
            source_name = self.emit(source)
            refs = positions(source.fields)
            if len(aggs)==1 and next(iter(aggs.values()))[0]=='min':
                _,e = next(iter(aggs.values()))
                code = f'{source_name} | key({projection([col(n) for n in keys],refs)} ; {term(expr(e),refs)}) | min | map($0, $1[0] ;)'
                self.lines.append(f'-- {name}: ({", ".join(r.fields)})\nlet {name} = {code};')
                return name
            vals = projection([e for _,e in aggs.values()],refs)
            outs = [f'$0[{i}]' for i in range(len(keys))]
            for i,(kind,e) in enumerate(aggs.values()):
                if kind=='count': out = 'len($1)'
                elif kind=='sum': out = f'fold($1, 0, ^1 + ^0[{i}])'
                elif kind=='min': out = f'fold($1, $1[0][{i}], if(^0[{i}] < ^1, ^0[{i}], ^1))'
                elif kind=='collect':
                    # A dedicated value-only collect avoids lists of singleton
                    # tuples and is useful for nested IC1/IC12 results.
                    assert len(aggs)==1
                    code = f'{source_name} | key({projection([col(n) for n in keys],refs)} ; {term(expr(e),refs)}) | value($1[0]) | collect | map($0, ($1) ;)'
                    self.lines.append(f'-- {name}: ({", ".join(r.fields)})\nlet {name} = {code};')
                    return name
                else: raise ValueError(kind)
                outs.append(out)
            code = f'{source_name} | key({projection([col(n) for n in keys],refs)} ; {vals}) | collect | map({", ".join(outs)} ;)'
        elif op == 'rank':
            source,order,limit,keys = args
            src = self.emit(source)
            refs = positions(source.fields)
            sort = projection(order,refs)
            packed = 'tuple('+','.join(refs[n] for n in source.fields)+')'
            code = f'{src} | key({projection([col(n) for n in keys],refs)} ; {sort}, {packed}) | collect | flatmap($1)'
            if limit is not None: code += f' | filter($1[0] < {limit})'
            code += ' | map('+', '.join(f'$1[1][{len(order)}][{i}]' for i in range(len(source.fields)))+', $1[0] ;)'
        elif op == 'closure':
            seed,nextrel,on,select,keys,best,condition = args
            s,n = self.emit(seed),self.emit(nextrel)
            refs = positions(seed.fields,1)
            refs.update({'next_'+k:v for k,v in positions(nextrel.fields,2).items()})
            fields = positions(seed.fields)
            k = projection([col(x) for x in keys],fields)
            b = projection([col(x) for x in best],fields)
            restore = {**{n:f'$0[{i}]' for i,n in enumerate(keys)},**{n:f'$1[{i}]' for i,n in enumerate(best)}}
            # Keep a flat-row recursive variable, with min in its definition.
            # Evaluate the condition on the joined environment before reshaping.
            combined = list(seed.fields)+['next_'+x for x in nextrel.fields]
            joined = ', '.join(refs[x] for x in combined)
            step_refs = positions(combined)
            self.lines.append(f'{name}_loop: {{\n let proposals = state | key({projection([col(x) for x in on],fields)} ; $0) | join(({n} | key({projection([col(x) for x in on.values()],positions(nextrel.fields))} ; $0)), ({joined} ;)) | filter({term(condition,step_refs)}) | map({projection(select.values(),step_refs)} ;);\n var state = ({s} + proposals) | key({k} ; {b}) | min | map({", ".join(restore[x] for x in seed.fields)} ;);\n}}')
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
            refs = {n:f'$0[{i}]' for i,n in enumerate(root.fields)}
            values = [refs[n] for n in root.fields if n not in ('rid','rank')]
            exports.append(f'export "{export}" = {name} | map({refs["rid"]}, {refs["rank"]} ; {", ".join(values)}) | arrange;')
        return '\n\n'.join([*self.lines,*exports])+'\n'
