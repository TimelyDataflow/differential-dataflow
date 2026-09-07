"""Deterministic smoke bindings and snapshot checks, outside engine timings."""
from collections import Counter
from datetime import datetime, timezone
import itertools
import struct

from .data import millis
from .rel import evaluate


def parameters(graph, query=None):
    people=sorted(graph['person'])
    messages=sorted(graph['message'])
    authors=Counter(m[3] for m in messages if m[1]==1)
    pid=authors.most_common(1)[0][0]
    knows=sorted(graph['knows'])
    p1,p2,_=next((edge for edge in knows if pid in edge[:2]),knows[0])
    tags={r[0]:r for r in graph['tag']}
    places={r[0]:r for r in graph['place']}
    classes={r[0]:r for r in graph['tagclass']}
    tag=Counter(r[1] for r in graph['mtag']).most_common(1)[0][0]
    classid=tags[tag][2]
    tagged_ids={r[0] for r in graph['mtag'] if r[1]==tag}
    tagday=Counter(m[13] for m in messages if m[0] in tagged_ids).most_common(1)[0][0]
    countries=Counter(places[p[8]][3] for p in people)
    countries=[places[k][1] for k,_ in countries.most_common()]
    parent=Counter(m[10] for m in messages if m[1]==1).most_common(1)[0][0]
    result = dict(rid=1,pid=pid,p1=p1,p2=p2,firstname=people[0][1],mid=parent,
        maxdate=millis('2014-01-01T00:00:00Z'),start=millis('2010-01-01T00:00:00Z'),
        end=millis('2014-01-01T00:00:00Z'),date=millis('2010-01-01T00:00:00Z'),days=1500,
        mindate=millis('2010-01-01T00:00:00Z'),month=people[0][9],tagname=tags[tag][1],
        classname=classes[classid][1],countryname=countries[0],countryx=countries[0],countryy=countries[1],
        country1=countries[0],country2=countries[1],workyear=2100,minimum=1,maximum=4,
        length=500,language='en',enabled=1,delta=12,maxknows=100,city1=people[0][8],city2=people[1][8],
        company=next(r[1] for r in graph['org'] if r[2].lower()=='company'),
        taga=tags[tag][1],tagb=tags[tag][1],datea=tagday,dateb=tagday,endmonth=2014*12+1)
    # Deterministic smoke parameters, not LDBC's official parameter generator.
    # Select witnesses from input facts; query answers are never fed to DDIR.
    homes={p[0]:places[p[8]][3] for p in people}
    if query=='bi2': result['date']=tagday
    if query=='ic3':
        visits={p[0]:set() for p in people}
        for m in messages:
            if m[3] in homes and m[4]!=homes[m[3]]: visits[m[3]].add(m[4])
        for a,b,_ in knows:
            author,start=(a,b) if len(visits[a])>=2 else (b,a)
            foreign=sorted(visits[author]-{homes[start]})
            if len(foreign)>=2:
                result.update(pid=start,countryx=places[foreign[0]][1],countryy=places[foreign[1]][1])
                break
    if query=='bi14':
        for a,b,_ in knows:
            if homes[a]!=homes[b]:
                result.update(country1=places[homes[a]][1],country2=places[homes[b]][1])
                break
    if query=='bi20':
        studies={p[0]:set() for p in people}
        for person,org,_ in graph['study']: studies[person].add(org)
        employers={p[0]:[] for p in people}
        orgs={r[0]:r for r in graph['org']}
        for person,org,_ in sorted(graph['work']): employers[person].append(orgs[org][1])
        for a,b,_ in knows:
            if studies[a]&studies[b] and (employers[a] or employers[b]):
                employee,start=(a,b) if employers[a] else (b,a)
                result.update(p2=start,company=employers[employee][0])
                break
    return result


def json_value(value):
    if isinstance(value,float):
        bits=struct.unpack('>Q',struct.pack('>d',value))[0]
        ordered=(~bits & ((1<<64)-1)) if bits>>63 else bits^(1<<63)
        payload=ordered^(1<<63)
        return dict(tag=0,payload=payload if payload < 1<<63 else payload-(1<<64))
    if isinstance(value,tuple): return [json_value(v) for v in value]
    if isinstance(value,bool): return int(value)
    return value


def reference(plan, schemas, data):
    encoded={name:[tuple(tuple(v.encode()) if kind=='bytes' else v for v,kind in zip(row,schema.values())) for row in data[name]] for name,schema in schemas}
    rows=evaluate(plan,encoded)
    values=[n for n in plan.fields if n not in ('rid','rank')]
    return sorted([[[r['rid'],r['rank']],[json_value(r[n]) for n in values],1] for r in rows], key=lambda row: row[0])


def requests(name, schema, params, rid):
    """One binding, or BI12's language-set relation, with explicit identity."""
    params = dict(params, rid=rid)
    if 'endmonth' in schema:
        end = datetime.fromtimestamp(params['end']/1000, timezone.utc)
        params['endmonth'] = end.year*12 + end.month
    if name == 'bi12' and params['language'] == []:
        params.update(language='', enabled=0)
    choices = []
    for field, kind in schema.items():
        value = params[field]
        values = value if isinstance(value, list) else [value]
        if isinstance(value, list) and (name, field) != ('bi12', 'language'):
            raise ValueError('only BI12 language takes a set of values')
        if any(type(v) is not (int if kind == 'int' else str) for v in values):
            raise ValueError(f'{name}: wrong type for {field}')
        choices.append(values)
    return set(itertools.product(*choices))


def alternate(graph, params):
    """Vary actual bindings, not just request IDs; not an official parameter mix."""
    result = dict(params)
    for field, relation, index in (('pid', 'person', 0), ('p1', 'person', 0),
                                   ('p2', 'person', 0), ('mid', 'message', 0),
                                   ('tagname', 'tag', 1), ('firstname', 'person', 1)):
        choices = sorted({row[index] for row in graph[relation]})
        value = params[field]
        result[field] = next((v for v in choices if v > value), choices[0])
    result['maxdate'] -= 86400000
    return result
