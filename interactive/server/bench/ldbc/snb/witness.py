"""Small hand-built graph with non-empty witnesses for every SNB read query.

This complements the generated fixture; it is not a scale-factor dataset.
"""
from .data import SCHEMA
from .data import millis


def graph():
    g={name:set() for name in SCHEMA}
    def add(relation,**row): g[relation].add(tuple(row[n] for n in SCHEMA[relation]))
    t=millis('2012-01-01T00:00:00Z')
    born=millis('1980-01-22T00:00:00Z')
    created=millis('2011-01-01T00:00:00Z')
    for base,name in ((100,'Alpha'),(200,'Beta'),(300,'Gamma')):
        add('place',id=base,name=name,type='Country',parent=-1)
        add('place',id=base+1,name=name+' City',type='City',parent=base)
    cities={1:301,2:101,3:201,4:301,5:101,6:201,7:101,8:201}
    for pid,city in cities.items():
        add('person',id=pid,first='Alex',last='A' if pid==2 else 'Alphabet' if pid==3 else f'Person{pid}',gender='female' if pid%2 else 'male',
            birthday=born,created=created,ip='127.0.0.1',browser='Browser',city=city,birthmonth=1,birthdaynum=22,creationmonth=2011*12+1)
        add('email',person=pid,value=f'{pid}@example.invalid')
        add('language',person=pid,value='en')
        add('interest',person=pid,tag=10)
        add('study',person=pid,org=20,year=2000+pid%3)
    add('org',id=20,name='University',type='University',place=101)
    add('org',id=21,name='Company',type='Company',place=100)
    add('work',person=7,org=21,year=2005)
    add('work',person=2,org=21,year=2008)
    for a,b in ((1,2),(1,3),(2,4),(3,4),(2,5),(5,7),(2,7),(4,5),(3,6),(6,8)):
        add('knows',src=a,dst=b,created=created+1000)
    for fid,moderator in ((1001,1),(1002,2),(1003,3),(1004,4)):
        add('forum',id=fid,title=f'Forum {fid}',created=created,moderator=moderator)
        add('ftag',forum=fid,tag=10)
    for fid,people in ((1001,(2,3,4)),(1002,(2,3,5)),(1003,(1,4,5,6,7,8)),(1004,(4,5))):
        for pid in people: add('member',forum=fid,person=pid,created=created+2000)
    for cid,name,parent in ((500,'Root',-1),(501,'TopicClass',500),(502,'SubTopicClass',501)):
        add('tagclass',id=cid,name=name,parent=parent)
    for tag,name,cls in ((10,'Topic',501),(11,'Other',501),(12,'Z',502),(13,'Alphabet',501)):
        add('tag',id=tag,name=name,**{'class':cls})
    def message(mid,author,when,parent=-1,forum=1001,country=None,tags=(10,11),content='text',language='en',image=''):
        add('message',id=mid,kind=int(parent>=0),created=when,creator=author,country=country or cities[author]-1,
            content=content,image=image,length=len(content),language=language if parent<0 else '',forum=forum if parent<0 else -1,
            parent=parent,year=2012,month=2012*12+1,day=when-when%86400000)
        for tag in tags: add('mtag',message=mid,tag=tag)
    # Explicit propagation witness: 1 posts in F1; 2 posts a day later in F2;
    # 3 replies, both 2/3 belong to F1, and 1 is not a member of F2.
    message(10001,1,t)
    message(10002,2,t+86400000,forum=1002,country=200)
    message(10003,3,t+86401000,parent=10002)
    message(10004,2,t+86402000,parent=10001)
    message(10005,5,t+86403000,parent=10002)
    message(10006,4,t+86404000,parent=10003,tags=(11,))
    message(10007,7,t+86405000,forum=1003)
    message(10008,2,t+86406000,country=300)
    message(10009,4,t+86407000,country=100,forum=1003)
    message(10010,4,t+86408000,country=200,forum=1003)
    message(10011,1,t+86409000,tags=(11,13))
    # Same timestamps, enough candidates to exercise replacement at rank 20;
    # empty text/image, Unicode, and different-length tag strings are present.
    for i in range(24):
        message(10100+i,2 if i%2==0 else 3,t+2*86400000,forum=1002,
                tags=(10,13) if i%2 else (10,11),content='' if i==0 else 'café',image='photo.png' if i==0 else '')
    for person,mid,offset in ((2,10001,1),(4,10001,2),(4,10011,2),(1,10001,3),(5,10007,4),(8,10002,5)):
        add('likes',person=person,message=mid,created=t+3*86400000+offset)
    return g


def params(name):
    base=dict(pid=1,p1=1,p2=5,firstname='Alex',mid=10001,tagname='Topic',classname='TopicClass',
        countryname='Alpha',countryx='Alpha',countryy='Beta',country1='Alpha',country2='Gamma',
        start=millis('2011-01-01T00:00:00Z'),end=millis('2014-01-01T00:00:00Z'),endmonth=2014*12+1,
        date=millis('2011-11-01T00:00:00Z'),mindate=millis('2010-01-01T00:00:00Z'),
        month=1,minimum=1,maximum=4,city1=301,city2=101,company='Company',language='en',
        taga='Topic',tagb='Other',datea=millis('2012-01-03T00:00:00Z'),dateb=millis('2012-01-03T00:00:00Z'))
    if name=='bi4': base['date']=millis('2010-01-01T00:00:00Z')
    if name=='ic12': base['classname']='Root'
    if name=='bi20': base['p2']=1
    return base


def alternate_params(name):
    # Person 4 is beyond two hops from 8; unlike 1 and 2, this requester has
    # no IC3 answer. Exercise parameter sensitivity through the real server.
    return {'pid': 8} if name == 'ic3' else {}


def changed(original):
    """A valid projected update: remove a friendship and a post, rename a person."""
    g={name:set(rows) for name,rows in original.items()}
    g['knows']={row for row in g['knows'] if row[:2]!=(1,2)}
    g['message']={row for row in g['message'] if row[0]!=10100}
    g['mtag']={row for row in g['mtag'] if row[0]!=10100}
    g['person']={tuple('Renamed' if i==1 and row[0]==2 else v for i,v in enumerate(row)) for row in g['person']}
    return g
