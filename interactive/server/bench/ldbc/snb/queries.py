"""Current SNB read queries, expressed using named fields and relational steps.

Specification: ldbc_snb_docs b2269610f433da72e7c97041f01680aae369a903.
Interactive 14 uses v2 (one cheapest interaction path), not v1's all-shortest
paths. The definitions compile to ordinary DDP; there are no Python query
callbacks in measured engine execution.
"""
from functools import cached_property

from .data import SCHEMA
from .rel import R, c, col, expr, choose, absolute, call, empty

DAY = 86400000
QUERIES = {}


def query(name, params, title):
    def register(fn):
        QUERIES[name] = (fn,dict(field.split(':') for field in ('rid:int '+params).split()),title)
        return fn
    return register


def used(e):
    e = expr(e)
    if e.op == 'column': return {e.args[0]}
    return set().union(*(used(a) for a in e.args if hasattr(a,'op')))


def finish(rows, outputs, order, limit=None):
    needed = {'rid'} | set().union(*(used(e) for e in (*outputs.values(),*order)))
    rows = rows.select(*(n for n in rows.fields if n in needed))
    return rows.rank(order,limit).select('rid','rank',**outputs)


def body(content, image): return choose(call('len',content)>0,content,image)
def sumof(e): return ('sum',expr(e))
def count(): return ('count',expr(1))


class Context:
    def __init__(self):
        self.tables = {name:R.source(name,schema) for name,schema in SCHEMA.items()}
    def __getattr__(self, name): return self.tables[name]

    @cached_property
    def edges(self):
        k = self.knows
        return k.union(k.select(src=c.dst,dst=c.src,created=c.created)).distinct().semi(self.person,{'src':'id'}).semi(self.person,{'dst':'id'})

    @cached_property
    def residents(self):
        return self.person.join(self.place,{'city':'id'},'city_').join(self.place,{'city_parent':'id'},'country_')

    @cached_property
    def threads(self):
        roots = self.message.where(c.kind==0).select(mid=c.id,root=c.id,forum=c.forum,author=c.creator,language=c.language)
        links = self.message.where(c.kind==1).select('parent',child=c.id)
        return roots.closure(links,{'mid':'parent'},
            dict(mid=c.next_child,root=c.root,forum=c.forum,author=c.author,language=c.language),
            keys=('mid',),best=('root','forum','author','language'))

    @cached_property
    def ancestry(self):
        roots = self.tagclass.select(descendant=c.id,ancestor=c.id)
        links = self.tagclass.where(c.parent>=0).select('id','parent')
        return roots.closure(links,{'ancestor':'id'},dict(descendant=c.descendant,ancestor=c.next_parent),
                             keys=('descendant','ancestor'),best=())

    @cached_property
    def tagged(self):
        return self.mtag.join(self.tag,{'tag':'id'},'tag_')

    @cached_property
    def replies(self):
        return self.message.where(c.kind==1).join(self.message,{'parent':'id'},'parent_')

    @cached_property
    def interactions(self):
        a = self.replies.select(comment=c.id,src=c.creator,dst=c.parent_creator,score2=choose(c.parent_kind==0,2,1)).where(c.src!=c.dst)
        return a.union(a.select('comment',src=c.dst,dst=c.src,score2=c.score2))

    @cached_property
    def weighted(self):
        pairs = self.interactions.group(('src','dst'),n=count()).semi(self.edges,{'src':'src','dst':'dst'})
        # Exact integer thresholds for max(round(40-sqrt(n)),1), no float
        # approximation and no lookup precomputation outside the dataflow.
        weight = 1+sum(choose(c.n < k*(k-1)+1,1,0) for k in range(1,40))
        return pairs.select('src','dst',weight=weight)

    def friends(self, q, hops):
        q = q.semi(self.person,{'pid':'id'})
        frontier = q.select(*q.fields,node=c.pid,distance=0)
        result = None
        for _ in range(hops):
            frontier = frontier.join(self.edges,{'node':'src'},'edge_').select(*q.fields,node=c.edge_dst,distance=c.distance+1)
            result = frontier if result is None else result.union(frontier)
        return result.where(c.node!=c.pid).group(('rid','node'),distance=('min',c.distance)).join(q,{'rid':'rid'})

    def shortest(self, q, edges, start='p1', path=False, floating=False):
        # Hop-indexed Bellman-Ford. Bounding by |V|-1 avoids count-to-infinity
        # when deletions disconnect a cyclic component from the source.
        bound = self.person.group((),bound=count())
        seed = q.join(bound).select('rid',node=col(start),hops=0,cost=call('float',0) if floating else 0,bound=c.bound-1,
                                   **({'path':call('list',col(start))} if path else {}))
        projection = dict(rid=c.rid,node=c.next_dst,hops=c.hops+1,cost=call('fadd',c.cost,c.next_weight) if floating else c.cost+c.next_weight,bound=c.bound)
        if path: projection['path'] = call('append',c.path,call('list',c.next_dst))
        reached = seed.closure(edges,{'node':'src',**({'rid':'rid'} if 'rid' in edges.fields else {})},projection,
            keys=('rid','node','hops'),best=('cost','bound',*(['path'] if path else [])),condition=c.hops<c.bound)
        if path:
            return reached.group(('rid','node'),best=('min',call('tuple',c.cost,c.path))).select('rid','node',cost=c.best.at(0),path=c.best.at(1))
        return reached.group(('rid','node'),cost=('min',c.cost))


@query('is1','pid:int','Profile of a person')
def is1(x,q):
    r=q.join(x.person,{'pid':'id'})
    return finish(r,{n:col(n) for n in ('first','last','birthday','ip','browser','city','gender','created')},[c.id])


@query('is2','pid:int','Recent messages of a person')
def is2(x,q):
    r=q.join(x.message,{'pid':'creator'}).join(x.threads,{'id':'mid'},'thread_').join(x.person,{'thread_author':'id'},'author_')
    return finish(r,dict(message=c.id,content=body(c.content,c.image),created=c.created,post=c.thread_root,
                        author=c.thread_author,first=c.author_first,last=c.author_last),[-c.created,-c.id],10)


@query('is3','pid:int','Friends of a person')
def is3(x,q):
    r=q.join(x.edges,{'pid':'src'}).join(x.person,{'dst':'id'},'p_')
    return finish(r,dict(friend=c.dst,first=c.p_first,last=c.p_last,created=c.created),[-c.created,c.dst])


@query('is4','mid:int','Content of a message')
def is4(x,q):
    r=q.join(x.message,{'mid':'id'})
    return finish(r,dict(created=c.created,content=body(c.content,c.image)),[c.id])


@query('is5','mid:int','Creator of a message')
def is5(x,q):
    r=q.join(x.message,{'mid':'id'}).join(x.person,{'creator':'id'},'p_')
    return finish(r,dict(person=c.creator,first=c.p_first,last=c.p_last),[c.creator])


@query('is6','mid:int','Forum of a message')
def is6(x,q):
    r=q.join(x.threads,{'mid':'mid'}).join(x.forum,{'forum':'id'},'f_').join(x.person,{'f_moderator':'id'},'p_')
    return finish(r,dict(forum=c.forum,title=c.f_title,moderator=c.f_moderator,first=c.p_first,last=c.p_last),[c.forum])


@query('is7','mid:int','Replies of a message')
def is7(x,q):
    r=q.join(x.replies,{'mid':'parent'}).join(x.person,{'creator':'id'},'p_')
    flags=x.edges.select('src','dst',known=1)
    r=r.left(flags,{'creator':'src','parent_creator':'dst'},dict(src=c.creator,dst=c.parent_creator,known=0))
    return finish(r,dict(comment=c.id,content=c.content,created=c.created,author=c.creator,first=c.p_first,last=c.p_last,
                        knows=choose(c.creator!=c.parent_creator,c.known,0)),[-c.created,c.creator])


@query('ic1','pid:int firstname:bytes','Transitive friends with a certain name')
def ic1(x,q):
    r=x.friends(q,3).join(x.residents,{'node':'id'},'p_').where(c.p_first==c.firstname)
    for relation,field,out in ((x.email,'value','emails'),(x.language,'value','languages')):
        grouped=relation.group(('person',),**{out:('collect',col(field))})
        r=r.left(grouped,{'node':'person'},{'person':c.node,out:empty('strings')}).select(*(n for n in r.fields),**{out:col(out)})
    for relation,out in ((x.study,'universities'),(x.work,'companies')):
        entries=relation.join(x.org,{'org':'id'},'o_').join(x.place,{'o_place':'id'},'place_')
        grouped=entries.group(('person',),**{out:('collect',call('tuple',c.o_name,c.year,c.place_name))})
        oldfields=r.fields
        r=r.left(grouped,{'node':'person'},{'person':c.node,out:empty('triples')}).select(*oldfields,**{out:col(out)})
    return finish(r,dict(person=c.node,last=c.p_last,distance=c.distance,birthday=c.p_birthday,created=c.p_created,
                        gender=c.p_gender,browser=c.p_browser,ip=c.p_ip,emails=c.emails,languages=c.languages,
                        city=c.p_city_name,universities=c.universities,companies=c.companies),[c.distance,c.p_last,c.node],20)


def recent(x,q,hops):
    r=x.friends(q,hops).join(x.message,{'node':'creator'},'m_').where(c.m_created<c.maxdate).join(x.person,{'node':'id'},'p_')
    return finish(r,dict(friend=c.node,first=c.p_first,last=c.p_last,message=c.m_id,
                        content=body(c.m_content,c.m_image),created=c.m_created),[-c.m_created,c.m_id],20)


@query('ic2','pid:int maxdate:int','Recent messages by friends')
def ic2(x,q): return recent(x,q,1)


@query('ic3','pid:int countryx:bytes countryy:bytes start:int days:int','Friends visiting given countries')
def ic3(x,q):
    r=x.friends(q,2).join(x.residents,{'node':'id'},'p_').where((c.p_country_name!=c.countryx)&(c.p_country_name!=c.countryy))
    r=r.join(x.message,{'node':'creator'},'m_').join(x.place,{'m_country':'id'},'loc_')
    r=r.where((c.m_created>=c.start)&(c.m_created<c.start+c.days*DAY))
    r=r.group(('rid','node','p_first','p_last'),nx=sumof(choose(c.loc_name==c.countryx,1,0)),ny=sumof(choose(c.loc_name==c.countryy,1,0)))
    r=r.where((c.nx>0)&(c.ny>0))
    return finish(r,dict(person=c.node,first=c.p_first,last=c.p_last,xCount=c.nx,yCount=c.ny,total=c.nx+c.ny),[-(c.nx+c.ny),c.node],20)


@query('ic4','pid:int start:int days:int','New topics')
def ic4(x,q):
    r=x.friends(q,1).join(x.message.where(c.kind==0),{'node':'creator'},'m_').join(x.tagged,{'m_id':'message'},'t_')
    old=r.where(c.m_created<c.start).select('rid','t_tag').distinct()
    current=r.where((c.m_created>=c.start)&(c.m_created<c.start+c.days*DAY)).anti(old,{'rid':'rid','t_tag':'t_tag'})
    r=current.group(('rid','t_tag_name'),n=count())
    return finish(r,dict(tag=c.t_tag_name,count=c.n),[-c.n,c.t_tag_name],10)


@query('ic5','pid:int mindate:int','New groups')
def ic5(x,q):
    members=x.friends(q,2).join(x.member,{'node':'person'}).where(c.created>c.mindate).select('rid','node','forum').distinct()
    posts=members.join(x.message.where(c.kind==0),{'node':'creator','forum':'forum'},'m_').group(('rid','forum'),n=count())
    r=members.select('rid','forum').distinct().left(posts,{'rid':'rid','forum':'forum'},dict(n=0)).join(x.forum,{'forum':'id'},'f_')
    return finish(r,dict(title=c.f_title,count=c.n),[-c.n,c.forum],20)


@query('ic6','pid:int tagname:bytes','Tag co-occurrence')
def ic6(x,q):
    r=x.friends(q,2).join(x.message.where(c.kind==0),{'node':'creator'},'m_').join(x.tagged,{'m_id':'message'},'t_').where(c.t_tag_name==c.tagname)
    r=r.select('rid','m_id','tagname').distinct().join(x.tagged,{'m_id':'message'},'other_').where(c.other_tag_name!=c.tagname)
    r=r.group(('rid','other_tag_name'),n=count())
    return finish(r,dict(tag=c.other_tag_name,count=c.n),[-c.n,c.other_tag_name],10)


@query('ic7','pid:int','Recent likers')
def ic7(x,q):
    r=q.join(x.message,{'pid':'creator'},'m_').join(x.likes,{'m_id':'message'},'l_')
    r=r.group(('rid','pid','l_person'),best=('min',call('tuple',-c.l_created,c.m_id)))
    r=r.select('rid','pid','l_person',when=-c.best.at(0),mid=c.best.at(1)).join(x.message,{'mid':'id'},'m_').join(x.person,{'l_person':'id'},'p_')
    flags=x.edges.select('src','dst',known=1)
    r=r.left(flags,{'pid':'src','l_person':'dst'},dict(src=c.pid,dst=c.l_person,known=0))
    return finish(r,dict(person=c.l_person,first=c.p_first,last=c.p_last,created=c.when,message=c.mid,
                        content=body(c.m_content,c.m_image),latency=call('idiv',c.when-c.m_created,60000),isNew=1-c.known),[-c.when,c.l_person],20)


@query('ic8','pid:int','Recent replies')
def ic8(x,q):
    r=q.join(x.replies,{'pid':'parent_creator'}).join(x.person,{'creator':'id'},'p_')
    return finish(r,dict(author=c.creator,first=c.p_first,last=c.p_last,created=c.created,comment=c.id,content=c.content),[-c.created,c.id],20)


@query('ic9','pid:int maxdate:int','Recent messages by friends or friends of friends')
def ic9(x,q): return recent(x,q,2)


@query('ic10','pid:int month:int','Friend recommendation')
def ic10(x,q):
    candidates=x.friends(q,2).where(c.distance==2).join(x.residents,{'node':'id'},'p_')
    nextmonth=choose(c.month==12,1,c.month+1)
    candidates=candidates.where(((c.p_birthmonth==c.month)&(c.p_birthdaynum>=21))|((c.p_birthmonth==nextmonth)&(c.p_birthdaynum<22)))
    posts=candidates.select('rid','pid','node').join(x.message.where(c.kind==0),{'node':'creator'},'m_')
    common=posts.join(x.mtag,{'m_id':'message'},'t_').semi(x.interest,{'pid':'person','t_tag':'tag'}).select('rid','node','m_id').distinct()
    positive=common.select('rid','node',score=1)
    negative=posts.anti(common,{'rid':'rid','node':'node','m_id':'m_id'}).select('rid','node',score=-1)
    scores=positive.union(negative).group(('rid','node'),score=sumof(c.score))
    r=candidates.left(scores,{'rid':'rid','node':'node'},dict(score=0))
    return finish(r,dict(person=c.node,first=c.p_first,last=c.p_last,score=c.score,gender=c.p_gender,city=c.p_city_name),[-c.score,c.node],10)


@query('ic11','pid:int countryname:bytes workyear:int','Job referral')
def ic11(x,q):
    r=x.friends(q,2).join(x.work,{'node':'person'}).where(c.year<c.workyear).join(x.org,{'org':'id'},'o_').join(x.place,{'o_place':'id'},'place_').where(c.place_name==c.countryname).join(x.person,{'node':'id'},'p_')
    r=r.rank([c.o_name],groups=('rid','year','node')).rename(rank='nameorder')
    return finish(r,dict(person=c.node,first=c.p_first,last=c.p_last,company=c.o_name,year=c.year),[c.year,c.node,-c.nameorder],10)


@query('ic12','pid:int classname:bytes','Expert search')
def ic12(x,q):
    tags=x.tagged.join(x.ancestry,{'tag_class':'descendant'}).join(x.tagclass,{'ancestor':'id'},'class_')
    r=x.friends(q,1).join(x.replies.where(c.parent_kind==0),{'node':'creator'},'reply_').join(tags,{'reply_parent':'message'},'t_').where(c.t_class_name==c.classname)
    counts=r.select('rid','node','reply_id').distinct().group(('rid','node'),n=count())
    tags=r.select('rid','node','t_tag_name').distinct().group(('rid','node'),tags=('collect',c.t_tag_name))
    r=counts.join(tags,{'rid':'rid','node':'node'}).join(x.person,{'node':'id'},'p_')
    return finish(r,dict(person=c.node,first=c.p_first,last=c.p_last,tags=c.tags,count=c.n),[-c.n,c.node],20)


@query('ic13','p1:int p2:int','Single shortest path')
def ic13(x,q):
    paths=x.shortest(q,x.edges.select('src','dst',weight=1)).join(q,{'rid':'rid'}).where(c.node==c.p2).select('rid','cost')
    r=q.left(paths,{'rid':'rid'},dict(cost=-1))
    return finish(r,dict(distance=c.cost),[c.rid])


@query('ic14','p1:int p2:int','Cheapest interaction path (v2)')
def ic14(x,q):
    r=x.shortest(q,x.weighted,path=True).join(q,{'rid':'rid'}).where(c.node==c.p2)
    return finish(r,dict(path=c.path,weight=c.cost),[c.rid])


def tagged_messages(x,q):
    return q.join(x.tagged,{'tagname':'tag_name'}).join(x.message,{'message':'id'},'m_')


@query('bi2','date:int classname:bytes','Tag evolution')
def bi2(x,q):
    r=q.join(x.tagclass,{'classname':'name'},'class_').join(x.tagged,{'class_id':'tag_class'},'t_').join(x.message,{'t_message':'id'},'m_')
    r=r.where((c.m_created>=c.date)&(c.m_created<c.date+200*DAY))
    r=r.group(('rid','t_tag_name'),n1=sumof(choose(c.m_created<c.date+100*DAY,1,0)),n2=sumof(choose(c.m_created>=c.date+100*DAY,1,0)))
    return finish(r,dict(tag=c.t_tag_name,window1=c.n1,window2=c.n2,diff=absolute(c.n1-c.n2)),[-absolute(c.n1-c.n2),c.t_tag_name],100)


@query('bi3','classname:bytes countryname:bytes','Popular topics in a country')
def bi3(x,q):
    r=q.join(x.tagclass,{'classname':'name'},'class_').join(x.tagged,{'class_id':'tag_class'},'t_').select('rid','countryname','t_message').distinct()
    r=r.join(x.threads,{'t_message':'mid'}).join(x.forum,{'forum':'id'},'f_').join(x.residents,{'f_moderator':'id'},'p_').where(c.p_country_name==c.countryname)
    r=r.group(('rid','forum','f_title','f_created','f_moderator'),n=count())
    return finish(r,dict(forum=c.forum,title=c.f_title,created=c.f_created,moderator=c.f_moderator,count=c.n),[-c.n,c.forum],20)


@query('bi4','date:int','Top message creators by country')
def bi4(x,q):
    forums=q.join(x.forum).where(c.created>c.date).join(x.member,{'id':'forum'},'member_').join(x.residents,{'member_person':'id'},'p_')
    popularity=forums.group(('rid','id','p_country_id'),n=count()).group(('rid','id'),negative=('min',-c.n))
    top=popularity.rank([c.negative,c.id],100).select('rid',forum=c.id)
    members=top.join(x.member,{'forum':'forum'}).select('rid','person').distinct()
    messages=top.join(x.threads,{'forum':'forum'}).join(x.message,{'mid':'id'},'m_')
    counts=messages.group(('rid','m_creator'),n=count()).rename(m_creator='person')
    r=members.left(counts,{'rid':'rid','person':'person'},dict(n=0)).join(x.person,{'person':'id'},'p_')
    return finish(r,dict(person=c.person,first=c.p_first,last=c.p_last,created=c.p_created,count=c.n),[-c.n,c.person],100)


@query('bi5','tagname:bytes','Most active posters of a topic')
def bi5(x,q):
    messages=tagged_messages(x,q).select('rid','m_id','m_creator').distinct()
    counts=messages.group(('rid','m_creator'),messages=count())
    likes=messages.join(x.likes,{'m_id':'message'}).group(('rid','m_creator'),likes=count())
    replies=messages.join(x.message.where(c.kind==1),{'m_id':'parent'},'r_').group(('rid','m_creator'),replies=count())
    r=counts.left(likes,{'rid':'rid','m_creator':'m_creator'},dict(likes=0)).left(replies,{'rid':'rid','m_creator':'m_creator'},dict(replies=0))
    score=c.messages+2*c.replies+10*c.likes
    return finish(r,dict(person=c.m_creator,replies=c.replies,likes=c.likes,messages=c.messages,score=score),[-score,c.m_creator],100)


@query('bi6','tagname:bytes','Most authoritative users on a topic')
def bi6(x,q):
    messages=tagged_messages(x,q).select('rid','m_id','m_creator').distinct()
    authors=messages.select('rid','m_creator').distinct()
    pairs=messages.join(x.likes,{'m_id':'message'}).select('rid','m_creator','person').distinct()
    popularity=x.message.join(x.likes,{'id':'message'},'l_').group(('creator',),popularity=count())
    scores=pairs.join(popularity,{'person':'creator'}).group(('rid','m_creator'),score=sumof(c.popularity))
    r=authors.left(scores,{'rid':'rid','m_creator':'m_creator'},dict(score=0))
    return finish(r,dict(person=c.m_creator,score=c.score),[-c.score,c.m_creator],100)


@query('bi7','tagname:bytes','Related topics')
def bi7(x,q):
    messages=tagged_messages(x,q).select('rid','m_id').distinct()
    replies=messages.join(x.message.where(c.kind==1),{'m_id':'parent'},'r_').anti(messages,{'rid':'rid','r_id':'m_id'})
    r=replies.join(x.tagged,{'r_id':'message'},'t_').group(('rid','t_tag_name'),n=count())
    return finish(r,dict(tag=c.t_tag_name,count=c.n),[-c.n,c.t_tag_name],100)


@query('bi8','tagname:bytes start:int end:int','Central person for a tag')
def bi8(x,q):
    interests=q.join(x.tag,{'tagname':'name'},'t_').join(x.interest,{'t_id':'tag'}).select('rid','person',score=100)
    messages=tagged_messages(x,q).where((c.m_created>c.start)&(c.m_created<c.end)).select('rid',person=c.m_creator,score=1)
    scores=interests.union(messages).group(('rid','person'),score=sumof(c.score))
    friends=scores.join(x.edges,{'person':'src'}).join(scores,{'rid':'rid','dst':'person'},'f_').group(('rid','person'),friends=sumof(c.f_score))
    r=scores.left(friends,{'rid':'rid','person':'person'},dict(friends=0))
    return finish(r,dict(person=c.person,score=c.score,friends=c.friends),[-(c.score+c.friends),c.person],100)


@query('bi9','start:int end:int','Top thread initiators')
def bi9(x,q):
    posts=q.join(x.message.where(c.kind==0)).where((c.created>=c.start)&(c.created<=c.end))
    threads=posts.select('rid','creator','id').group(('rid','creator'),threads=count())
    messages=posts.join(x.threads,{'id':'root'},'t_').join(x.message,{'t_mid':'id'},'m_').where((c.m_created>=c.start)&(c.m_created<=c.end))
    counts=messages.group(('rid','creator'),messages=count())
    r=threads.join(counts,{'rid':'rid','creator':'creator'}).join(x.person,{'creator':'id'},'p_')
    return finish(r,dict(person=c.creator,first=c.p_first,last=c.p_last,threads=c.threads,messages=c.messages),[-c.messages,c.creator],100)


@query('bi10','pid:int countryname:bytes classname:bytes minimum:int maximum:int','Experts in social circle')
def bi10(x,q):
    # The parameter maximum is not a compile-time unrolling bound.
    paths=x.shortest(q,x.edges.select('src','dst',weight=1),start='pid').join(q,{'rid':'rid'})
    paths=paths.where((c.cost>=c.minimum)&(c.cost<=c.maximum)&(c.node!=c.pid)).join(x.residents,{'node':'id'},'p_').where(c.p_country_name==c.countryname)
    qualified=paths.join(x.message,{'node':'creator'},'m_').join(x.tagged,{'m_id':'message'},'t_').join(x.tagclass,{'t_tag_class':'id'},'class_').where(c.class_name==c.classname).select('rid','node','m_id').distinct()
    r=qualified.join(x.tagged,{'m_id':'message'},'tag_').group(('rid','node','tag_tag_name'),n=count())
    return finish(r,dict(person=c.node,tag=c.tag_tag_name,count=c.n),[-c.n,c.tag_tag_name,c.node],100)


@query('bi11','countryname:bytes start:int end:int','Friend triangles')
def bi11(x,q):
    residents=q.join(x.residents,{'countryname':'country_name'}).select('rid','start','end',person=c.id)
    edges=residents.join(x.edges,{'person':'src'}).where((c.created>=c.start)&(c.created<=c.end)).semi(residents,{'rid':'rid','dst':'person'}).select('rid','src','dst')
    triangles=edges.join(edges,{'rid':'rid','dst':'src'},'e_').where((c.src<c.dst)&(c.dst<c.e_dst)).semi(edges,{'rid':'rid','e_dst':'src','src':'dst'})
    counts=triangles.group(('rid',),n=count())
    r=q.left(counts,{'rid':'rid'},dict(n=0))
    return finish(r,dict(count=c.n),[c.rid])


@query('bi12','start:int length:int language:bytes enabled:int','Distribution of message counts')
def bi12(x,q):
    # Request length is a threshold; a request relation can contain multiple
    # languages with the same rid to represent the specified language set.
    # A disabled row retains request identity for an empty language set.
    request=q.rename(length='threshold',language='wantedlanguage')
    selected=request.where(c.enabled!=0).join(x.message).where((call('len',c.content)>0)&(c.created>c.start)&(c.length<c.threshold))
    selected=selected.join(x.threads,{'id':'mid'},'t_').where(c.t_language==c.wantedlanguage).select('rid','creator','id').distinct()
    counts=selected.group(('rid','creator'),messages=count()).rename(creator='person')
    persons=q.select('rid').distinct().join(x.person.select(person=c.id))
    r=persons.left(counts,{'rid':'rid','person':'person'},dict(messages=0)).group(('rid','messages'),people=count())
    return finish(r,dict(messages=c.messages,people=c.people),[-c.people,-c.messages])


@query('bi14','country1:bytes country2:bytes','International dialog')
def bi14(x,q):
    pairs=q.join(x.residents,{'country1':'country_name'},'p1_').join(x.edges,{'p1_id':'src'}).join(x.residents,{'dst':'id'},'p2_').where(c.p2_country_name==c.country2)
    pairs=pairs.select('rid',p1=c.p1_id,p2=c.p2_id,city=c.p1_city,cityname=c.p1_city_name)
    replies=x.replies.select(src=c.creator,dst=c.parent_creator).distinct()
    likes=x.likes.join(x.message,{'message':'id'},'m_').select(src=c.person,dst=c.m_creator).distinct()
    scores=pairs.select('rid','p1','p2',score=0)
    for relation,on,points in ((replies,{'p1':'src','p2':'dst'},4),(replies,{'p2':'src','p1':'dst'},1),
                               (likes,{'p1':'src','p2':'dst'},10),(likes,{'p2':'src','p1':'dst'},1)):
        scores=scores.union(pairs.semi(relation,on).select('rid','p1','p2',score=points))
    scores=scores.group(('rid','p1','p2'),score=sumof(c.score))
    r=pairs.join(scores,{'rid':'rid','p1':'p1','p2':'p2'}).rank([-c.score,c.p1,c.p2],1,groups=('rid','city')).rename(rank='cityrank')
    return finish(r,dict(person1=c.p1,person2=c.p2,city=c.cityname,score=c.score),[-c.score,c.p1,c.p2],100)


@query('bi16','taga:bytes datea:int tagb:bytes dateb:int maxknows:int','Fake news detection')
def bi16(x,q):
    def subgraph(tag,date):
        r=q.join(x.tagged,{tag:'tag_name'},'t_').join(x.message,{'t_message':'id'},'m_').where(c.m_day==col(date)).select('rid','maxknows',person=c.m_creator,mid=c.m_id).distinct()
        counts=r.group(('rid','person','maxknows'),messages=count())
        people=r.select('rid','person').distinct()
        degrees=people.join(x.edges,{'person':'src'}).semi(people,{'rid':'rid','dst':'person'}).group(('rid','person'),degree=count())
        return counts.left(degrees,{'rid':'rid','person':'person'},dict(degree=0)).where(c.degree<=c.maxknows).select('rid','person','messages')
    a,b=subgraph('taga','datea'),subgraph('tagb','dateb')
    r=a.join(b,{'rid':'rid','person':'person'},'b_')
    return finish(r,dict(person=c.person,countA=c.messages,countB=c.b_messages),[-(c.messages+c.b_messages),c.person],20)


@query('bi17','tagname:bytes delta:int','Information propagation analysis')
def bi17(x,q):
    messages=tagged_messages(x,q).select('rid','delta',mid=c.m_id,creator=c.m_creator,created=c.m_created,parent=c.m_parent).distinct().join(x.threads,{'mid':'mid'}).select('rid','delta','mid','creator','created','parent','forum')
    # Build the later discussion first; count distinct message2 per initiator,
    # not all combinations of messages/memberships that witness propagation.
    discussions=messages.join(messages,{'rid':'rid','mid':'parent'},'reply_').where(c.creator!=c.reply_creator)
    r=messages.join(discussions,{'rid':'rid'},'d_').where((c.forum!=c.d_forum)&(c.created+c.delta*3600000<c.d_created))
    r=r.semi(x.member,{'forum':'forum','d_creator':'person'}).semi(x.member,{'forum':'forum','d_reply_creator':'person'}).anti(x.member,{'d_forum':'forum','creator':'person'})
    r=r.select('rid','creator','d_mid').distinct().group(('rid','creator'),n=count())
    return finish(r,dict(person=c.creator,count=c.n),[-c.n,c.creator],10)


@query('bi18','tagname:bytes','Friend recommendation')
def bi18(x,q):
    people=q.join(x.tag,{'tagname':'name'},'tag_').join(x.interest,{'tag_id':'tag'}).select('rid','person').distinct()
    r=people.join(x.edges,{'person':'src'},'e_').join(x.edges,{'e_dst':'src'},'f_').where(c.person!=c.f_dst).semi(people,{'rid':'rid','f_dst':'person'}).anti(x.edges,{'person':'src','f_dst':'dst'})
    r=r.group(('rid','person','f_dst'),n=count())
    return finish(r,dict(person1=c.person,person2=c.f_dst,count=c.n),[-c.n,c.person,c.f_dst],20)


@query('bi19','city1:int city2:int','Interaction path between cities')
def bi19(x,q):
    # One start per person, carried in the internal request key. Expand request
    # identity to (original rid, source) through a deterministic id relation.
    starts=q.join(x.person,{'city1':'city'},'p_').select('rid','city2',source=c.p_id)
    indexed=starts.rank([c.source],groups=('rid',)).rename(rank='startindex')
    # Source IDs are globally unique. Use them as internal request IDs; preserve
    # the outer request in a separate field by running paths per (rid,source).
    # The composite key is encoded as a tuple, not a collision-prone hash.
    seeds=indexed.select(rid=call('tuple',c.rid,c.source),p1=c.source)
    paths=x.shortest(seeds,x.weighted).select(internal=c.rid,node=c.node,cost=c.cost)
    r=paths.select(rid=c.internal.at(0),source=c.internal.at(1),node=c.node,cost=c.cost).join(q,{'rid':'rid'}).join(x.person,{'node':'id'},'target_').where(c.target_city==c.city2)
    best=r.group(('rid',),best=('min',c.cost))
    r=r.join(best,{'rid':'rid'}).where(c.cost==c.best)
    return finish(r,dict(person1=c.source,person2=c.node,weight=c.cost),[c.source,c.node])


@query('bi20','company:bytes p2:int','Recruitment')
def bi20(x,q):
    same=x.study.join(x.study,{'org':'org'},'b_').where(c.person!=c.b_person).semi(x.edges,{'person':'src','b_person':'dst'})
    edges=same.select(src=c.person,dst=c.b_person,weight=absolute(c.year-c.b_year)+1).group(('src','dst'),weight=('min',c.weight))
    employees=q.join(x.org,{'company':'name'},'o_').join(x.work,{'o_id':'org'}).select('rid','person').distinct()
    valid=q.anti(employees,{'rid':'rid','p2':'person'})
    paths=x.shortest(valid,edges,start='p2').semi(employees,{'rid':'rid','node':'person'}).join(q,{'rid':'rid'}).where(c.node!=c.p2)
    best=paths.group(('rid',),best=('min',c.cost))
    r=paths.join(best,{'rid':'rid'}).where(c.cost==c.best)
    return finish(r,dict(person=c.node,weight=c.cost),[c.cost,c.node],20)


@query('bi1','end:int','Posting summary')
def bi1(x,q):
    selected=q.join(x.message).where(c.created<c.end)
    totals=selected.group(('rid',),total=count())
    category=choose(c.length<40,0,choose(c.length<80,1,choose(c.length<160,2,3)))
    groups=selected.select('rid','year','kind','length',category=category).group(('rid','year','kind','category'),n=count(),lengths=sumof(c.length))
    r=groups.join(totals,{'rid':'rid'})
    return finish(r,dict(year=c.year,isComment=c.kind,category=c.category,count=c.n,
        average=call('fdiv',call('float',c.lengths),call('float',c.n)),sumLength=c.lengths,
        percentage=call('fdiv',call('float',100*c.n),call('float',c.total))),[-c.year,c.kind,c.category])


@query('bi13','countryname:bytes end:int endmonth:int','Zombies in a country')
def bi13(x,q):
    candidates=q.join(x.residents,{'countryname':'country_name'}).where(c.created<c.end)
    counts=candidates.join(x.message,{'id':'creator'},'m_').where((c.m_created>=c.created)&(c.m_created<=c.end)).group(('rid','id'),n=count())
    zombies=candidates.left(counts,{'rid':'rid','id':'id'},dict(n=0)).where(c.n<c.endmonth-c.creationmonth+1)
    liked=zombies.join(x.message,{'id':'creator'},'m_').join(x.likes,{'m_id':'message'},'l_').join(x.person,{'l_person':'id'},'liker_').where(c.liker_created<c.end)
    liked=liked.select('rid',zombie=c.id,liker=c.l_person)
    flags=zombies.select('rid',liker=c.id,isZombie=1)
    liked=liked.left(flags,{'rid':'rid','liker':'liker'},dict(isZombie=0))
    counts=liked.group(('rid','zombie'),total=count(),zombies=sumof(c.isZombie))
    r=zombies.select('rid',zombie=c.id).left(counts,{'rid':'rid','zombie':'zombie'},dict(total=0,zombies=0))
    r=r.select(*r.fields,score=choose(c.total>0,call('fdiv',call('float',c.zombies),call('float',c.total)),call('float',0)))
    return finish(r,dict(person=c.zombie,zombieLikes=c.zombies,totalLikes=c.total,score=c.score),[call('fneg',c.score),c.zombie],100)


@query('bi15','p1:int p2:int start:int end:int','Trusted connection paths through forums')
def bi15(x,q):
    interactions=q.join(x.interactions).join(x.threads,{'comment':'mid'},'t_').join(x.forum,{'t_forum':'id'},'f_')
    interactions=interactions.where((c.f_created>=c.start)&(c.f_created<=c.end)).group(('rid','src','dst'),score2=sumof(c.score2))
    pairs=q.select('rid').join(x.edges.select('src','dst')).left(interactions,{'rid':'rid','src':'src','dst':'dst'},dict(score2=0))
    edges=pairs.select('rid','src','dst',weight=call('fdiv',call('float',2),call('float',c.score2+2)))
    paths=x.shortest(q,edges,floating=True).join(q,{'rid':'rid'}).where(c.node==c.p2).select('rid','cost')
    r=q.left(paths,{'rid':'rid'},dict(cost=call('float',-1)))
    return finish(r,dict(weight=c.cost),[c.rid])
