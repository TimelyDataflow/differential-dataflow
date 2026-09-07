"""Project the 18 SNB BI composite-merged-fk snapshot tables; no query results."""
from datetime import datetime, timezone

from snapshot import read_table

# Field names are shared with the named relational query definitions.
SCHEMA = {
    'person': 'id:int first:bytes last:bytes gender:bytes birthday:int created:int ip:bytes browser:bytes city:int birthmonth:int birthdaynum:int creationmonth:int',
    'message': 'id:int kind:int created:int creator:int country:int content:bytes image:bytes length:int language:bytes forum:int parent:int year:int month:int day:int',
    'forum': 'id:int title:bytes created:int moderator:int',
    'knows': 'src:int dst:int created:int',
    'member': 'forum:int person:int created:int',
    'tag': 'id:int name:bytes class:int',
    'tagclass': 'id:int name:bytes parent:int',
    'place': 'id:int name:bytes type:bytes parent:int',
    'org': 'id:int name:bytes type:bytes place:int',
    'interest': 'person:int tag:int',
    'mtag': 'message:int tag:int',
    'ftag': 'forum:int tag:int',
    'likes': 'person:int message:int created:int',
    'study': 'person:int org:int year:int',
    'work': 'person:int org:int year:int',
    'email': 'person:int value:bytes',
    'language': 'person:int value:bytes',
}
SCHEMA = {name: dict(field.split(':') for field in fields.split()) for name, fields in SCHEMA.items()}
ENTITIES = ('Person', 'Forum', 'Post', 'Comment', 'Person_knows_Person',
            'Forum_hasMember_Person', 'Forum_hasTag_Tag', 'Person_hasInterest_Tag',
            'Person_likes_Comment', 'Person_likes_Post', 'Person_studyAt_University',
            'Person_workAt_Company', 'Post_hasTag_Tag', 'Comment_hasTag_Tag')
STATIC = ('Place', 'Organisation', 'Tag', 'TagClass')
EDGE_KEYS = {
    'Person_knows_Person': ('Person1Id', 'Person2Id'),
    'Forum_hasMember_Person': ('ForumId', 'PersonId'),
    'Forum_hasTag_Tag': ('ForumId', 'TagId'),
    'Person_hasInterest_Tag': ('PersonId', 'TagId'),
    'Person_likes_Comment': ('PersonId', 'CommentId'),
    'Person_likes_Post': ('PersonId', 'PostId'),
    'Person_studyAt_University': ('PersonId', 'UniversityId'),
    'Person_workAt_Company': ('PersonId', 'CompanyId'),
    'Post_hasTag_Tag': ('PostId', 'TagId'),
    'Comment_hasTag_Tag': ('CommentId', 'TagId'),
}


def key(entity, row):
    if entity in EDGE_KEYS:
        fields = tuple(int(row[f]) for f in EDGE_KEYS[entity])
        return tuple(sorted(fields)) if entity == 'Person_knows_Person' else fields
    return int(row['id'])


def number(row, name):
    return int(row.get(name) or -1)


def date(text):
    return datetime.fromisoformat(text.replace('Z', '+00:00')).astimezone(timezone.utc)


def millis(text):
    delta = date(text) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000


def load(snapshot):
    """Read an existing initial_snapshot directory. Never download data."""
    tables = {entity: {} for entity in (*ENTITIES, *STATIC)}
    for entity in tables:
        for row in read_table(snapshot, "static" if entity in STATIC else "dynamic", entity):
            identity = key(entity, row)
            if identity in tables[entity]:
                raise ValueError(f"duplicate {entity} {identity}")
            tables[entity][identity] = row
    return project(tables)


def project(tables):
    result = {name: set() for name in SCHEMA}
    def add(name, *fields):
        assert len(fields) == len(SCHEMA[name]), name
        result[name].add(tuple(fields))
    for pid,r in tables['Person'].items():
        birthday = date(r['birthday']+'T00:00:00+00:00')
        created = date(r['creationDate'])
        add('person', pid,r['firstName'],r['lastName'],r['gender'],millis(birthday.isoformat()),millis(r['creationDate']),
            r['locationIP'],r['browserUsed'],number(r,'LocationCityId'),birthday.month,birthday.day,created.year*12+created.month)
        for source,target in (('email','email'),('language','language')):
            for value in r[source].split(';'):
                if value:
                    add(target,pid,value)
    for tag,kind in enumerate(('Post','Comment')):
        for mid,r in tables[kind].items():
            created = date(r['creationDate'])
            add('message',mid,tag,millis(r['creationDate']),number(r,'CreatorPersonId'),number(r,'LocationCountryId'),
                r['content'],r.get('imageFile',''),number(r,'length'),r.get('language',''),number(r,'ContainerForumId'),
                number(r,'ParentPostId') if r.get('ParentPostId') else number(r,'ParentCommentId'),
                created.year,created.year*12+created.month,millis(created.replace(hour=0,minute=0,second=0,microsecond=0).isoformat()))
    for fid,r in tables['Forum'].items():
        add('forum',fid,r['title'],millis(r['creationDate']),number(r,'ModeratorPersonId'))
    for (a,b),r in tables['Person_knows_Person'].items():
        add('knows',a,b,millis(r['creationDate']))
    for _,r in tables['Forum_hasMember_Person'].items():
        add('member',number(r,'ForumId'),number(r,'PersonId'),millis(r['creationDate']))
    for kind, relation, fields in (
        ('Tag','tag',('id','name','TypeTagClassId')),
        ('TagClass','tagclass',('id','name','SubclassOfTagClassId')),
        ('Place','place',('id','name','type','PartOfPlaceId')),
        ('Organisation','org',('id','name','type','LocationPlaceId')),
        ('Person_hasInterest_Tag','interest',('PersonId','TagId')),
        ('Post_hasTag_Tag','mtag',('PostId','TagId')),
        ('Comment_hasTag_Tag','mtag',('CommentId','TagId')),
        ('Forum_hasTag_Tag','ftag',('ForumId','TagId')),
        ('Person_likes_Post','likes',('PersonId','PostId','creationDate')),
        ('Person_likes_Comment','likes',('PersonId','CommentId','creationDate')),
        ('Person_studyAt_University','study',('PersonId','UniversityId','classYear')),
        ('Person_workAt_Company','work',('PersonId','CompanyId','workFrom')),
    ):
        for r in tables[kind].values():
            add(relation,*(millis(r[f]) if f=='creationDate' else r[f] if f in ('name','type') else number(r,f) for f in fields))
    ids = [m[0] for m in result['message']]
    assert len(ids) == len(set(ids)), 'Post and Comment IDs overlap'
    return result
