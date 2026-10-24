"""Update projection, explicit targets, and the independently specified cascades."""
import gzip
import csv
import gc
import io
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import weakref

from snb import data, updates, witness
from snb.rel import R, SourceRows, c, evaluate
from snapshot import csv_rows, read_table
from update_stream import ReferenceAnswers, checkpoint_epochs, summary, write_json


def graph():
    result={n:set() for n in data.SCHEMA}
    person=next(iter(witness.graph()['person']))
    result['person']={(*person[:0],i,*person[1:]) for i in (1,2,3)}
    result['forum']={(10,'Wall of One',0,1),(11,'Album 0 of One',0,1),(12,'Group for X',0,1),(13,'Wall of Two',0,2)}
    def message(mid,kind,creator,forum=-1,parent=-1):
        return (mid,kind,0,creator,0,'content','',7,'en',forum,parent,2012,201201,0)
    result['message']={message(100,0,2,10),message(101,0,1,12),message(102,0,2,12),
                       message(103,0,2,13),message(200,1,2,parent=100),message(201,1,3,parent=200),
                       message(202,1,1,parent=102),message(203,1,3,parent=202),message(204,1,2,parent=103)}
    result['likes']={(3,mid,0) for mid in (100,101,102,103,200,201,202,203,204)}|{(1,103,1)}
    result['mtag']={(r[0],7) for r in result['message']}
    result['member']={(f,p,0) for f in (10,11,12,13) for p in (1,2,3)}
    result['ftag']={(f,7) for f in (10,11,12,13)}
    result['knows']={(1,2,0),(2,3,0)}
    for name in ('interest','study','work','email','language'):
        width=len(data.SCHEMA[name])
        result[name]={(1,*range(1,width)),(2,*range(1,width))}
    return result


class UpdatesTests(unittest.TestCase):
    def test_reference_reuse_requires_the_same_state_plan_binding_and_oracle(self):
        journal = dict(initial_sha256='snapshot',batches=[dict(epoch=1,sha256='delta')])
        report = dict(journal=journal,plan_sha256={'is1':'plan'},reference_sha256={'reference':'code'})
        answer = [[[0,0],[7],1]]
        capture = dict(status='passed',actual_path='answer.json',
                       answer_sha256=hashlib.sha256(json.dumps(answer).encode()).hexdigest())
        query = dict(binding=[[0]],initial=capture,checks=[dict(capture,epoch=1,status='captured')])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root/'report.json',report)
            write_json(root/'is1.json',query)
            write_json(root/'answer.json',answer)
            reused = ReferenceAnswers(root,report,journal,'is1',[(0,)])
            self.assertEqual(set(reused.captures),{0},'unvalidated captures cannot supply expected answers')
            self.assertEqual(reused.read(0)[0],answer)
            self.assertIsNone(reused.read(1))
            mismatches = [dict(report,reference_sha256={'reference':'changed'}),
                          dict(report,plan_sha256={'is1':'changed'})]
            for changed in mismatches:
                self.assertFalse(ReferenceAnswers(root,changed,journal,'is1',[(0,)]).captures)
            for changed in [dict(journal,initial_sha256='changed'),
                            dict(journal,batches=[dict(epoch=1,sha256='changed')])]:
                self.assertFalse(ReferenceAnswers(root,report,changed,'is1',[(0,)]).captures)
            self.assertFalse(ReferenceAnswers(root,report,journal,'is1',[(1,)]).captures)
            write_json(root/'answer.json',[])
            with self.assertRaisesRegex(ValueError,'reused reference capture checksum mismatch'):
                reused.read(0)

    def test_reference_source_views_encode_only_fields_that_are_read(self):
        encoded = []
        class Text(str):
            def encode(self, *args, **kwargs):
                encoded.append(str(self))
                return super().encode(*args, **kwargs)
        schema = {'id':'int', 'text':'bytes'}
        raw = [(1,Text('é')), (2,Text('unused'))]
        source = R.source('probe',schema)
        plan = source.where(c.id == 1).select('text')
        result = evaluate(plan,{'probe':SourceRows(raw,schema)})
        self.assertEqual(result,[{'text':tuple('é'.encode())}])
        self.assertEqual(encoded,['é'])
        eager = [(1,tuple('é'.encode())), (2,tuple('unused'.encode()))]
        self.assertEqual(result,evaluate(plan,{'probe':eager}))

    def test_sf1_interest_headers_in_snapshot_and_insert(self):
        text = 'creationDate|personId|interestId\n2012-12-01T00:00:00Z|3|7\n'
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root/'dynamic'/'Person_hasInterest_Tag'
            folder.mkdir(parents=True)
            path = folder/'part.csv.gz'
            with gzip.open(path, 'wt') as target:
                target.write(text)
            snapshot = list(read_table(root, 'dynamic', 'Person_hasInterest_Tag'))
            inserted = updates.Batch('2012-12-01', 'insert-person', (('Person_hasInterest_Tag', path),)).rows()
            expected = {(3,7)}
            self.assertEqual(data.key('Person_hasInterest_Tag', snapshot[0]), (3,7))
            self.assertEqual(updates.project_rows({'Person_hasInterest_Tag': snapshot})['interest'], expected)
            self.assertEqual(updates.project_rows(inserted)['interest'], expected)
        ambiguous = 'PersonId|personId|TagId\n1|2|3\n'
        with self.assertRaisesRegex(ValueError, 'duplicate CSV header'):
            list(csv_rows(io.StringIO(ambiguous), 'Person_hasInterest_Tag'))

    def test_checkpoint_schedule_keeps_class_firsts_and_final_partial_interval(self):
        batches=[dict(epoch=i+1,update_class=kind) for i,kind in enumerate(('a','a','b','a','b','c'))]
        self.assertEqual(checkpoint_epochs(batches,'replay',4),{1,3,4,6})
        self.assertEqual(checkpoint_epochs(batches,'replay',4,False),{4,6})
        self.assertEqual(checkpoint_epochs(batches,'replay',0,False),{6})
        self.assertEqual(checkpoint_epochs([],'replay',4),set())
        self.assertEqual(checkpoint_epochs(batches,'strict',check_from=3),{3,4,5,6})

    def test_summary_separates_completed_updates_from_validated_checkpoints(self):
        kind='insert-person'
        completed=[dict(epoch=i,update_class=kind,update_ms=ms,projected_row_changes=1)
                   for i,ms in ((1,1),(2,3))]
        capture=dict(completed[1],status='captured',peek_ms=1)
        record=dict(query='is1',epoch=2,updates=completed,checks=[capture])
        report=dict(workers=1,queries=[record],validation=dict(epochs=[0,2]),
                    journal=dict(batches=[dict(epoch=i,update_class=kind) for i in (1,2)]))
        with tempfile.TemporaryDirectory() as tmp:
            output=Path(tmp)
            summary(report,output)
            with (output/'coverage.csv').open() as source:
                row=next(csv.DictReader(source))
            self.assertEqual((row['status'],row['checked'],row['completed_batches']),('awaiting-validation','0','2'))
            capture.update(status='passed',reference_ms=1)
            summary(report,output)
            with (output/'coverage.csv').open() as source:
                row=next(csv.DictReader(source))
            self.assertEqual((row['status'],row['checked'],row['batches']),('checkpoint-validated','1','2'))
            self.assertEqual(float(row['update_median_ms']),2, 'latency must include unchecked update epochs')

    def test_completed_reference_does_not_retain_input_snapshot(self):
        class Rows(list):pass
        enabled=gc.isenabled()
        gc.disable()
        try:
            rows=Rows([(1,)])
            observed=weakref.ref(rows)
            sources={'probe':rows}
            answer=evaluate(R.source('probe',{'value':'int'}),sources)
            del rows,sources
            self.assertEqual(answer,[{'value':1}])
            self.assertIsNone(observed(),'completed evaluation retained the previous input snapshot')
        finally:
            if enabled:gc.enable()

    def test_streaming_snapshot_matches_existing_projection_across_chunks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for entity in (*data.ENTITIES,*data.STATIC):
                folder=root/('static' if entity in data.STATIC else 'dynamic')/entity
                folder.mkdir(parents=True)
                (folder/'part.csv').write_text('id\n')
            row=dict(id='0',creationDate='2012-12-01T00:00:00Z',locationIP='1.2.3.4',browserUsed='Browser',
                     content='quoted | é\nsecond line',length='24',CreatorPersonId='3',LocationCountryId='0',
                     ParentPostId='100',ParentCommentId='')
            path=root/'dynamic'/'Comment'/'part.csv'
            with path.open('w',newline='') as target:
                writer=csv.DictWriter(target,fieldnames=row,delimiter='|',escapechar='\\',doublequote=False)
                writer.writeheader()
                writer.writerows(dict(row,id=str(i)) for i in range(1001))
            self.assertEqual(updates.load(root),data.load(root))
            with path.open('a',newline='') as target:
                writer=csv.DictWriter(target,fieldnames=row,delimiter='|',escapechar='\\',doublequote=False)
                writer.writerow(row)
            with self.assertRaisesRegex(ValueError,'duplicate Comment 0'):
                updates.load(root)

    def test_delta_validation_is_atomic(self):
        original=graph()
        before={name:set(rows) for name,rows in original.items()}
        changes={'person':dict(removed=[next(iter(original['person']))],added=[]),
                 'knows':dict(removed=[(99,100,0)],added=[])}
        with self.assertRaisesRegex(ValueError,'inconsistent knows delta'):
            updates.apply_delta(original,changes)
        self.assertEqual(original,before)

    def test_delta_allows_replacement_but_rejects_a_surviving_duplicate(self):
        original = graph()
        row = next(iter(original['person']))
        replacement = (*row[:-1], row[-1]+1)
        changes = {'person': dict(removed=[row], added=[replacement])}
        updates.apply_delta(original, changes)
        self.assertIn(replacement, original['person'])
        updates.apply_delta(original, {'person': dict(removed=[replacement], added=[replacement])})
        before = {name: set(rows) for name,rows in original.items()}
        with self.assertRaisesRegex(ValueError, 'inconsistent person delta'):
            updates.apply_delta(original, {'person': dict(removed=[], added=[replacement])})
        self.assertEqual(original, before)

    def apply(self,state,kind,rows):
        before={n:set(rs) for n,rs in state.graph.items()}
        batch=state.apply(kind,rows)
        updates.apply_delta(before,batch['changes'])
        self.assertEqual(before,state.graph,'oracle consumes precisely the journalled server delta')
        return batch

    def test_person_keeps_groups_and_cascades_through_other_authors(self):
        state=updates.State(graph())
        batch=self.apply(state,'delete-person',{'Person':[{'id':'1'}]})
        self.assertEqual(batch['matched_targets'],1)
        self.assertEqual({r[0] for r in state.graph['person']},{2,3})
        self.assertEqual({r[0] for r in state.graph['forum']},{12,13})
        self.assertIn((12,'Group for X',0,-1),state.graph['forum'])
        self.assertEqual({r[0] for r in state.graph['message']},{102,103,204})
        self.assertEqual(state.graph['likes'],{(3,102,0),(3,103,0),(3,204,0)})
        self.assertEqual(state.graph['mtag'],{(102,7),(103,7),(204,7)})
        self.assertEqual(state.graph['knows'],{(2,3,0)})
        self.assertTrue(all(r[1]!=1 and r[0] not in (10,11) for r in state.graph['member']))
        for name in ('interest','study','work','email','language'):
            self.assertEqual({r[0] for r in state.graph[name]},{2})
        repeated=self.apply(state,'delete-post',{'Post':[{'id':'100'}]})
        self.assertEqual(repeated['absent_targets'],1)
        self.assertFalse(repeated['changes'],'a cascaded target is never retracted twice')

    def test_forum_post_comment_and_non_cascading_membership(self):
        cases=[('delete-forum','Forum',10,{101,102,103,202,203,204}),
               ('delete-post','Post',100,{101,102,103,202,203,204}),
               ('delete-comment','Comment',200,{100,101,102,103,202,203,204})]
        for kind,entity,target,remaining in cases:
            with self.subTest(kind=kind):
                state=updates.State(graph())
                self.apply(state,kind,{entity:[{'id':str(target)}]})
                self.assertEqual({r[0] for r in state.graph['message']},remaining)
                self.assertEqual({r[1] for r in state.graph['likes']},remaining)
                self.assertEqual({r[0] for r in state.graph['mtag']},remaining)
        state=updates.State(graph());before=set(state.graph['message'])
        self.apply(state,'delete-member',{'Forum_hasMember_Person':[{'ForumId':'12','PersonId':'1'}]})
        self.assertEqual(state.graph['message'],before)
        self.assertNotIn((12,1,0),state.graph['member'])

    def test_undirected_friendship_and_like_deletes(self):
        state=updates.State(graph())
        self.apply(state,'delete-knows',{'Person_knows_Person':[{'Person1Id':'2','Person2Id':'1'}]})
        self.assertEqual(state.graph['knows'],{(2,3,0)})
        self.apply(state,'delete-like-post',{'Person_likes_Post':[{'PersonId':'3','PostId':'100'}]})
        self.assertNotIn((3,100,0),state.graph['likes'])
        self.assertIn((3,200,0),state.graph['likes'])

    def test_insertion_projection_and_cascade_after_new_comment(self):
        state=updates.State(graph())
        row=dict(id='205',creationDate='2012-12-01T00:00:00Z',locationIP='1.2.3.4',browserUsed='Firefox',
                 content='quoted | é',length='10',CreatorPersonId='3',LocationCountryId='0',ParentPostId='',ParentCommentId='201')
        self.apply(state,'insert-comment',{'Comment':[row],'Comment_hasTag_Tag':[{'CommentId':'205','TagId':'9'}]})
        self.assertIn(205,state.nodes['message'])
        self.assertIn((205,9),state.graph['mtag'])
        with self.assertRaisesRegex(ValueError,'existing message identity'):
            state.apply('insert-comment',{'Comment':[row]})
        self.apply(state,'delete-post',{'Post':[{'id':'100'}]})
        self.assertNotIn(205,state.nodes['message'])
        self.assertNotIn((205,9),state.graph['mtag'])

    def test_native_partition_order_and_decompressed_copy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for phase,entities in (('inserts',('Person_knows_Person',)),('deletes',('Person_knows_Person',))):
                for day in ('2012-12-02','2012-12-01'):
                    folder=root/phase/'dynamic'/entities[0]/f'batch_id={day}'
                    folder.mkdir(parents=True)
                    text=('creationDate' if phase=='inserts' else 'deletionDate')+'|Person1Id|Person2Id\n2012-12-01T00:00:00Z|1|2\n'
                    (folder/'part.csv').write_text(text)
                    with gzip.open(folder/'part.csv.gz','wt') as target:target.write(text)
            batches=updates.batches(root,['insert-knows','delete-knows'])
            self.assertEqual([(b.day,b.kind) for b in batches],[(d,k) for d in ('2012-12-01','2012-12-02')
                             for k in ('insert-knows','delete-knows')])
            self.assertEqual(len(batches[0].rows()['Person_knows_Person']),1)


if __name__=='__main__':unittest.main()
