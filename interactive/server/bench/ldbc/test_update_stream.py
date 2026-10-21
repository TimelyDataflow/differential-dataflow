#!/usr/bin/env python3
"""Server-backed update/cascade/reinsertion smoke; no downloaded data needed."""
import argparse
import gzip
import json
from pathlib import Path
import pickle
import subprocess
import sys
from unittest.mock import patch

import measure
from run import digest, positive
from snb import updates, witness
from suite import CATALOGUE, HERE, compile_queries
from update_stream import adapter_hashes, checkpoint_epochs, run_query, write_json


def fixture(root):
    root.mkdir(parents=True)
    (root/'batches').mkdir()
    graph=witness.graph()
    graph['forum']={(fid,'Group '+title,created,moderator) for fid,title,created,moderator in graph['forum']}
    original={name:set(rows) for name,rows in graph.items()}
    with gzip.open(root/'initial.pickle.gz','wb') as target:
        pickle.dump(graph,target)
    manifest=dict(format_version='snb-update-journal-1',fixture='mixed-author cascades and ID reuse',
                  initial_rows={n:len(rs) for n,rs in graph.items()},adapter_sha256=adapter_hashes(),
                  initial_sha256=digest(root/'initial.pickle.gz'),batches=[])
    date='2012-01-04T00:00:00Z'
    inserts={
        'person':{'Person':[dict(id='9',firstName='New',lastName='Person',gender='female',birthday='1980-01-22',
                               creationDate=date,locationIP='127.0.0.1',browserUsed='Browser',LocationCityId='101',
                               email='new@example.invalid',language='en')],
                  'Person_hasInterest_Tag':[dict(PersonId='9',TagId='10')],
                  'Person_studyAt_University':[dict(PersonId='9',UniversityId='20',classYear='2001')],
                  'Person_workAt_Company':[dict(PersonId='9',CompanyId='21',workFrom='2008')]},
        'forum':{'Forum':[dict(id=str(i),title=title,creationDate=date,ModeratorPersonId='9')
                          for i,title in ((1005,'Wall of New'),(1006,'Group of New'))],
                 'Forum_hasTag_Tag':[dict(ForumId=str(i),TagId='10') for i in (1005,1006)]},
        'post':{'Post':[dict(id=str(i),creationDate=date,CreatorPersonId='9',LocationCountryId='100',
                             content='new post',imageFile='',length='8',language='en',ContainerForumId=str(forum))
                        for i,forum in ((20001,1005),(20004,1006))],
                'Post_hasTag_Tag':[dict(PostId=str(i),TagId='10') for i in (20001,20004)]},
        'comment':{'Comment':[dict(id=str(i),creationDate=date,CreatorPersonId=str(person),LocationCountryId='100',
                                  content='other author',length='12',ParentPostId=post,ParentCommentId=comment)
                              for i,person,post,comment in ((20002,2,'20001',''),(20003,3,'','20002'))],
                   'Comment_hasTag_Tag':[dict(CommentId=str(i),TagId='10') for i in (20002,20003)]},
        'like-post':{'Person_likes_Post':[dict(PersonId='1',PostId='20001',creationDate=date)]},
        'like-comment':{'Person_likes_Comment':[dict(PersonId='1',CommentId='20003',creationDate=date)]},
        'knows':{'Person_knows_Person':[dict(Person1Id='1',Person2Id='9',creationDate=date)]},
        'member':{'Forum_hasMember_Person':[dict(ForumId='1005',PersonId='3',creationDate=date)]},
    }
    deletes={
        'person':{'Person':[dict(id='9')]},
        'forum':{'Forum':[dict(id=str(i)) for i in (1005,1006)]},
        'post':{'Post':[dict(id=str(i)) for i in (20001,20004)]},
        'comment':{'Comment':[dict(id='20002')]},
        'like-post':{'Person_likes_Post':[dict(PersonId='1',PostId='20001')]},
        'like-comment':{'Person_likes_Comment':[dict(PersonId='1',CommentId='20003')]},
        'knows':{'Person_knows_Person':[dict(Person1Id='9',Person2Id='1')]},
        'member':{'Forum_hasMember_Person':[dict(ForumId='1005',PersonId='3')]},
    }
    state=updates.State(graph)
    def record(result,day):
        epoch=len(manifest['batches'])+1
        batch=dict(epoch=epoch,day=day,files=[],**result)
        path=Path('batches')/f'{epoch:04}.json.gz'
        with gzip.open(root/path,'wt') as target:json.dump(batch,target)
        manifest['batches'].append({k:v for k,v in batch.items() if k!='changes'} |
                                   dict(path=str(path),sha256=digest(root/path)))
    def append(phase,kind,rows,cycle):
        record(state.apply(phase+'-'+kind,rows),f'2012-01-0{cycle+3}')
    # First remove the person and cascade through other authors; later explicit
    # targets are no-ops. Then reuse all IDs and exercise direct edge/message
    # removals before deleting the person. Both cycles restore the initial graph.
    for cycle in (1,2):
        for kind,rows in inserts.items():append('insert',kind,rows,cycle)
        order=list(deletes) if cycle==1 else ['like-post','like-comment','knows','member','comment','post','forum','person']
        for kind in order:append('delete',kind,deletes[kind],cycle)
        assert graph==original, f'cycle {cycle} did not restore the graph'
    # Drain every original person, including the bound requester, until all
    # messages and friendships are empty. Reverse the exact cascades to check
    # resurrection of seeds, paths, nested values and zero/nonzero aggregates.
    history=[]
    for person in range(1,9):
        result=state.apply('delete-person',{'Person':[dict(id=str(person))]})
        record(result,'2012-01-06')
        history.append(result)
    assert not graph['person'] and not graph['message'] and not graph['knows']
    for result in reversed(history):
        changes={name:dict(removed=d['added'],added=d['removed']) for name,d in result['changes'].items()}
        updates.apply_delta(graph,changes)
        record(dict(update_class='stress-restore-cascade',changes=changes),'2012-01-07')
    assert graph==original, 'drain/restore did not restore the graph'
    write_json(root/'manifest.json',manifest)


def injected_checkpoint_failure(args):
    """A bad captured answer must survive, and its strict retry must be honest."""
    name='is2'
    output=(args.output/'injected-failure').resolve()
    output.mkdir()
    journal=(args.output/'journal').resolve()
    manifest=json.loads((journal/'manifest.json').read_text())
    options=argparse.Namespace(server=args.server.resolve(),workers=args.workers,output=output,prepared=journal,
        parameters={name:witness.params(name)},check_mode='replay',check_every=7,check_first_class=True,
        check_from=1,timeout=120,reference_timeout=120,max_rss_gib=6,max_total_rss_gib=10)
    epochs=checkpoint_epochs(manifest['batches'],'replay',7)
    report=dict(journal=manifest,queries=[],selected_queries=[name],workers=args.workers,
                validation=dict(mode='replay',epochs=[0,*sorted(epochs)]))
    original=measure.decode_answer
    def corrupt_final(frames,event):
        actual=original(frames,event)
        if report['queries'][0]['epoch']==48 and actual:
            actual[0][2]+=1
        return actual
    with patch('update_stream.measure.decode_answer',side_effect=corrupt_final):
        run_query(options,name,compile_queries([name])[name],manifest,report)
    record=report['queries'][0]
    assert record['failure_kind']=='wrong-answer'
    assert record['failure_interval']==dict(after_epoch=42,through_epoch=48)
    investigation=record['investigation']
    assert investigation['returncode']==0 and not investigation['reproduced']
    retry=json.loads((Path(investigation['output'])/f'{name}.json').read_text())
    assert [c['epoch'] for c in retry['checks']]==list(range(43,49))
    assert record['status']=='failed', 'a passing retry must not erase the captured mismatch'
    print('Injected checkpoint mismatch preserved; strict interval retry correctly reported no reproduction.',flush=True)


def reused_checkpoint_checks(args, report):
    """A different worker count must compare every capture without recomputing Python."""
    name = 'is2'
    output = (args.output/'reused-reference').resolve()
    output.mkdir()
    manifest = json.loads((args.output/'journal/manifest.json').read_text())
    options = argparse.Namespace(server=args.server.resolve(),workers=1 if args.workers!=1 else 4,
        output=output,prepared=(args.output/'journal').resolve(),parameters={name:witness.params(name)},
        check_mode='replay',check_every=args.check_every,check_first_class=True,check_from=1,
        reference_from=(args.output/'run').resolve(),timeout=120,reference_timeout=120,
        max_rss_gib=6,max_total_rss_gib=10)
    target = dict(report,workers=options.workers,queries=[],selected_queries=[name],
                  command='test reuse of validated answers across worker counts')
    with patch('update_stream.timed_reference',side_effect=AssertionError('unexpected oracle recomputation')):
        run_query(options,name,compile_queries([name])[name],manifest,target)
    record = target['queries'][0]
    assert record['status']=='passed',record.get('error')
    assert len(record['updates'])==48 and record['retirement_checked']
    assert not record['reference_calls']
    assert all(c.get('reference_reused_from') for c in [record['initial'],*record['checks']])
    print('Every checkpoint matched validated answers from the other worker count; zero oracle calls.',flush=True)

    # A different checkpoint schedule reuses its overlap and computes the rest
    # against the graph reached by applying every intervening update.
    options.output = (args.output/'partial-reference').resolve()
    options.output.mkdir()
    options.check_every = 5
    target = dict(target,queries=[],validation=dict(mode='replay',epochs=[0,*sorted(
        checkpoint_epochs(manifest['batches'],'replay',options.check_every))]))
    run_query(options,name,compile_queries([name])[name],manifest,target)
    record = target['queries'][0]
    assert record['status']=='passed',record.get('error')
    expected = set(target['validation']['epochs'])-set(record['reference_reuse']['eligible_epochs'])
    assert expected, 'the schedule must exercise a cache miss'
    assert {c['epoch'] for c in record['reference_calls']} == expected
    assert any(c.get('reference_reused_from') for c in record['checks'])
    print('Partial reference reuse computes exactly the missing checkpoints.',flush=True)


def supplied_plan_checks(args):
    """A wrong candidate must use the usual oracle and survive the strict retry."""
    output = (args.output/'supplied-plan').resolve()
    output.mkdir()
    candidate = output/'empty.ddp'
    program = compile_queries(['is2'])['is2']['program']
    assert program.count(' | arrange;') == 1
    candidate.write_text(program.replace(' | arrange;', ' | filter((_, _) -> 0) | arrange;'))
    with (output/'run.log').open('w') as log:
        result = subprocess.run([sys.executable,str(HERE/'update_stream.py'),
            '--server',str(args.server.resolve()),'--workers',str(args.workers),
            '--prepared',str((args.output/'journal').resolve()),'--queries','is2',
            '--parameters',str((args.output/'parameters.json').resolve()),
            '--batch-limit','1','--plan',str(candidate),'--check-mode','replay',
            '--check-every','0','--no-check-first-class','--output',str(output/'run')],
            stdout=log,stderr=subprocess.STDOUT)
    assert result.returncode == 1
    record = json.loads((output/'run/is2.json').read_text())
    assert record['failure_kind'] == 'wrong-answer',record.get('error')
    retry = record['investigation']
    assert retry['reproduced'] and retry['status'] == 'failed'
    saved = output/'run/is2.ddp'
    assert saved.read_text() == candidate.read_text()
    assert (Path(retry['output'])/'is2.ddp').read_text() == candidate.read_text()
    print('Wrong supplied plan rejected by the logical oracle and preserved in the strict retry.',flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server',type=Path,required=True)
    parser.add_argument('--workers',type=positive,default=1)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--check-mode',choices=('strict','replay'),default='strict')
    parser.add_argument('--check-every',type=positive,default=7)
    args=parser.parse_args()
    args.output.mkdir(parents=True)
    fixture(args.output/'journal')
    bindings=args.output/'parameters.json'
    write_json(bindings,{name:witness.params(name) for name in CATALOGUE})
    subprocess.run([sys.executable,str(HERE/'update_stream.py'),'--server',str(args.server.resolve()),
                    '--workers',str(args.workers),'--prepared',str((args.output/'journal').resolve()),
                    '--check-mode',args.check_mode,'--check-every',str(args.check_every),
                    '--parameters',str(bindings.resolve()),'--output',str((args.output/'run').resolve())],check=True)
    report=json.loads((args.output/'run/report.json').read_text())
    for record in report['queries']:
        assert len(record['updates'])==48
        assert record['replay']['projected_row_changes']==458
        assert record['retirement_checked']
        assert len(record['checks'])==len(report['validation']['epochs'])-1
        assert [c['epoch'] for c in record['reference_calls']]==report['validation']['epochs']
        if args.check_mode=='replay':
            assert not record['replay']['includes_reference']
            assert all(c['started_ms']>=record['replay']['finished_ms'] for c in record['reference_calls'])
    if args.check_mode=='replay':
        reused_checkpoint_checks(args,report)
        injected_checkpoint_failure(args)
        supplied_plan_checks(args)


if __name__=='__main__':main()
