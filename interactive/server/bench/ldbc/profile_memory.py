#!/usr/bin/env python3
"""Measure input-only and query-active process memory over a prepared update stream.

No Python answers are recomputed. Captures can be compared with saved answers;
this profile does not replace the correctness checks in update_stream.py.
"""
import argparse
import ctypes
import gc
import gzip
import hashlib
import json
import os
from pathlib import Path
import pickle
import platform
import signal
import subprocess
import sys
import threading
import time

from client import Server, decode, parse_shape
from snb import data
from snb.rel import source_shape
from snb.parameters import requests
from suite import CATALOGUE, compile_queries, graph_program
from update_stream import Budget, adapter_hashes, write_json


class Memory:
    """RSS/physical footprint at boundaries and sampled within each phase."""
    def __init__(self, server, limit):
        self.server, self.limit = server, limit * 2**30
        self.phase, self.peaks = 'startup', {}
        self.lock, self.stop = threading.Lock(), threading.Event()
        class Usage(ctypes.Structure):
            _fields_ = [('uuid', ctypes.c_ubyte*16), *[(n, ctypes.c_uint64) for n in
                ('user','system','idle','interrupt','pageins','wired','resident','footprint','start','exit')]]
        self.Usage = Usage
        self.lib = ctypes.CDLL('/usr/lib/libproc.dylib') if sys.platform == 'darwin' else None
        self.thread = threading.Thread(target=self.monitor, daemon=True)
        self.thread.start()

    def sample(self):
        if self.server.process.poll() is not None:
            return None
        if self.lib:
            usage = self.Usage()
            if self.lib.proc_pid_rusage(self.server.process.pid, 0, ctypes.byref(usage)):
                raise OSError('proc_pid_rusage failed')
            result = dict(rss_bytes=usage.resident, physical_footprint_bytes=usage.footprint)
        else:
            ps = subprocess.run(['ps','-o','rss=','-p',str(self.server.process.pid)],
                                capture_output=True,text=True,check=True)
            result = dict(rss_bytes=int(ps.stdout.strip())*1024)
        with self.lock:
            peak = self.peaks.setdefault(self.phase, {})
            for key,value in result.items(): peak[key] = max(peak.get(key,0),value)
        if result['rss_bytes'] > self.limit:
            self.server.failure = f'server RSS exceeded {self.limit/2**30:g} GiB'
            self.server.process.kill()
        return result

    def monitor(self):
        while not self.stop.wait(.1):
            try:
                self.sample()
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                if self.server.process.poll() is None:
                    self.server.failure = f'memory sampler failed: {error}'
                    self.server.process.kill()
                return

    def close(self):
        self.stop.set()
        self.thread.join(timeout=3)


def profile(args):
    name = args.case
    query = name != 'input-only'
    destination = args.output/name
    destination.mkdir()
    manifest = json.loads((args.prepared/'manifest.json').read_text())
    record = dict(query=name,status='running',points={},events=[],updates=[],answer_validation={},
        source_revision=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        source_status=subprocess.check_output(['git','status','--porcelain'],text=True),
        binary_sha256=hashlib.sha256(args.server.read_bytes()).hexdigest(),
        initial_sha256=manifest['initial_sha256'],workers=args.workers,host=platform.node(),
        command=sys.argv,scope='Process-memory profile; saved-answer comparison only, no fresh oracle.')
    started = time.perf_counter()
    server = memory = None
    budget = Budget(None,args.max_total_rss_gib)
    graph = rows = None
    phase = 'load-snapshot'
    try:
        with gzip.open(args.prepared/'initial.pickle.gz','rb') as f: graph = pickle.load(f)
        binding = []
        program = None
        if query:
            plan = compile_queries([name])[name]
            parameters = json.loads(args.parameters.read_text())[name]
            binding = sorted(requests(name,plan['parameters'],parameters,0))
            program = plan['program']
            if args.bounded_topk and name in ('ic2','ic5','ic9'):
                from bounded_topk import program as topk
                buckets = [1] if name=='ic2' else [1024,32,1]
                program = topk(buckets,'identity' if name=='ic5' else 'value',query=name)
                record['ranking'] = dict(buckets=buckets,hash='identity' if name=='ic5' else 'value')
            (destination/(name+'.ddp')).write_text(program)
            record.update(binding=binding,plan_sha256=hashlib.sha256(program.encode()).hexdigest())
            request_shape = (parse_shape(source_shape(plan['parameters'])),'unit')
        shapes = [(parse_shape(source_shape(schema)),'unit') for schema in data.SCHEMA.values()]
        with Server(args.server,destination/'server.log',args.workers,args.timeout,args.max_rss_gib) as server:
            previous = budget
            previous.close()
            budget = Budget(server,args.max_total_rss_gib)
            budget.peak,budget.peak_footprint = previous.peak,previous.peak_footprint
            memory = Memory(server,args.max_rss_gib)
            def command(label,commands):
                nonlocal phase
                phase = memory.phase = label
                tick_started = time.perf_counter()
                metrics,replies = server.commands(commands)
                record['events'].append(dict(phase=label,elapsed_s=time.perf_counter()-tick_started,**metrics))
                return replies
            def settle():
                for _ in range(3): command('settle',['tick'])
            def mark(label):
                value = memory.sample()
                if value is None: raise RuntimeError(server.failure or 'server exited')
                record['points'][label] = dict(seconds=time.perf_counter()-started,**value)
                write_json(destination/'report.json',record)
            def read(label):
                frames, = command('read-'+label,[f'peek {name}.answer as corgi'])
                actual = decode(frames)
                write_json(destination/(label+'.json'),actual)
                validation = 'captured-unchecked'
                if args.expected:
                    saved = json.loads((args.expected/(name+'.json')).read_text())
                    if actual != saved[label]: raise AssertionError(f'{name}/{label} differs from saved answer')
                    validation = 'matched-saved-validated-answer' if saved['validated'] else 'matched-saved-unvalidated-capture'
                record['answer_validation'][label] = validation
            command('install-inputs',[f'load graph begin\n{graph_program()}\n{{rid}} end-load','tick'])
            for i,table in enumerate(data.SCHEMA):
                rows = sorted(graph[table])
                for offset in range(0,len(rows),1000):
                    command('admit-inputs',[Server.feed('graph',i,rows[offset:offset+1000],1,shapes[i])])
            command('settle-inputs',['tick'])
            graph = rows = None
            gc.collect()
            settle();mark('base_initial')
            command('install-query',[f'load {name} begin\n{program}\n{{rid}} end-load','tick'] if query else ['tick'])
            mark('query_installed')
            command('bind',[Server.feed(name,0,binding,1,request_shape),'tick'] if query else ['tick'])
            settle();mark('active_initial')
            if query: read('initial')
            replay_started = time.perf_counter()
            count = 0
            for batch in manifest['batches']:
                with gzip.open(args.prepared/batch['path'],'rt') as f: changes = json.load(f)['changes']
                commands = []
                for i,table in enumerate(data.SCHEMA):
                    if table in changes:
                        for side,diff in (('removed',-1),('added',1)):
                            rows = changes[table][side]
                            count += len(rows)
                            for offset in range(0,len(rows),1000):
                                commands.append(Server.feed('graph',i,rows[offset:offset+1000],diff,shapes[i]))
                command('update',[*commands,'tick'])
                record['updates'].append(dict(epoch=batch['epoch'],wire_ms=record['events'][-1]['wire_ms']))
            record.update(replay_s=time.perf_counter()-replay_started,epochs=len(manifest['batches']),projected_changes=count)
            settle();mark('active_final')
            if query: read('final')
            mark('before_retirement')
            command('retire',[Server.feed(name,0,binding,-1,request_shape),'tick'] if query else ['tick'])
            settle();mark('after_retirement')
            if query:
                frames, = command('read-empty',[f'peek {name}.answer as corgi'])
                assert decode(frames)==[], 'request retirement did not empty the answer'
            if budget.failure or server.failure: raise MemoryError(budget.failure or server.failure)
            record['status'] = 'profiled'
    except BaseException as error:
        resource = budget.failure or (server.failure if server else None)
        record.update(status='failed',failure_kind='resource' if resource else 'saved-answer-mismatch'
            if isinstance(error,AssertionError) else 'error',phase=phase,error=resource or repr(error))
    finally:
        if memory:
            memory.close();record['phase_peaks']=memory.peaks
        budget.close()
        record.update(elapsed_s=time.perf_counter()-started,combined_peak_rss_bytes=budget.peak,
                      combined_peak_physical_footprint_bytes=budget.peak_footprint)
        write_json(destination/'report.json',record)
    return record


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--prepared',type=Path,required=True)
    p.add_argument('--server',type=Path,required=True)
    p.add_argument('--parameters',type=Path,required=True,help='JSON query -> complete request parameters, as in update_stream.py')
    p.add_argument('--expected',type=Path,help='directory of saved initial/final answers and validation flags')
    p.add_argument('--bounded-topk',action='store_true',help='bounded IC2; three-layer IC5/IC9 ranking')
    p.add_argument('--queries',nargs='+',default=['all'])
    p.add_argument('--workers',type=int,default=1)
    p.add_argument('--timeout',type=float,default=120)
    p.add_argument('--job-timeout',type=float,default=300)
    p.add_argument('--max-rss-gib',type=float,default=6)
    p.add_argument('--max-total-rss-gib',type=float,default=10)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--case',help=argparse.SUPPRESS)
    args=p.parse_args()
    args.server=args.server.resolve(strict=True)
    if args.case:
        result=profile(args)
        raise SystemExit(0 if result['status']=='profiled' else 1)
    args.output.mkdir()
    manifest=json.loads((args.prepared/'manifest.json').read_text())
    assert manifest['adapter_sha256']==adapter_hashes(), 'journal adapter differs'
    for name,sha in [('initial.pickle.gz',manifest['initial_sha256']),*((b['path'],b['sha256']) for b in manifest['batches'])]:
        assert hashlib.sha256((args.prepared/name).read_bytes()).hexdigest()==sha,name
    names=CATALOGUE if args.queries==['all'] else args.queries
    assert all(n in CATALOGUE for n in names)
    campaign=dict(status='running',current=None,results=[],sample_interval_s=.1,
                  scope='Separate fresh servers; input-only control and fixed requests. No fresh Python evaluation.')
    for name in ['input-only',*names]:
        command=[sys.executable,str(Path(__file__).resolve()),*sys.argv[1:],'--case',name]
        campaign['current']=name;write_json(args.output/'campaign.json',campaign)
        print(name,'starting',flush=True)
        with (args.output/(name+'.stdout')).open('w') as log:
            child=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            capped=False
            try:
                child.wait(timeout=args.job_timeout)
            except subprocess.TimeoutExpired:
                capped=True
                child.send_signal(signal.SIGINT)
                try: child.wait(timeout=10)
                except subprocess.TimeoutExpired: os.killpg(child.pid,signal.SIGKILL);child.wait()
            finally:
                try:os.killpg(child.pid,signal.SIGTERM)
                except ProcessLookupError:pass
        path=args.output/name/'report.json'
        record=json.loads(path.read_text()) if path.exists() else dict(query=name,status='failed',error='job exited without a report')
        if capped:
            record.update(status='failed',failure_kind='job-timeout',wall_time_capped=True)
            write_json(path,record)
        campaign['results'].append(dict(query=name,status=record['status'],returncode=child.returncode,report=str(path)))
        print(name,record['status'],record.get('error',''),flush=True)
        if record.get('failure_kind')=='saved-answer-mismatch' or name=='input-only' and record['status']!='profiled':
            campaign['status']='stopped';break
    else:campaign['status']='complete'
    campaign['current']=None;write_json(args.output/'campaign.json',campaign)
    if campaign['status']!='complete' or any(r['status']!='profiled' for r in campaign['results']):
        raise SystemExit(1)


if __name__=='__main__':main()
