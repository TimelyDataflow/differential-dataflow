#!/usr/bin/env python3
"""Check maintained SNB reads after real BI CSV update batches, one query at a time."""
import argparse
from collections import Counter
import csv
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
import shlex
import statistics
import subprocess
import sys
import threading
import time
import _thread

from client import Server, decode, parse_shape
import measure
from run import checked, digest, positive
from snb import data, updates
from snb.rel import source_shape
from snb.parameters import parameters, reference, requests
from suite import CATALOGUE, HERE, SPEC, compile_queries, graph_program


def write_json(path, value):
    temporary = path.with_suffix(path.suffix+'.tmp')
    temporary.write_text(json.dumps(value, indent=2)+'\n')
    temporary.replace(path)


def adapter_hashes():
    return {name: digest(HERE/name) for name in ('snb/updates.py', 'snb/data.py', 'snapshot.py')}


def prepare(root, output, classes, limit, first, last):
    output.mkdir(parents=True)
    journal = output/'batches'
    journal.mkdir()
    started = time.perf_counter()
    graph = updates.load(root/'initial_snapshot')
    with gzip.open(output/'initial.pickle.gz', 'wb', compresslevel=1) as target:
        pickle.dump(graph, target, protocol=pickle.HIGHEST_PROTOCOL)
    manifest = dict(format_version='snb-update-journal-1', root=str(root),
                    selection=dict(classes=list(classes),first_day=first,last_day=last,batch_limit=limit),
                    initial_rows={n:len(rs) for n,rs in graph.items()}, adapter_sha256=adapter_hashes(),
                    initial_sha256=digest(output/'initial.pickle.gz'), batches=[])
    state = updates.State(graph)
    selected = updates.batches(root, classes, first, last)
    if limit is not None:
        selected = selected[:limit]
    if not selected and limit != 0:
        raise ValueError('no update batches selected')
    for epoch, batch in enumerate(selected, 1):
        item = {**batch.metadata(root), **state.apply(batch.kind, batch.rows()), 'epoch': epoch,
                'file_sha256': {str(p.relative_to(root)):digest(p) for _,p in batch.files}}
        item['projected_removed'] = sum(len(d['removed']) for d in item['changes'].values())
        item['projected_added'] = sum(len(d['added']) for d in item['changes'].values())
        target = journal/f'{epoch:04}.json.gz'
        with gzip.open(target, 'wt', encoding='utf-8', compresslevel=1) as stream:
            json.dump(item, stream)
        manifest['batches'].append({k:v for k,v in item.items() if k != 'changes'} |
                                   dict(path=str(target.relative_to(output)), sha256=digest(target)))
        print(f'prepared {epoch}: {batch.day}/{batch.kind}: +{item["projected_added"]}/-{item["projected_removed"]}', flush=True)
    manifest['prepare_ms'] = 1000*(time.perf_counter()-started)
    write_json(output/'manifest.json', manifest)
    return manifest


class Budget:
    """Sample BOTH Python and its server; interrupt before either exhausts this host."""
    def __init__(self, server, gib):
        self.stop = threading.Event()
        self.failure = None
        self.peak = 0
        self.peak_footprint = 0
        # On macOS, resident memory alone misses pages moved into compression.
        # rusage_info_v0 is defined in the platform's sys/resource.h.
        class Usage(ctypes.Structure):
            _fields_ = [('uuid',ctypes.c_ubyte*16),
                        *[(n,ctypes.c_uint64) for n in ('user','system','idle','interrupt','pageins',
                                                       'wired','resident','footprint','start','exit')]]
        libproc = ctypes.CDLL('/usr/lib/libproc.dylib') if sys.platform=='darwin' else None
        def monitor():
            while not self.stop.wait(0.5):
                pids = [os.getpid(), *([server.process.pid] if server else [])]
                result = subprocess.run(['ps','-o','rss=','-p',','.join(map(str,pids))],
                                        capture_output=True, text=True, check=False)
                rss = sum(int(line) for line in result.stdout.splitlines())*1024
                self.peak = max(self.peak, rss)
                footprint = 0
                if libproc:
                    for pid in pids:
                        usage = Usage()
                        if libproc.proc_pid_rusage(pid,0,ctypes.byref(usage)) == 0:
                            footprint += usage.footprint
                self.peak_footprint = max(self.peak_footprint,footprint)
                if max(rss,footprint) > gib*1024**3:
                    self.failure = f'Python + server RSS/physical footprint exceeded {gib} GiB'
                    if server:
                        server.process.kill()
                    _thread.interrupt_main()
                    return
        self.thread = threading.Thread(target=monitor, daemon=True)
        self.thread.start()

    def close(self):
        self.stop.set()
        self.thread.join(timeout=3)


def timed_reference(seconds, plan, sources, graph, active):
    def timeout(*_):
        raise TimeoutError(f'Python reference exceeded {seconds} seconds')
    previous = signal.signal(signal.SIGALRM, timeout)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        return reference(plan, sources, {**graph, 'request':active})
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def checkpoint_epochs(batches, mode, every=64, first_class=True, check_from=1):
    if mode == 'strict':
        return {b['epoch'] for b in batches if b['epoch'] >= check_from}
    selected, seen = set(), set()
    for batch in batches:
        epoch, kind = batch['epoch'], batch['update_class']
        if (every and epoch % every == 0) or (first_class and kind not in seen):
            selected.add(epoch)
        seen.add(kind)
    if batches:
        selected.add(batches[-1]['epoch'])
    return selected


def replay_stats(completed, started_ms, finished_ms):
    rows = sum(b['projected_row_changes'] for b in completed)
    wire_ms = sum(b['update_ms'] for b in completed)
    elapsed_ms = finished_ms-started_ms
    return dict(epochs=len(completed), projected_row_changes=rows, wire_ms=wire_ms,
                started_ms=started_ms, finished_ms=finished_ms, elapsed_ms=elapsed_ms,
                projected_rows_per_second_wire=1000*rows/wire_ms if wire_ms else None,
                projected_rows_per_second_replay=1000*rows/elapsed_ms if elapsed_ms else None)


class ReferenceAnswers:
    """Previously validated captures, independent of the server's worker count."""
    def __init__(self, root, report, manifest, name, binding):
        self.root, self.captures, self.reason = root, {}, 'not requested'
        if root is None:
            return
        previous = json.loads((root/'report.json').read_text())
        journal = previous['journal']
        compatible = (previous.get('reference_sha256') == report['reference_sha256']
                      and previous.get('plan_sha256',{}).get(name) == report['plan_sha256'][name]
                      and journal['initial_sha256'] == manifest['initial_sha256']
                      and [(b['epoch'],b['sha256']) for b in journal['batches']]
                          == [(b['epoch'],b['sha256']) for b in manifest['batches']])
        self.reason = 'different inputs, plan, or reference code'
        if not compatible:
            return
        path = root/f'{name}.json'
        if not path.is_file():
            self.reason = 'query has no saved reference checks'
            return
        query = json.loads(path.read_text())
        self.reason = 'different request binding'
        if query.get('binding') != json.loads(json.dumps(binding)):
            return
        self.reason = None
        captures = [(0,query.get('initial',{})), *[(c['epoch'],c) for c in query['checks']]]
        self.captures = {epoch:c for epoch,c in captures if c.get('status') == 'passed' and 'actual_path' in c}

    def read(self, epoch):
        capture = self.captures.get(epoch)
        if capture is None:
            return None
        actual = json.loads((self.root/capture['actual_path']).read_text())
        if hashlib.sha256(json.dumps(actual).encode()).hexdigest() != capture['answer_sha256']:
            raise ValueError(f'reused reference capture checksum mismatch: {capture["actual_path"]}')
        return actual, dict(run=str(self.root), epoch=epoch, answer_sha256=capture['answer_sha256'])


def run_query(args, name, plan, manifest, report):
    started_query = time.perf_counter()
    record = dict(query=name, status='running', events=[], updates=[], checks=[],
                  reference_calls=[], epoch=0, last_validated_epoch=-1)
    report['queries'].append(record)
    budget = Budget(None, args.max_total_rss_gib)
    graph = server = None
    reuse = ReferenceAnswers(None,report,manifest,name,[])
    selected = checkpoint_epochs(manifest['batches'], args.check_mode, args.check_every,
                                 args.check_first_class, args.check_from)
    deferred = args.check_mode == 'replay'
    if deferred:
        (args.output/'answers').mkdir(exist_ok=True)
    try:
        record['phase'] = 'load-snapshot'
        phase_started = time.perf_counter()
        with gzip.open(args.prepared/'initial.pickle.gz', 'rb') as source:
            graph = pickle.load(source)
        record['snapshot_load_ms'] = 1000*(time.perf_counter()-phase_started)
        phase_started = time.perf_counter()
        binding = parameters(graph, name)
        binding.update(args.parameters.get(name, {}))
        active = requests(name, plan['parameters'], binding, 0)
        record['binding'] = sorted(active)
        record['binding_prepare_ms'] = 1000*(time.perf_counter()-phase_started)
        with Server(args.server, args.output/f'{name}.log', args.workers, args.timeout, args.max_rss_gib) as server:
            previous = budget
            previous.close()
            budget = Budget(server, args.max_total_rss_gib)
            budget.peak, budget.peak_footprint = previous.peak, previous.peak_footprint
            def command(phase, make):
                record['phase'] = phase
                metrics, replies = measure.commands(server, make)
                record['events'].append(dict(epoch=record['epoch'], phase=phase, **metrics))
                return replies

            def compare(actual, want, label):
                try:
                    checked(actual, want, f'{name}/{label}')
                except AssertionError:
                    prefix = args.output/f'{name}-epoch-{record["epoch"]:04}'
                    write_json(prefix.with_suffix('.actual.json'), actual)
                    write_json(prefix.with_suffix('.expected.json'), want)
                    write_json(prefix.with_suffix('.parameters.json'), {name: binding})
                    command_line = [sys.executable, str(HERE/'update_stream.py'), '--prepared', str(args.prepared),
                                    '--server', str(args.server), '--queries', name, '--workers', str(args.workers),
                                    '--parameters', str(prefix.with_suffix('.parameters.json')),
                                    '--batch-limit', str(record['epoch']), '--check-mode', 'strict',
                                    '--check-from', str(max(1,record['last_validated_epoch']+1)),
                                    '--timeout', str(args.timeout), '--reference-timeout', str(args.reference_timeout),
                                    '--max-rss-gib', str(args.max_rss_gib),
                                    '--max-total-rss-gib', str(args.max_total_rss_gib),
                                    '--output', str(prefix)+'-replay']
                    if getattr(args, 'plan', None):
                        command_line += ['--plan', str(args.output/f'{name}.ddp')]
                    record['reproduce'] = shlex.join(command_line)
                    record['failure_interval'] = dict(after_epoch=record['last_validated_epoch'],
                                                      through_epoch=record['epoch'])
                    raise

            def expected():
                record['phase'] = 'reference'
                started = time.perf_counter()
                try:
                    want = timed_reference(args.reference_timeout, plan['plan'], plan['sources'], graph, active)
                finally:
                    duration = 1000*(time.perf_counter()-started)
                    record['reference_calls'].append(dict(epoch=record['epoch'], duration_ms=duration,
                                                         started_ms=1000*(started-started_query)))
                return want, duration

            def read(label):
                want, reference_ms = expected() if not deferred else (None,None)
                replies, = command('peek', lambda:[f'peek {name}.answer as corgi'])
                metrics = record['events'][-1]
                actual = measure.decode_answer(replies, metrics)
                result = dict(rows=len(actual), peek_ms=metrics['wire_ms'],
                              answer_sha256=hashlib.sha256(json.dumps(actual).encode()).hexdigest())
                if deferred:
                    path = Path('answers')/f'{name}-{record["epoch"]:04}.json'
                    write_json(args.output/path,actual)
                    result.update(status='captured',actual_path=str(path))
                else:
                    compare(actual, want, label)
                    result.update(status='passed',reference_ms=reference_ms)
                    record['last_validated_epoch'] = record['epoch']
                return result

            def validate(capture, label):
                record['phase'] = 'validate-capture'
                actual = json.loads((args.output/capture['actual_path']).read_text())
                if hashlib.sha256(json.dumps(actual).encode()).hexdigest() != capture['answer_sha256']:
                    raise ValueError(f'captured answer checksum mismatch: {capture["actual_path"]}')
                reused = reuse.read(record['epoch'])
                if reused is None:
                    want, reference_ms = expected()
                else:
                    want, provenance = reused
                    reference_ms = 0
                    capture['reference_reused_from'] = provenance
                compare(actual,want,label)
                capture.update(status='passed',reference_ms=reference_ms)
                record['last_validated_epoch'] = record['epoch']

            # With `--feed frames` (the default), every feed is one binary frame at its input's
            # declared shapes, so the harness's own encoding is not what is measured.
            frames = args.feed == 'frames'
            graph_shape = [(parse_shape(source_shape(schema)), 'unit') if frames else None for schema in data.SCHEMA.values()]
            request_shape = (parse_shape(source_shape(plan['parameters'])), 'unit') if frames else None
            for program, text in [('graph', graph_program()), (name, plan['program'])]:
                command('install', lambda:[f'load {program} begin\n{text}\n{{rid}} end-load','tick'])
            record['snapshot_sort_ms'] = 0
            for i, table in enumerate(data.SCHEMA):
                phase_started = time.perf_counter()
                rows = sorted(graph[table])
                record['snapshot_sort_ms'] += 1000*(time.perf_counter()-phase_started)
                for offset in range(0,len(rows),1000):
                    command('load', lambda:[Server.feed('graph',i,rows[offset:offset+1000],1,graph_shape[i])])
            command('bind', lambda:[Server.feed(name,0,sorted(active),1,request_shape),'tick'])
            record['initial'] = read('initial')
            print(f'{name}/initial: {record["initial"]["status"]}', flush=True)
            if deferred:
                # Release the Python snapshot during the server replay. Its
                # independent replay and oracle evaluation happen after exit.
                graph = None
                gc.collect()
            replay_started = 1000*(time.perf_counter()-started_query)
            for batch in manifest['batches']:
                record.update(epoch=batch['epoch'], current=batch)
                with gzip.open(args.prepared/batch['path'],'rt',encoding='utf-8') as source:
                    changes = json.load(source)['changes']
                if not deferred:
                    updates.apply_delta(graph, changes)
                def feed():
                    commands = []
                    for i, table in enumerate(data.SCHEMA):
                        if table in changes:
                            for side,diff in (('removed',-1),('added',1)):
                                rows = changes[table][side]
                                for offset in range(0,len(rows),1000):
                                    commands.append(Server.feed('graph',i,rows[offset:offset+1000],diff,graph_shape[i]))
                    return [*commands,'tick']
                command('update', feed)
                metrics = record['events'][-1]
                update = dict(epoch=batch['epoch'], day=batch['day'], update_class=batch['update_class'],
                              update_ms=metrics['wire_ms'], prepare_ms=metrics['prepare_ms'],
                              projected_row_changes=sum(len(d['removed'])+len(d['added']) for d in changes.values()),
                              combined_peak_rss_bytes=budget.peak,
                              combined_peak_physical_footprint_bytes=budget.peak_footprint)
                record['updates'].append(update)
                if batch['epoch'] in selected:
                    check = dict(update,**read(batch['path']))
                    record['checks'].append(check)
                    if not deferred:
                        write_json(args.output/f'{name}.json', record)
                        print(f'{name}/{batch["day"]}/{batch["update_class"]}: checked; update {check["update_ms"]:.3f} ms', flush=True)
            record['replay'] = replay_stats(record['updates'], replay_started,
                                            1000*(time.perf_counter()-started_query))
            record['replay']['includes_reference'] = not deferred
            command('retire', lambda:[Server.feed(name,0,sorted(active),-1,request_shape),'tick'])
            actual, = command('empty', lambda:[f'peek {name}.answer as corgi'])
            compare(decode(actual), [], 'retired')
            record['retirement_checked'] = True
        if deferred:
            record['status'] = 'validating'
            write_json(args.output/f'{name}.json',record)
            stats = record['replay']
            print(f'{name}/replay: {stats["epochs"]} epochs, {stats["projected_row_changes"]} projected row changes, '
                  f'{stats["projected_rows_per_second_replay"] or 0:.0f} rows/s including checkpoint capture; '
                  f'validating {len(record["checks"])+1} checkpoints',flush=True)
            previous = budget
            previous.close()
            budget = Budget(None,args.max_total_rss_gib)
            budget.peak, budget.peak_footprint = previous.peak, previous.peak_footprint
            reuse = ReferenceAnswers(getattr(args,'reference_from',None),report,manifest,name,record['binding'])
            record['reference_reuse'] = dict(run=str(reuse.root) if reuse.root else None,
                                             eligible_epochs=sorted(reuse.captures), reason=reuse.reason)
            captures = {0:record['initial'], **{c['epoch']:c for c in record['checks']}}
            record.update(epoch=0)
            record.pop('current',None)
            if captures.keys() <= reuse.captures.keys():
                # Every checkpoint already has an oracle-validated answer: no
                # second snapshot load or Python journal replay is needed.
                for epoch,capture in captures.items():
                    record['epoch'] = epoch
                    validate(capture,f'epoch-{epoch}')
                    write_json(args.output/f'{name}.json',record)
            else:
                record['phase'] = 'load-validation-snapshot'
                phase_started = time.perf_counter()
                with gzip.open(args.prepared/'initial.pickle.gz','rb') as source:
                    graph = pickle.load(source)
                record['validation_snapshot_load_ms'] = 1000*(time.perf_counter()-phase_started)
                replay = record['validation_replay'] = dict(epochs=0,decode_ms=0,apply_ms=0)
                validate(record['initial'],'initial')
                for batch in manifest['batches']:
                    record.update(epoch=batch['epoch'],current=batch)
                    phase_started = time.perf_counter()
                    with gzip.open(args.prepared/batch['path'],'rt',encoding='utf-8') as source:
                        changes = json.load(source)['changes']
                    decoded_at = time.perf_counter()
                    updates.apply_delta(graph,changes)
                    replay['decode_ms'] += 1000*(decoded_at-phase_started)
                    replay['apply_ms'] += 1000*(time.perf_counter()-decoded_at)
                    replay['epochs'] += 1
                    if batch['epoch'] in captures:
                        validate(captures[batch['epoch']],batch['path'])
                        write_json(args.output/f'{name}.json',record)
        record['status'] = 'passed'
    except BaseException as error:
        if isinstance(error, KeyboardInterrupt) and not (budget and budget.failure):
            record['status'] = 'interrupted'
            raise
        resource = (budget.failure if budget else None) or (server.failure if server else None)
        record.update(status='failed', error=(resource or f'{type(error).__name__}: {error}'),
                      failure_kind='resource' if resource else 'wrong-answer' if isinstance(error,AssertionError)
                      else 'timeout' if isinstance(error,TimeoutError) else 'error')
        print(f'{name}: FAILED at epoch {record["epoch"]}: {record["error"]}', flush=True)
        if deferred and record['failure_kind']=='wrong-answer' and record.get('reproduce'):
            # The timed server has exited. Retry the suspect interval with an
            # oracle check at every epoch; preserve both observations if timing
            # or another difference makes the strict replay pass.
            budget.close()
            graph = None
            gc.collect()
            destination = args.output/f'{name}-epoch-{record["epoch"]:04}-investigation'
            command_line = shlex.split(record['reproduce'])
            command_line[command_line.index('--output')+1] = str(destination)
            log = args.output/f'{name}-investigation.log'
            with log.open('wb') as target:
                result = subprocess.run(command_line,stdout=target,stderr=subprocess.STDOUT)
            record['investigation'] = dict(command=shlex.join(command_line),output=str(destination),
                                           log=str(log),returncode=result.returncode,reproduced=False)
            path = destination/f'{name}.json'
            if path.exists():
                retry = json.loads(path.read_text())
                record['investigation'].update(reproduced=retry.get('failure_kind')=='wrong-answer',
                                                first_bad_epoch=retry.get('epoch') if retry.get('failure_kind')=='wrong-answer' else None,
                                                status=retry['status'])
    finally:
        if server:
            record['peak_server_rss_bytes_sampled'] = server.peak_rss
        if budget:
            budget.close()
            record['peak_combined_rss_bytes_sampled'] = budget.peak
            record['peak_combined_physical_footprint_bytes_sampled'] = budget.peak_footprint
        record['elapsed_ms'] = 1000*(time.perf_counter()-started_query)
        write_json(args.output/f'{name}.json', record)
        summary(report, args.output)
        write_json(args.output/'report.json', report)
    del graph
    gc.collect()


def summary(report, output):
    rows = []
    classes = sorted({b['update_class'] for b in report['journal']['batches']})
    by_name = {record['query']:record for record in report['queries']}
    planned = set(report.get('validation',{}).get('epochs',[b['epoch'] for b in report['journal']['batches']]))
    for name in report.get('selected_queries',by_name):
        record = by_name.get(name,dict(query=name,checks=[],epoch=0))
        for kind in classes:
            captures = [c for c in record['checks'] if c['update_class']==kind]
            checks = [c for c in captures if c.get('status')=='passed']
            completed = [b for b in record.get('updates',[]) if b['update_class']==kind]
            expected = sum(b['update_class']==kind for b in report['journal']['batches'])
            checkpoints = sum(b['update_class']==kind and b['epoch'] in planned for b in report['journal']['batches'])
            failed_here = record.get('current',{}).get('update_class') == kind
            status = ('passed' if len(checks)==expected else
                      record.get('failure_kind','incomplete') if failed_here else
                      'initial-'+record['failure_kind'] if record.get('failure_kind') and not record['epoch'] else
                      'not-run' if name not in by_name else
                      'incomplete' if len(completed)<expected else
                      'awaiting-validation' if len(checks)<checkpoints else
                      'checkpoint-validated' if checks else 'exercised-unchecked')
            rows.append(dict(query=record['query'], update_class=kind, status=status,
                checked=len(checks), checkpoints=checkpoints, completed_batches=len(completed), batches=expected,
                prepared_batches=report['journal'].get('prepared_class_batches',{}).get(kind,expected),
                workers=report['workers'],
                update_median_ms=statistics.median(c['update_ms'] for c in completed) if completed else '',
                update_max_ms=max((c['update_ms'] for c in completed),default=''),
                projected_row_changes=sum(c['projected_row_changes'] for c in completed),
                peek_median_ms=statistics.median(c['peek_ms'] for c in checks) if checks else '',
                reference_median_ms=statistics.median(c['reference_ms'] for c in checks) if checks else '',
                peak_combined_rss_bytes=record.get('peak_combined_rss_bytes_sampled',0),
                peak_combined_physical_footprint_bytes=record.get('peak_combined_physical_footprint_bytes_sampled',0)))
    if rows:
        with (output/'coverage.csv').open('w',newline='') as target:
            writer=csv.DictWriter(target,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)


def main():
    # Background shells can pass SIGINT as ignored. The memory budget uses
    # interrupt_main(), so restore Python's handler before starting any work.
    signal.signal(signal.SIGINT, signal.default_int_handler)
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data',type=Path,help='BI directory containing initial_snapshot/, inserts/, deletes/')
    parser.add_argument('--prepared',type=Path,help='reuse an existing exact delta journal')
    parser.add_argument('--prepare-only',action='store_true')
    parser.add_argument('--server',type=Path)
    parser.add_argument('--output',type=Path,required=True,help='new artifact directory')
    parser.add_argument('--classes',nargs='+',choices=list(updates.CLASSES),default=list(updates.CLASSES),
                        help='classes to prepare; omitted classes are never applied')
    parser.add_argument('--batch-limit',type=int,help='check this many epochs (0 checks only the initial snapshot)')
    parser.add_argument('--first-day')
    parser.add_argument('--last-day')
    parser.add_argument('--queries',nargs='+',default=['all'])
    parser.add_argument('--plan',type=Path,
                        help='supplied DDP for one selected query; Python checks its usual logical definition')
    parser.add_argument('--parameters',type=Path,help='query-name -> one parameter override object')
    parser.add_argument('--workers',type=positive,default=1)
    parser.add_argument('--feed',choices=('text','frames'),default='frames',
                        help='feed rows as text lines, or as binary frames of columns')
    parser.add_argument('--timeout',type=positive,default=120)
    parser.add_argument('--reference-timeout',type=positive,default=120)
    parser.add_argument('--check-mode',choices=('strict','replay'),default='strict',
                        help='strict: reference after every epoch; replay: capture checkpoints, validate after server exit')
    parser.add_argument('--check-every',type=int,default=64,
                        help='replay mode: periodic checkpoint interval; 0 disables periodic checkpoints')
    parser.add_argument('--check-first-class',action=argparse.BooleanOptionalAction,default=True,
                        help='replay mode: also capture the first epoch of each update class')
    parser.add_argument('--check-from',type=positive,default=1,
                        help='strict replay investigation: skip intermediate checks before this epoch')
    parser.add_argument('--reference-from',type=Path,
                        help='replay: reuse validated captures from a compatible run, otherwise evaluate Python')
    parser.add_argument('--max-rss-gib',type=positive,default=6)
    parser.add_argument('--max-total-rss-gib',type=positive,default=10)
    args=parser.parse_args()
    if args.batch_limit is not None and args.batch_limit < 0:
        parser.error('--batch-limit must be nonnegative')
    if args.check_every < 0:
        parser.error('--check-every must be nonnegative')
    if args.check_mode=='replay' and args.check_from!=1:
        parser.error('--check-from applies to strict investigations only')
    if args.reference_from:
        if args.check_mode != 'replay':
            parser.error('--reference-from applies to replay mode only')
        args.reference_from = args.reference_from.resolve(strict=True)
        if args.reference_from == args.output.resolve():
            parser.error('--reference-from must name a different run')
    if args.prepared and (args.first_day or args.last_day or args.classes != list(updates.CLASSES)):
        parser.error('--classes/--first-day/--last-day select raw batches during preparation only')
    if not args.prepared and not args.data:
        parser.error('--data or --prepared is required')
    if not args.prepare_only and not args.server:
        parser.error('--server is required')
    args.output=args.output.resolve()
    args.output.mkdir(parents=True,exist_ok=True)
    if any(args.output.iterdir()):
        parser.error('--output must be empty or new')
    if args.prepared:
        args.prepared=args.prepared.resolve(strict=True)
        manifest=json.loads((args.prepared/'manifest.json').read_text())
        if manifest['adapter_sha256']!=adapter_hashes():
            parser.error('prepared journal adapter differs from this checkout')
        for path, sha in [('initial.pickle.gz', manifest['initial_sha256']),
                          *((b['path'], b['sha256']) for b in manifest['batches'])]:
            if digest(args.prepared/path) != sha:
                parser.error(f'prepared journal checksum mismatch: {path}')
        manifest['prepared_class_batches']=dict(Counter(b['update_class'] for b in manifest['batches']))
        manifest['prepared_epochs']=len(manifest['batches'])
        if args.batch_limit is not None:
            manifest['batches'] = manifest['batches'][:args.batch_limit]
    else:
        args.prepared=args.output/'journal'
        budget=Budget(None,args.max_total_rss_gib)
        try:
            manifest=prepare(args.data.resolve(strict=True),args.prepared,args.classes,args.batch_limit,args.first_day,args.last_day)
        except KeyboardInterrupt:
            if budget.failure:
                raise MemoryError(budget.failure) from None
            raise
        finally:
            budget.close()
    if args.prepare_only:
        return
    args.server=args.server.resolve(strict=True)
    names=[]
    for selector in args.queries:
        selected=CATALOGUE if selector=='all' else [n for n in CATALOGUE if n.startswith(selector)] if selector in ('is','ic','bi') else [selector]
        for name in selected:
            if name not in CATALOGUE:
                parser.error(f'unknown query {name}')
            if name not in names:
                names.append(name)
    args.parameters=json.loads(args.parameters.read_text()) if args.parameters else {}
    plans=compile_queries(names)
    if args.plan:
        if len(names) != 1:
            parser.error('--plan requires exactly one selected query')
        args.plan = args.plan.resolve(strict=True)
        plans[names[0]]['program'] = args.plan.read_text()
    for name,plan in plans.items():
        (args.output/f'{name}.ddp').write_text(plan['program'])
    revision=subprocess.run(['git','rev-parse','HEAD'],capture_output=True,text=True,check=True).stdout.strip()
    status=subprocess.run(['git','status','--porcelain'],capture_output=True,text=True,check=True).stdout
    validation=dict(mode=args.check_mode,every=args.check_every if args.check_mode=='replay' else 1,
                    first_each_class=args.check_first_class,check_from=args.check_from,
                    epochs=[0,*sorted(checkpoint_epochs(manifest['batches'],args.check_mode,args.check_every,
                                                       args.check_first_class,args.check_from))])
    report=dict(format_version='snb-update-coverage-2',status='running',spec_commit=SPEC,revision=revision,
                validation=validation,
                git_status=status,plan_sha256={n:digest(args.output/f'{n}.ddp') for n in names},
                command=shlex.join([sys.executable,*sys.argv]),
                runner_sha256=digest(Path(__file__)),
                reference_sha256={n:digest(HERE/n) for n in ('snb/rel.py','snb/parameters.py','snb/queries.py')},
                limits=dict(command_timeout_s=args.timeout,reference_timeout_s=args.reference_timeout,
                            server_rss_gib=args.max_rss_gib,total_rss_gib=args.max_total_rss_gib),
                host=platform.node(),platform=platform.platform(),workers=args.workers,binary_sha256=digest(args.server),
                adapter_sha256=adapter_hashes(),journal=manifest,prepared=str(args.prepared),
                selected_queries=names,queries=[])
    write_json(args.output/'report.json',report)
    try:
        for name in names:
            run_query(args,name,plans[name],manifest,report)
        report['status']='passed' if all(q['status']=='passed' for q in report['queries']) else 'failed'
    except BaseException:
        report['status']='interrupted'
        raise
    finally:
        summary(report,args.output)
        write_json(args.output/'report.json',report)
    print(f'Report: {args.output/"report.json"}',flush=True)
    if report['status']!='passed':
        raise SystemExit(1)


if __name__=='__main__':
    main()
