#!/usr/bin/env python3
"""Late query installation and retired requests preserve the fixture's answers."""
import argparse
import gzip
import json
from pathlib import Path
import pickle
import subprocess
import sys

from snb import updates,witness
from snb.parameters import parameters,reference,requests
from suite import compile_queries
from test_update_stream import fixture
from summarize_memory import summarize


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--server',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    args=p.parse_args();args.output.mkdir(parents=True)
    fixture(args.output/'journal')
    with gzip.open(args.output/'journal/initial.pickle.gz','rb') as f:initial=pickle.load(f)
    final={name:set(rows) for name,rows in initial.items()}
    manifest=json.loads((args.output/'journal/manifest.json').read_text())
    for batch in manifest['batches']:
        with gzip.open(args.output/'journal'/batch['path'],'rt') as f:updates.apply_delta(final,json.load(f)['changes'])
    expected=args.output/'expected';expected.mkdir()
    bank={};names=['is1','ic9','bi15','bi17']
    for name,plan in compile_queries(names).items():
        bank[name]=parameters(initial,name);bank[name].update(witness.params(name))
        active=requests(name,plan['parameters'],bank[name],0)
        (expected/(name+'.json')).write_text(json.dumps(dict(validated=True,
            initial=reference(plan['plan'],plan['sources'],{**initial,'request':active}),
            final=reference(plan['plan'],plan['sources'],{**final,'request':active})))+'\n')
    (args.output/'parameters.json').write_text(json.dumps(bank)+'\n')
    subprocess.run([sys.executable,str(Path(__file__).with_name('profile_memory.py')),
        '--prepared',str(args.output/'journal'),'--server',str(args.server.resolve()),
        '--parameters',str(args.output/'parameters.json'),'--expected',str(expected),
        '--queries',*names,'--output',str(args.output/'profiles')],check=True)
    rows=summarize(args.output/'profiles')
    assert len(rows)==len(names)
    for row in rows:
        assert row['status']=='profiled' and row['active_final_rss_gib']>0
        assert row['after_retirement_rss_gib']>0
        assert row['initial_answer']==row['final_answer']=='matched-saved-validated-answer'
    # Reports must remain usable after an artifact directory changes machines.
    campaign_path=args.output/'profiles/campaign.json'
    campaign=json.loads(campaign_path.read_text())
    for entry in campaign['results']:entry['report']='/unavailable/original/host/report.json'
    campaign_path.write_text(json.dumps(campaign)+'\n')
    assert summarize(args.output/'profiles')==rows
    print('Four late-installed queries match Python at initial/final states and retire; input control and summary complete.')


if __name__=='__main__':main()
