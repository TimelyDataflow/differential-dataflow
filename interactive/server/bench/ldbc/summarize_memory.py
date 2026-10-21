#!/usr/bin/env python3
"""Rank saved process-memory profiles by query-active memory above input storage."""
import argparse
import csv
import json
from pathlib import Path


def summarize(root):
    root=Path(root)
    control=json.loads((root/'input-only/report.json').read_text())
    if control['status']!='profiled': raise ValueError('input-only control did not complete')
    campaign=json.loads((root/'campaign.json').read_text())
    peak=lambda q,key:max((p.get(key,0) for p in q.get('phase_peaks',{}).values()),default=0)
    rows=[]
    for entry in campaign['results']:
        if entry['query']=='input-only':continue
        path=root/entry['query']/'report.json'
        q=json.loads(path.read_text())
        if q.get('binary_sha256'):
            assert (q['binary_sha256'],q['initial_sha256'],q['workers'])==(control['binary_sha256'],control['initial_sha256'],control['workers'])
        points=q.get('points',{})
        def memory(point,key='rss_bytes'):
            return points[point][key]/2**30 if point in points and key in points[point] else None
        def extra(point):
            return (points[point]['rss_bytes']-control['points'][point]['rss_bytes'])/2**30 if point in points else None
        phase=q.get('phase')
        if phase=='settle' and 'before_retirement' in points and 'after_retirement' not in points:
            phase='post-retirement settling'
        rows.append(dict(query=q['query'],status=q['status'],failure_phase=phase,
            failure_kind=q.get('failure_kind'),error=q.get('error'),
            input_initial_rss_gib=control['points']['active_initial']['rss_bytes']/2**30,
            input_final_rss_gib=control['points']['active_final']['rss_bytes']/2**30,
            own_base_initial_rss_gib=memory('base_initial'),query_installed_rss_gib=memory('query_installed'),
            active_initial_rss_gib=memory('active_initial'),active_initial_extra_gib=extra('active_initial'),
            active_final_rss_gib=memory('active_final'),active_final_extra_gib=extra('active_final'),
            after_retirement_rss_gib=memory('after_retirement'),after_retirement_extra_gib=extra('after_retirement'),
            sampled_peak_rss_gib=peak(q,'rss_bytes')/2**30 if q.get('phase_peaks') else None,
            sampled_peak_extra_gib=(peak(q,'rss_bytes')-peak(control,'rss_bytes'))/2**30 if q.get('phase_peaks') else None,
            sampled_peak_phase=max(q.get('phase_peaks',{}),key=lambda p:q['phase_peaks'][p].get('rss_bytes',0),default=None),
            peak_censored=q['status']!='profiled',
            sampled_peak_physical_footprint_gib=peak(q,'physical_footprint_bytes')/2**30 or None,
            replay_s=q.get('replay_s'),elapsed_s=q.get('elapsed_s'),
            initial_answer=q.get('answer_validation',{}).get('initial'),final_answer=q.get('answer_validation',{}).get('final'),
            plan_sha256=q.get('plan_sha256'),source_revision=q.get('source_revision'),
            report=str(path.relative_to(root))))
    rows.sort(key=lambda q:(q['active_final_extra_gib'] is None,-(q['active_final_extra_gib'] or 0)))
    with (root/'memory.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(rows[0]),lineterminator='\n');writer.writeheader();writer.writerows(rows)
    (root/'memory.json').write_text(json.dumps(dict(scope='Observed process RSS above a separate matched input-only control. Includes allocator retention; not query-owned live bytes. Negative differences are preserved. Resource cutoffs censor peaks. Saved-answer matching is not fresh Python checking.',queries=rows),indent=2)+'\n')
    return rows


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path);args=p.parse_args()
    for r in summarize(args.directory):
        extra='unavailable' if r['active_final_extra_gib'] is None else f"{r['active_final_extra_gib']:.3f} GiB"
        peak='unavailable' if r['sampled_peak_extra_gib'] is None else f"{r['sampled_peak_extra_gib']:.3f} GiB"
        print(f"{r['query']:5} active extra {extra:>13}; peak extra {peak}; {r['status']} {r['failure_phase'] or ''}")


if __name__=='__main__':main()
