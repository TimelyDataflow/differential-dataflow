"""Snapshot dialect and timing boundaries, independent of a running server."""
import gzip
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import measure
from readout import account
from snapshot import csv_rows
from snb import data
import workload


class HarnessTests(unittest.TestCase):
    def test_snapshot_readers_share_the_spark_dialect(self):
        text = '\n'.join((
            'id|name|TypeTagClassId',
            '1|"a|b"|2',
            r'2|"say \"hi\" and \\ end"|2',
            r'3|unquoted\literal|2',
            '4|"é\nsecond line"|2',
            '5|after multiline|2',
            '',
        ))
        want = {(1, 'a|b'), (2, 'say "hi" and \\ end'), (3, r'unquoted\literal'),
                (4, 'é\nsecond line'), (5, 'after multiline')}
        with tempfile.TemporaryDirectory() as folder:
            snapshot = Path(folder)
            for entity in (*data.ENTITIES, *data.STATIC):
                directory = snapshot / ('static' if entity in data.STATIC else 'dynamic') / entity
                directory.mkdir(parents=True)
                (directory / 'part.csv').write_text('id\n', encoding='utf-8')
            tag = snapshot / 'static' / 'Tag' / 'part.csv'
            compressed = tag.with_suffix('.csv.gz')
            # Plain, compressed, and a partition plus its decompressed copy
            # must all represent the same rows, exactly once.
            for layout in ('plain', 'both', 'gzip'):
                with self.subTest(layout=layout):
                    if layout == 'plain':
                        tag.write_text(text, encoding='utf-8')
                    elif layout == 'both':
                        with gzip.open(compressed, 'wt', encoding='utf-8', newline='') as out:
                            out.write(text)
                    else:
                        tag.unlink()
                    self.assertEqual(workload.load(snapshot)['tag'], want)
                    self.assertEqual({row[:2] for row in data.load(snapshot)['tag']}, want)
        self.assertEqual(list(csv_rows(io.StringIO('id|name\n1|""\n2|\n'))),
                         [{'id': '1', 'name': ''}, {'id': '2', 'name': ''}])
        with self.assertRaisesRegex(ValueError, 'field count'):
            list(csv_rows(io.StringIO('id|name\n1|extra|field\n')))

    def test_phase_metrics_remain_separate(self):
        class FakeServer:
            def commands(self, commands):
                self.commands_seen = commands
                return dict(encode_ms=3000, wire_ms=5000), [[]]
        server = FakeServer()
        # Deliberately distinct durations. No wall-clock thresholds or sleeps.
        with patch('measure.time.perf_counter', side_effect=[0, 2, 10, 20, 27]):
            metrics, replies = measure.commands(server, lambda: ['peek answer'])
            self.assertEqual(measure.decode_answer(replies[0], metrics), [])
        self.assertEqual(server.commands_seen, ['peek answer'])
        self.assertEqual({k: metrics[k] for k in measure.TIMINGS},
                         dict(prepare_ms=2000, encode_ms=3000, wire_ms=5000,
                              decode_ms=7000, client_ms=17000))
        event = dict(metrics, warmup=False, state='initial', phase='read')
        summary = measure.summarize([event, dict(event, warmup=True, wire_ms=999999)])
        self.assertEqual(summary['initial/read']['wire_ms']['median_ms'], 5000)
        self.assertEqual(summary['initial/read']['client_ms']['median_ms'], 17000)
        self.assertEqual(measure.total([event, event]), {k: 2*metrics[k] for k in measure.TIMINGS})

    def test_readout_counts_cycles_without_hiding_retirement(self):
        def event(state, phase, amount, **extra):
            return dict({m: amount for m in measure.TIMINGS}, state=state, phase=phase,
                        round=0, warmup=False, **extra)
        events = [event('setup', 'initial_tick', 100),
                  event('initial', 'bind', 2), event('initial', 'read:q', 3),
                  event('initial', 'batch_answers', 5, derived=True),
                  event('restored', 'restore', 7),
                  event('restored', 'empty:q', 11, answer_sha256='checked'),
                  dict(event('initial', 'bind', 50), round=-1, warmup=True),
                  dict(event('retired', 'retire', 200), warmup=True)]
        run = dict(queries=['q'], events=events)
        totals = account(run, 1)
        self.assertEqual({k: v['wire_ms'] for k, v in totals.items()},
                         {'setup': 100, 0: 23, 'warmup': 50, 'retirement': 200})
        with self.assertRaisesRegex(ValueError, 'incomplete measured rounds'):
            account(run, 2)
        with self.assertRaisesRegex(ValueError, 'missing checked restored answer'):
            account(dict(run, events=[e for e in events if e['phase'] != 'empty:q']), 1)


if __name__ == '__main__':
    unittest.main()
