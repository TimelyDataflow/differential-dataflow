"""Interactive v2 operations: one per entity at its own time, cascades equal to updates.State."""
import gzip
from pathlib import Path
import tempfile
import unittest

from snb import operations, updates
from test_updates import graph


def copy(g):
    return {name: set(rows) for name, rows in g.items()}


class CascadeTests(unittest.TestCase):
    def same(self, steps):
        """Each step gives Cascade and the scanning oracle the same delta and graph."""
        oracle, cascade = updates.State(copy(graph())), operations.Cascade(copy(graph()))
        for kind, rows in steps:
            self.assertEqual(cascade.apply(kind, rows), oracle.apply(kind, rows)['changes'], kind)
            self.assertEqual(cascade.graph, oracle.graph, kind)

    def test_every_delete_class_matches_the_oracle(self):
        cases = [
            ('delete-person', {'Person': [{'id': '1'}]}),
            ('delete-forum', {'Forum': [{'id': '10'}]}),
            ('delete-post', {'Post': [{'id': '100'}]}),
            ('delete-comment', {'Comment': [{'id': '200'}]}),
            ('delete-member', {'Forum_hasMember_Person': [{'ForumId': '12', 'PersonId': '1'}]}),
            ('delete-knows', {'Person_knows_Person': [{'Person1Id': '2', 'Person2Id': '1'}]}),
            ('delete-like-post', {'Person_likes_Post': [{'PersonId': '3', 'PostId': '100'}]}),
        ]
        for kind, rows in cases:
            with self.subTest(kind=kind):
                self.same([(kind, rows)])

    def test_sequences_with_inserts_and_repeated_targets(self):
        comment = dict(id='205', creationDate='2012-12-01T00:00:00Z', locationIP='1.2.3.4', browserUsed='Firefox',
                       content='quoted | é', length='10', CreatorPersonId='3', LocationCountryId='0',
                       ParentPostId='', ParentCommentId='201')
        self.same([('insert-comment', {'Comment': [comment], 'Comment_hasTag_Tag': [{'CommentId': '205', 'TagId': '9'}]}),
                   ('delete-person', {'Person': [{'id': '1'}]}),
                   ('delete-post', {'Post': [{'id': '100'}]}),
                   ('delete-post', {'Post': [{'id': '100'}]}),
                   ('delete-person', {'Person': [{'id': '2'}]})])


class OperationTests(unittest.TestCase):
    def test_entities_carry_their_attribute_edges_and_run_in_time_order(self):
        files = {
            'inserts/dynamic/Person/batch_id=2012-12-01/part-0.csv':
                'creationDate|id|firstName|lastName|gender|birthday|locationIP|browserUsed|LocationCityId|language|email\n'
                '2012-12-01T05:00:00.000+00:00|7|A|B|male|1990-01-01|1.1.1.1|Chrome|0|en|a@b\n',
            'inserts/dynamic/Person_hasInterest_Tag/batch_id=2012-12-01/part-0.csv':
                'creationDate|personId|interestId\n2012-12-01T05:00:00.000+00:00|7|3\n2012-12-01T05:00:00.000+00:00|7|4\n',
            'deletes/dynamic/Person/batch_id=2012-12-01/part-0.csv.gz':
                'deletionDate|id\n2012-12-01T01:00:00.000+00:00|1\n',
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for kinds in updates.CLASSES.values():
                for entity in kinds:
                    for phase in ('inserts', 'deletes'):
                        (root / phase / 'dynamic' / entity).mkdir(parents=True, exist_ok=True)
            for name, text in files.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                if name.endswith('.gz'):
                    with gzip.open(path, 'wt') as f:
                        f.write(text)
                else:
                    path.write_text(text)
            ops = operations.operations(root)
        self.assertEqual([op.kind for op in ops], ['delete-person', 'insert-person'])
        self.assertLess(ops[0].time, ops[1].time)
        self.assertEqual(sorted(r['TagId'] for r in ops[1].rows['Person_hasInterest_Tag']), ['3', '4'])
        self.assertEqual(ops[1].rows['Person_studyAt_University'], [])


if __name__ == '__main__':
    unittest.main()
