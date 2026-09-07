"""Read-suite witnesses, hand-checked answers, and both engine backends."""
import struct
import unittest

from snb.queries import Context,QUERIES
from snb.rel import R,Compiler
from snb.parameters import parameters,reference
from snb import witness


def decode_float(encoded):
    ordered=(encoded['payload'] & ((1<<64)-1))^(1<<63)
    bits=ordered^(1<<63) if ordered>>63 else ~ordered & ((1<<64)-1)
    return struct.unpack('>d',struct.pack('>Q',bits))[0]


class SuiteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.graph=witness.graph()

    def rows(self,name,graph=None,**overrides):
        graph=self.graph if graph is None else graph
        fn,schema,_=QUERIES[name]
        params=parameters(graph);params.update(witness.params(name));params.update(overrides)
        plan=fn(Context(),R.source('request',schema));cc=Compiler();cc.compile(plan)
        rows=reference(plan,cc.inputs,{**graph,'request':{tuple(params[n] for n in schema)}})
        return [r[1] for r in sorted(rows,key=lambda r:r[0])]

    def test_complete_catalog_and_positive_witnesses(self):
        self.assertEqual(set(QUERIES),{f'is{i}' for i in range(1,8)}|{f'ic{i}' for i in range(1,15)}|{f'bi{i}' for i in range(1,21)})
        for name in QUERIES:
            with self.subTest(query=name): self.assertTrue(self.rows(name))

    def test_hand_checked_graph_answers(self):
        self.assertEqual(self.rows('bi11'),[[1]])
        self.assertEqual(self.rows('bi11',countryname='Gamma'),[[0]])
        self.assertEqual(self.rows('bi17'),[[1,1]])
        self.assertEqual(self.rows('bi20'),[[2,2]])
        self.assertEqual(self.rows('ic13'),[[2]])
        self.assertEqual(self.rows('ic13',p2=1),[[0]])
        self.assertEqual(self.rows('ic14'),[[[1,2,5],78]])
        self.assertEqual(decode_float(self.rows('bi15')[0][0]),1.0)
        changed=witness.changed(self.graph)
        self.assertEqual(self.rows('ic13',changed),[[3]])
        self.assertEqual(self.rows('ic14',changed),[])
        self.assertAlmostEqual(decode_float(self.rows('bi15',changed)[0][0]),8/3)

    def test_hand_checked_ranking_and_optional_matches(self):
        self.assertEqual([r[3] for r in self.rows('ic2')],list(range(10100,10120)))
        self.assertEqual(bytes(self.rows('ic2')[0][4]),b'photo.png')
        self.assertEqual([r[0] for r in self.rows('ic7')],[1,4,2])
        self.assertEqual([r[-1] for r in self.rows('ic7')],[1,1,0])
        self.assertEqual([r[4] for r in self.rows('ic7')],[10001,10001,10001])
        self.assertEqual([(bytes(r[0]).decode(),r[1]) for r in self.rows('ic4')],[('Topic',26),('Other',14),('Alphabet',12)])
        self.assertEqual(self.rows('bi12'),[[1,2],[0,2],[14,1],[13,1],[3,1],[2,1]])
        self.assertEqual(self.rows('bi12',language='',enabled=0),[[0,8]])
        # Empty nested collections for people without work history are real
        # typed empty lists, not missing rows or singleton placeholder tuples.
        rows=self.rows('ic1')
        self.assertEqual(len(rows),7)
        self.assertTrue(any(not r[-1] for r in rows))

    def test_ic3_depends_on_request_and_foreign_messages(self):
        # Person 4 lives in Gamma, is two hops from 1, and has one message
        # in each of Alpha and Beta. From 8, person 4 is beyond two hops.
        self.assertEqual(self.rows('ic3'), [[4, list(b'Alex'), list(b'Person4'), 1, 1, 2]])
        self.assertEqual(self.rows('ic3', **witness.alternate_params('ic3')), [])
        # Retract the only Alpha message (and its tags). One visited country
        # is insufficient even though the friendship paths still exist.
        changed = {name: set(rows) for name, rows in self.graph.items()}
        changed['message'] = {row for row in changed['message'] if row[0] != 10009}
        changed['mtag'] = {row for row in changed['mtag'] if row[0] != 10009}
        self.assertEqual(self.rows('ic3', changed), [])

    def test_hand_checked_floating_aggregates(self):
        rows=self.rows('bi1')
        self.assertEqual([(r[0],r[1],r[2],r[3],r[5]) for r in rows],[(2012,0,0,31,120),(2012,1,0,4,16)])
        self.assertAlmostEqual(decode_float(rows[0][4]),120/31)
        self.assertAlmostEqual(decode_float(rows[0][6]),3100/35)
        self.assertEqual([(r[0],r[1],r[2],decode_float(r[3])) for r in self.rows('bi13')],[(7,1,1,1.0),(2,0,1,0.0),(5,0,0,0.0)])



if __name__ == "__main__":
    unittest.main()
