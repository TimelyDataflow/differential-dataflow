"""Read-suite witnesses, hand-checked answers, and both engine backends."""
import unittest

from snb.queries import Context,QUERIES
from snb.rel import R,Compiler,SourceRows,c,evaluate
from snb.parameters import parameters,reference
from snb import witness


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
        self.assertEqual(self.rows('bi15')[0][0],1.0)
        changed=witness.changed(self.graph)
        self.assertEqual(self.rows('ic13',changed),[[3]])
        self.assertEqual(self.rows('ic14',changed),[])
        self.assertAlmostEqual(self.rows('bi15',changed)[0][0],8/3)

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

    def test_ic3_counts_messages_that_project_to_the_same_fields(self):
        changed = {name:set(rows) for name,rows in self.graph.items()}
        for identity, original in ((20001,10009),(20002,10010)):
            message = next(row for row in self.graph['message'] if row[0] == original)
            changed['message'].add((identity,*message[1:]))
        self.assertEqual(self.rows('ic3',changed),
                         [[4,list(b'Alex'),list(b'Person4'),2,2,4]])

    def test_hand_checked_floating_aggregates(self):
        rows=self.rows('bi1')
        self.assertEqual([(r[0],r[1],r[2],r[3],r[5]) for r in rows],[(2012,0,0,31,120),(2012,1,0,4,16)])
        self.assertAlmostEqual(rows[0][4],120/31)
        self.assertAlmostEqual(rows[0][6],3100/35)
        self.assertEqual([(r[0],r[1],r[2],r[3]) for r in self.rows('bi13')],[(7,1,1,1.0),(2,0,1,0.0),(5,0,0,0.0)])

    def test_shared_reference_inputs_are_read_once(self):
        schema={'key':'int','value':'int'}
        source=R.source('rows',schema)
        left=source.select('key',value=c.value+1)
        right=source.where(c.value>=0).select('key',value=c.value*2)
        plan=left.union(right).group(('key',),n=('count',c.value),total=('sum',c.value))
        data={'rows':SourceRows(iter([(1,2),(1,2),(2,-1)]),schema)}
        self.assertEqual(evaluate(plan,data),
                         [{'key':1,'n':4,'total':14},{'key':2,'n':1,'total':0}])

    def test_explicit_reference_cache_spans_exports(self):
        schema={'key':'int','value':'int'}
        source=R.source('rows',schema)
        data={'rows':SourceRows(iter([(1,2),(1,2),(2,-1)]),schema)}
        cache={}
        first_plan=source.select('key',value=c.value+1)
        second_plan=source.select('key',value=c.value*2)
        first=evaluate(first_plan,data,cache)
        second=evaluate(second_plan,data,cache)
        self.assertEqual([row['value'] for row in first],[3,3,0])
        self.assertEqual([row['value'] for row in second],[4,4,-2])

    def test_ic12_class_filter_and_request_identity(self):
        self.assertEqual(self.rows('ic12',classname='Root'),self.rows('ic12'))
        self.assertEqual(self.rows('ic12',classname='Missing'),[])
        fn,schema,_=QUERIES['ic12']
        binding=parameters(self.graph,'ic12');binding.update(witness.params('ic12'))
        active={tuple(rid if field=='rid' else binding[field] for field in schema) for rid in (0,1)}
        plan=fn(Context(),R.source('request',schema));compiler=Compiler();compiler.compile(plan)
        rows=reference(plan,compiler.inputs,{**self.graph,'request':active})
        for rid in (0,1):
            self.assertEqual([row[1] for row in rows if row[0][0]==rid],self.rows('ic12'))

    def test_bi15_ignores_interactions_without_friendship(self):
        changed={name:set(rows) for name,rows in self.graph.items()}
        original=next(row for row in self.graph['message'] if row[0]==10004)
        for identity in range(30000,30020):
            row=list(original);row[0]=identity;row[3]=8
            changed['message'].add(tuple(row))
        self.assertEqual(self.rows('bi15',changed),self.rows('bi15'))

    def test_bi17_counts_a_discussion_once_across_witnesses(self):
        changed={name:set(rows) for name,rows in self.graph.items()}
        original=next(row for row in self.graph['message'] if row[0]==10001)
        changed['message'].add((30001,*original[1:]))
        changed['mtag'].update((30001,tag) for mid,tag in self.graph['mtag'] if mid==10001)
        changed['member'].update((forum,person,created+1) for forum,person,created in self.graph['member'])
        self.assertEqual(self.rows('bi17',changed),[[1,1]])
        changed['member'].add((1002,1,original[2]))
        self.assertEqual(self.rows('bi17',changed),[])



if __name__ == "__main__":
    unittest.main()
