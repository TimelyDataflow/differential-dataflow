"""Emit the benchmark programs without loading data or installing a server."""
import sys
from pathlib import Path

ldbc = Path(__file__).resolve().parents[2] / 'bench' / 'ldbc'
sys.path.insert(0, str(ldbc))
from snb.indexes import graph_program
from interactive_v2.graph import cascade_program

emitters = {'graph': graph_program, 'cascade': cascade_program}
sys.stdout.write(emitters[sys.argv[1]]())
