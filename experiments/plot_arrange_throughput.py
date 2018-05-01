import shutil, tempfile
from executor import execute
from os import listdir
import sys

commit = "dirty-e74441d0c062c7ec8d6da9bbf1972bd9397b2670"
print("commit {}".format(commit))

def eprint(*args):
    print(*args, file=sys.stderr)

def parsename(name):
    kvs = name.split('_')
    name, params = kvs[0], kvs[1:]
    def parsekv(kv):
        k, v = kv.split('=')
        if v.isdigit():
            v = int(v)
        return (k, v)
    return (name, [parsekv(kv) for kv in params])

allfiles = [(parsename(f), f) for f in listdir("results/{}/arrange".format(commit))]
allparams = list(zip(*allfiles))[0]
pivoted = list(zip(*list(zip(*allparams))[1]))
assert(all(all(y[0] == x[0][0] for y in x) for x in pivoted))
params = dict(list((x[0][0], set(list(zip(*x))[1])) for x in pivoted))
filedict = list((set(p[1]), f) for p, f in allfiles)
eprint(params)
# print(filedict)

tempdir = tempfile.mkdtemp("arrange-{}".format(commit))
# for f in list(zip(*allfiles))[1]:
filtering = { ('rate', 2000000), ('work', 4) }

def groupingstr(s):
    return '_'.join("{}={}".format(x[0], str(x[1])) for x in s)

plotscript = "set terminal pdf;  set title \"throughput\"; plot "
for comp in {'arrange', 'maintain', 'count'}:
    eprint(comp, groupingstr(filtering))
    F = filtering.union({ ('comp', comp), })
    datafile = "{}/{}".format(tempdir, groupingstr(F))
    execute('echo "" > {}'.format(datafile))
    for p, f in sorted(filedict, key=lambda x: dict(x[0])['w']):
        if p.issuperset(F):
            assert(execute('cat results/{}/arrange/{} | grep THROUGHPUT | cut -f 3,4 >> {}'.format(commit, f, datafile)))
    plotscript += "\"{}\" using 1:2 with linespoints title \"{}\", ".format(datafile, comp)

assert(execute('mkdir -p plots/{}/arrange'.format(commit)))
eprint(plotscript)
assert(execute('gnuplot > plots/{}/arrange/{}.pdf'.format(commit, groupingstr(filtering)), input=plotscript))
eprint('plots/{}/arrange/{}.pdf'.format(commit, groupingstr(filtering)))

# for comp in params['comp']:
#     plot = 
# 
# for w in sorted(params['w']) for :
#     print(w)

shutil.rmtree(tempdir)
