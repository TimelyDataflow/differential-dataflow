import shutil, tempfile
from executor import execute
from os import listdir
import sys

commit = "dirty-e74441d0c062c7ec8d6da9bbf1972bd9397b2670"
experiment = "arrange"
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

allfiles = [(parsename(f), f) for f in listdir("results/{}/{}".format(commit, experiment))]
allparams = list(zip(*allfiles))[0]
pivoted = list(zip(*list(zip(*allparams))[1]))
assert(all(all(y[0] == x[0][0] for y in x) for x in pivoted))
params = dict(list((x[0][0], set(list(zip(*x))[1])) for x in pivoted))
filedict = list((set(p[1]), f) for p, f in allfiles)
eprint(params)

def groupingstr(s):
    return '_'.join("{}={}".format(x[0], str(x[1])) for x in sorted(s, key=lambda x: x[0]))

def i_load_varies(): # commit = "dirty-8380c53277307b6e9e089a8f6f79886b36e20428" experiment = "arrange-open-loop"
    tempdir = tempfile.mkdtemp("{}-{}".format(experiment, commit))

    filtering = { ('w', 16), }
    for work in params['work']:
        for comp in {'arrange', 'maintain', 'count'}:
            F = filtering.union({ ('work', work), ('comp', comp), })
            eprint(F)
            # print('\n'.join(str(p) for p, f in sorted(filedict, key=lambda x: dict(x[0])['rate']) if p.issuperset(F)))
            plotscript = "set terminal pdf size 6cm,4cm; set logscale x; set logscale y; " \
                    "set bmargin at screen 0.2; " \
                    "set xrange [50000:5000000000.0]; " \
                    "set format x \"10^{%T}\"; " \
                    "set yrange [0.005:1.01]; " \
                    "set xlabel \"nanoseconds\"; " \
                    "set format x \"10^{%T}\"; " \
                    "set ylabel \"complementary cdf\"; " \
                    "set key left bottom Left reverse font \",10\"; " \
                    "plot "
            dt = 2
            for p, f in sorted(filedict, key=lambda x: dict(x[0])['rate']):
                if p.issuperset(F):
                    datafile = "{}/i_load_varies_{}".format(tempdir, f)
                    assert(execute('cat results/{}/{}/{} | grep LATENCYFRACTION | cut -f 3,4 > {}'.format(commit, experiment, f, datafile)))
                    plotscript += "\"{}\" using 1:2 with lines lw 2 dt {} title \"{}\", ".format(datafile, dt, dict(p)['rate'])
                    dt += 1

            assert(execute('mkdir -p plots/{}/{}'.format(commit, experiment)))
            eprint(plotscript)
            assert(execute('gnuplot > plots/{}/{}/i_load_varies_{}.pdf'.format(commit, experiment, groupingstr(F)), input=plotscript))
            eprint('plots/{}/{}/i_load_varies_{}.pdf'.format(commit, experiment, groupingstr(F)))

    # shutil.rmtree(tempdir)


def iv_throughput(): # commit = "dirty-e74441d0c062c7ec8d6da9bbf1972bd9397b2670" experiment = "arrange"
    tempdir = tempfile.mkdtemp("{}-{}".format(experiment, commit))

    filtering = { ('rate', 10000), ('work', 4) }
    plotscript = "set terminal pdf size 6cm,4cm; " \
            "set bmargin at screen 0.2; " \
            "set format x \"10^{%T}\"; " \
            "set xlabel \"cores\"; " \
            "set ylabel \"throughput (records/s)\"; " \
            "set key left top Left reverse font \",10\"; " \
            "plot "
    for comp in {'arrange', 'maintain', 'count'}:
        F = filtering.union({ ('comp', comp), })
        # eprint(comp, groupingstr(filtering), [x for p, x in filedict if p.issuperset(F)])
        datafile = "{}/iv_throughput_{}".format(tempdir, groupingstr(F))
        execute('echo "" > {}'.format(datafile))
        for p, f in sorted(filedict, key=lambda x: dict(x[0])['w']):
            if p.issuperset(F):
                # eprint("results/{}/{}/{}".format(commit, experiment, f))
                assert(execute('cat results/{}/{}/{} | grep THROUGHPUT | cut -f 3,4 >> {}'.format(commit, experiment, f, datafile)))
        plotscript += "\"{}\" using 1:2 with linespoints title \"{}\", ".format(datafile, comp)

    assert(execute('mkdir -p plots/{}/{}'.format(commit, experiment)))
    eprint(plotscript)
    assert(execute('gnuplot > plots/{}/{}/iv_throughput_{}.pdf'.format(commit, experiment, groupingstr(filtering)), input=plotscript))
    eprint('plots/{}/{}/iv_throughput_{}.pdf'.format(commit, experiment, groupingstr(filtering)))

    shutil.rmtree(tempdir)


