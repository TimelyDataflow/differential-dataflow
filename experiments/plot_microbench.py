import shutil, tempfile
from executor import execute
from os import listdir
import sys
import math

commit = sys.argv[sys.argv.index('--commit') + 1]
experiment = sys.argv[sys.argv.index('--experiment') + 1]

# commit = "dirty-e74441d0c062c7ec8d6da9bbf1972bd9397b2670"
# experiment = "arrange-open-loop"
print("commit {}, experiment {}".format(commit, experiment))


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
    return (name, sorted([parsekv(kv) for kv in params], key=lambda x: x[0]))

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

    filtering = { ('w', 1), ('comp', 'maintain'), ('dparam', 50), ('work', 4), ('recs', 32000000), ('inputstrategy', 'ms'), }
    F = filtering
    eprint(F)
    # print('\n'.join(str(p) for p, f in sorted(filedict, key=lambda x: dict(x[0])['rate']) if p.issuperset(F)))
    plotscript = "set terminal pdf size 4.8cm,3.2cm; set logscale x; set logscale y; " \
            "set bmargin at screen 0.2; " \
            "set xrange [10000:5000000000.0]; " \
            "set key samplen 2; " \
            "set format x \"10^{%T}\"; " \
            "set yrange [0.0001:1.01]; " \
            "set xlabel \"nanoseconds\"; " \
            "set format y \"10^{%T}\"; " \
            "set bmargin at screen 0.25; " \
            "set ylabel \"complementary cdf\"; " \
            "set key left bottom Left reverse font \",10\"; " \
            "plot "

    rates_plotted = set()
    dt = 2
    for p, f in sorted(filedict, key=lambda x: dict(x[0])['rate']):
        eprint(p)
        if p.issuperset(F) and dict(p)['rate'] not in rates_plotted and (dict(p)['keys'] / 10) == dict(p)['rate']:
            rates_plotted.add(dict(p)['rate'])
            eprint(p)
            datafile = "{}/i_load_varies_{}".format(tempdir, f)
            assert(execute('cat results/{}/{}/{} | grep LATENCYFRACTION | cut -f 3,4 > {}'.format(commit, experiment, f, datafile)))
            plotscript += "\"{}\" using 1:2 with lines lw 2 dt ({}, 2) title \"{}\", ".format(datafile, dt, dict(p)['rate'])
            dt += 2

    plotscript += "\n"

    assert(execute('mkdir -p plots/{}/{}'.format(commit, experiment)))
    eprint(plotscript)
    assert(execute('gnuplot > plots/{}/{}/i_load_varies_{}.pdf'.format(commit, experiment, groupingstr(F)), input=plotscript))
    eprint('plots/{}/{}/i_load_varies_{}.pdf'.format(commit, experiment, groupingstr(F)))

    shutil.rmtree(tempdir)

def ii_strong_scaling(): # commit = "dirty-e74441d0c062c7ec8d6da9bbf1972bd9397b2670" experiment = "arrange-open-loop"
    tempdir = tempfile.mkdtemp("{}-{}".format(experiment, commit))

    filtering = { ('rate', 1000000), }
    for work in params['work']:
        for comp in { 'maintain', }:
            F = filtering.union({ ('work', work), ('comp', comp), ('keys', 10000000), ('recs', 32000000), ('inputstrategy', 'ms'), })
            eprint(F)
            # print('\n'.join(str(p) for p, f in sorted(filedict, key=lambda x: dict(x[0])['rate']) if p.issuperset(F)))
            plotscript = "set terminal pdf size 4.8cm,3.2cm; set logscale x; set logscale y; " \
                    "set bmargin at screen 0.2; " \
                    "set xrange [10000:5000000000.0]; " \
                    "set key samplen 2; " \
                    "set format x \"10^{%T}\"; " \
                    "set yrange [0.0001:1.01]; " \
                    "set xlabel \"nanoseconds\"; " \
                    "set format y \"10^{%T}\"; " \
                    "set bmargin at screen 0.25; " \
                    "set ylabel \"complementary cdf\"; " \
                    "set key left bottom Left reverse font \",10\"; " \
                    "plot "

            w_plotted = set()
            dt = 2
            for p, f in sorted(filedict, key=lambda x: dict(x[0])['w']):
                if p.issuperset(F) and dict(p)['w'] not in w_plotted:
                    w_plotted.add(dict(p)['w'])
                    datafile = "{}/ii_strong_scaling_{}".format(tempdir, f)
                    assert(execute('cat results/{}/{}/{} | grep LATENCYFRACTION | cut -f 3,4 > {}'.format(commit, experiment, f, datafile)))
                    plotscript += "\"{}\" using 1:2 with lines lw 2 dt ({}, 2) title \"{}\", ".format(datafile, dt, dict(p)['w'])
                    dt += 2

            assert(execute('mkdir -p plots/{}/{}'.format(commit, experiment)))
            eprint(plotscript)
            assert(execute('gnuplot > plots/{}/{}/ii_strong_scaling_{}.pdf'.format(commit, experiment, groupingstr(F)), input=plotscript))
            eprint('plots/{}/{}/ii_strong_scaling_{}.pdf'.format(commit, experiment, groupingstr(F)))

    shutil.rmtree(tempdir)

def iii_weak_scaling(): # commit = "dirty-e74441d0c062c7ec8d6da9bbf1972bd9397b2670" experiment = "arrange-open-loop"
    tempdir = tempfile.mkdtemp("{}-{}".format(experiment, commit))

    # TODO filtering = { ('dparam', 10), ('keys', 10000000), ('recs', 32000000), }
    filtering = { ('dparam', 50), ('inputstrategy', 'ms'), }
    for work in params['work']:
        for comp in {'arrange', 'maintain', 'count'}:
            F = filtering.union({ ('work', work), ('comp', comp), })
            eprint(F)
            # print('\n'.join(str(p) for p, f in sorted(filedict, key=lambda x: dict(x[0])['rate']) if p.issuperset(F)))
            plotscript = "set terminal pdf size 4.8cm,3.2cm; set logscale x; set logscale y; " \
                    "set bmargin at screen 0.2; " \
                    "set xrange [10000:5000000000.0]; " \
                    "set key samplen 2; " \
                    "set format x \"10^{%T}\"; " \
                    "set yrange [0.0001:1.01]; " \
                    "set xlabel \"nanoseconds\"; " \
                    "set format y \"10^{%T}\"; " \
                    "set bmargin at screen 0.25; " \
                    "set ylabel \"complementary cdf\"; " \
                    "set key left bottom Left reverse font \",10\"; " \
                    "plot "

            dt = 2
            data = False
            w_plotted = set()
            for p, f in sorted(filedict, key=lambda x: dict(x[0])['w']):
                if p.issuperset(F):
                    eprint(p)
                if (p.issuperset(F) and
                        dict(p)['rate'] == dict(p)['w'] * 1000000 and
                        dict(p)['recs'] == dict(p)['w'] * (32000000 // 32) and
                        dict(p)['keys'] == dict(p)['w'] * (10000000 // 32) and
                        dict(p)['w'] not in w_plotted):
                    w_plotted.add(dict(p)['w'])
                    data = True
                    datafile = "{}/iii_weak_scaling_{}".format(tempdir, f)
                    assert(execute('cat results/{}/{}/{} | grep LATENCYFRACTION | cut -f 3,4 > {}'.format(commit, experiment, f, datafile)))
                    plotscript += "\"{}\" using 1:2 with lines lw 2 dt ({}, 2) title \"{}\", ".format(datafile, dt, dict(p)['w'])
                    dt += 2

            if data:
                assert(execute('mkdir -p plots/{}/{}'.format(commit, experiment)))
                eprint(plotscript)
                assert(execute('gnuplot > plots/{}/{}/iii_weak_scaling_{}.pdf'.format(commit, experiment, groupingstr(F)), input=plotscript))
                eprint('plots/{}/{}/iii_weak_scaling_{}.pdf'.format(commit, experiment, groupingstr(F)))

    shutil.rmtree(tempdir)


def iv_throughput(): # commit = "dirty-e74441d0c062c7ec8d6da9bbf1972bd9397b2670" experiment = "arrange"
    tempdir = tempfile.mkdtemp("{}-{}".format(experiment, commit))

    filtering = { ('rate', 100000), ('work', 4), ('keys', 10000000), ('recs', 32000000), ('inputstrategy', 'ms'), }

    plotscript = "set terminal pdf size 4.8cm,3.2cm; " \
            "set bmargin at screen 0.2; " \
            "set xrange [-1:34]; " \
            "set yrange [-10000000:160000000]; " \
            "set xtics (1, 4, 8, 16, 32); " \
            "set ytics 50000000; " \
            "set xlabel \"cores\"; " \
            "set bmargin at screen 0.25; " \
            "set ylabel \"throughput (records/s)\"; " \
            "set format y \"%.0s%c\"; " \
            "set key left top Left reverse font \",10\"; " \
            "plot "

            # "set format y \"10^{%T}\"; " \
            # "set ytics offset 0.7; " \

    pt = [4, 6, 8]
    titlemap = {
            'arrange': 'batch formation',
            'maintain': 'trace maintenance',
            'count': 'count',
    }
    for comp in ['arrange', 'maintain', 'count']:
        F = filtering.union({ ('comp', comp), })
        # eprint(comp, groupingstr(filtering), [x for p, x in filedict if p.issuperset(F)])
        datafile = "{}/iv_throughput_{}".format(tempdir, groupingstr(F))
        execute('echo "" > {}'.format(datafile))
        for p, f in sorted(filedict, key=lambda x: dict(x[0])['w']):
            if p.issuperset(F):
                eprint("results/{}/{}/{}".format(commit, experiment, f))
                assert(execute('cat results/{}/{}/{} | grep THROUGHPUT | cut -f 3,4 >> {}'.format(commit, experiment, f, datafile)))
        plotscript += "\"{}\" using 1:2 with linespoints pointtype {} pointsize 0.4 title \"{}\", ".format(datafile, pt[0], titlemap[comp])
        pt = pt[1:]

    plotscript += "\n"

    assert(execute('mkdir -p plots/{}/{}'.format(commit, experiment)))
    eprint(plotscript)
    # plotfile = "{}/iv_throughput_plot.gnuplot".format(tempdir)
    assert(execute('gnuplot > plots/{}/{}/iv_throughput_{}.pdf'.format(commit, experiment, groupingstr(filtering)), input=plotscript))
    eprint('plots/{}/{}/iv_throughput_{}.pdf'.format(commit, experiment, groupingstr(filtering)))

    # shutil.rmtree(tempdir)

def v_amortization(): # USE STRONG SCALING
    tempdir = tempfile.mkdtemp("{}-{}".format(experiment, commit))

    filtering = { ('keys', 10000000), ('recs', 32000000), ('rate', 1000000), ('inputstrategy', 'ms'),  }
    for work in params['work']:
        for comp in {'arrange', 'maintain', 'count'}:
            F = filtering.union({ ('comp', comp), })
            eprint(F)
            # print('\n'.join(str(p) for p, f in sorted(filedict, key=lambda x: dict(x[0])['rate']) if p.issuperset(F)))
            plotscript = "set terminal pdf size 4.8cm,3.2cm; set logscale x; set logscale y; " \
                    "set bmargin at screen 0.2; " \
                    "set xrange [10000:5000000000.0]; " \
                    "set key samplen 2; " \
                    "set format x \"10^{%T}\"; " \
                    "set yrange [0.0001:1.01]; " \
                    "set xlabel \"nanoseconds\"; " \
                    "set format y \"10^{%T}\"; " \
                    "set bmargin at screen 0.25; " \
                    "set ylabel \"complementary cdf\"; " \
                    "set key left bottom Left reverse font \",10\"; " \
                    "plot "
            data = False
            w_plotted = set()
            for p, f in sorted(filedict, key=lambda x: (dict(x[0])['w'], str(dict(x[0])['work']))):
                if p.issuperset(F) and dict(p)['w'] in [1, 32] and (dict(p)['w'], dict(p)['work']) not in w_plotted:
                    w_plotted.add((dict(p)['w'], dict(p)['work']))
                    eprint(p)
                    data = True
                    datafile = "{}/v_amortization_{}".format(tempdir, f)
                    assert(execute('cat results/{}/{}/{} | grep LATENCYFRACTION | cut -f 3,4 > {}'.format(commit, experiment, f, datafile)))
                    w_dt = ({
                        1: 1,
                        32: 4,
                    })[dict(p)['w']]
                    work_dt = ({
                        1: 4,
                        4: 8,
                        'max': 12,
                    })[dict(p)['work']]
                    work_s = ({
                        1: 'lazy',
                        4: 'default',
                        'max': 'eager',
                    })[dict(p)['work']]

                    plotscript += "\"{}\" using 1:2 with lines lw 2 dt ({}, 2, {}, 2) title \"{}\", ".format(datafile, work_dt, w_dt, "{}, {}".format(dict(p)['w'], work_s))

            if data:
                assert(execute('mkdir -p plots/{}/{}'.format(commit, experiment)))
                eprint(plotscript)
                assert(execute('gnuplot > plots/{}/{}/v_amortization_{}.pdf'.format(commit, experiment, groupingstr(F)), input=plotscript))
                eprint('plots/{}/{}/v_amortization_{}.pdf'.format(commit, experiment, groupingstr(F)))

    shutil.rmtree(tempdir)

def vi_install():
    tempdir = tempfile.mkdtemp("{}-{}".format(experiment, commit))

    filtering = { ('keys', 10000000), ('recs', 32000000), ('w', 32), }
    # print('\n'.join(str(p) for p, f in sorted(filedict, key=lambda x: dict(x[0])['rate']) if p.issuperset(F)))
    plotscript = "set terminal pdf size 4.8cm,3.2cm; set logscale x; set logscale y; " \
            "set bmargin at screen 0.2; " \
            "set xrange [10000:5000000000.0]; " \
            "set key samplen 2; " \
            "set format x \"10^{%T}\"; " \
            "set yrange [0.0001:1.01]; " \
            "set xlabel \"nanoseconds\"; " \
            "set format y \"10^{%T}\"; " \
            "set bmargin at screen 0.25; " \
            "set ylabel \"complementary cdf\"; " \
            "set key left bottom Left reverse font \",10\"; " \
            "plot "

    dt = 2
    for p, f in filedict: #sorted(filedict, key=lambda x: dict(x[0])['w']):
        if p.issuperset(filtering):
            eprint(p)
            for size in [2**x for x in [0, 8, 16, 17, 18, 19, 20]]:
                datafile = "{}/vi_install_{}_{}".format(tempdir, f, size)
                eprint(datafile)
                assert(execute('cat results/{}/{}/{} | grep LATENCY | awk \'$3 == {}\' | cut -f 3-5 > {}'.format(commit, experiment, f, size, datafile)))
                plotscript += "\"{}\" using 2:3 with lines lw 2 dt ({}, 2) title \"{}\", ".format(datafile, dt, "2^{{{}}}".format(int(math.log2(size))))
                dt += 2

    assert(execute('mkdir -p plots/{}/{}'.format(commit, experiment)))
    eprint(plotscript)
    assert(execute('gnuplot > plots/{}/{}/vi_install_{}.pdf'.format(commit, experiment, groupingstr(filtering)), input=plotscript))
    eprint('plots/{}/{}/vi_install_{}.pdf'.format(commit, experiment, groupingstr(filtering)))

    # shutil.rmtree(tempdir)
