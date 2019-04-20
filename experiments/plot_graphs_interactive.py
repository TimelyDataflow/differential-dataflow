import shutil, tempfile
from executor import execute
from os import listdir
import sys

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

def prepare(commit, experiment):
    print("commit {}, experiment {}".format(commit, experiment))
    allfiles = [(parsename(f), f) for f in listdir("results/{}/{}".format(commit, experiment))]
    allparams = list(zip(*allfiles))[0]
    pivoted = list(zip(*list(zip(*allparams))[1]))
    assert(all(all(y[0] == x[0][0] for y in x) for x in pivoted))
    params = dict(list((x[0][0], set(list(zip(*x))[1])) for x in pivoted))
    filedict = list((set(p[1]), f) for p, f in allfiles)
    eprint(params)
    return (filedict, params)

def groupingstr(s):
    return '_'.join("{}={}".format(x[0], str(x[1])) for x in sorted(s, key=lambda x: x[0]))

def i_single_query():
    commit = "73399ca494"
    experiment = "graphs-interactive-neu-zwei"

    filedict, params = prepare(commit, experiment)

    tempdir = tempfile.mkdtemp("{}-{}".format(experiment, commit))

    filtering = { ('rate', 200000), ('w', 32), ('nodes', 10000000), ('edges', 32000000), }

    plotscript = "set terminal pdf size 5.2cm,3.5cm font \"Arial,10\"; set logscale x; set logscale y; " \
            "set bmargin at screen 0.2; " \
            "set xrange [10000:5000000000.0]; " \
            "set key samplen 2; " \
            "set format x \"10^{%T}\"; " \
            "set yrange [0.0001:1.01]; " \
            "set xlabel \"nanoseconds\" offset 0,0.3; " \
            "set format y \"10^{%T}\"; " \
            "set bmargin at screen 0.25; " \
            "set ylabel \"complementary cdf\"; " \
            "set key left bottom Left reverse font \",10\"; " \
            "plot "

    query_names = {
            1: 'lookup',
            2: '1-hop',
            3: '2-hop',
            4: '4-hop',
    }

    dt = 2
    for p, f in sorted(filedict, key=lambda x: dict(x[0])['query']):
        if p.issuperset(filtering):
            datafile = "{}/i_single_query_{}".format(tempdir, f)
            # assert(execute('cat results/{}/{}/{} > {}'.format(commit, experiment, f, datafile)))
            plotscript += "\"results/{}/{}/{}\" using 1:2 with lines lw 2 dt ({}, 2) title \"{}\", ".format(commit, experiment, f, dt, query_names[dict(p)['query']])
            dt += 3

    assert(execute('mkdir -p plots/{}/{}'.format(commit, experiment)))
    eprint(plotscript)
    execute('cat > {}/plotscript.plt'.format(tempdir), input=plotscript)
    assert(execute('gnuplot {}/plotscript.plt > plots/{}/{}/i_single_query_{}.pdf'.format(tempdir, commit, experiment, groupingstr(filtering)), input=plotscript))
    eprint('plots/{}/{}/i_single_query_{}.pdf'.format(commit, experiment, groupingstr(filtering)))

    shutil.rmtree(tempdir)

def ii_sharing():
    commit = "5b0a6d19e5"
    experiment = "graphs-interactive-neu"

    filedict, params = prepare(commit, experiment)

    tempdir = tempfile.mkdtemp("{}-{}".format(experiment, commit))
    eprint("tempdir {}".format(tempdir))

    filtering = { ('rate', 200000), ('w', 32), ('nodes', 10000000), ('edges', 32000000), ('goal', 1800), }

    plotscript = "set terminal pdf size 5.2cm,3.5cm font \"Arial,10\"; set logscale x; set logscale y; " \
            "set bmargin at screen 0.2; " \
            "set xrange [10000:5000000000.0]; " \
            "set key samplen 2; " \
            "set format x \"10^{%T}\"; " \
            "set yrange [0.0001:1.01]; " \
            "set xlabel \"nanoseconds\" offset 0,0.3; " \
            "set format y \"10^{%T}\"; " \
            "set bmargin at screen 0.25; " \
            "set ylabel \"complementary cdf\"; " \
            "set key left bottom Left reverse font \",10\"; " \
            "plot "

    shared_names = {
            'no': 'not shared',
            'shared': 'shared',
            }

    dt = 4
    for p, f in filedict:
        if p.issuperset(filtering):
            eprint(p)
            datafile = "{}/ii_sharing_{}".format(tempdir, f)
            assert(execute('cat results/{}/{}/{} | grep LATENCY | cut -f 2,3 > {}'.format(commit, experiment, f, datafile)))
            plotscript += "\"{}\" using 1:2 with lines lw 2 dt ({}, 2) title \"{}\", ".format(datafile, dt, shared_names[dict(p)['shared']])
            dt += 4

    assert(execute('mkdir -p plots/{}/{}'.format(commit, experiment)))
    eprint(plotscript)
    execute('cat > {}/plotscript.plt'.format(tempdir), input=plotscript)
    assert(execute('gnuplot {}/plotscript.plt > plots/{}/{}/ii_sharing_{}.pdf'.format(tempdir, commit, experiment, groupingstr(filtering)), input=plotscript))
    eprint('plots/{}/{}/ii_sharing_{}.pdf'.format(commit, experiment, groupingstr(filtering)))

    shutil.rmtree(tempdir)

def iv_boomerang():
    commit = "f74b611db1"
    experiment = "graphs-interactive-neu"

    filedict, params = prepare(commit, experiment)

    tempdir = tempfile.mkdtemp("{}-{}".format(experiment, commit))
    eprint("tempdir {}".format(tempdir))

    filtering = { ('w', 32), ('nodes', 10000000), ('edges', 32000000), ('goal', 1800), }

    plotscript = "set terminal pdf size 5.2cm,3.5cm font \"Arial,10\"; set logscale y; " \
            "set bmargin at screen 0.2; " \
            "set key samplen 2; " \
            "set format y \"10^{%T}\"; " \
            "set xlabel \"rate\" offset 0,0.3; " \
            "set xtics 100000; " \
            "set bmargin at screen 0.25; " \
            "set ylabel \"nanoseconds\"; " \
            "set key left bottom Left reverse font \",10\"; " \
            "plot "

    shared_names = {
            'no': 'not shared',
            'shared': 'shared',
            }

    dt = 4
    eprint(params['shared'])
    eprint(sorted(params['rate']))
    for sharing in params['shared']:
        sharing_filtering = filtering.union({ ('shared', sharing) })
        thisname = "_".join("{}={}".format(a, b) for a, b in sharing_filtering)
        datafile = "{}/iv_boomerang_{}".format(tempdir, thisname)
        for rate in sorted(params['rate']):
            rate_filtering = sharing_filtering.union({ ('rate', rate) })
            for p, f in filedict:
                if p.issuperset(rate_filtering):
                    eprint(p)
                    maxlat = execute('cat results/{}/{}/{} | grep LATENCY | awk \'{{ if ($3 < .001) {{ print $2 }} }}\' | cut -f 2 | head -n 1'.format(commit, experiment, f), capture=True)
                    execute('echo "{}" "{}" >> {}'.format(rate, maxlat, datafile))
        eprint(datafile)
        plotscript += "\"{}\" using 1:2 with linespoints lw 2 dt ({}, 2) title \"{}\", ".format(datafile, dt, shared_names[sharing])
        dt += 4

    assert(execute('mkdir -p plots/{}/{}'.format(commit, experiment)))
    eprint(plotscript)
    execute('cat > {}/plotscript.plt'.format(tempdir), input=plotscript)
    assert(execute('gnuplot {}/plotscript.plt > plots/{}/{}/iv_boomerang_{}.pdf'.format(tempdir, commit, experiment, groupingstr(filtering)), input=plotscript))
    eprint('plots/{}/{}/iv_boomerang_{}.pdf'.format(commit, experiment, groupingstr(filtering)))

    shutil.rmtree(tempdir)

def iii_memory_rss():
    commit = "5b0a6d19e5"
    experiment = "graphs-interactive-neu"

    filedict, params = prepare(commit, experiment)

    tempdir = tempfile.mkdtemp("{}-{}".format(experiment, commit))

    filtering = set()
    for rate in params['rate']:
        F = filtering.union({ ('rate', rate), ('goal', 1800) })
        eprint(F)

        plotscript = "set terminal pdf size 5.2cm,3.5cm font \"Arial,10\"; set logscale y; " \
                "set bmargin at screen 0.25; " \
                "set xrange [-200:3500]; " \
                "set yrange [1000000000:1000000000000.0]; " \
                "set xlabel \"elapsed seconds\" offset 0,0.3; " \
                "set xtics 0,1000,3000; " \
                "set ylabel \"resident set size\"; " \
                "set format y \"%.0s %cB\"; " \
                "set key left top Left reverse font \",10\"; " \
                "set style fill  transparent solid 0.35 noborder; " \
                "set style circle radius 10; " \
                "plot "

        shared_names = {
                'no': 'not shared',
                'shared': 'shared',
                }

        dt = 2
        for p, f in filedict:
            if p.issuperset(F):
                datafile = "{}/iii_memory_rss_{}".format(tempdir, f)
                assert(execute('cat results/{}/{}/{} | grep RSS | awk \'NR % 10 == 0\' | cut -f 2,3 > {}'.format(commit, experiment, f, datafile)))
                plotscript += "\"{}\" using ($1/1000000000):2 with lines lw 1.5 title \"{}\", ".format(datafile, shared_names[dict(p)['shared']])
                dt += 4

        assert(execute('mkdir -p plots/{}/{}'.format(commit, experiment)))
        eprint(plotscript)
        execute('cat > {}/plotscript.plt'.format(tempdir), input=plotscript)
        assert(execute('gnuplot {}/plotscript.plt > plots/{}/{}/iii_memory_rss_{}.pdf'.format(tempdir, commit, experiment, groupingstr(F)), input=plotscript))
        eprint('plots/{}/{}/iii_memory_rss_{}.pdf'.format(commit, experiment, groupingstr(F)))

