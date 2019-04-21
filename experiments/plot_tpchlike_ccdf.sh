commit=5b0a6d19e5

mkdir -p plots/$commit/i-tpchlike-mixing
temp_dir=$(mktemp -d)
echo 'temp_dir' $temp_dir

colors=("red" "blue")

common="set terminal pdf size 5.2cm,3.5cm font \"Arial,10\"; set logscale x; set format x \"%.0s %cs\"; set xlabel \"latency\" offset 0,.5; set ylabel \"complementary cdf\" offset 1.5,0; set arrow from graph 0, 0.5 to graph 1, 0.5 nohead lt rgb \"red\"; set label \"median\" at graph .75, 0.55 font \",9\" textcolor \"red\"; set arrow from graph 0, 0.1 to graph 1, 0.1 nohead lt rgb \"red\"; set label \"p90\" at graph .84, 0.15 font \",9\" textcolor \"red\"; set key right top Left reverse font \",9\"; "

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="$common plot "
  echo GROUP $g
  dt=4
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | grep '\[MEASURE\]' | awk '{print $5}' | hdrhist ccdf > $temp_dir/install-$f
    if [ "$(echo $f | cut -d '_' -f 7)" == "arrange=false" ]; then
      title="not shared"
    else
      title="shared"
    fi
    plotscript="$plotscript \"$temp_dir/install-$f\" using (\$1/1000000000):2 with lines lw 2 dt ($dt, 2) title \"$title\", "
    dt=$(expr $dt + 4)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/ccdf-install-$g.pdf
done

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="$common plot "
  echo GROUP $g
  dt=4
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | grep '\[MEASURE\]' | awk '{print $6}' | hdrhist ccdf > $temp_dir/uninstall-$f
    if [ "$(echo $f | cut -d '_' -f 7)" == "arrange=false" ]; then
      title="not shared"
    else
      title="shared"
    fi
    plotscript="$plotscript \"$temp_dir/uninstall-$f\" using (\$1/1000000000):2 with lines lw 2 dt ($dt, 2) title \"$title\", "
    dt=$(expr $dt + 4)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/ccdf-uninstall-$g.pdf
done

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="$common plot "
  echo GROUP $g
  dt=4
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | grep '\[MEASURE\]' | awk '{print $7}' | hdrhist ccdf > $temp_dir/work-$f
    if [ "$(echo $f | cut -d '_' -f 7)" == "arrange=false" ]; then
      title="not shared"
    else
      title="shared"
    fi
    plotscript="$plotscript \"$temp_dir/work-$f\" using (\$1/1000000000):2 with lines lw 2 dt ($dt, 2) title \"$title\", "
    dt=$(expr $dt + 4)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/ccdf-work-$g.pdf
done

rm -R $temp_dir
