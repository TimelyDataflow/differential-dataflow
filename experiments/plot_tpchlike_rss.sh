commit=5b0a6d19e5

mkdir -p plots/$commit/i-tpchlike-mixing
temp_dir=$(mktemp -d)
echo 'temp_dir' $temp_dir

colors=("red" "blue")

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="set terminal pdf size 5.2cm,3.5cm font \"Arial,10\"; set logscale y; set yrange [10000000000:120000000000.0]; set style line 1 lw 2 lc \"#38618C\" dt 1; set style line 2 lw 2 lc \"#28A361\" dt 1; set format y \"%.0s%cB\"; set ytics (10000000000.0,30000000000.0,60000000000.0,90000000000.0); set xtics 1000,2000; set xlabel \"round\" offset 0,.5; set ylabel \"resident set size\"; unset key; set tmargin 0.5; set rmargin 1.0; set label \"not shared\" at graph .7, graph .68 font \",9\" textcolor \"#38618C\"; set label \"shared\" at graph .7, graph .34 font \",9\" textcolor \"#28A361\"; set xrange [-200:]; plot "
  echo GROUP $g
  dt=1
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | awk '/\[MEASURE\]/{ if (round != $3) { counter = 0 }; round = $3 }; /\[RSS\]/{ if (round > 0) { printf "%d.%02d %d\n", round, counter, $3; counter += 1 } }' > $temp_dir/rss-$f

    if [ "$(echo $f | cut -d '_' -f 7)" == "arrange=false" ]; then
      title="not shared"
    else
      title="shared"
    fi

    plotscript="$plotscript \"$temp_dir/rss-$f\" using 1:2 with lines ls $dt title \"$title\", "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/rss-$g.pdf
done

rm -R $temp_dir
