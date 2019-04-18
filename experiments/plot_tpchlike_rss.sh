commit=dirty-780a95ec9f

mkdir -p plots/$commit/i-tpchlike-mixing
temp_dir=$(mktemp -d)
echo 'temp_dir' $temp_dir

colors=("red" "blue")

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="set terminal png truecolor enhanced size 600,400; set logscale y; set key bottom right; set yrange [10000000000:120000000000.0]; set title \"rss_$g\" noenhanced; set style fill transparent solid 0.01 noborder; set format y \"%.0s %cB\"; plot "
  echo GROUP $g
  dt=0
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | awk '/\[MEASURE\]/{ if (round != $3) { counter = 0 }; round = $3 }; /\[RSS\]/{ if (round > 0) { printf "%d.%02d %d\n", round, counter, $3; counter += 1 } }' > $temp_dir/rss-$f
    plotscript="$plotscript \"$temp_dir/rss-$f\" using (\$1/1000000000):2 with lines lw 2 dt $dt lc \"${colors[$dt]}\" title \"$(echo $f | cut -d '_' -f 7)\", "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/rss-$g.png
done

rm -R $temp_dir
