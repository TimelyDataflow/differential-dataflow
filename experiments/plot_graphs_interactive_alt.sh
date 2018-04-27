commit=dirty-620a7265f62b04da41131f59b91d21dbaf07c70b

# mkdir -p plots/$commit/graphs-interactive-alt
# 
# for f in `ls results/$commit/graphs-interactive-alt`; do
#   grep LATENCY < results/$commit/graphs-interactive-alt/$f | cut -f2-3 | gnuplot -p -e "set terminal pdf; set logscale x; set logscale y; set xrange [100000:1000000000]; set yrange [0.005:1.01]; set title \"$f\" noenhanced; unset key; plot \"/dev/stdin\" using 1:2 with lines lc black lw 2" > plots/$commit/graphs-interactive-alt/$f.pdf
# done

mkdir -p plots/$commit/graphs-interactive-alt-throughput
temp_dir=$(mktemp -d)

for g in `ls results/$commit/graphs-interactive-alt | cut -d '_' -f 3,8-10 | sort | uniq`; do
  plotscript="set terminal pdf; set logscale x; set logscale y; set xrange [100000:1000000000]; set yrange [0.005:1.01]; set title \"$f\" noenhanced; plot "
  echo GROUP $g
  dt=2
  for file in `ls results/$commit/graphs-interactive-alt/*$(echo $g | sed 's/_/*/')`; do
    f=$(basename $file)
    grep LATENCY < $file | cut -f2-3 > $temp_dir/$f
    plotscript="$plotscript \"$temp_dir/$f\" using 1:2 with lines lc black lw 2 dt $dt title \"$(echo $f | cut -d '_' -f 6)\", "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/graphs-interactive-alt-throughput/$g.pdf
done

rm -R $temp_dir
