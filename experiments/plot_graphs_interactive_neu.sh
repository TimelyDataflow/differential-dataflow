commit=dirty-a1fada910fb5725708e9bfd2b0e4ce921824455c

# mkdir -p plots/$commit/graphs-interactive-neu
# 
# for f in `ls results/$commit/graphs-interactive-neu`; do
#   grep LATENCY < results/$commit/graphs-interactive-neu/$f | cut -f2-3 | gnuplot -p -e "set terminal pdf; set logscale x; set logscale y; set xrange [100000:1000000000]; set yrange [0.005:1.01]; set title \"$f\" noenhanced; unset key; plot \"/dev/stdin\" using 1:2 with lines lc black lw 2" > plots/$commit/graphs-interactive-neu/$f.pdf
# done

mkdir -p plots/$commit/graphs-interactive-neu-throughput
temp_dir=$(mktemp -d)

for g in `ls results/$commit/graphs-interactive-neu | cut -d '_' -f 3,8-10 | sort | uniq`; do
  plotscript="set terminal pdf; set logscale x; set logscale y; set xrange [100000:10000000000.0]; set yrange [0.005:1.01]; set title \"$g\" noenhanced; plot "
  echo GROUP $g
  dt=2
  for file in `ls results/$commit/graphs-interactive-neu/*_$(echo $g | sed 's/_/_*/')`; do
    f=$(basename $file)
    echo $f
    grep LATENCY < $file | cut -f2-3 > $temp_dir/$f
    plotscript="$plotscript \"$temp_dir/$f\" using 1:2 with lines lc black lw 2 dt $dt title \"$(echo $f | cut -d '_' -f 6)\", "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/graphs-interactive-neu-throughput/$g.pdf
done

rm -R $temp_dir
