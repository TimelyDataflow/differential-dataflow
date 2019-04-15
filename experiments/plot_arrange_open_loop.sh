commit=dirty-92e7606c8f74178a0402325b01bd0dc5ad8a6f19

mkdir -p plots/$commit/arrange-open-loop

for f in `ls results/$commit/arrange-open-loop`; do
  title=`echo $f | cut -d '_' -f 2,3,6- | sed 's/_/\\\\n/g'`
  grep LATENCYFRACTION < results/$commit/arrange-open-loop/$f | cut -f3-4 | gnuplot -p -e "set terminal pdf; set logscale x; set logscale y; set xrange [100000:1000000000]; set title \"$title\" noenhanced; unset key; plot \"/dev/stdin\" using 1:2 with lines lc black lw 2" > plots/$commit/arrange-open-loop/$f.pdf
done

# mkdir -p plots/$commit/arrange-open-loop-throughput
# temp_dir=$(mktemp -d)
# 
# for g in `ls results/$commit/arrange-open-loop | cut -d '_' -f 3,8-10 | sort | uniq`; do
#   plotscript="set terminal pdf; set logscale x; set logscale y; set xrange [100000:1000000000]; set yrange [0.005:1.01]; set title \"$f\" noenhanced; plot "
#   echo GROUP $g
#   dt=2
#   for file in `ls results/$commit/arrange-open-loop/*$(echo $g | sed 's/_/*/')`; do
#     f=$(basename $file)
#     grep LATENCY < $file | cut -f2-3 > $temp_dir/$f
#     plotscript="$plotscript \"$temp_dir/$f\" using 1:2 with lines lc black lw 2 dt $dt title \"$(echo $f | cut -d '_' -f 6)\", "
#     dt=$(expr $dt + 1)
#   done
#   gnuplot -p -e "$plotscript" > plots/$commit/arrange-open-loop-throughput/$g.pdf
# done
# 
# rm -R $temp_dir
