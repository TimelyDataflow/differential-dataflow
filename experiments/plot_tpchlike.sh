commit=dirty-156db7019f

# mkdir -p plots/$commit/graphs-interactive-neu
# 
# for f in `ls results/$commit/graphs-interactive-neu`; do
#   grep LATENCY < results/$commit/graphs-interactive-neu/$f | cut -f2-3 | gnuplot -p -e "set terminal pdf; set logscale x; set logscale y; set xrange [100000:1000000000]; set yrange [0.005:1.01]; set title \"$f\" noenhanced; unset key; plot \"/dev/stdin\" using 1:2 with lines lc black lw 2" > plots/$commit/graphs-interactive-neu/$f.pdf
# done

mkdir -p plots/$commit/i-tpchlike-mixing
temp_dir=$(mktemp -d)

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="set terminal png truecolor enhanced size 1000,600; set logscale y; set title \"install_$g\" noenhanced; set style fill transparent solid 0.01 noborder; set style circle radius 15; plot "
  echo GROUP $g
  dt=2
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | awk '{print $3, $7}' | cut -f2-3 > $temp_dir/install-$f
    plotscript="$plotscript \"$temp_dir/install-$f\" using 1:2 with circles title \"$(echo $f | cut -d '_' -f 7)\", "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/timeline-install-$g.png
done

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="set terminal png truecolor enhanced size 1000,600; set logscale y; set title \"update_$g\" noenhanced; set style fill transparent solid 0.01 noborder; set style circle radius 15; plot "
  echo GROUP $g
  dt=2
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | awk '{print $3, $11}' | cut -f2-3 > $temp_dir/update-$f
    plotscript="$plotscript \"$temp_dir/update-$f\" using 1:2 with circles title \"$(echo $f | cut -d '_' -f 7)\", "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/timeline-update-$g.png
done

rm -R $temp_dir
