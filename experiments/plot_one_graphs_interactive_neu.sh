commit=dirty-a1fada910fb5725708e9bfd2b0e4ce921824455c

mkdir -p plots/$commit/graphs-interactive-neu-one
temp_dir=$(mktemp -d)

for g in `ls results/$commit/graphs-interactive-neu/*rate=1000000_* | cut -d '_' -f 3,6 | sort | uniq`; do
  plotscript="set terminal pdf; set logscale x; set logscale y; set xrange [100000:10000000000.0]; set yrange [0.005:1.01]; set title \"$g\" noenhanced; set style line 2 lc rgb \"black\"; set style line 2 lc rgb \"blue\"; plot "
  echo GROUP $g
  dt_shared=2
  dt_no=2
  for file in `ls results/$commit/graphs-interactive-neu/*_$(echo $g | sed 's/_/_*/g')* | sort -t _ -k 10 -nr`; do
    f=$(basename $file)
    echo $f '->' $dt
    grep LATENCY < $file | cut -f2-3 > $temp_dir/$f

    if [[ $file = *"shared=shared" ]]; then
      plotscript="$plotscript \"$temp_dir/$f\" using 1:2 with lines lt 1 lc 1 lw 2 dt ($dt_shared, 6) title \"$(echo $f | cut -d '_' -f 8-9 | sed 's/_/-/g')\", "
      dt_shared=$(expr $dt_shared + 3)
    else
      plotscript="$plotscript \"$temp_dir/$f\" using 1:2 with lines lt 2 lc 2 lw 2 dt ($dt_no, 6) title \"$(echo $f | cut -d '_' -f 8-9 | sed 's/_/-/g')\", "
      dt_no=$(expr $dt_no + 3)
    fi
  done
  gnuplot -p -e "$plotscript" > plots/$commit/graphs-interactive-neu-one/$g.pdf
done

gnuplot -p -e "\
set terminal pdf size 6cm,4cm;
 set logscale x;
 set logscale y;
 set rmargin at screen 0.9;
 set bmargin at screen 0.2;
 set xrange [50000:5000000000.0];
 set yrange [0.005:1.01];
 set xlabel \"nanoseconds\";
 set format x \"10^{%T}\";
 set ylabel \"complementary cdf\";
 set key left bottom Left reverse font \",10\";
 set style line 1 linecolor rgb \"#000000\";
 set style line 2 linecolor rgb \"#888888\";
 plot \
 \"$temp_dir/graphs-interactive-neu_n=1_w=16_nodes=10000000_edges=32000000_rate=1000000_goal=5_queries=1_shared=no\" using 1:2 with lines ls 2 lw 2 dt (2, 6) title \"1 q\", \
 \"$temp_dir/graphs-interactive-neu_n=1_w=16_nodes=10000000_edges=32000000_rate=1000000_goal=5_queries=10000_shared=no\" using 1:2 with lines ls 2 lw 2 dt (14, 6) title \"10000 q\", \
 \"$temp_dir/graphs-interactive-neu_n=1_w=16_nodes=10000000_edges=32000000_rate=1000000_goal=5_queries=100000_shared=no\" using 1:2 with lines ls 2 lw 2 dt (23, 6) title \"100000 q\", \
 \"$temp_dir/graphs-interactive-neu_n=1_w=16_nodes=10000000_edges=32000000_rate=1000000_goal=5_queries=1_shared=shared\" using 1:2 with lines ls 1 lw 2 dt (2, 6) title \"1 q, sharing\", \
 \"$temp_dir/graphs-interactive-neu_n=1_w=16_nodes=10000000_edges=32000000_rate=1000000_goal=5_queries=10000_shared=shared\" using 1:2 with lines ls 1 lw 2 dt (14, 6) title \"10000 q, sharing\", \
 \"$temp_dir/graphs-interactive-neu_n=1_w=16_nodes=10000000_edges=32000000_rate=1000000_goal=5_queries=100000_shared=shared\" using 1:2 with lines ls 1 lw 2 dt (23, 6) title \"100000 q, sharing\", \
 " > plots/$commit/graphs-interactive-neu-one/w=16_rate=1000000_manual.pdf


rm -R $temp_dir
