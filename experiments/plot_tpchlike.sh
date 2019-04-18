commit=5b0a6d19e5

mkdir -p plots/$commit/i-tpchlike-mixing
temp_dir=$(mktemp -d)
echo 'temp_dir' $temp_dir

colors=("red" "blue")

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="set terminal png truecolor enhanced size 600,400; set key opaque bottom right; set logscale y; set title \"install_$g\" noenhanced; set style fill transparent solid 0.01 noborder; set style circle radius 15; set format y \"%.0s %cs\"; plot "
  echo GROUP $g
  dt=0
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | grep '\[MEASURE\]' | awk '{print $3, $5}' > $temp_dir/install-$f
    plotscript="$plotscript \"$temp_dir/install-$f\" using 1:(\$2/1000000000) with circles fc \"${colors[$dt]}\" notitle, "
    plotscript="$plotscript NaN with circles fc \"${colors[$dt]}\" fs solid 1.0 title \"$(echo $f | cut -d '_' -f 7)\", "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/timeline-install-$g.png
done

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="set terminal png truecolor enhanced size 600,400; set key opaque bottom right; set logscale y; set title \"uninstall_$g\" noenhanced; set style fill transparent solid 0.01 noborder; set style circle radius 15; set format y \"%.0s %cs\"; plot "
  echo GROUP $g
  dt=0
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | grep '\[MEASURE\]' | awk '{print $3, $6}' > $temp_dir/uninstall-$f
    plotscript="$plotscript \"$temp_dir/uninstall-$f\" using 1:(\$2/1000000000) with circles fc \"${colors[$dt]}\" notitle, "
    plotscript="$plotscript NaN with circles fc \"${colors[$dt]}\" fs solid 1.0 title \"$(echo $f | cut -d '_' -f 7)\", "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/timeline-uninstall-$g.png
done

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="set terminal png truecolor enhanced size 600,400; set key opaque bottom right; set logscale y; set title \"work_$g\" noenhanced; set style fill transparent solid 0.01 noborder; set style circle radius 15; set format y \"%.0s %cs\"; plot "
  echo GROUP $g
  dt=0
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | grep '\[MEASURE\]' | awk '{print $3, $7}' > $temp_dir/work-$f
    plotscript="$plotscript \"$temp_dir/work-$f\" using 1:(\$2/1000000000) with circles fc \"${colors[$dt]}\" notitle, "
    plotscript="$plotscript NaN with circles fc \"${colors[$dt]}\" fs solid 1.0 title \"$(echo $f | cut -d '_' -f 7)\", "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/timeline-work-$g.png
done

rm -R $temp_dir
