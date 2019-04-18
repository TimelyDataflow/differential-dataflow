commit=dirty-780a95ec9f

mkdir -p plots/$commit/i-tpchlike-mixing
temp_dir=$(mktemp -d)
echo 'temp_dir' $temp_dir

colors=("red" "blue")

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="set terminal png truecolor enhanced size 500,300; set tmargin 0; set bmargin 0; set lmargin 1; set rmargin 1; set key opaque top left; set style fill transparent solid 1 noborder; set logscale x; set format x \"%.0s %cs\"; set yrange [0:20000]; set multiplot layout 2,1 margins 0.15,0.95,.1,.95 spacing 0,0; set xrange [0.0001:5]; "
  echo GROUP $g
  dt=0
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | grep '\[MEASURE\]' | awk '{print $5}' | hdrhist > $temp_dir/install-$f
    plotscript="$plotscript plot \"$temp_dir/install-$f\" using (\$1/1000000000):3 with boxes lw 2 dt $dt lc \"${colors[$dt]}\" title \"$(echo $f | cut -d '_' -f 7)\"; "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/hist-install-$g.png
done

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="set terminal png truecolor enhanced size 500,300; set tmargin 0; set bmargin 0; set lmargin 1; set rmargin 1; set key opaque top left; set style fill transparent solid 1 noborder; set logscale x; set format x \"%.0s %cs\"; set yrange [0:20000]; set multiplot layout 2,1 margins 0.15,0.95,.1,.95 spacing 0,0; set xrange [0.0001:5]; "
  echo GROUP $g
  dt=0
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | grep '\[MEASURE\]' | awk '{print $6}' | hdrhist > $temp_dir/uninstall-$f
    plotscript="$plotscript plot \"$temp_dir/uninstall-$f\" using (\$1/1000000000):3 with boxes lw 2 dt $dt lc \"${colors[$dt]}\" title \"$(echo $f | cut -d '_' -f 7)\"; "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/hist-uninstall-$g.png
done

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="set terminal png truecolor enhanced size 500,300; set tmargin 0; set bmargin 0; set lmargin 1; set rmargin 1; set key opaque top left; set style fill transparent solid 1 noborder; set logscale x; set format x \"%.0s %cs\"; set yrange [0:20000]; set multiplot layout 2,1 margins 0.15,0.95,.1,.95 spacing 0,0; set xrange [0.0001:5]; "
  echo GROUP $g
  dt=0
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | grep '\[MEASURE\]' | awk '{print $7}' | hdrhist > $temp_dir/work-$f
    plotscript="$plotscript plot \"$temp_dir/work-$f\" using (\$1/1000000000):3 with boxes lw 2 dt $dt lc \"${colors[$dt]}\" title \"$(echo $f | cut -d '_' -f 7)\"; "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/hist-work-$g.png
done

rm -R $temp_dir
