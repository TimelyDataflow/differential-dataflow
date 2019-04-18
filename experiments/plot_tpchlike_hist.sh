commit=5b0a6d19e5

mkdir -p plots/$commit/i-tpchlike-mixing
temp_dir=$(mktemp -d)
echo 'temp_dir' $temp_dir

colors=("red" "blue")

common="set terminal pdf size 5.2cm,3.5cm font \"Arial,10\"; set key samplen 2; set key opaque left top Left reverse; set ytics 5,5; set format y \"%.0f%%\"; "
multiplot="set multiplot layout 2,1 margins 0.15,0.95,.25,.95 spacing 0,.05; "

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="$common set tmargin 0; set bmargin 0; set lmargin 1; set rmargin 1; set key opaque top left; set style fill transparent solid 1 noborder; set logscale x; set format x \"%.0s %cs\"; $multiplot; set xrange [0.0001:5]; "
  echo GROUP $g
  dt=0
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | grep '\[MEASURE\]' | awk '{ if ($3 != prev) { prev = $3; cur = $5; print cur } else { cur = $5 >= cur ? $5 : cur; }; }' | hdrhist hist > $temp_dir/install-$f
    lines=`cat $temp_dir/install-$f | wc -l`
    if [ "$(echo $f | cut -d '_' -f 7)" == "arrange=false" ]; then
      title="not shared"
    else
      title="shared"
    fi
    if [ $dt -eq 0 ]; then
      plotscript="$plotscript unset xtics; unset xlabel; "
    else
      plotscript="$plotscript set xtics; set xlabel \"latency\" offset 0,.4; "
    fi
    plotscript="$plotscript plot \"$temp_dir/install-$f\" using (\$1/1000000000):(\$2*100) with boxes lw 2 dt $dt lc \"${colors[$dt]}\" title \"$title\"; "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/hist-install-$g.pdf
done

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="$common set tmargin 0; set bmargin 0; set lmargin 1; set rmargin 1; set key opaque top left; set style fill transparent solid 1 noborder; set logscale x; set format x \"%.0s %cs\"; $multiplot; set xrange [0.0001:5]; "
  echo GROUP $g
  dt=0
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | grep '\[MEASURE\]' | awk '{ if ($3 != prev) { prev = $3; cur = $6; print cur } else { cur = $6 >= cur ? $6 : cur; }; }' | hdrhist hist > $temp_dir/uninstall-$f
    if [ "$(echo $f | cut -d '_' -f 7)" == "arrange=false" ]; then
      title="not shared"
    else
      title="shared"
    fi
    if [ $dt -eq 0 ]; then
      plotscript="$plotscript unset xtics; unset xlabel; "
    else
      plotscript="$plotscript set xtics; set xlabel \"latency\" offset 0,.4; "
    fi
    plotscript="$plotscript plot \"$temp_dir/uninstall-$f\" using (\$1/1000000000):(\$2*100) with boxes lw 2 dt $dt lc \"${colors[$dt]}\" title \"$(echo $f | cut -d '_' -f 7)\"; "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/hist-uninstall-$g.pdf
done

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="$common set tmargin 0; set bmargin 0; set lmargin 1; set rmargin 1; set key opaque top left; set style fill transparent solid 1 noborder; set logscale x; set format x \"%.0s %cs\"; $multiplot; set xrange [0.0001:5]; "
  echo GROUP $g
  dt=0
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | grep '\[MEASURE\]' | awk '{ if ($3 != prev) { prev = $3; cur = $7; print cur } else { cur = $7 >= cur ? $7 : cur; }; }' | hdrhist hist > $temp_dir/work-$f
    if [ "$(echo $f | cut -d '_' -f 7)" == "arrange=false" ]; then
      title="not shared"
    else
      title="shared"
    fi
    if [ $dt -eq 0 ]; then
      plotscript="$plotscript unset xtics; unset xlabel; "
    else
      plotscript="$plotscript set xtics; set xlabel \"latency\" offset 0,.4; "
    fi
    plotscript="$plotscript plot \"$temp_dir/work-$f\" using (\$1/1000000000):(\$2*100) with boxes lw 2 dt $dt lc \"${colors[$dt]}\" title \"$(echo $f | cut -d '_' -f 7)\"; "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/hist-work-$g.pdf
done

rm -R $temp_dir
