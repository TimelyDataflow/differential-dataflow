commit=dirty-780a95ec9f

mkdir -p plots/$commit/i-tpchlike-mixing
temp_dir=$(mktemp -d)
echo 'temp_dir' $temp_dir

colors=("red" "blue")

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="set terminal png truecolor enhanced size 1000,600; set title \"install_$g\" noenhanced; set style fill transparent solid 0.01 noborder; set logscale x; set format x \"%.0s %cs\"; plot "
  echo GROUP $g
  dt=0
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | grep '\[MEASURE\]' | awk '{print $5}' | hdrhist > $temp_dir/install-$f
    plotscript="$plotscript \"$temp_dir/install-$f\" using (\$1/1000000000):2 with lines lw 2 dt $dt lc \"${colors[$dt]}\" title \"$(echo $f | cut -d '_' -f 7)\", "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/ccdf-install-$g.png
done

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="set terminal png truecolor enhanced size 1000,600; set title \"uninstall_$g\" noenhanced; set style fill transparent solid 0.01 noborder; set logscale x; set format x \"%.0s %cs\"; plot "
  echo GROUP $g
  dt=0
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | grep '\[MEASURE\]' | awk '{print $6}' | hdrhist > $temp_dir/uninstall-$f
    plotscript="$plotscript \"$temp_dir/uninstall-$f\" using (\$1/1000000000):2 with lines lw 2 dt $dt lc \"${colors[$dt]}\" title \"$(echo $f | cut -d '_' -f 7)\", "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/ccdf-uninstall-$g.png
done

for g in `ls results/$commit/i-tpchlike-mixing | cut -d '_' -f 4-6 | sort | uniq`; do
  plotscript="set terminal png truecolor enhanced size 1000,600; set title \"work_$g\" noenhanced; set style fill transparent solid 0.01 noborder; set logscale x; set format x \"%.0s %cs\"; plot "
  echo GROUP $g
  dt=0
  for file in `ls results/$commit/i-tpchlike-mixing/*_$(echo $g | sed 's/_/_*/')_*`; do
    f=$(basename $file)
    echo $f
    cat $file | grep '\[MEASURE\]' | awk '{print $7}' | hdrhist > $temp_dir/work-$f
    plotscript="$plotscript \"$temp_dir/work-$f\" using (\$1/1000000000):2 with lines lw 2 dt $dt lc \"${colors[$dt]}\" title \"$(echo $f | cut -d '_' -f 7)\", "
    dt=$(expr $dt + 1)
  done
  gnuplot -p -e "$plotscript" > plots/$commit/i-tpchlike-mixing/ccdf-work-$g.png
done

rm -R $temp_dir
