
######################################
temp_dir=$(mktemp -d)

cat experiments-sf10-filtered.txt | awk '$3 == 1000000 && $4 == 1' | cut -f 1,5 > $temp_dir/w1
join $temp_dir/w1 $temp_dir/w1 | awk '{ print $1,$3/$2 }' > $temp_dir/ow1
cat experiments-sf10-filtered.txt | awk '$3 == 1000000 && $4 == 2' | cut -f 1,5 > $temp_dir/tw2
join $temp_dir/w1 $temp_dir/tw2 | awk '{ print $1,$3/$2 }' > $temp_dir/w2
cat experiments-sf10-filtered.txt | awk '$3 == 1000000 && $4 == 4' | cut -f 1,5 > $temp_dir/tw4
join $temp_dir/w1 $temp_dir/tw4 | awk '{ print $1,$3/$2 }' > $temp_dir/w4
cat experiments-sf10-filtered.txt | awk '$3 == 1000000 && $4 == 8' | cut -f 1,5 > $temp_dir/tw8
join $temp_dir/w1 $temp_dir/tw8 | awk '{ print $1,$3/$2 }' > $temp_dir/w8
cat experiments-sf10-filtered.txt | awk '$3 == 1000000 && $4 == 16' | cut -f 1,5 > $temp_dir/tw16
join $temp_dir/w1 $temp_dir/tw16 | awk '{ print $1,$3/$2 }' > $temp_dir/w16
cat experiments-sf10-filtered.txt | awk '$3 == 1000000 && $4 == 32' | cut -f 1,5 > $temp_dir/tw32
join $temp_dir/w1 $temp_dir/tw32 | awk '{ print $1,$3/$2 }' > $temp_dir/w32

join $temp_dir/w2 $temp_dir/w4 > $temp_dir/join1
join $temp_dir/join1 $temp_dir/w8 > $temp_dir/join2
join $temp_dir/join2 $temp_dir/w16 > $temp_dir/join3
join $temp_dir/join3 $temp_dir/w32 > $temp_dir/wall

gnuplot -p -e "\
  set terminal pdf size 4.8cm,3.2cm;
   set logscale y;
   set xtics 2,4,22;
   set bmargin at screen 0.25;
   set xlabel \"query\";
   set xrange [0:23];
   set yrange [1:100];
   set ylabel \"relative throughput\";
   set key left top Left reverse font \",10\";
   set key samplen 2;
   plot \
     \"$temp_dir/w32\" using 1:2 with lines lt 7 lw 2 dt (2, 2) title \"w=32\", \
   \"$temp_dir/w16\" using 1:2 with lines lt 6 lw 2 dt (4, 2) title \"w=16\", \
   \"$temp_dir/w8\" using 1:2  with lines lt 5 lw 2 dt (6, 2) title \"w=8\", \
   \"$temp_dir/w4\" using 1:2  with lines lt 4 lw 2 dt (8, 2) title \"w=4\", \
   \"$temp_dir/w2\" using 1:2  with lines lt 3 lw 2 dt (10, 2) title \"w=2\"
   " > plots/tpch_3.pdf

gnuplot -p -e "\
  set terminal pdf size 4.8cm,3.2cm;
   set datafile separator whitespace;
   set bmargin at screen 0.25;
   set boxwidth 0.8;
   set xtics 2,4,22;
   set xlabel \"query\";
   set yrange [0:2];
   set ylabel \"relative throughput\";
   set ytics (1,2,3);
   set format y \"10^{%g}\";
   set ytics offset 0.7;
   set key top center horizontal Left reverse font \",10\";
   set key samplen 1.5;
   set key height .6 width 2;
   set style data histogram;
   set style histogram rowstacked;
   set style fill solid .5;
   everyfour(col) = ((int(column(col))-2)%4 ==0)?stringcolumn(1):\"\";
   plot \
     \"$temp_dir/wall\" using (log10(column(2))):xticlabels(everyfour(1)) title \"w=2\", \
     \"$temp_dir/wall\" using ( (log10(column(3))-log10(column(2))) > 0 ? (log10(column(3))-log10(column(2))) : 0 )      title \"w=4\", \
     \"$temp_dir/wall\" using ( (log10(column(4))-log10(column(3))) > 0 ? (log10(column(4))-log10(column(3))) : 0 )      title \"w=8\", \
     \"$temp_dir/wall\" using ( (log10(column(5))-log10(column(4))) > 0 ? (log10(column(5))-log10(column(4))) : 0 )      title \"w=16\", \
     \"$temp_dir/wall\" using ( (log10(column(6))-log10(column(5))) > 0 ? (log10(column(6))-log10(column(5))) : 0 )      title \"w=32\",
   " > plots/tpch_3_stacked.pdf

rm -R $temp_dir

######################################
temp_dir=$(mktemp -d)

cat experiments-sf10-filtered.txt | awk '$3 == 1 && $4 == 1' | cut -f 1,5 > $temp_dir/b1_w1
cat experiments-sf10-filtered.txt | awk '$3 == 1000000 && $4 == 1' | cut -f 1,5 > $temp_dir/b1000000_w1
join $temp_dir/b1_w1 $temp_dir/b1000000_w1 > $temp_dir/w1
cat experiments-sf10-filtered.txt | awk '$3 == 1000000 && $4 == 32' | cut -f 1,5 > $temp_dir/b1000000_w32
join $temp_dir/w1 $temp_dir/b1000000_w32 > $temp_dir/kpg

gnuplot -p -e "\
  set terminal pdf size 4.8cm,3.2cm;
   set logscale y;
   set bmargin at screen 0.25;
   set xtics 2,4,22;
   set xlabel \"query\";
   set xrange [0:23];
   set yrange [1:*];
   set ylabel \"throughput (tuples/sec)\";
   set format y \"10^{%T}\";
   set ytics offset 0.7;
   set key left bottom Left reverse font \",10\";
   set key samplen 2;
   plot \
     \"$temp_dir/w1\" using 1:2 with points pointtype 7 ps .5 title \"w=1, b=1\", \
     \"$temp_dir/w1\" using 1:3 with points pointtype 4 ps .5 title \"w=1, b=1M\", \
     \"$temp_dir/b1000000_w32\" using 1:2 with points pointtype 5 ps .5 title \"w=32, b=1M\", \
   \"experiments-hotdog.txt\" using 1:2 pointtype 2 ps .5 lc rgb \"black\" title \"DBToaster\"
   " > plots/tpch_1.pdf

gnuplot -p -e "\
  set terminal pdf size 4.8cm,3.2cm;
   set datafile separator whitespace;
   set bmargin at screen 0.25;
   set boxwidth 0.8;
   set xtics 2,4,22;
   set xlabel \"query\";
   set yrange [1:*];
   set ylabel \"throughput (tuples/sec)\";
   set format y \"10^{%g}\";
   set ytics offset 0.7;
   set key left bottom Left reverse font \",10\";
   set key samplen 1.5;
   set key opaque box lt 1 lw 1 lc \"black\" height .4 width 1;
   set style data histogram;
   set style histogram rowstacked;
   set style fill solid .5;
   everyfour(col) = ((int(column(col))-2)%4 ==0)?stringcolumn(1):\"\";
   plot \
     \"$temp_dir/kpg\" using (log10(column(2))):xticlabels(everyfour(1)) title \"w=1, b=1\", \
     \"$temp_dir/kpg\" using (log10(column(3))-log10(column(2))) title \"w=1, b=1M\", \
     \"$temp_dir/kpg\" using (log10(column(4))-log10(column(3))) title \"w=32, b=1M\", \
     \"experiments-hotdog.txt\" using (log10(column(2))) with points pointtype 2 ps .5 lc rgb \"black\" title \"DBToaster\"
   " > plots/tpch_1_stacked.pdf

rm -R $temp_dir

######################################
temp_dir=$(mktemp -d)

cat experiments-sf10-filtered.txt | awk '$3 == 1 && $4 == 1' | cut -f 1,5 > $temp_dir/w1
join $temp_dir/w1 $temp_dir/w1 | awk '{ print $1,$3/$2 }' > $temp_dir/ow1
cat experiments-sf10-filtered.txt | awk '$3 == 10 && $4 == 1' | cut -f 1,5 > $temp_dir/tw10
join $temp_dir/w1 $temp_dir/tw10 | awk '{ print $1,$3/$2 }' > $temp_dir/w10
cat experiments-sf10-filtered.txt | awk '$3 == 100 && $4 == 1' | cut -f 1,5 > $temp_dir/tw100
join $temp_dir/w1 $temp_dir/tw100 | awk '{ print $1,$3/$2 }' > $temp_dir/w100
cat experiments-sf10-filtered.txt | awk '$3 == 1000 && $4 == 1' | cut -f 1,5 > $temp_dir/tw1000
join $temp_dir/w1 $temp_dir/tw1000 | awk '{ print $1,$3/$2 }' > $temp_dir/w1000
cat experiments-sf10-filtered.txt | awk '$3 == 10000 && $4 == 1' | cut -f 1,5 > $temp_dir/tw10000
join $temp_dir/w1 $temp_dir/tw10000 | awk '{ print $1,$3/$2 }' > $temp_dir/w10000
cat experiments-sf10-filtered.txt | awk '$3 == 100000 && $4 == 1' | cut -f 1,5 > $temp_dir/tw100000
join $temp_dir/w1 $temp_dir/tw100000 | awk '{ print $1,$3/$2 }' > $temp_dir/w100000
cat experiments-sf10-filtered.txt | awk '$3 == 1000000 && $4 == 1' | cut -f 1,5 > $temp_dir/tw1000000
join $temp_dir/w1 $temp_dir/tw1000000 | awk '{ print $1,$3/$2 }' > $temp_dir/w1000000

join $temp_dir/w10 $temp_dir/w100 > $temp_dir/join1
join $temp_dir/join1 $temp_dir/w1000 > $temp_dir/join2
join $temp_dir/join2 $temp_dir/w10000 > $temp_dir/join3
join $temp_dir/join3 $temp_dir/w100000 > $temp_dir/join4
join $temp_dir/join4 $temp_dir/w1000000 > $temp_dir/wall

gnuplot -p -e "\
  set terminal pdf size 4.8cm,3.2cm;
   set logscale y;
   set bmargin at screen 0.25;
   set xtics 2,4,22;
   set xlabel \"query\";
   set xrange [0:23];
   set yrange [1:*];
   set ylabel \"relative throughput\";
   set key left bottom Left reverse font \",10\";
   plot \
     \"$temp_dir/w1000000\" using 1:2 with lines lt 7 lw 2 dt (2, 2) title \"b=10^6\", \
     \"$temp_dir/w100000\" using 1:2 with lines lt 6 lw 2 dt (4, 2) title  \"b=10^5\", \
     \"$temp_dir/w10000\" using 1:2  with lines lt 5 lw 2 dt (6, 2) title  \"b=10^4\", \
     \"$temp_dir/w1000\" using 1:2  with lines lt 4 lw 2 dt (8, 2) title   \"b=10^3\", \
     \"$temp_dir/w100\" using 1:2  with lines lt 3 lw 2 dt (10, 2) title   \"b=10^2\", \
     \"$temp_dir/w10\" using 1:2  with lines lt 2 lw 2 dt (12, 2) title    \"b=10^1\"
   " > plots/tpch_2.pdf

gnuplot -p -e "\
  set terminal pdf size 4.8cm,3.2cm;
   set datafile separator whitespace;
   set bmargin at screen 0.25;
   set boxwidth 0.8;
   set xtics 2,4,22;
   set xlabel \"query\";
   set yrange [0:4];
   set ylabel \"relative throughput\";
   set ytics (1,2,3);
   set format y \"10^{%g}\";
   set ytics offset 0.7;
   set key top center horizontal Left reverse font \",10\";
   set key samplen 1.5;
   set key height .6 width 2;
   set style data histogram;
   set style histogram rowstacked;
   set style fill solid .5;
   everyfour(col) = ((int(column(col))-2)%4 ==0)?stringcolumn(1):\"\";
   plot \
     \"$temp_dir/wall\" using (log10(column(2))):xticlabels(everyfour(1)) title \"b=10^1\", \
     \"$temp_dir/wall\" using ( (log10(column(3))-log10(column(2))) > 0 ? (log10(column(3))-log10(column(2))) : 0 )      title \"b=10^2\", \
     \"$temp_dir/wall\" using ( (log10(column(4))-log10(column(3))) > 0 ? (log10(column(4))-log10(column(3))) : 0 )      title \"b=10^3\", \
     \"$temp_dir/wall\" using ( (log10(column(5))-log10(column(4))) > 0 ? (log10(column(5))-log10(column(4))) : 0 )      title \"b=10^4\", \
     \"$temp_dir/wall\" using ( (log10(column(6))-log10(column(5))) > 0 ? (log10(column(6))-log10(column(5))) : 0 )      title \"b=10^5\", \
     \"$temp_dir/wall\" using ( (log10(column(7))-log10(column(6))) > 0 ? (log10(column(7))-log10(column(6))) : 0 )      title \"b=10^6\",
   " > plots/tpch_2_stacked.pdf

rm -R $temp_dir

######################################
