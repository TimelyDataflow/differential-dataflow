
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

gnuplot -p -e "\
  set terminal pdf size 6cm,4cm;
   set logscale y;
   set rmargin at screen 0.9;
   set bmargin at screen 0.2;
   set xtics 2,4,22;
   set xlabel \"query\";
   set xrange [0:23];
   set yrange [1:60];
   set ylabel \"relative throughput\";
   set key left top Left reverse font \",10\";
   plot \
   \"$temp_dir/w32\" using 1:2 with lines lt 7 title \"w=32\", \
   \"$temp_dir/w16\" using 1:2 with lines lt 6 title \"w=16\", \
   \"$temp_dir/w8\" using 1:2  with lines lt 5 title \"w=8\", \
   \"$temp_dir/w4\" using 1:2  with lines lt 4 title \"w=4\", \
   \"$temp_dir/w2\" using 1:2  with lines lt 3 title \"w=2\"
   " > plots/tpch_3.pdf

rm -R $temp_dir

######################################
temp_dir=$(mktemp -d)

cat experiments-sf10-filtered.txt | awk '$3 == 1 && $4 == 1' | cut -f 1,5 > $temp_dir/b1_w1
cat experiments-sf10-filtered.txt | awk '$3 == 1000000 && $4 == 1' | cut -f 1,5 > $temp_dir/b1000000_w1
join $temp_dir/b1_w1 $temp_dir/b1000000_w1 > $temp_dir/w1
cat experiments-sf10-filtered.txt | awk '$3 == 1000000 && $4 == 32' | cut -f 1,5 > $temp_dir/b1000000_w32

gnuplot -p -e "\
  set terminal pdf size 6cm,4cm;
   set logscale y;
   set rmargin at screen 0.9;
   set bmargin at screen 0.2;
   set xtics 2,4,22;
   set xlabel \"query\";
   set xrange [0:23];
   set ylabel \"absolute throughput (tuples/sec)\";
   set key left bottom Left reverse font \",10\";
   plot \
   \"$temp_dir/w1\" using 1:2 with lines lt 7 title \"w=1, b=1\", \
   \"$temp_dir/w1\" using 1:3 with lines lt 7 title \"w=1, b=1M\", \
   \"$temp_dir/b1000000_w32\" using 1:2 with lines lt 6 title \"w=32, b=1M\", \
   \"experiments-hotdog.txt\" using 1:2 pointtype 6 ps .5 lc rgb \"black\" title \"hotdog\"
   " > plots/tpch_1.pdf

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

gnuplot -p -e "\
  set terminal pdf size 6cm,4cm;
   set logscale y;
   set rmargin at screen 0.9;
   set bmargin at screen 0.2;
   set xtics 2,4,22;
   set xlabel \"query\";
   set xrange [0:23];
   set yrange [1:*];
   set ylabel \"absolute throughput (tuples/sec)\";
   set key left bottom Left reverse font \",10\";
   plot \
   \"$temp_dir/w1000000\" using 1:2 with lines lt 7 title \"w=10^6\", \
   \"$temp_dir/w100000\" using 1:2 with lines lt 6 title \"w=10^5\", \
   \"$temp_dir/w10000\" using 1:2  with lines lt 5 title \"w=10^4\", \
   \"$temp_dir/w1000\" using 1:2  with lines lt 4 title \"w=10^3\", \
   \"$temp_dir/w100\" using 1:2  with lines lt 3 title \"w=10^2\", \
   \"$temp_dir/w10\" using 1:2  with lines lt 3 title \"w=10^1\"
   " > plots/tpch_2.pdf

rm -R $temp_dir

######################################
