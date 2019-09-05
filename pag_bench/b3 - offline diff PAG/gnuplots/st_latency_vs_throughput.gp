set terminal pdf enhanced color font "Helvetica,8" size 6,8 

set style line 1 lc rgb "#0060ad" lt 1 lw 2 pt 2 ps 1
set style line 2 lc rgb "#0060ad" lt 1 lw 2 pt 2 ps 1 dashtype 2
set style line 3 lc rgb "#00796B" lt 1 lw 2 pt 2 ps 1
set style line 4 lc rgb "#00796B" lt 1 lw 2 pt 2 ps 1 dashtype 2
set style line 5 lc rgb "#8BC34A" lt 1 lw 2 pt 2 ps 1
set style line 6 lc rgb "#8BC34A" lt 1 lw 2 pt 2 ps 1 dashtype 2
set style line 7 lc rgb "#F4511E" lt 1 lw 2 pt 2 ps 1
set style line 8 lc rgb "#F4511E" lt 1 lw 2 pt 2 ps 1 dashtype 2
set style line 9 lc rgb "#F88967" lt 1 lw 2 pt 2 ps 1
set style line 10 lc rgb "#F88967" lt 1 lw 2 pt 2 ps 1 dashtype 2
set style line 11 lc rgb "#BC3409" lt 1 lw 2 pt 2 ps 1
set style line 12 lc rgb "#BC3409" lt 1 lw 2 pt 2 ps 1 dashtype 2
set style line 13 lc rgb "#BC08A7" lt 1 lw 2 pt 2 ps 1 dashtype 3

set logscale y 10
set format y "10^{%L}"
# # set yrange [0.0001:10000]
# set yrange [100000:400000000]
  
set logscale x 10
set format x "10^{%L}"
# # set xrange [10000:1000000000]
# set xrange [0.01:1]

set samples 20000 

set output "plots/latency_vs_throughput.pdf"

set multiplot layout 4,1 rowsfirst
set xlabel "latency [s]"
set ylabel "throughput [log events / s]"

set title "PAG construction latency vs throughput [150K]"
plot \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_1_5.csv 1 prepped/prepped_tc_32_5.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 1 title "w1", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_2_5.csv 1 prepped/prepped_tc_32_5.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 3 title "w2", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_4_5.csv 1 prepped/prepped_tc_32_5.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 5 title "w4", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_8_5.csv 1 prepped/prepped_tc_32_5.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 7 title "w8", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_16_5.csv 1 prepped/prepped_tc_32_5.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 9 title "w16", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_32_5.csv 1 prepped/prepped_tc_32_5.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 11 title "w32"

set title "PAG construction latency vs throughput [250K]"
plot \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_1_50.csv 1 prepped/prepped_tc_32_50.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 1 title "w1", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_2_50.csv 1 prepped/prepped_tc_32_50.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 3 title "w2", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_4_50.csv 1 prepped/prepped_tc_32_50.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 5 title "w4", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_8_50.csv 1 prepped/prepped_tc_32_50.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 7 title "w8", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_16_50.csv 1 prepped/prepped_tc_32_50.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 9 title "w16", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_32_50.csv 1 prepped/prepped_tc_32_50.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 11 title "w32"

set title "PAG construction latency vs throughput [500K]"
plot \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_1_200.csv 1 prepped/prepped_tc_32_200.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 1 title "w1", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_2_200.csv 1 prepped/prepped_tc_32_200.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 3 title "w2", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_4_200.csv 1 prepped/prepped_tc_32_200.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 5 title "w4", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_8_200.csv 1 prepped/prepped_tc_32_200.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 7 title "w8", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_16_200.csv 1 prepped/prepped_tc_32_200.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 9 title "w16", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_32_200.csv 1 prepped/prepped_tc_32_200.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 11 title "w32"

set title "PAG construction latency vs throughput [1M]"
plot \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_1_500.csv 1 prepped/prepped_tc_32_500.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 1 title "w1", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_2_500.csv 1 prepped/prepped_tc_32_500.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 3 title "w2", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_4_500.csv 1 prepped/prepped_tc_32_500.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 5 title "w4", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_8_500.csv 1 prepped/prepped_tc_32_500.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 7 title "w8", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_16_500.csv 1 prepped/prepped_tc_32_500.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 9 title "w16", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_32_500.csv 1 prepped/prepped_tc_32_500.csv | xsv select 2,6 | xsv fmt -t ' ' | sort -n" using 1:($2/$1) ls 11 title "w32"
