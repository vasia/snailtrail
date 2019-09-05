set terminal pdf enhanced color font "Helvetica,14" size 6,8 

set style line 1 lc rgb "#0060ad" lt 1 lw 1 pt 2 ps 1
set style line 2 lc rgb "#0060ad" lt 1 lw 1 pt 2 ps 1 dashtype "_"
set style line 3 lc rgb "#00796B" lt 1 lw 1 pt 2 ps 1
set style line 4 lc rgb "#00796B" lt 1 lw 1 pt 2 ps 1 dashtype "_"
set style line 5 lc rgb "#8BC34A" lt 1 lw 1 pt 2 ps 1
set style line 6 lc rgb "#8BC34A" lt 1 lw 1 pt 2 ps 1 dashtype "_"
set style line 7 lc rgb "#F4511E" lt 1 lw 1 pt 2 ps 1
set style line 8 lc rgb "#F4511E" lt 1 lw 1 pt 2 ps 1 dashtype "_"
set style line 9 lc rgb "#F88967" lt 1 lw 1 pt 2 ps 1
set style line 10 lc rgb "#F88967" lt 1 lw 1 pt 2 ps 1 dashtype "_"
set style line 11 lc rgb "#BC3409" lt 1 lw 1 pt 2 ps 1
set style line 12 lc rgb "#BC3409" lt 1 lw 1 pt 2 ps 1 dashtype "_"
set style line 13 lc rgb "#BC08A7" lt 1 lw 1 pt 2 ps 1 dashtype "."

set logscale y 10
set format y "10^{%L}"
# set yrange [0.001:1]
  
set logscale x 10
set format x "10^{%L}"
set xrange [0.0001:20]

set samples 20000 

set output "plots/st_latency_cdf.pdf"

set multiplot layout 4,1 rowsfirst
set xlabel "t [s]"
set ylabel "CDF"

input(c) = sprintf('< xsv select 2 %s -d '' '' | sort -n | uniq -c | awk ''BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}''', c)

set title "PAG construction offline latency [150K]\n{/*0.8 y% of epochs take less than x s}"
stats input("prepped/prepped_st_1_5.csv") using 3 name "stat1"
stats input("prepped/prepped_st_2_5.csv") using 3 name "stat2"
stats input("prepped/prepped_st_4_5.csv") using 3 name "stat4"
stats input("prepped/prepped_st_8_5.csv") using 3 name "stat8"
stats input("prepped/prepped_st_16_5.csv") using 3 name "stat16"
stats input("prepped/prepped_st_32_5.csv") using 3 name "stat32"
stats input("prepped/prepped_tc_32_5.csv") using 3 name "stattc"
plot \
  input("prepped/prepped_st_1_5.csv") using 1:($3 / stat1_max) with lines ls 1 title "w1", \
  input("prepped/prepped_st_2_5.csv") using 1:($3 / stat2_max) with lines ls 3 title "w2", \
  input("prepped/prepped_st_4_5.csv") using 1:($3 / stat4_max) with lines ls 5 title "w4", \
  input("prepped/prepped_st_8_5.csv") using 1:($3 / stat8_max) with lines ls 7 title "w8", \
  input("prepped/prepped_st_16_5.csv") using 1:($3 / stat16_max) with lines ls 9 title "w16", \
  input("prepped/prepped_st_32_5.csv") using 1:($3 / stat32_max) with lines ls 11 title "w32", \
  input("prepped/prepped_tc_32_5.csv") using 1:($3 / stattc_max) with lines ls 13 title "tri32"

set title "PAG construction offline latency [250K]\n{/*0.8 y% of epochs take less than x s}"
stats input("prepped/prepped_st_1_50.csv") using 3 name "stat1"
stats input("prepped/prepped_st_2_50.csv") using 3 name "stat2"
stats input("prepped/prepped_st_4_50.csv") using 3 name "stat4"
stats input("prepped/prepped_st_8_50.csv") using 3 name "stat8"
stats input("prepped/prepped_st_16_50.csv") using 3 name "stat16"
stats input("prepped/prepped_st_32_50.csv") using 3 name "stat32"
stats input("prepped/prepped_tc_32_50.csv") using 3 name "stattc"
plot \
  input("prepped/prepped_st_1_50.csv") using 1:($3 / stat1_max) with lines ls 1 title "w1", \
  input("prepped/prepped_st_2_50.csv") using 1:($3 / stat2_max) with lines ls 3 title "w2", \
  input("prepped/prepped_st_4_50.csv") using 1:($3 / stat4_max) with lines ls 5 title "w4", \
  input("prepped/prepped_st_8_50.csv") using 1:($3 / stat8_max) with lines ls 7 title "w8", \
  input("prepped/prepped_st_16_50.csv") using 1:($3 / stat16_max) with lines ls 9 title "w16", \
  input("prepped/prepped_st_32_50.csv") using 1:($3 / stat32_max) with lines ls 11 title "w32", \
  input("prepped/prepped_tc_32_50.csv") using 1:($3 / stattc_max) with lines ls 13 title "tri32"

set title "PAG construction offline latency [500K]\n{/*0.8 y% of epochs take less than x s}"
stats input("prepped/prepped_st_1_200.csv") using 3 name "stat1"
stats input("prepped/prepped_st_2_200.csv") using 3 name "stat2"
stats input("prepped/prepped_st_4_200.csv") using 3 name "stat4"
stats input("prepped/prepped_st_8_200.csv") using 3 name "stat8"
stats input("prepped/prepped_st_16_200.csv") using 3 name "stat16"
stats input("prepped/prepped_st_32_200.csv") using 3 name "stat32"
stats input("prepped/prepped_tc_32_200.csv") using 3 name "stattc"
plot \
  input("prepped/prepped_st_1_200.csv") using 1:($3 / stat1_max) with lines ls 1 title "w1", \
  input("prepped/prepped_st_2_200.csv") using 1:($3 / stat2_max) with lines ls 3 title "w2", \
  input("prepped/prepped_st_4_200.csv") using 1:($3 / stat4_max) with lines ls 5 title "w4", \
  input("prepped/prepped_st_8_200.csv") using 1:($3 / stat8_max) with lines ls 7 title "w8", \
  input("prepped/prepped_st_16_200.csv") using 1:($3 / stat16_max) with lines ls 9 title "w16", \
  input("prepped/prepped_st_32_200.csv") using 1:($3 / stat32_max) with lines ls 11 title "w32", \
  input("prepped/prepped_tc_32_200.csv") using 1:($3 / stattc_max) with lines ls 13 title "tri32"

