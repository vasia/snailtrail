set terminal pdf enhanced color font "Helvetica,8" size 6,6 

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
set style line 15 lc rgb "#FF0000" lt 1 lw 2 pt 2 ps 1 dashtype 5

set logscale y 10
set format y "10^{%L}"
# set yrange [200000:200000000]

set logscale x 10
set format x "10^{%L}"
# set xrange [50000:1000000000]

set samples 20000 

set output "plots/latency_cdf.pdf"

set tmargin 3
set multiplot layout 3,2 rowsfirst title "Offline latency [500K, 32W]\n{/*0.8 y% of epochs take less than x s}" 
set xlabel "t [s]"
set ylabel "CDF"

input(c) = sprintf('< xsv select 2 %s -d '' '' | sort -n | uniq -c | awk ''BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}''', c)

stats input("prepped/prepped_maxepoch_32_200.csv") using 3 name "stat1"
stats input("prepped/prepped_maxmsg_32_200.csv") using 3 name "stat2"
stats input("prepped/prepped_maxop_32_200.csv") using 3 name "stat4"
stats input("prepped/prepped_maxprog_32_200.csv") using 3 name "stat8"
stats input("prepped/prepped_metrics_32_200.csv") using 3 name "stat16"
stats input("prepped/prepped_pag_32_200.csv") using 3 name "stat32"
stats input("prepped/prepped_algo_32_200.csv") using 3 name "stat64"
stats input("prepped/prepped_tc_32_200.csv") using 3 name "stattc"

plot \
  input("prepped/prepped_tc_32_200.csv") using 1:($3 / stattc_max) with lines ls 13 title "tri32", \
  input("prepped/prepped_pag_32_200.csv") using 1:($3 / stat32_max) with lines ls 15 title "PAG", \
  input("prepped/prepped_maxepoch_32_200.csv") using 1:($3 / stat1_max) with lines ls 1 title "max-epoch"

plot \
  input("prepped/prepped_tc_32_200.csv") using 1:($3 / stattc_max) with lines ls 13 title "tri32", \
  input("prepped/prepped_pag_32_200.csv") using 1:($3 / stat32_max) with lines ls 15 title "PAG", \
  input("prepped/prepped_maxmsg_32_200.csv") using 1:($3 / stat2_max) with lines ls 3 title "max-msg"

plot \
  input("prepped/prepped_tc_32_200.csv") using 1:($3 / stattc_max) with lines ls 13 title "tri32", \
  input("prepped/prepped_pag_32_200.csv") using 1:($3 / stat32_max) with lines ls 15 title "PAG", \
  input("prepped/prepped_maxop_32_200.csv") using 1:($3 / stat4_max) with lines ls 5 title "max-op"

plot \
  input("prepped/prepped_tc_32_200.csv") using 1:($3 / stattc_max) with lines ls 13 title "tri32", \
  input("prepped/prepped_pag_32_200.csv") using 1:($3 / stat32_max) with lines ls 15 title "PAG", \
  input("prepped/prepped_maxprog_32_200.csv") using 1:($3 / stat8_max) with lines ls 7 title "max-prog"

plot \
  input("prepped/prepped_tc_32_200.csv") using 1:($3 / stattc_max) with lines ls 13 title "tri32", \
  input("prepped/prepped_pag_32_200.csv") using 1:($3 / stat32_max) with lines ls 15 title "PAG", \
  input("prepped/prepped_metrics_32_200.csv") using 1:($3 / stat16_max) with lines ls 9 title "metrics"

plot \
  input("prepped/prepped_tc_32_200.csv") using 1:($3 / stattc_max) with lines ls 13 title "tri32", \
  input("prepped/prepped_pag_32_200.csv") using 1:($3 / stat32_max) with lines ls 15 title "PAG", \
  input("prepped/prepped_algo_32_200.csv") using 1:($3 / stat64_max) with lines ls 11 title "algo"
