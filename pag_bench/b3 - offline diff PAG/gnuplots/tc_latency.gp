set terminal pdf enhanced color font "Helvetica,8" #size 6,8 

set style line 1 lc rgb "#0060ad" lt 1 lw 2 pt 2 ps 1
set style line 2 lc rgb "#0060ad" lt 1 lw 2 pt 2 ps 1 dashtype 2
set style line 3 lc rgb "#00796B" lt 1 lw 2 pt 2 ps 1
set style line 4 lc rgb "#00796B" lt 1 lw 2 pt 2 ps 1 dashtype 2
set style line 5 lc rgb "#8BC34A" lt 1 lw 2 pt 2 ps 1
set style line 6 lc rgb "#8BC34A" lt 1 lw 2 pt 2 ps 1 dashtype 2
set style line 7 lc rgb "#F4511E" lt 1 lw 2 pt 2 ps 1
set style line 8 lc rgb "#F4511E" lt 1 lw 2 pt 2 ps 1 dashtype 2

set logscale y 10
set format y "10^{%L}"

set samples 20000 

set output "plots/tc_32_latency.pdf"

set title "triangles latency"
set xlabel "epoch"
set ylabel "time [s]"

stats "prepped/prepped_tc_32_5.csv" using 2 name "stat5"
stats "prepped/prepped_tc_32_50.csv" using 2 name "stat50"
stats "prepped/prepped_tc_32_200.csv" using 2 name "stat200"
stats "prepped/prepped_tc_32_500.csv" using 2 name "stat500"
plot \
  "prepped/prepped_tc_32_5.csv" using 1:2 with lines ls 1 smooth bezier title "150K", \
  stat5_mean notitle with lines ls 2, \
  "prepped/prepped_tc_32_50.csv" using 1:2 with lines ls 3 smooth bezier title "250K", \
  stat50_mean notitle with lines ls 4, \
  "prepped/prepped_tc_32_200.csv" using 1:2 with lines ls 5 smooth bezier title "500K", \
  stat200_mean notitle with lines ls 6, \
  "prepped/prepped_tc_32_500.csv" using 1:2 with lines ls 7 smooth bezier title "1M", \
  stat500_mean notitle with lines ls 8
