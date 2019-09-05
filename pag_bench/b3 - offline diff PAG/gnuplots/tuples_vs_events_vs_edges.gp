set terminal pdf enhanced color font "Helvetica,8" size 6,8 

set style line 1 lc rgb "#0060ad" lt 1 lw 2 pt 2 ps 1
set style line 2 lc rgb "#0060ad" lt 1 lw 2 pt 2 ps 1 dashtype 2
set style line 3 lc rgb "#00796B" lt 1 lw 2 pt 2 ps 1
set style line 4 lc rgb "#00796B" lt 1 lw 2 pt 2 ps 1 dashtype 2
set style line 5 lc rgb "#8BC34A" lt 1 lw 2 pt 2 ps 1
set style line 6 lc rgb "#8BC34A" lt 1 lw 2 pt 2 ps 1 dashtype 2

set logscale y 10
set format y "10^{%L}"
# set yrange [1000:10000000]

set samples 20000 

set output "plots/tuples_vs_events_vs_edges.pdf"

set multiplot layout 4,1 rowsfirst
set xlabel "epoch"
set ylabel "count"

set title "tuples vs. log events vs. edges [150K]"
stats "prepped/prepped_tc_32_5.csv" using 3 name "statevs"
stats "prepped/prepped_tuples_32_5.csv" using 2 name "stattup"
stats "prepped/prepped_st_32_5.csv" using 3 name "statedg"
plot \
  "prepped/prepped_tc_32_5.csv" using 1:3 with lines ls 1 smooth bezier title "log events", \
  statevs_mean notitle with lines ls 2, \
  "prepped/prepped_tuples_32_5.csv" using 1:2 with lines ls 3 smooth bezier title "tuples", \
  stattup_mean notitle with lines ls 4, \
  "prepped/prepped_st_32_5.csv" using 1:3 with lines ls 5 smooth bezier title "edges", \
  statedg_mean notitle with lines ls 6

set title "tuples vs. log events vs. edges [250K]"
stats "prepped/prepped_tc_32_50.csv" using 3 name "statevs"
stats "prepped/prepped_tuples_32_50.csv" using 2 name "stattup"
stats "prepped/prepped_st_32_50.csv" using 3 name "statedg"
plot \
  "prepped/prepped_tc_32_50.csv" using 1:3 with lines ls 1 smooth bezier title "log events", \
  statevs_mean notitle with lines ls 2, \
  "prepped/prepped_tuples_32_50.csv" using 1:2 with lines ls 3 smooth bezier title "tuples", \
  stattup_mean notitle with lines ls 4, \
  "prepped/prepped_st_32_50.csv" using 1:3 with lines ls 5 smooth bezier title "edges", \
  statedg_mean notitle with lines ls 6

set title "tuples vs. log events vs. edges [500K]"
stats "prepped/prepped_tc_32_200.csv" using 3 name "statevs"
stats "prepped/prepped_tuples_32_200.csv" using 2 name "stattup"
stats "prepped/prepped_st_32_200.csv" using 3 name "statedg"
plot \
  "prepped/prepped_tc_32_200.csv" using 1:3 with lines ls 1 smooth bezier title "log events", \
  statevs_mean notitle with lines ls 2, \
  "prepped/prepped_tuples_32_200.csv" using 1:2 with lines ls 3 smooth bezier title "tuples", \
  stattup_mean notitle with lines ls 4, \
  "prepped/prepped_st_32_200.csv" using 1:3 with lines ls 5 smooth bezier title "edges", \
  statedg_mean notitle with lines ls 6

set title "tuples vs. log events vs. edges [1M]"
stats "prepped/prepped_tc_32_500.csv" using 3 name "statevs"
stats "prepped/prepped_tuples_32_500.csv" using 2 name "stattup"
stats "prepped/prepped_st_32_500.csv" using 3 name "statedg"
plot \
  "prepped/prepped_tc_32_500.csv" using 1:3 with lines ls 1 smooth bezier title "log events", \
  statevs_mean notitle with lines ls 2, \
  "prepped/prepped_tuples_32_500.csv" using 1:2 with lines ls 3 smooth bezier title "tuples", \
  stattup_mean notitle with lines ls 4, \
  "prepped/prepped_st_32_500.csv" using 1:3 with lines ls 5 smooth bezier title "edges", \
  statedg_mean notitle with lines ls 6
