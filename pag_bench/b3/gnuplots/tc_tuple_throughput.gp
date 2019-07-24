set terminal pdf enhanced color font "Helvetica,14" #size 6,8 

set style line 1 lc rgb "#0060ad" lt 1 lw 1 pt 2 ps 1
set style line 2 lc rgb "#0060ad" lt 1 lw 1 pt 2 ps 1 dashtype "_"
set style line 3 lc rgb "#00796B" lt 1 lw 1 pt 2 ps 1
set style line 4 lc rgb "#00796B" lt 1 lw 1 pt 2 ps 1 dashtype "_"
set style line 5 lc rgb "#8BC34A" lt 1 lw 1 pt 2 ps 1
set style line 6 lc rgb "#8BC34A" lt 1 lw 1 pt 2 ps 1 dashtype "_"
set style line 7 lc rgb "#F4511E" lt 1 lw 1 pt 2 ps 1
set style line 8 lc rgb "#F4511E" lt 1 lw 1 pt 2 ps 1 dashtype "_"

# set logscale y 10
# set format y "10^{%L}"

set samples 20000 

set output "plots/tc_32_tuple_throughput.pdf"

set title "triangles tuple throughput"
set xlabel "epoch"
set ylabel "tuples / s"

stats "prepped/prepped_tc_32_5.csv" using 2 name "statdur5"
stats "prepped/prepped_tc_32_50.csv" using 2 name "statdur50"
stats "prepped/prepped_tc_32_200.csv" using 2 name "statdur200"
stats "prepped/prepped_tc_32_500.csv" using 2 name "statdur500"
stats "prepped/prepped_tuples_32_5.csv" using 2 name "statevs5"
stats "prepped/prepped_tuples_32_50.csv" using 2 name "statevs50"
stats "prepped/prepped_tuples_32_200.csv" using 2 name "statevs200"
stats "prepped/prepped_tuples_32_500.csv" using 2 name "statevs500"
plot \
  "< xsv join -n -d ' ' 1 prepped/prepped_tc_32_5.csv 1 prepped/prepped_tuples_32_5.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 1 smooth bezier title "150K", \
  (statevs5_mean / statdur5_mean) notitle with lines ls 2, \
  "< xsv join -n -d ' ' 1 prepped/prepped_tc_32_50.csv 1 prepped/prepped_tuples_32_50.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 3 smooth bezier title "250K", \
  (statevs50_mean / statdur50_mean) notitle with lines ls 4, \
  "< xsv join -n -d ' ' 1 prepped/prepped_tc_32_200.csv 1 prepped/prepped_tuples_32_200.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 5 smooth bezier title "500K", \
  (statevs200_mean / statdur200_mean) notitle with lines ls 6, \
  "< xsv join -n -d ' ' 1 prepped/prepped_tc_32_500.csv 1 prepped/prepped_tuples_32_500.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 7 smooth bezier title "1M", \
  (statevs500_mean / statdur500_mean) notitle with lines ls 8
