set terminal pdf enhanced color font "Helvetica,14" size 4,8 

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
# set yrange [0.0001:20]
  
set logscale x 10
set format x "10^{%L}"
set xrange [10000:1000000000]

# set samples 20000 

set output "plots/latency_vs_throughput.pdf"

set multiplot layout 4,1 rowsfirst
set ylabel "latency [s]"
set xlabel "throughput [log events / s]"

set title "PAG construction latency vs throughput [150K]"
plot \
  "prepped/prepped_lat_vs_tp_1_5.csv" using 4:1 with lines ls 1 title "w1", \
  "prepped/prepped_lat_vs_tp_2_5.csv" using 4:1 with lines ls 3 title "w2", \
  "prepped/prepped_lat_vs_tp_4_5.csv" using 4:1 with lines ls 5 title "w4", \
  "prepped/prepped_lat_vs_tp_8_5.csv" using 4:1 with lines ls 7 title "w8", \
  "prepped/prepped_lat_vs_tp_16_5.csv" using 4:1 with lines ls 9 title "w16", \
  "prepped/prepped_lat_vs_tp_32_5.csv" using 4:1 with lines ls 11 title "w32"

set title "PAG construction latency vs throughput [250K]"
plot \
  "prepped/prepped_lat_vs_tp_1_50.csv" using 4:1 with lines ls 1 title "w1", \
  "prepped/prepped_lat_vs_tp_2_50.csv" using 4:1 with lines ls 3 title "w2", \
  "prepped/prepped_lat_vs_tp_4_50.csv" using 4:1 with lines ls 5 title "w4", \
  "prepped/prepped_lat_vs_tp_8_50.csv" using 4:1 with lines ls 7 title "w8", \
  "prepped/prepped_lat_vs_tp_16_50.csv" using 4:1 with lines ls 9 title "w16", \
  "prepped/prepped_lat_vs_tp_32_50.csv" using 4:1 with lines ls 11 title "w32"

set title "PAG construction latency vs throughput [500K]"
plot \
  "prepped/prepped_lat_vs_tp_1_200.csv" using 4:1 with lines ls 1 title "w1", \
  "prepped/prepped_lat_vs_tp_2_200.csv" using 4:1 with lines ls 3 title "w2", \
  "prepped/prepped_lat_vs_tp_4_200.csv" using 4:1 with lines ls 5 title "w4", \
  "prepped/prepped_lat_vs_tp_8_200.csv" using 4:1 with lines ls 7 title "w8", \
  "prepped/prepped_lat_vs_tp_16_200.csv" using 4:1 with lines ls 9 title "w16", \
  "prepped/prepped_lat_vs_tp_32_200.csv" using 4:1 with lines ls 11 title "w32"

set title "PAG construction latency vs throughput [1M]"
plot \
  "prepped/prepped_lat_vs_tp_1_500.csv" using 4:1 with lines ls 1 title "w1", \
  "prepped/prepped_lat_vs_tp_2_500.csv" using 4:1 with lines ls 3 title "w2", \
  "prepped/prepped_lat_vs_tp_4_500.csv" using 4:1 with lines ls 5 title "w4", \
  "prepped/prepped_lat_vs_tp_8_500.csv" using 4:1 with lines ls 7 title "w8", \
  "prepped/prepped_lat_vs_tp_16_500.csv" using 4:1 with lines ls 9 title "w16", \
  "prepped/prepped_lat_vs_tp_32_500.csv" using 4:1 with lines ls 11 title "w32"
