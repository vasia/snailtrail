set terminal pdf enhanced color font "Helvetica,8" size 4,8 

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
# set yrange [0.0001:20]
  
set logscale x 10
set format x "10^{%L}"
# set xrange [10000:1000000000]

set samples 20000 

set output "plots/st_scaling.pdf"

set multiplot layout 3,1 rowsfirst

set ylabel "epoch latency [s]"
set xlabel "events / epoch"
set title "PAG Latency Scaling"
plot \
  "prepped/prepped_scaling_1.csv" using ($1/1000):2 with lines smooth bezier ls 1 title "w1", \
  "prepped/prepped_scaling_2.csv" using ($1/1000):2 with lines smooth bezier ls 3 title "w2", \
  "prepped/prepped_scaling_4.csv" using ($1/1000):2 with lines smooth bezier ls 5 title "w4", \
  "prepped/prepped_scaling_8.csv" using ($1/1000):2 with lines smooth bezier ls 7 title "w8", \
  "prepped/prepped_scaling_16.csv" using ($1/1000):2 with lines smooth bezier ls 9 title "w16", \
  "prepped/prepped_scaling_32.csv" using ($1/1000):2 with lines smooth bezier ls 11 title "w32"

set ylabel "throughput [events/s]"
set xlabel "events / epoch"
set title "PAG Throughput Scaling"
plot \
  "prepped/prepped_scaling_1.csv" using ($1/1000):3 with lines smooth bezier ls 1 title "w1", \
  "prepped/prepped_scaling_2.csv" using ($1/1000):3 with lines smooth bezier ls 3 title "w2", \
  "prepped/prepped_scaling_4.csv" using ($1/1000):3 with lines smooth bezier ls 5 title "w4", \
  "prepped/prepped_scaling_8.csv" using ($1/1000):3 with lines smooth bezier ls 7 title "w8", \
  "prepped/prepped_scaling_16.csv" using ($1/1000):3 with lines smooth bezier ls 9 title "w16", \
  "prepped/prepped_scaling_32.csv" using ($1/1000):3 with lines smooth bezier ls 11 title "w32"

set ylabel "throughput [events/s]"
set xlabel "latency [s]"
set title "PAG Throughput vs. Latency"
plot \
  "prepped/prepped_scaling_1.csv" using 2:3 with lines smooth bezier ls 1 title "w1", \
  "prepped/prepped_scaling_2.csv" using 2:3 with lines smooth bezier ls 3 title "w2", \
  "prepped/prepped_scaling_4.csv" using 2:3 with lines smooth bezier ls 5 title "w4", \
  "prepped/prepped_scaling_8.csv" using 2:3 with lines smooth bezier ls 7 title "w8", \
  "prepped/prepped_scaling_16.csv" using 2:3 with lines smooth bezier ls 9 title "w16", \
  "prepped/prepped_scaling_32.csv" using 2:3 with lines smooth bezier ls 11 title "w32"
