set terminal pdf enhanced color font "Helvetica,14" size 6,8 

set style line 1 lc rgb "#0060ad" lt 1 lw 1 pt 2 ps 1
set style line 2 lc rgb "#00796B" lt 1 lw 1 pt 2 ps 1
set style line 3 lc rgb "#8BC34A" lt 1 lw 1 pt 2 ps 1
set style line 4 lc rgb "#F4511E" lt 1 lw 1 pt 2 ps 1
# set style line 5 lc rgb "#F88967" lt 1 lw 1 pt 2 ps 1
# set style line 6 lc rgb "#BC3409" lt 1 lw 1 pt 2 ps 1
# set style line 7 lc rgb "#BC08A7" lt 1 lw 1 pt 2 ps 1
# set style line 8 lc rgb "#bc8c07" lt 1 lw 1 pt 2 ps 1

# set logscale x 10
# set format x "10^{%L}"

# set logscale y 10
# set format y "10^{%L}"

set key inside left top Left reverse


set output "plots/tc_events.pdf"
set multiplot layout 4,1 rowsfirst


set title "TC generated events" 
set xlabel "epoch"
set ylabel "events"
plot "tmp/tc_prepped.csv" using 1:3 with lines ls 3 title "per epoch", \
  "tmp/tc_prepped.csv" using 1:3 with lines ls 4 smooth cumulative title "cumulative"


set title "TC latency" 
set xlabel "epoch"
set ylabel "time [s]"
plot "tmp/tc_prepped.csv" using 1:($2 / 1000) with lines ls 3 title "per epoch", \
"tmp/tc_prepped.csv" using 1:($2 / 1000) smooth cumulative with lines ls 4 title "cumulative"

set title "TC throughput" 
set xlabel "epoch"
set ylabel "events / s"
plot "tmp/tc_prepped.csv" using 1:($3 / ($2 / 1000)) with lines notitle


set title "TC scaling" 
set xlabel "events"
set ylabel "epoch duration [s]"
plot "tmp/tc_prepped.csv" using 3:($2 / 1000) notitle


