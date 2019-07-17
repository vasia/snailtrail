set terminal pdf enhanced color font "Helvetica,14" size 6,8 

set style line 1 lc rgb "#0060ad" lt 1 lw 1 pt 2 ps 1
set style line 2 lc rgb "#00796B" lt 1 lw 1 pt 2 ps 1
set style line 3 lc rgb "#8BC34A" lt 1 lw 1 pt 2 ps 1
set style line 4 lc rgb "#F4511E" lt 1 lw 1 pt 2 ps 1
# set style line 5 lc rgb "#F88967" lt 1 lw 1 pt 2 ps 1
# set style line 6 lc rgb "#BC3409" lt 1 lw 1 pt 2 ps 1
# set style line 7 lc rgb "#BC08A7" lt 1 lw 1 pt 2 ps 1
# set style line 8 lc rgb "#bc8c07" lt 1 lw 1 pt 2 ps 1
set style line 9 lc rgb "#ff0000" lt 1 lw 1 pt 2 ps 1

# set logscale x 10
# set format x "10^{%L}"

set logscale y 10
set format y "10^{%L}"

set key inside left top Left reverse


set output sprintf("plots/%s events.pdf", my_title)
set multiplot layout 4,1 rowsfirst

stats "tmp/prepped.csv" using 3
set title sprintf("%s generated %s", my_title, my_event)
set xlabel "epoch"
set ylabel sprintf("%s", my_event)
plot "tmp/prepped.csv" using 1:3 with lines ls 3 notitle, \
STATS_mean notitle ls 9
  # "tmp/prepped.csv" using 1:3 with lines ls 4 smooth cumulative title "cumulative"


stats "tmp/prepped.csv" using ($2 / 1000)
set title sprintf("%s latency", my_title)
set xlabel "epoch"
set ylabel "time [s]"
plot "tmp/prepped.csv" using 1:($2 / 1000) with lines ls 3 notitle, \
STATS_mean notitle ls 9
# "tmp/prepped.csv" using 1:($2 / 1000) smooth cumulative with lines ls 4 title "cumulative"


stats "tmp/prepped.csv" using 3 name "EVS"
stats "tmp/prepped.csv" using ($2 / 1000) name "DUR"
set title sprintf("%s throughput", my_title)
set xlabel "epoch"
set ylabel sprintf("%s / s", my_event)
plot "tmp/prepped.csv" using 1:($3 / ($2 / 1000)) with lines notitle, \
(EVS_mean / DUR_mean) notitle ls 9


set title sprintf("%s scaling", my_title)
set xlabel sprintf("%s", my_event)
set ylabel "epoch duration [s]"
plot "tmp/prepped.csv" using 3:($2 / 1000) notitle


