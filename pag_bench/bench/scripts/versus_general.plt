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

# set logscale y 10
# set format y "10^{%L}"

set key inside left top Left reverse


set output "plots/versus events.pdf"
set multiplot layout 4,1 rowsfirst

stats "tmp/TC_prepped.csv" using 3 name "TC"
stats "tmp/ST_prepped.csv" using 3 name "ST"
set title "generated"
set xlabel "epoch"
set ylabel "events / edges"
plot "tmp/TC_prepped.csv" using 1:3 with lines title "TC", \
TC_mean notitle ls 9, \
 "tmp/ST_prepped.csv" using 1:3 with lines title "ST", \
ST_mean notitle ls 9


stats "tmp/TC_prepped.csv" using ($2 / 1000) name "TC"
stats "tmp/ST_prepped.csv" using ($2 / 1000) name "ST"
set title "latency"
set xlabel "epoch"
set ylabel "time [s]"
plot "tmp/TC_prepped.csv" using 1:($2 / 1000) with lines title "TC", \
TC_mean notitle ls 9, \
"tmp/ST_prepped.csv" using 1:($2 / 1000) with lines title "ST", \
ST_mean notitle ls 9


stats "tmp/TC_prepped.csv" using 3 name "TC_EVS"
stats "tmp/TC_prepped.csv" using ($2 / 1000) name "TC_DUR"
stats "tmp/ST_prepped.csv" using 3 name "ST_EVS"
stats "tmp/ST_prepped.csv" using ($2 / 1000) name "ST_DUR"
set title "throughput"
set xlabel "epoch"
set ylabel "[events/edges] / s"
plot "tmp/TC_prepped.csv" using 1:($3 / ($2 / 1000)) with lines title "TC", \
TC_EVS_mean / TC_DUR_mean notitle ls 9, \
"tmp/ST_prepped.csv" using 1:($3 / ($2 / 1000)) with lines title "ST", \
ST_EVS_mean / ST_DUR_mean notitle ls 9


set title "scaling"
set xlabel "events / edges" 
set ylabel "epoch duration [s]"
plot "tmp/TC_prepped.csv" using 3:($2 / 1000) title "TC", \
 "tmp/ST_prepped.csv" using 3:($2 / 1000) title "ST"


