set terminal pdf enhanced color font "Helvetica,14" size 6,4 

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

set logscale y 10
set format y "10^{%L}"

set key inside left top Left reverse

set output "plots/versus cdfs.pdf"
set multiplot layout 2,1 rowsfirst

stats "tmp/TC_latency_cdf.csv" using 3 name "TC"
stats "tmp/ST_latency_cdf.csv" using 3 name "ST"
set title "latency\n{/*0.8 y% of epochs take longer than x ms}"
set xlabel "epoch completion time [ms]"
set ylabel "CCDF"
plot "tmp/TC_latency_cdf.csv" using 1:(1-$3 / TC_max) with lines title "TC", \
"tmp/ST_latency_cdf.csv" using 1:(1-$3 / ST_max) with lines title "ST"


stats "tmp/TC_throughput_cdf.csv" using 3 name "TC"
stats "tmp/ST_throughput_cdf.csv" using 3 name "ST"
set title "throughput\n{/*0.8 y% of epochs process less than x [events/edges] / s}"
set xlabel "[events/edges] / s processed"
set ylabel "CCDF"
plot "tmp/TC_throughput_cdf.csv" using ($1 * 1000):($3 / TC_max) with lines title "TC", \
"tmp/ST_throughput_cdf.csv" using ($1 * 1000):($3 / ST_max) with lines title "ST"



