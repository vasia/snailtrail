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

set output sprintf("plots/%s cdfs.pdf", my_title)
set multiplot layout 2,1 rowsfirst

# xsv select 2 prepped.csv -d " " | sort -n | uniq -c | awk 'BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}' > latency_cdf.csv
stats "tmp/latency_cdf.csv" using 3
set title sprintf("%s latency\n{/*0.8 y%% of epochs take longer than x ms}", my_title)
set xlabel "epoch completion time [ms]"
set ylabel "CCDF"
plot "tmp/latency_cdf.csv" using 1:(1-$3 / STATS_max) with lines notitle


# xsv select 2,3 prepped.csv -d " " | awk -F ',' '{print ($2 / $1)}' | sort -n | uniq -c | awk 'BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}' > throughput_cdf.csv
stats "tmp/throughput_cdf.csv" using 3
set title sprintf("%s throughput\n{/*0.8 y%% of epochs process less than x %s / s}", my_title, my_event)
set xlabel sprintf("%s / s processed", my_event)
set ylabel "CCDF"
plot "tmp/throughput_cdf.csv" using ($1 * 1000):($3 / STATS_max) with lines notitle



