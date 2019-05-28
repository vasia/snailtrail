# in order to prepare the data (assuming single column of latencies):
# xsv select 1 raw/... | sort -n | uniq --count | awk 'BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}' > parsed.dat

set terminal pdf enhanced color font "Helvetica,14"

set title my_title 

set style line 1 lc rgb "#0060ad" lt 1 lw 1 pt 2 ps 1
set style line 2 lc rgb "#00796B" lt 1 lw 1 pt 2 ps 1
set style line 3 lc rgb "#8BC34A" lt 1 lw 1 pt 2 ps 1
set style line 4 lc rgb "#F4511E" lt 1 lw 1 pt 2 ps 1
set style line 5 lc rgb "#F88967" lt 1 lw 1 pt 2 ps 1
set style line 6 lc rgb "#BC3409" lt 1 lw 1 pt 2 ps 1
set style line 7 lc rgb "#BC08A7" lt 1 lw 1 pt 2 ps 1
set style line 8 lc rgb "#bc8c07" lt 1 lw 1 pt 2 ps 1

set xlabel "Completion Time (ms)"
set ylabel "CCDF"

set logscale x 10
set format x "10^{%L}"

set logscale y 10
set format y "10^{%L}"

set key inside left bottom Left reverse

set output "latencies.pdf"

data = system('ls latency_dat/*')
count = 0

do for [file in data] {
  stats sprintf("%s", file) using 3;
  count = count + 1
}
  
plot for [i=1:count] sprintf("latency_dat/out_0_%d_%d.dat", i, count) using 1:(1-$3/STATS_max) ls i with lines title sprintf("w%d", i)
