#!/bin/sh
# CLA: input file without ending, workers

# general plots
for i in 1 2 4 8 16 32 do
 awk -F '|' -f scripts/prep.awk -v workers=$2 "../${1}${i}.csv" | awk -F " " '$2 != 0' | sort -n > tmp/prepped${i}.csv
done

# cdfs
# xsv select 2 tmp/prepped.csv -d " " | sort -n | uniq -c | awk 'BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}' > tmp/latency_cdf.csv
# xsv select 2,3 tmp/prepped.csv -d " " | awk -F ',' '{print ($2 / $1)}' | sort -n | uniq -c | awk 'BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}' > tmp/throughput_cdf.csv

# plotting
gnuplot scripts/st_general.plt
