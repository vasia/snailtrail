#!/bin/sh
# CLA: input file, title, [pag edges / events], workers

# general plots
awk -F '|' -f scripts/prep.awk -v workers=$4 $1 | awk -F " " '$2 != 0' | sort -n > tmp/prepped.csv

# cdfs
xsv select 2 tmp/prepped.csv -d " " | sort -n | uniq -c | awk 'BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}' > tmp/latency_cdf.csv
xsv select 2,3 tmp/prepped.csv -d " " | awk -F ',' '{print ($2 / $1)}' | sort -n | uniq -c | awk 'BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}' > tmp/throughput_cdf.csv

# plotting
gnuplot -e "my_title='$2'; my_event='$3'" scripts/general.plt
gnuplot -e "my_title='$2'; my_event='$3'" scripts/cdf.plt

mv tmp/prepped.csv tmp/$2_prepped.csv
mv tmp/latency_cdf.csv tmp/$2_latency_cdf.csv
mv tmp/throughput_cdf.csv tmp/$2_throughput_cdf.csv
