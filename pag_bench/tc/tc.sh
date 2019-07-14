#!/bin/sh
# CLA: input file

# general plots
awk -F '|' -f scripts/tc_prep.awk $1 | sort -n > tmp/tc_prepped.csv

# cdfs
xsv select 2 tmp/tc_prepped.csv -d " " | sort -n | uniq -c | awk 'BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}' > tmp/tc_latency_cdf.csv
xsv select 2,3 tmp/tc_prepped.csv -d " " | awk -F ',' '{print ($2 / $1)}' | sort -n | uniq -c | awk 'BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}' > tmp/tc_throughput_cdf.csv

# plotting
gnuplot scripts/tc.plt
gnuplot scripts/tccdf.plt

rm tmp/*
