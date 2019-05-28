#!/bin/sh
# CLA: number of source computation workers
# set -x #echo on

echo "running latency plotting with $1 source computation workers"
echo "plot name: $2"

mkdir "st_$1/latency_dat"

echo "formatting latency data"
cd "st_$1/csv"
for i in $(seq 1 $1)
do
  echo "formatting $i of $1"
  FILE=out_0_$i'_'$1
  xsv select 1 "$FILE.csv" | sort -n | uniq -c | awk 'BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}' > "../latency_dat/$FILE.dat"
done

echo "plotting"
cd ..
cp ../plot_latencies.plt plot_latencies.tmp 
gnuplot -e "my_title='$2'" plot_latencies.tmp
rm plot_latencies.tmp
