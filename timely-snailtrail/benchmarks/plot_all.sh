#!/bin/sh
# CLA: number of source computation workers
# set -x #echo on

echo "plotting all with $1 source computation workers"
echo "plots' name: $2"
sleep 2

./plot_latencies.sh $1 "latency, $2"
./plot_size.sh $1 "size, $2"
