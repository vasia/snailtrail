#!/bin/sh
# CLA: number of source computation workers
# set -x #echo on

echo "running benchmarks with $1 source computation workers"
sleep 2

mkdir "st_$1"
mkdir "st_$1/csv"

echo "starting benchmarks"
cd ..
for i in $(seq 1 $1)
do
  echo "bench $i"
  sleep 1
  cargo run --release --example inspect $i $1 f
done

echo "(re)moving files"
mv -f out_0*.csv "benchmarks/st_$1/csv/"
rm out_*.csv

echo "benchmark completed"
