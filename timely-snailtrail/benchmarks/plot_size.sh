#!/bin/sh
# CLA: number of source computation workers 
# set -x #echo on

echo "running size plot of $1 source computation workers"
echo "plot name: $2"

mkdir "st_$1/size_dat"

echo "formatting size data"
cd "st_$1/csv"
for i in $(seq 1 $1)
do
  echo "formatting $i of $1"
  FILE=out_0_$i'_'$1
  xsv select 4,2 "$FILE.csv" | xsv fmt -t ' ' > "../size_dat/$FILE.dat"
done

echo "plotting"
cd ..
cp ../plot_size.plt plot_size.tmp 
gnuplot -e "my_title='$2'" plot_size.tmp
rm plot_size.tmp
