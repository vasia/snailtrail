#!/bin/sh
# CLA: workers

./bench.sh ../tc_stats.csv TC "events" $1
./bench.sh ../st_stats.csv ST "pag edges" $1
gnuplot scripts/versus_cdf.plt
gnuplot scripts/versus_general.plt
  
# rm tmp/*
