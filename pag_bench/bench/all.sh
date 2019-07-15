#!/bin/sh

./bench.sh ../tc_stats.csv TC "events"
./bench.sh ../st_stats.csv ST "pag edges"
gnuplot scripts/versus_cdf.plt
gnuplot scripts/versus_general.plt
  
rm tmp/*
