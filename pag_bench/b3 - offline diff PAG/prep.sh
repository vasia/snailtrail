#!/usr/local/bin/bash

cd raw

# tuples
for i in 5 50 200 500
do
  cat tuples_32_${i}_* > ../prepped/tuples_32_${i}.csv;
  awk -F '|' -f ../scripts/prep_tuples.awk ../prepped/tuples_32_${i}.csv | sort -n > ../prepped/prepped_tuples_32_${i}.csv
done
 
# tc 
for i in 5 50 200 500
do
  awk -F '|' -f ../scripts/prep.awk -v workers=32 tc_32_${i}.csv | awk -F " " '$2 != 0' | sort -n | awk  -F ' ' -f ../scripts/spread.awk | sort -n > ../prepped/prepped_tc_32_${i}.csv
done

# st
for i in 5 50 200 500
do
  for j in 1 2 4 8 16 32
  do
    awk -F '|' -f ../scripts/prep.awk -v workers=$j "st_${j}_${i}.csv" | awk -F " " '$2 != 0' | sort -n | awk  -F ' ' -f ../scripts/spread.awk | sort -n > ../prepped/prepped_st_${j}_${i}.csv
  done
done
  
  
# scaling
for j in 1 2 4 8 16 32
do
  rm -rf ../prepped/prepped_scaling_${j}.csv || true
  touch ../prepped/prepped_scaling_${j}.csv
  for i in 5 50 200 500
  do
    xsv join -n -d ' ' 1 ../prepped/prepped_st_${j}_${i}.csv 1 ../prepped/prepped_tc_32_${i}.csv | xsv select 2,6 | awk -F ',' '{events+=$2; time+=sprintf("%f",$1)}END{printf "%.8f %.8f %.8f\n", events/NR, time/NR, events/time;}' >> ../prepped/prepped_scaling_${j}.csv
  done
done
