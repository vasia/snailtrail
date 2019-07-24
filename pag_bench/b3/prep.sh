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
  awk -F '|' -f ../scripts/prep.awk -v workers=32 tc_32_${i}.csv | awk -F " " '$2 != 0' | sort -n > ../prepped/prepped_tc_32_${i}.csv
done

# st
for i in 5 50 200 500
do
  for j in 1 2 4 8 16 32
  do
    awk -F '|' -f ../scripts/prep.awk -v workers=$j "st_${j}_${i}.csv" | awk -F " " '$2 != 0' | sort -n > ../prepped/prepped_st_${j}_${i}.csv
  done
done

# lat vs tp
for i in 5 50 200 500
do
  for j in 1 2 4 8 16 32
  do
    xsv select 2 ../prepped/prepped_st_${j}_${i}.csv -d ' ' | sort -n | uniq -c | awk 'BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}' > tmp_lat_${j}_${i}.csv;
    xsv join -n -d ' ' 1 ../prepped/prepped_st_${j}_${i}.csv 1 ../prepped/prepped_tc_32_${i}.csv | xsv select 2,6 | awk -F ',' '{printf "%.0f\n", ($2 / $1)}' | sort -n | uniq -c |  awk 'BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}' > tmp_tp_${j}_${i}.csv;
    xsv join -n -d ' ' 3 tmp_lat_${j}_${i}.csv 3 tmp_tp_${j}_${i}.csv | xsv fmt -t ' ' > ../prepped/prepped_lat_vs_tp_${j}_${i}.csv;
    rm tmp_lat_${j}_${i}.csv;
    rm tmp_tp_${j}_${i}.csv;
  done
done
  
  
# requires bash 4+ to work
declare -A sizes;
sizes=( [5]=100000 [50]=250000 [200]=500000 [500]=1000000 [5000]=10000000 [50000]=100000000)

# scaling lat
for j in 1 2 4 8 16 32
do
  rm -rf ../prepped/prepped_scaling_lat_${j}.csv || true
  touch ../prepped/prepped_scaling_lat_${j}.csv
  for i in 5 50 200 500
  do
    printf "${sizes[${i}]} " >> ../prepped/prepped_scaling_lat_${j}.csv;
    xsv select 2 ../prepped/prepped_st_${j}_${i}.csv -d ' ' | awk '{sum+=$1}END{print sum/NR}' >> ../prepped/prepped_scaling_lat_${j}.csv;
  done
done

# scaling tp 
for j in 1 2 4 8 16 32
do
  rm -rf ../prepped/prepped_scaling_tp_${j}.csv || true
  touch ../prepped/prepped_scaling_tp_${j}.csv
  for i in 5 50 200 500
  do
    printf "${sizes[${i}]} " >> ../prepped/prepped_scaling_tp_${j}.csv;
    awk -F ' ' '{sum += $2}END{printf "%.0f ", sum}' ../prepped/prepped_st_${j}_${i}.csv >> ../prepped/prepped_scaling_tp_${j}.csv;
    awk -F ' ' '{sum += $3}END{printf "%.0f\n", sum}' ../prepped/prepped_tc_32_${i}.csv >> ../prepped/prepped_scaling_tp_${j}.csv;
  done
done
