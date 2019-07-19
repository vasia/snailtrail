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

# paste "../prepped/prepped_st_32_200.csv" "../prepped/prepped_tc_32_200.csv" | xsv select 2,6 -d " " | awk -F ',' '{print ($2 / $1)}' | sort -n | uniq -c | awk 'BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}'

# TODO: what kinds of CDFs do we want to plot?
# xsv select 2 ../prepped/prepped_tc_32_${i}.csv -d " " | sort -n | uniq -c | awk 'BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}' > ../prepped/prepped_tc_32_${i}_latency.csv
# xsv select 2,3 tmp/prepped.csv -d " " | awk -F ',' '{print ($2 / $1)}' | sort -n | uniq -c | awk 'BEGIN{sum=0}{print $2,$1,sum; sum=sum+$1}' > tmp/throughput_cdf.csv

