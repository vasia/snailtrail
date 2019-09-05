#!/usr/local/bin/bash

cd raw

# tc 
awk -F '|' -f ../scripts/prep.awk -v workers=32 tc_32_200.csv | awk -F " " '$2 != 0' | sort -n | awk  -F ' ' -f ../scripts/spread.awk | sort -n > ../prepped/prepped_tc_32_200.csv

# st
awk -F '|' -f ../scripts/prep.awk "algo_32_200.csv" | awk -F " " '$2 != 0' | sort -n | awk  -F ' ' -f ../scripts/spread.awk | sort -n > ../prepped/prepped_algo_32_200.csv
awk -F '|' -f ../scripts/prep.awk "maxepoch_32_200.csv" | awk -F " " '$2 != 0' | sort -n | awk  -F ' ' -f ../scripts/spread.awk | sort -n > ../prepped/prepped_maxepoch_32_200.csv
awk -F '|' -f ../scripts/prep.awk "maxmsg_32_200.csv" | awk -F " " '$2 != 0' | sort -n | awk  -F ' ' -f ../scripts/spread.awk | sort -n > ../prepped/prepped_maxmsg_32_200.csv
awk -F '|' -f ../scripts/prep.awk "maxop_32_200.csv" | awk -F " " '$2 != 0' | sort -n | awk  -F ' ' -f ../scripts/spread.awk | sort -n > ../prepped/prepped_maxop_32_200.csv
awk -F '|' -f ../scripts/prep.awk "maxprog_32_200.csv" | awk -F " " '$2 != 0' | sort -n | awk  -F ' ' -f ../scripts/spread.awk | sort -n > ../prepped/prepped_maxprog_32_200.csv
awk -F '|' -f ../scripts/prep.awk "metrics_32_200.csv" | awk -F " " '$2 != 0' | sort -n | awk  -F ' ' -f ../scripts/spread.awk | sort -n > ../prepped/prepped_metrics_32_200.csv
awk -F '|' -f ../scripts/prep.awk "pag_32_200.csv" | awk -F " " '$2 != 0' | sort -n | awk  -F ' ' -f ../scripts/spread.awk | sort -n > ../prepped/prepped_pag_32_200.csv
