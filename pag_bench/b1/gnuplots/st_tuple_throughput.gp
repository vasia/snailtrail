set terminal pdf enhanced color font "Helvetica,14" size 6,8 

set style line 1 lc rgb "#0060ad" lt 1 lw 1 pt 2 ps 1
set style line 2 lc rgb "#0060ad" lt 1 lw 1 pt 2 ps 1 dashtype "_"
set style line 3 lc rgb "#00796B" lt 1 lw 1 pt 2 ps 1
set style line 4 lc rgb "#00796B" lt 1 lw 1 pt 2 ps 1 dashtype "_"
set style line 5 lc rgb "#8BC34A" lt 1 lw 1 pt 2 ps 1
set style line 6 lc rgb "#8BC34A" lt 1 lw 1 pt 2 ps 1 dashtype "_"
set style line 7 lc rgb "#F4511E" lt 1 lw 1 pt 2 ps 1
set style line 8 lc rgb "#F4511E" lt 1 lw 1 pt 2 ps 1 dashtype "_"
set style line 9 lc rgb "#F88967" lt 1 lw 1 pt 2 ps 1
set style line 10 lc rgb "#F88967" lt 1 lw 1 pt 2 ps 1 dashtype "_"
set style line 11 lc rgb "#BC3409" lt 1 lw 1 pt 2 ps 1
set style line 12 lc rgb "#BC3409" lt 1 lw 1 pt 2 ps 1 dashtype "_"
set style line 13 lc rgb "#BC08A7" lt 1 lw 1 pt 2 ps 1 dashtype "."


set logscale y 10
set format y "10^{%L}"
set yrange [20000:200000000]

set samples 20000 

set output "plots/st_tuple_throughput.pdf"

set multiplot layout 4,1 rowsfirst
set xlabel "epoch"
set ylabel "tuples / s"

set title "PAG construction offline tuple throughput [150K]"
plot \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_1_5.csv 1 prepped/prepped_tuples_32_5.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 1 smooth bezier title "w1", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_2_5.csv 1 prepped/prepped_tuples_32_5.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 3 smooth bezier title "w2", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_4_5.csv 1 prepped/prepped_tuples_32_5.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 5 smooth bezier title "w4", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_8_5.csv 1 prepped/prepped_tuples_32_5.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 7 smooth bezier title "w8", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_16_5.csv 1 prepped/prepped_tuples_32_5.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 9 smooth bezier title "w16", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_32_5.csv 1 prepped/prepped_tuples_32_5.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 11 smooth bezier title "w32", \
  "< xsv join -n -d ' ' 1 prepped/prepped_tc_32_5.csv 1 prepped/prepped_tuples_32_5.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 13 smooth bezier title "tri32"

set title "PAG construction offline tuple throughput [250K]"
plot \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_1_50.csv 1 prepped/prepped_tuples_32_50.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 1 smooth bezier title "w1", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_2_50.csv 1 prepped/prepped_tuples_32_50.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 3 smooth bezier title "w2", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_4_50.csv 1 prepped/prepped_tuples_32_50.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 5 smooth bezier title "w4", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_8_50.csv 1 prepped/prepped_tuples_32_50.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 7 smooth bezier title "w8", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_16_50.csv 1 prepped/prepped_tuples_32_50.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 9 smooth bezier title "w16", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_32_50.csv 1 prepped/prepped_tuples_32_50.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 11 smooth bezier title "w32", \
  "< xsv join -n -d ' ' 1 prepped/prepped_tc_32_50.csv 1 prepped/prepped_tuples_32_50.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 13 smooth bezier title "tri32"

set title "PAG construction offline tuple throughput [500K]"
plot \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_1_200.csv 1 prepped/prepped_tuples_32_200.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 1 smooth bezier title "w1", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_2_200.csv 1 prepped/prepped_tuples_32_200.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 3 smooth bezier title "w2", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_4_200.csv 1 prepped/prepped_tuples_32_200.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 5 smooth bezier title "w4", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_8_200.csv 1 prepped/prepped_tuples_32_200.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 7 smooth bezier title "w8", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_16_200.csv 1 prepped/prepped_tuples_32_200.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 9 smooth bezier title "w16", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_32_200.csv 1 prepped/prepped_tuples_32_200.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 11 smooth bezier title "w32", \
  "< xsv join -n -d ' ' 1 prepped/prepped_tc_32_200.csv 1 prepped/prepped_tuples_32_200.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 13 smooth bezier title "tri32"

set title "PAG construction offline tuple throughput [1M]"
plot \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_1_500.csv 1 prepped/prepped_tuples_32_500.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 1 smooth bezier title "w1", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_2_500.csv 1 prepped/prepped_tuples_32_500.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 3 smooth bezier title "w2", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_4_500.csv 1 prepped/prepped_tuples_32_500.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 5 smooth bezier title "w4", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_8_500.csv 1 prepped/prepped_tuples_32_500.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 7 smooth bezier title "w8", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_16_500.csv 1 prepped/prepped_tuples_32_500.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 9 smooth bezier title "w16", \
  "< xsv join -n -d ' ' 1 prepped/prepped_st_32_500.csv 1 prepped/prepped_tuples_32_500.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 11 smooth bezier title "w32", \
  "< xsv join -n -d ' ' 1 prepped/prepped_tc_32_500.csv 1 prepped/prepped_tuples_32_500.csv | xsv fmt -t ' '" using 1:($5 / $2) with lines ls 13 smooth bezier title "tri32"
