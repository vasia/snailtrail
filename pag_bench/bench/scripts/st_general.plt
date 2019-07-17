set terminal pdf enhanced color font "Helvetica,14" size 6,8 

set style line 1 lc rgb "#0060ad" lt 1 lw 1 pt 2 ps 1
set style line 2 lc rgb "#00796B" lt 1 lw 1 pt 2 ps 1
set style line 3 lc rgb "#8BC34A" lt 1 lw 1 pt 2 ps 1
set style line 4 lc rgb "#F4511E" lt 1 lw 1 pt 2 ps 1
# set style line 5 lc rgb "#F88967" lt 1 lw 1 pt 2 ps 1
# set style line 6 lc rgb "#BC3409" lt 1 lw 1 pt 2 ps 1
# set style line 7 lc rgb "#BC08A7" lt 1 lw 1 pt 2 ps 1
# set style line 8 lc rgb "#bc8c07" lt 1 lw 1 pt 2 ps 1
set style line 9 lc rgb "#ff0000" lt 1 lw 1 pt 2 ps 1

# set logscale x 10
# set format x "10^{%L}"

# set logscale y 10
# set format y "10^{%L}"

set key inside left top Left reverse


set output "plots/st general.pdf"
set multiplot layout 4,1 rowsfirst
set samples 20000 


set title "st generated pag edges"
set xlabel "epoch"
set ylabel "pag edges" 
set yrange [0:1500000]
plot "tmp/prepped1.csv" using 1:3 smooth bezier with lines title "1 w", \
"tmp/prepped2.csv" using 1:3 smooth bezier with lines title "2 w", \
"tmp/prepped4.csv" using 1:3 smooth bezier with lines title "4 w", \
"tmp/prepped8.csv" using 1:3 smooth bezier with lines title "8 w", \
"tmp/prepped16.csv" using 1:3 smooth bezier with lines title "16 w", \
"tmp/prepped32.csv" using 1:3 smooth bezier with lines title "32 w"
unset yrange


set title "latency"
set xlabel "epoch"
set ylabel "time [ms]"
set yrange [0:100]
plot "tmp/prepped1.csv" using 1:2 with lines smooth bezier title "1 w", \
"tmp/prepped2.csv" using 1:2 with lines smooth bezier title "2 w", \
"tmp/prepped4.csv" using 1:2 with lines smooth bezier title "4 w", \
"tmp/prepped8.csv" using 1:2 with lines smooth bezier title "8 w", \
"tmp/prepped16.csv" using 1:2 with lines smooth bezier title "16 w", \
"tmp/prepped32.csv" using 1:2 with lines smooth bezier title "32 w"
unset yrange

set title "throughput"
set xlabel "epoch"
set ylabel "pag edges / s"
set yrange [0:100000]
plot "tmp/prepped1.csv" using 1:($3 / $2) with lines smooth bezier title "1 w", \
"tmp/prepped2.csv" using 1:($3 / $2) with lines smooth bezier title "2 w", \
"tmp/prepped4.csv" using 1:($3 / $2) with lines smooth bezier title "4 w", \
"tmp/prepped8.csv" using 1:($3 / $2) with lines smooth bezier title "8 w", \
"tmp/prepped16.csv" using 1:($3 / $2) with lines smooth bezier title "16 w", \
"tmp/prepped32.csv" using 1:($3 / $2) with lines smooth bezier title "32 w"
unset yrange


set title "scaling"
set xlabel "pag edges" 
set ylabel "epoch duration [ms]"
plot "tmp/prepped1.csv" using 3:2 title "1 w", \
"tmp/prepped2.csv" using 3:2 title "2 w", \
"tmp/prepped4.csv" using 3:2 title "4 w", \
"tmp/prepped8.csv" using 3:2 title "8 w", \
"tmp/prepped16.csv" using 3:2 title "16 w", \
"tmp/prepped32.csv" using 3:2 title "32 w"



