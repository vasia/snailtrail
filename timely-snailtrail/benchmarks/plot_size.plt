set terminal pdf enhanced color font "Helvetica,14"

set title my_title 

set style line 1 lc rgb "#0060ad" lt 1 lw 1 pt 2 ps 1
set style line 2 lc rgb "#00796B" lt 1 lw 1 pt 2 ps 1
set style line 3 lc rgb "#8BC34A" lt 1 lw 1 pt 2 ps 1
set style line 4 lc rgb "#F4511E" lt 1 lw 1 pt 2 ps 1
set style line 5 lc rgb "#F88967" lt 1 lw 1 pt 2 ps 1
set style line 6 lc rgb "#BC3409" lt 1 lw 1 pt 2 ps 1
set style line 7 lc rgb "#BC08A7" lt 1 lw 1 pt 2 ps 1
set style line 8 lc rgb "#bc8c07" lt 1 lw 1 pt 2 ps 1

set xlabel "sys time"
set ylabel "#pag edges"

set key inside left top Left reverse

set output "size.pdf"

data = system('ls size_dat/*')
count = 0

do for [file in data] {
  stats sprintf("%s", file) using 2;
  count = count + 1
}
  
plot for [i=1:count] sprintf("size_dat/out_0_%d_%d.dat", i, count) using 1:2 ls i with impulses title sprintf("w%d", i)
