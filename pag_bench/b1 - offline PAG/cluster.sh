for i in 5 50 200 500
do
  echo "triangles w32, $i";
  time triangles livejournal.graph $i 1000 1 4096 "../files/tuples_32_${i}" -- -w32 > "../files/tc_32_${i}.csv";
  df -h | grep shm;
  for j in 1 2 4 8 16 32
  do
    echo "inspect w${j}, $i";
    time inspect $j 32 1 > ../files/st_${j}_${i}.csv;
  done
  rm /dev/shm/*.dump;
  echo "-------";
done

echo "triangles w32, 5000";
time triangles livejournal.graph 500 1000 1 4096 "../files/tuples_32_5000" -- -w32 > "../files/tc_32_5000.csv";
df -h | grep shm;
for j in 1 2 4 8 16 32
do
  echo "inspect w${j}, 5000";
  time inspect $j 32 1 > ../files/st_${j}_5000.csv;
done
rm /dev/shm/*.dump;

echo "triangles w32, 50000";
time triangles livejournal.graph 500 1000 1 4096 "../files/tuples_32_50000" -- -w32 > "../files/tc_32_50000.csv";
df -h | grep shm;
for j in 1 2 4 8 16 32
do
  echo "inspect w${j}, 50000";
  time cargo run --release -- -s 32 -w ${j} -f /dev/shm inspect > ../files/st_${j}_50000.csv;
done
rm /dev/shm/*.dump;
