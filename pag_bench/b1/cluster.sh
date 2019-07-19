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
