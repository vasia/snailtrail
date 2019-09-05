time triangles livejournal.graph ${1} 1000 1 10000 "../files/tuples_offline_${2}_${1}" -- -w${2} > "../files/tc_offline_${2}_${1}.csv"
time inspect $1 $2 1 > ../files/st_offline_${1}_${2}_${3}.csv

time env SNAILTRAIL_ADDR=192.168.1.64:1234 triangles livejournal.graph ${1} 1000 1 $3 "../files/tuples_online_${2}_${1}" -- -w${2} > "../files/tc_online_${2}_${1}.csv"
time inspect $1 $2 1 0.0.0.0 1234 > ../files/st_online_${1}_${2}_${3}.csv

 
