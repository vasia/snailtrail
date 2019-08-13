time env SNAILTRAIL_ADDR=192.168.1.64:1234 triangles livejournal.graph ${1} 1000 1 $3 "../files/tuples_online_${2}_${1}" -- -w${2} > "../files/tc_online_${2}_${1}.csv"
