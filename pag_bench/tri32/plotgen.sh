for v in st tc; do for i in 1 5 10 50 100 200 300 400 500; do ./bench.sh ../bsb2/raw/${v}_stats${i}.csv "${v}${i}" elements 32; cp -r plots/ ../bsb2/plots; cp -r tmp/ ../bsb2/tmp/; done; done
