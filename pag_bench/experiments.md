201907061658 SnailTrail Benchmarking
#thesis #benchmark

# PAG Join Comparison

## Stats

- triangles 30K, 2w, 2lbf (134s)
- macbook
- 41945340 events
- 750136 control edges
- 40178576 all edges (cross-epoch edges included)
- 40173232 all edges (no cross-epoch edges)

## pag construction (differential joins)

- 1 4 1: 47.8s
- 2 4 1: 27.5s
- 4 4 1: 32.3s

## pag construction (timely combined join)

- 1 4 1: 31.1s
- 2 4 1: 18s  
- 4 4 1: 21.2s

- 1 4 10: 39.7s
- 2 4 10: 26s
- 4 4 10: 24s

# Cluster Experiments

## influence of triangles batch size (`$i`) on events / epoch  

- tri32
- `triangles livejournal.graph $i 1000 1 4096 -- -w32`

| workers | `$i` | events / epoch | time [s] | dump size [G] |
|---|---|---|---|---|
| 16 | 1 | 40000 | 9 | 3.8 | 
| 16 | 5 | 45000 | 12 | 4.4 | 
| 16 | 10 | 50000 | 14 | 4.9 | 
| 16 | 50 | 75000 | 34 | 8.1 | 
| 16 | 100 | 100000 | 60 | 12 | 
| 16 | 200 | 175000 | 114 | 19 | 
| 16 | 300 | 210000 | 171 | 25 | 
| 16 | 400 | 290000 | 231 | 32 | 
| 16 | 500 | 320000 | 293 | 37 | 
| 32 | 1 | 140000 | 22 | 14 | 
| 32 | 5  | 150000 | 25  | 16  |
| 32 | 10 | 175000 | 31 | 19 | 
| 32 | 50 | 250000 | 67 | 30 | 
| 32 | 100 | 350000 | 108 | 41 | 
| 32 | 200 | 500000 | 194 | 61 | 
| 32 | 300 | 700000 | 286 | 83 | 
| 32 | 400 | 900000 | 384 | 103 | 
| 32 | 500 | 1000000 | 490 | 122 | 

## Offline, timely PAG

### b1
- fdr1
- dump to in-mem partition

### b2
- sgs-r820-01 (same as original SnailTrail)
- **Results are very much comparable with fdr1!**: Comparing fdr1 results with original SnailTrail is possible

### b3
- fdr1
- dump to in-mem partition
- timely LRS, differential PAG
