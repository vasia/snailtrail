# Use: awk -F '<delimiter>' -f <this file> <input file> | sort -n > <output file>
# 
# converts a delimited file in format
# worker | epoch | epoch elapsed | overall events   
# to
# epoch | avg elapsed | sum overall events  
# grouped by epoch

{
  elapsed[$2] += $3;
  overall[$2] += $4;
}
END {
  for (epoch in elapsed) {
    printf "%s %s %s\n", epoch, (elapsed[epoch] / workers / 1000000000), overall[epoch];
  }
}
