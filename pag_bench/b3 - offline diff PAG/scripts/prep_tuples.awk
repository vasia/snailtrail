# Use: awk -F '<delimiter>' -f <this file> <input file> | sort -n > <output file>
# 
# converts a delimited file in format
# worker | epoch | tuples   
# to
# epoch | sum tuples  
# grouped by epoch

{
  tuples[$2] += $3;
}
END {
  for (epoch in tuples) {
    printf "%s %s\n", epoch, tuples[epoch];
  }
}
