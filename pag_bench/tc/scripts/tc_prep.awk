# Use: awk -F '<delimiter>' -f <this file> <input file> | sort -n > <output file>
# 
# converts a delimited file in format
# worker | epoch | epoch elapsed | overall events | pag events  
# to
# epoch | avg elapsed | sum overall events | sum pag events | cum sum overall | cum sum pag
# grouped by epoch

{
  elapsed[$2] += $3;
  count[$2]++;
  overall[$2] += $4;
  pag[$2] += $5;
}
END {
  for (epoch in elapsed) {
    printf "%s %s %s %s\n", epoch, ((elapsed[epoch] / count[epoch]) / 1000), overall[epoch], pag[epoch];
  }
}
