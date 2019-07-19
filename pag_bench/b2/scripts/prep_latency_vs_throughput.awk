{
  time[$1] += $2;
  throughput[$1] += $3 / $2;
}
END {
  for (epoch in time) {
    printf "%s %s\n", time[epoch], throughput[epoch];
  }
}
