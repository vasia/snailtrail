{
  if(prev_epoch)
  {
    c = ($1 - prev_epoch);
    for(i = prev_epoch; i < $1; i++)
    {
      elapsed[i] = prev_elapsed / c;
      overall[i] = prev_overall / c;
    }
  }
  elapsed[$1] = $2;
  overall[$1] = $3;
  prev_epoch = $1;   
  prev_elapsed = $2;
  prev_overall = $3;
}
END {
  for (epoch in elapsed) {
    printf "%s %.8f %.8f\n", epoch, elapsed[epoch], overall[epoch];
  }
}
