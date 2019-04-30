#!/bin/bash
set -e
#set -x
OUTDIR="${OUTDIR:-../../}"
#OUTDIR="${OUTDIR:---outdir /mnt/local/moritz/critical_path/moritzho}"

#MODE="plot"
MODE_SUMMARY="${MODE:-summary}"
MODE_CUSTOM="${MODE:-custom}"
MODE_LATENCY="${MODE:-latency}"
MODE_THROUGHPUT="${MODE:-throughput}"
MODE_SCALABILITY="${MODE_THROUGHPUT}"

SUMMARY_PARAMS="--min-window 1 --max-window 32 \
--min-workers 8 --max-workers 8 \
--min-epochs 16 --max-epochs 16 \
--min-threshold 1000000000 --max-threshold 1000000000"

TEST_PARAMS="--min-window 1 --max-window 4 \
--min-workers 8 --max-workers 8 \
--min-epochs 16 --max-epochs 16 \
--min-threshold 1000 --max-threshold 1000"

SCALABILITY_PARAMS="--min-window .5 --max-window 256 \
   --min-workers 1 --max-workers 32 \
   --min-epochs 1 --max-epochs 256 \
   --min-threshold 1000000000 --max-threshold 1000000000 \
   --no-summary"

PAG_SCALABILITY_PARAMS="--min-window .5 --max-window 256 \
   --min-workers 1 --max-workers 32 \
   --min-epochs 1 --max-epochs 256 \
   --min-threshold 1000000000 --max-threshold 1000000000 \
   --no-bc"

MD="--message-delay 100000"

# .1s
MW="--waiting-message 100000000"

SPARK="--spark-driver-hack --config config_spark"
SPARK_DRIVER="--spark-driver-hack --config config_spark_driver"
SPARK_OP="--spark-driver-hack --config config_spark_op"

TIMELY_PARAMS="--no-insert-waiting true"
HERON_PARAMS="--no-insert-waiting true"

spark_exp_dir="/mnt/scratch/dcmodel_amadeus/snailtrail_spark"
msgpack_spark_list=(
  # ${spark_exp_dir}/yahoo_batch_1g/yahoo_batch_w2000_p96_e6.msgpack
  # ${spark_exp_dir}/yahoo_streaming_1g/yahoo_streaming_w2000_p96_e6.msgpack
#  ${spark_exp_dir}/yahoo_batch_rep_1g/yahoo_batch_rep_w2000_p96_e6.msgpack
#  ${spark_exp_dir}/yahoo_batch_1g/yahoo_batch_w2000_p96_e3.msgpack
#  ${spark_exp_dir}/yahoo_streaming_1g/yahoo_streaming_w2000_p96_e3.msgpack
#  ${spark_exp_dir}/yahoo_batch_rep_1g/yahoo_batch_rep_w2000_p96_e3.msgpack
#  ${spark_exp_dir}/yahoo_batch_1g/yahoo_batch_w2000_p96_e1.msgpack
#  ${spark_exp_dir}/yahoo_streaming_1g/yahoo_streaming_w2000_p96_e1.msgpack
#  ${spark_exp_dir}/yahoo_batch_rep_1g/yahoo_batch_rep_w2000_p96_e1.msgpack
#  ${spark_exp_dir}/pagerank/spark_pagerank_livejournal_i10_e6_p48.msgpack
  # ${spark_exp_dir}/pagerank/spark_pagerank_livejournal_i10_e6_p288.msgpack
#  ${spark_exp_dir}/pagerank/spark_pagerank_livejournal_i10_e6_p192.msgpack
#  ${spark_exp_dir}/pagerank/spark_pagerank_livejournal_i10_e6_p96.msgpack
)

msgpack_spark_driver_list=(
#  ${spark_exp_dir}/ousterhout/2015_03_01_bdb_sf5_48g_memory_event_log.msgpack
#  ${spark_exp_dir}/ousterhout/2015_03_01_bdb_sf5_48g_disk_event_log.msgpack
)

msgpack_spark_list_scalability=(
  # ousterhout/2015_03_03_tpcds_sf5000_1user_6iterations_event_log.msgpack
  # ${spark_exp_dir}/ousterhout/2015_03_02_tpcds_sf100_20machines_memory.msgpack
  # ousterhout/2015_03_02_tpcds_1user_sf100_memory_6iterations_event_log.msgpack
  # ousterhout/2015_03_01_tpcds_sf5000_disk_event_log.msgpack
  # ousterhout/2015_03_01_bdb_sf5_48g_memory_event_log.msgpack
  # ousterhout/2015_03_01_bdb_sf5_48g_disk_event_log.msgpack
)

for msgpack in "${msgpack_spark_list[@]}"
do
  ./run_benchmark.py $MODE_SUMMARY "${msgpack}" --outdir $OUTDIR      $SUMMARY_PARAMS $SPARK || exit
  ./run_benchmark.py $MODE_SUMMARY "${msgpack}" --outdir "$OUTDIR/md" $SUMMARY_PARAMS $SPARK $MD || exit
done

for msgpack in "${msgpack_spark_driver_list[@]}"
do
  ./run_benchmark.py $MODE_SUMMARY "${msgpack}" --outdir $OUTDIR      --prefix driver $SUMMARY_PARAMS $SPARK_DRIVER || exit
  ./run_benchmark.py $MODE_SUMMARY "${msgpack}" --outdir "$OUTDIR/md" --prefix driver $SUMMARY_PARAMS $SPARK_DRIVER $MD || exit
done

for msgpack in "${msgpack_spark_list_scalability[@]}"
do
  ./run_benchmark.py $MODE_SCALABILITY "${msgpack}" --outdir "$OUTDIR/no_summary" $SCALABILITY_PARAMS $SPARK || exit
  ./run_benchmark.py $MODE_SCALABILITY "${msgpack}" --outdir "$OUTDIR/no_bc"      $PAG_SCALABILITY_PARAMS $SPARK || exit
done

msgpack_spark_list_scalability=(
#  /mnt/scratch/dcmodel_amadeus/snailtrail/flink-sessonization-no-skew.msgpack
)

for msgpack in "${msgpack_spark_list_scalability[@]}"
do
  ./run_benchmark.py $MODE_SCALABILITY "${msgpack}" --outdir "$OUTDIR/no_summary" $SCALABILITY_PARAMS || exit
  ./run_benchmark.py $MODE_SCALABILITY "${msgpack}" --outdir "$OUTDIR/no_bc"      $PAG_SCALABILITY_PARAMS || exit
done


timely_msgpack_list=(
  # /mnt/scratch/dcmodel_amadeus/snailtrail_timely/jupiter_run/pagerank_p_2_w_16.msgpack
  # /mnt/scratch/dcmodel_amadeus/snailtrail_timely/jupiter_run/pagerank_p_2_w_4.msgpack
  # /mnt/scratch/dcmodel_amadeus/snailtrail_timely/jupiter_run/pagerank_p_1_w_16.msgpack
  # /mnt/scratch/dcmodel_amadeus/snailtrail_timely/jupiter_run/pagerank_p_1_w_4.msgpack
)

timely_msgpack_list_scalability=(
  # /mnt/scratch/dcmodel_amadeus/snailtrail_timely/hydrogen_run/sessionization_p_1_w_2.msgpack
  # /mnt/scratch/dcmodel_amadeus/snailtrail_timely/hydrogen_run/sessionization_p_1_w_4.msgpack
  # /mnt/scratch/dcmodel_amadeus/snailtrail_timely/hydrogen_run/sessionization_p_2_w_2.msgpack
  # /mnt/scratch/dcmodel_amadeus/snailtrail_timely/hydrogen_run/sessionization_p_2_w_4.msgpack
  # /mnt/scratch/dcmodel_amadeus/snailtrail_timely/jupiter_run/pagerank_p_1_w_4.msgpack
  # /mnt/scratch/dcmodel_amadeus/snailtrail_timely/jupiter_run/pagerank_p_1_w_16.msgpack
  # /mnt/scratch/dcmodel_amadeus/snailtrail_timely/jupiter_run/pagerank_p_2_w_4.msgpack
  # /mnt/scratch/dcmodel_amadeus/snailtrail_timely/jupiter_run/pagerank_p_2_w_16.msgpack
)

for msgpack in "${timely_msgpack_list[@]}"
do
  ./run_benchmark.py $MODE_SUMMARY "${msgpack}" --outdir $OUTDIR      $SUMMARY_PARAMS $TIMELY_PARAMS || exit
  ./run_benchmark.py $MODE_SUMMARY "${msgpack}" --outdir "$OUTDIR/md" $SUMMARY_PARAMS $TIMELY_PARAMS $MD || exit
done

for msgpack in "${timely_msgpack_list_scalability[@]}"
do
  ./run_benchmark.py $MODE_SCALABILITY "${msgpack}" --outdir "$OUTDIR/no_summary" $SCALABILITY_PARAMS     $TIMELY_PARAMS || exit
  ./run_benchmark.py $MODE_SCALABILITY "${msgpack}" --outdir "$OUTDIR/no_bc"      $PAG_SCALABILITY_PARAMS $TIMELY_PARAMS || exit
done

summary_only_msgpack_list=(
/Users/malte/Dev/eth/snailtrail/pag-construction/test/snailtrail.msgpack
# /mnt/scratch/dcmodel_amadeus/snailtrail/flink-sessonization/flink-sessonization.msgpack
# /mnt/scratch/dcmodel_amadeus/snailtrail/flink-yahoo-traces/1a/flink-trace-yahoo-1a
#  /mnt/scratch/dcmodel_amadeus/snailtrail/flink-yahoo-traces/1c/flink-trace-yahoo-1c
#  /mnt/scratch/dcmodel_amadeus/snailtrail/flink-yahoo-traces/2b/flink-trace-yahoo-2b
#  /mnt/scratch/dcmodel_amadeus/snailtrail/flink-yahoo-traces/2c/flink-trace-yahoo-2c
#  /mnt/scratch/dcmodel_amadeus/snailtrail/flink-yahoo-traces/3b/flink-trace-yahoo-3b
#  /mnt/scratch/dcmodel_amadeus/snailtrail/flink-yahoo-traces/3c/flink-trace-yahoo-3c
#  /mnt/scratch/dcmodel_amadeus/snailtrail/flink-yahoo-traces/3d/flink-trace-yahoo-3d
#  /mnt/scratch/dcmodel_amadeus/snailtrail/flink-yahoo-skewed/flink-trace-yahoo-skewed
)

for msgpack in "${summary_only_msgpack_list[@]}"
do
  ./run_benchmark.py $MODE_SUMMARY  --input "${msgpack}" --outdir "${OUTDIR}/md" --config config_default $SUMMARY_PARAMS $MD || exit
  # ./run_benchmark.py $MODE_SUMMARY  --input "${msgpack}" --outdir "${OUTDIR}/md" --config config_range_5_80 config_flink config_flink_op --prefix op $SUMMARY_PARAMS $MD || exit
done

# pushd /home/moritzho/.herondata/topologies/local/moritzho
# for i in *; do echo $i; cd $i; cat snailtrail* > exp_$i.msgpack; cd ..; done
# popd
heron_msgpack_list=(
  # ~/dev/repos/critical-path/logformat/rust/logrecords.msgpack
  # ~/dev/repos/critical-path/logformat/rust/dhalion_experiment.msgpack
  # ~/dev/repos/critical-path/logformat/rust/dhalion_exp3.msgpack
#  /mnt/local/moritz/dhalion/dhalion_experiment.msgpack
  # /mnt/scratch/dcmodel_amadeus/snailtrail_dhalion/exp*/snailtrail.logrecords.metricsmgr-1.0
)

  # ./run_benchmark.py $MODE_SUMMARY "${msgpack}" --outdir "${OUTDIR}/mw_on" $SUMMARY_PARAMS $HERON_PARAMS $MW --config config_dhalion || exit
# ./run_benchmark.py $MODE_SUMMARY --name dhalion_wc --input "${heron_msgpack_list[@]}" --outdir "${OUTDIR}/mw_off" $SUMMARY_PARAMS $HERON_PARAMS --config config_dhalion || exit
# ./run_benchmark.py $MODE_CUSTOM --name dhalion_wc --input "${heron_msgpack_list[@]}" --outdir "${OUTDIR}/mw_off" $SUMMARY_PARAMS $HERON_PARAMS --config config_dhalion || exit

# ./run_benchmark.py $MODE_CUSTOM  --name dhalion_wc_2 --input /mnt/scratch/dcmodel_amadeus/snailtrail_dhalion_2/exp*/snailtrail.logrecords.metricsmgr-1.0 --outdir "${OUTDIR}/mw_off" $SUMMARY_PARAMS $HERON_PARAMS --config config_dhalion || exit
# ./run_benchmark.py $MODE_SUMMARY --name dhalion_wc_2 --input /mnt/scratch/dcmodel_amadeus/snailtrail_dhalion_2/exp*/snailtrail.logrecords.metricsmgr-1.0 --outdir "${OUTDIR}/mw_off" $SUMMARY_PARAMS $HERON_PARAMS --config config_dhalion || exit

# ./run_benchmark.py $MODE_CUSTOM  --name dhalion_wc_3 --input /mnt/scratch/dcmodel_amadeus/snailtrail_dhalion_3/exp*/snailtrail.logrecords.metricsmgr-1.0 --outdir "${OUTDIR}/mw_off" $SUMMARY_PARAMS $HERON_PARAMS --config config_dhalion || exit
# ./run_benchmark.py $MODE_SUMMARY --name dhalion_wc_3 --input /mnt/scratch/dcmodel_amadeus/snailtrail_dhalion_3/exp*/snailtrail.logrecords.metricsmgr-1.0 --outdir "${OUTDIR}/mw_off" $SUMMARY_PARAMS $HERON_PARAMS --config config_dhalion || exit

# Run tests

if [ "$MODE" = "run" ]; then
  python ./run_test.py
fi

test_msgpack_list=(
  test/*.msgpack
)

# ./run_benchmark.py $MODE_SUMMARY --name test --input "${test_msgpack_list[@]}" --outdir "${OUTDIR}/" $TEST_PARAMS || exit
