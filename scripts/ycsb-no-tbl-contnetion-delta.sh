#!/bin/bash
echo "Running YCSB (no tbl contention delta)"
for i in $(seq 0 31); do
  bin/ycsb.sh run sparkDelta \
    -P workloads/workloada \
    -p threadcount=1 \
    -p spark.resultFile=./result_rdl${i}-no \
    -p table=usertable${i} \
    -p spark.lakehouse=delta \
    > ./load_result_rdl_a${i}-no.txt 2>&1 &
done

wait
echo "All jobs finished."