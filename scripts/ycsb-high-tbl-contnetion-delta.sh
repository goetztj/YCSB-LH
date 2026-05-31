#!/bin/bash
echo "Running YCSB (high tbl contention delta)"
for i in $(seq 0 3); do
  bin/ycsb.sh run sparkDelta \
    -P workloads/workloada \
    -p threadcount=8 \
    -p spark.resultFile=./result_rdl${i}-high \
    -p table=usertable${i} \
    -p spark.lakehouse=delta \
    > ./load_result_rdl_a${i}-high.txt 2>&1 &
  
done

wait
echo "All jobs finished."