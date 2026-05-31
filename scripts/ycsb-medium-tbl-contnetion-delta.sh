#!/bin/bash
echo "Running YCSB (medium tbl contention delta)"
for i in $(seq 0 7); do
  bin/ycsb.sh run sparkDelta \
    -P workloads/workloada \
    -p threadcount=4 \
    -p spark.resultFile=./result_rdl${i}-medium \
    -p table=usertable${i} \
    -p spark.lakehouse=delta \
    > ./load_result_rdl_a${i}-medium.txt 2>&1 &
  
done

wait
echo "All jobs finished."