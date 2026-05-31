#!/bin/bash
echo "Running YCSB (high tbl contention iceberg)"
for i in $(seq 0 3); do
  bin/ycsb.sh run sparkIceberg \
    -P workloads/workloada \
    -p threadcount=8 \
    -p spark.resultFile=./result_rib${i}-high \
    -p table=usertable${i} \
    -p spark.lakehouse=iceberg \
    > ./load_result_rib_a${i}-high.txt 2>&1 &
  
done

wait
echo "All jobs finished."