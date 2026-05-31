#!/bin/bash
echo "Running YCSB (no tbl contention iceberg)"
for i in $(seq 0 31); do
  bin/ycsb.sh run sparkIceberg \
    -P workloads/workloada \
    -p threadcount=1 \
    -p spark.resultFile=./result_rib${i}-no \
    -p table=usertable${i} \
    -p spark.lakehouse=iceberg \
    > ./load_result_rib_a${i}-no.txt 2>&1 &
done

wait
echo "All jobs finished."