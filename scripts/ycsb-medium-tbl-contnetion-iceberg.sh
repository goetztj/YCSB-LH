#!/bin/bash
echo "Running YCSB (medium tbl contention iceberg)"
for i in $(seq 0 7); do
  bin/ycsb.sh run sparkIceberg \
    -P workloads/workloada \
    -p threadcount=4 \
    -p spark.resultFile=./result_rib${i}-medium \
    -p table=usertable${i} \
    -p spark.lakehouse=iceberg \
    > ./load_result_rib_a${i}-medium.txt 2>&1 &
  
done

wait
echo "All jobs finished."