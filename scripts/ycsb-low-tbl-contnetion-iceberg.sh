#!/bin/bash
echo "Running YCSB (low tbl contention iceberg)"
for i in $(seq 0 15); do
  bin/ycsb.sh run sparkIceberg \
    -P workloads/workloada \
    -p threadcount=2 \
    -p spark.resultFile=./result_rib${i}-low \
    -p table=usertable${i} \
    -p spark.lakehouse=iceberg \
    > ./load_result_rib_a${i}-low.txt 2>&1 &
  
done

wait
echo "All jobs finished."