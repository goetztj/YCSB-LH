#!/bin/bash
echo "Running YCSB (low tbl contention delta)"
for i in $(seq 0 15); do
  bin/ycsb.sh run sparkDelta \
    -P workloads/workloada \
    -p threadcount=2 \
    -p spark.resultFile=./result_rdl${i}-low \
    -p table=usertable${i} \
    -p spark.lakehouse=delta \
    > ./load_result_rdl_a${i}-low.txt 2>&1 &
  
done

wait
echo "All jobs finished."