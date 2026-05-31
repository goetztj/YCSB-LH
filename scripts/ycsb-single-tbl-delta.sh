#!/bin/bash
echo "Running YCSB (single tbl delta)"

bin/ycsb.sh run sparkDelta \
    -P workloads/workloada \
    -p threadcount=32 \
    -p spark.resultFile=./result_rdl${i}-single \
    -p table=usertable${i} \
    -p spark.lakehouse=delta \
    > ./load_result_rdl_a${i}-single.txt 2>&1
  
echo "All jobs finished."