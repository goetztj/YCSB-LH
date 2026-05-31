#!/bin/bash
echo "Running YCSB (single tbl iceberg)"

bin/ycsb.sh run sparkIceberg \
    -P workloads/workloada \
    -p threadcount=32 \
    -p spark.resultFile=./result_rib${i}-single \
    -p table=usertable${i} \
    -p spark.lakehouse=iceberg \
    > ./load_result_rib_a${i}-single.txt 2>&1
  
echo "All jobs finished."